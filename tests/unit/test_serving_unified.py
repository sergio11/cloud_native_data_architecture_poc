"""
Modern Data Architecture POC - Unit Tests: RF-11 Unified Serving Query.
Verifies no-double-count semantics of QueryServingUseCase + SqlServingAdapter
against an isolated tmp_path SQLite database. Adapters/use case are constructed
DIRECTLY - src.container is never imported (other agents own its wiring).
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence

import pytest

from src.adapters.serving.sql_serving import SqlServingAdapter
from src.use_cases.query_serving import QueryServingUseCase

Row = tuple[str | None, ...]


def _setup_db(
    tmp_path,
    batch_rows: Sequence[Row],
    stream_rows: Sequence[Row],
    superseded_column: bool = False,
):
    """Creates the two physical tables with the storage adapter's column layout
    (plain TEXT columns), optionally with the versioned time-travel shape."""
    db_path = tmp_path / "lakehouse.db"
    conn = sqlite3.connect(str(db_path))
    try:
        extra = ", _row_superseded TEXT" if superseded_column else ""
        conn.execute(
            "CREATE TABLE gold_sales_daily_metrics ("
            "order_date TEXT, status TEXT, total_orders TEXT, "
            f"gross_revenue TEXT, average_order_value TEXT{extra})"
        )
        conn.execute(
            "CREATE TABLE stream_sales_orders_windowed ("
            "order_date TEXT, status TEXT, window_orders TEXT, "
            f"window_revenue TEXT, avg_order_value TEXT{extra})"
        )
        n = 6 if superseded_column else 5
        placeholders = ", ".join(["?"] * n)
        conn.executemany(
            f"INSERT INTO gold_sales_daily_metrics VALUES ({placeholders})", batch_rows
        )
        conn.executemany(
            f"INSERT INTO stream_sales_orders_windowed VALUES ({placeholders})",
            stream_rows,
        )
        conn.commit()
    finally:
        conn.close()
    return str(db_path)


def _make_use_case(db_path: str) -> QueryServingUseCase:
    return QueryServingUseCase(query_executor=SqlServingAdapter(db_path=db_path))


def test_batch_wins_no_double_count(tmp_path, monkeypatch):
    monkeypatch.delenv("TRINO_HOST", raising=False)
    db_path = _setup_db(
        tmp_path,
        batch_rows=[("2026-09-20", "confirmed", "5", "1000.0", "200.0")],
        stream_rows=[
            ("2026-09-20", "confirmed", "3", "600.0", "200.0"),
            ("2026-09-26", "confirmed", "2", "300.0", "150.0"),
        ],
    )

    rows = _make_use_case(db_path).execute_unified_revenue_query()

    assert len(rows) == 2
    tiers = {r["source_tier"] for r in rows}
    assert tiers == {"batch_historical", "stream_speed"}

    shared = [r for r in rows if r["order_date"] == "2026-09-20"]
    assert len(shared) == 1
    assert shared[0]["source_tier"] == "batch_historical"
    assert shared[0]["total_orders"] == 5
    assert shared[0]["total_revenue"] == 1000.0
    assert shared[0]["avg_order_value"] == 200.0

    stream_only = [r for r in rows if r["order_date"] == "2026-09-26"]
    assert len(stream_only) == 1
    assert stream_only[0]["source_tier"] == "stream_speed"
    assert stream_only[0]["total_orders"] == 2
    assert stream_only[0]["total_revenue"] == 300.0


def test_order_date_filter_limit_and_types(tmp_path, monkeypatch):
    monkeypatch.delenv("TRINO_HOST", raising=False)
    db_path = _setup_db(
        tmp_path,
        batch_rows=[
            ("2026-09-18", "confirmed", "1", "100.0", "100.0"),
            ("2026-09-20", "confirmed", "5", "1000.0", "200.0"),
        ],
        stream_rows=[
            ("2026-09-19", "shipped", "7", "700.0", "100.0"),
            ("2026-09-26", "confirmed", "2", "300.0", "150.0"),
        ],
    )
    uc = _make_use_case(db_path)

    filtered = uc.execute_unified_revenue_query(order_date_gte="2026-09-21")
    assert [r["order_date"] for r in filtered] == ["2026-09-26"]
    assert filtered[0]["source_tier"] == "stream_speed"

    limited = uc.execute_unified_revenue_query(order_date_gte="2026-09-19", limit=2)
    assert len(limited) == 2
    assert [r["order_date"] for r in limited] == ["2026-09-26", "2026-09-20"]

    typed = uc.execute_unified_revenue_query(limit=20)
    assert len(typed) == 4
    for r in typed:
        assert isinstance(r["total_orders"], int)
        assert isinstance(r["total_revenue"], float)
        assert isinstance(r["avg_order_value"], float)


def test_fallback_logging(tmp_path, monkeypatch, caplog):
    monkeypatch.delenv("TRINO_HOST", raising=False)
    db_path = _setup_db(
        tmp_path,
        batch_rows=[("2026-09-20", "confirmed", "5", "1000.0", "200.0")],
        stream_rows=[],
    )
    adapter = SqlServingAdapter(db_path=db_path)

    with caplog.at_level(logging.INFO):
        rows = adapter.execute_query("SELECT order_date FROM gold_sales_daily_metrics")

    assert rows == [{"order_date": "2026-09-20"}]
    assert any(
        "Serving query executed by backend=" in record.getMessage()
        for record in caplog.records
    )

    with pytest.raises(RuntimeError, match="serving query failed"):
        adapter.execute_query("SELECT * FROM table_that_does_not_exist")


def test_superseded_rows_excluded_when_column_present(tmp_path, monkeypatch):
    monkeypatch.delenv("TRINO_HOST", raising=False)
    db_path = _setup_db(
        tmp_path,
        batch_rows=[
            ("2026-09-20", "confirmed", "5", "1000.0", "200.0", None),
            ("2026-09-20", "confirmed", "9", "900.0", "180.0", "2026-09-21T00:00:00"),
        ],
        stream_rows=[
            ("2026-09-20", "confirmed", "3", "600.0", "200.0", None),
            ("2026-09-25", "shipped", "4", "400.0", "100.0", "2026-09-26T00:00:00"),
            ("2026-09-26", "confirmed", "2", "300.0", "150.0", None),
        ],
        superseded_column=True,
    )

    rows = _make_use_case(db_path).execute_unified_revenue_query()

    assert len(rows) == 2
    assert all(r["order_date"] != "2026-09-25" for r in rows)

    shared = [r for r in rows if r["order_date"] == "2026-09-20"]
    assert len(shared) == 1
    assert shared[0]["source_tier"] == "batch_historical"
    assert shared[0]["total_orders"] == 5
    assert shared[0]["total_revenue"] == 1000.0
