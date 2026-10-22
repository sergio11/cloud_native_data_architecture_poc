"""Unit tests: serving executes SQL verbatim.

The use case references ``<physical>_live`` views and the adapter executes the
SQL byte-identical on both backends.
"""

from __future__ import annotations

import sqlite3
import sys
import types
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from src.adapters.serving import sql_serving as serving_module
from src.adapters.serving.sql_serving import SqlServingAdapter
from src.domain.naming import gold_table, serving_view_name, stream_table
from src.use_cases.query_serving import QueryServingUseCase

REPO_ROOT = Path(__file__).resolve().parents[2]


class _CapturingExecutor:
    """Test double recording the SQL handed to the outbound port."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute_query(self, sql: str) -> list[dict[str, Any]]:
        self.statements.append(sql)
        return []


class _FakeCursor:
    def fetchall(self) -> list[Any]:
        return []

    def fetchone(self) -> Any:
        return None


class _FakeConn:
    """Fake SQLite connection: records every statement, returns no rows."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> _FakeCursor:
        self.statements.append(sql)
        return _FakeCursor()


def _install_fake_sqlite(monkeypatch: Any, captured: list[_FakeConn]) -> None:
    @contextmanager
    def _fake_sqlite_connection(*args: Any, **kwargs: Any) -> Iterator[_FakeConn]:
        fake = _FakeConn()
        captured.append(fake)
        yield fake

    monkeypatch.setattr(serving_module, "sqlite_connection", _fake_sqlite_connection)


def _generated_sql(order_date_gte: str | None = None, limit: int = 20) -> str:
    """Generates the serving SQL through the public use-case entrypoint."""
    executor = _CapturingExecutor()
    QueryServingUseCase(query_executor=executor).execute(order_date_gte=order_date_gte, limit=limit)  # type: ignore[arg-type]
    assert len(executor.statements) == 1
    return executor.statements[0]


def test_serving_view_name_mapping_is_deterministic() -> None:
    assert serving_view_name(gold_table("daily_metrics")) == "gold_sales_daily_metrics_live"
    assert serving_view_name(stream_table()) == "stream_sales_orders_windowed_live"


def test_generated_sql_references_live_views_not_logical_names() -> None:
    sql = _generated_sql(order_date_gte="2026-09-20", limit=7)
    assert "gold_sales_daily_metrics_live" in sql
    assert "stream_sales_orders_windowed_live" in sql
    assert "gold.sales.daily_metrics" not in sql
    assert "stream.sales.orders_windowed" not in sql


def test_rewrite_bridge_is_gone() -> None:
    assert not hasattr(serving_module, "_LOGICAL_TO_PHYSICAL")
    assert not hasattr(SqlServingAdapter, "_rewrite_logical_names")


def test_sqlite_receives_byte_identical_sql(monkeypatch: Any) -> None:
    monkeypatch.delenv("TRINO_HOST", raising=False)
    captured: list[_FakeConn] = []
    _install_fake_sqlite(monkeypatch, captured)

    sql = _generated_sql(order_date_gte="2026-09-20", limit=7)
    assert SqlServingAdapter(db_path="dummy.db").execute_query(sql) == []

    assert len(captured) == 1
    # View provisioning runs first; the serving statement itself goes last, intact.
    assert captured[0].statements[-1] == sql


def test_trino_receives_byte_identical_sql(monkeypatch: Any) -> None:
    captured: list[str] = []

    class _FakeTrinoCursor:
        description = (("order_date",), ("status",))

        def execute(self, sql: str) -> None:
            captured.append(sql)

        def fetchall(self) -> list[tuple[Any, ...]]:
            return []

    class _FakeTrinoConnection:
        def cursor(self) -> _FakeTrinoCursor:
            return _FakeTrinoCursor()

        def close(self) -> None:
            return None

    fake_dbapi = types.SimpleNamespace(connect=lambda *args, **kwargs: _FakeTrinoConnection())
    monkeypatch.setitem(sys.modules, "trino", types.SimpleNamespace(dbapi=fake_dbapi))
    monkeypatch.setitem(sys.modules, "trino.dbapi", fake_dbapi)
    monkeypatch.setenv("TRINO_HOST", "trino-test")

    sql = _generated_sql(limit=5)
    assert SqlServingAdapter(db_path="dummy.db").execute_query(sql) == []
    assert captured == [sql]


def _setup_db(tmp_path: Path, with_superseded_column: bool) -> str:
    db_path = tmp_path / "lakehouse.db"
    conn = sqlite3.connect(str(db_path))
    try:
        extra = ", _row_superseded TEXT" if with_superseded_column else ""
        conn.execute(
            "CREATE TABLE gold_sales_daily_metrics (order_date TEXT, status TEXT, total_orders TEXT, "
            f"gross_revenue TEXT, average_order_value TEXT{extra})"
        )
        conn.execute(
            "CREATE TABLE stream_sales_orders_windowed (order_date TEXT, status TEXT, window_orders TEXT, "
            f"window_revenue TEXT, avg_order_value TEXT{extra})"
        )
        if with_superseded_column:
            conn.executemany(
                "INSERT INTO gold_sales_daily_metrics VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("2026-09-20", "confirmed", "5", "1000.0", "200.0", None),
                    ("2026-09-20", "confirmed", "9", "900.0", "180.0", "2026-09-21T00:00:00"),
                ],
            )
            conn.executemany(
                "INSERT INTO stream_sales_orders_windowed VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("2026-09-20", "confirmed", "3", "600.0", "200.0", None),
                    ("2026-09-25", "shipped", "4", "400.0", "100.0", "2026-09-26T00:00:00"),
                    ("2026-09-26", "confirmed", "2", "300.0", "150.0", None),
                ],
            )
        else:
            conn.executemany(
                "INSERT INTO gold_sales_daily_metrics VALUES (?, ?, ?, ?, ?)",
                [("2026-09-20", "confirmed", "5", "1000.0", "200.0")],
            )
            conn.executemany(
                "INSERT INTO stream_sales_orders_windowed VALUES (?, ?, ?, ?, ?)",
                [("2026-09-26", "confirmed", "2", "300.0", "150.0")],
            )
        conn.commit()
    finally:
        conn.close()
    return str(db_path)


def test_superseded_rows_excluded_end_to_end(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.delenv("TRINO_HOST", raising=False)
    db_path = _setup_db(tmp_path, with_superseded_column=True)

    rows = QueryServingUseCase(query_executor=SqlServingAdapter(db_path=db_path)).execute()

    assert len(rows) == 2
    assert all(r["order_date"] != "2026-09-25" for r in rows)
    shared = [r for r in rows if r["order_date"] == "2026-09-20"]
    assert len(shared) == 1
    assert shared[0]["source_tier"] == "batch_historical"
    assert shared[0]["total_orders"] == 5

    conn = sqlite3.connect(db_path)
    try:
        views = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'view';")}
    finally:
        conn.close()
    assert {"gold_sales_daily_metrics_live", "stream_sales_orders_windowed_live"} <= views


def test_live_views_pass_through_without_version_column(tmp_path: Path, monkeypatch: Any) -> None:
    monkeypatch.delenv("TRINO_HOST", raising=False)
    db_path = _setup_db(tmp_path, with_superseded_column=False)

    rows = QueryServingUseCase(query_executor=SqlServingAdapter(db_path=db_path)).execute()

    assert len(rows) == 2
    assert {r["source_tier"] for r in rows} == {"batch_historical", "stream_speed"}


def test_trino_artifacts_declare_pass_through_live_views() -> None:
    unified = (REPO_ROOT / "pipelines" / "serving" / "unified_view.sql").read_text(encoding="utf-8")
    configmap = (REPO_ROOT / "infra" / "helm" / "s5-serving" / "templates" / "trino-views-configmap.yaml").read_text(
        encoding="utf-8"
    )
    for view in ("gold_sales_daily_metrics_live", "stream_sales_orders_windowed_live"):
        assert view in unified
        assert view in configmap
    # Trino twins are pure pass-through over the versioned sources.
    # Single "iceberg" catalog replaces the deprecated "gold" and "stream" names.
    assert "SELECT * FROM iceberg.sales.daily_metrics;" in unified
    assert "SELECT * FROM iceberg.sales.orders_windowed;" in unified
    # VIEW 1 keeps its semantics: same anti-join on the same keys.
    assert "WHERE NOT EXISTS (" in unified
    assert "b.order_date = s.order_date" in unified
    assert "b.status = s.status" in unified
