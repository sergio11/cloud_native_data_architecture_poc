"""Unit tests: Silver explicit typing + NULL-id audit. Hermetic: tmp_path only."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter
from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.domain.entities import QualityGateEvaluation
from src.domain.naming import physical_sql_name, silver_table
from src.use_cases.cleanse_silver import CleanseSilverUseCase
from tests.helpers import StubLineage

REPO_ROOT = Path(__file__).resolve().parents[2]
SILVER_ORDERS_SUITE = str(REPO_ROOT / "quality" / "expectations" / "silver_orders_suite.json")
STATUSES = ["confirmed", "pending", "shipped", "cancelled"]


class _PassEngine:
    """QualityEnginePort stub that always passes (isolates typing from gating)."""

    def evaluate_suite(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        threshold: float = 0.95,
        table_name: str = "",
    ) -> QualityGateEvaluation:
        return QualityGateEvaluation(
            suite_name="stub",
            table_name=table_name,
            total_assertions=1,
            passed_assertions=1,
            pass_rate=1.0,
            threshold=threshold,
            passed=True,
        )


def _order_row(order_id: Any, total: Any, index: int) -> dict[str, Any]:
    return {
        "id": order_id,
        "customer_id": 100 + index,
        "total_amount": total,
        "status": STATUSES[index % len(STATUSES)],
        "order_date": "2026-09-01",
        "created_at": "2026-09-01T10:00:00+00:00",
    }


def _typeof_map(db_path: str, logical: str) -> dict[str, str]:
    """Returns column -> typeof() of the first live row, queried directly in SQLite."""
    physical = physical_sql_name(logical)
    # NOTE: sqlite3 connections are NOT closed by the context-manager protocol
    # (``with conn`` only commits/rolls back), so close explicitly: an implicit
    # GC close trips the suite's ``filterwarnings = error`` in a later test.
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT typeof(id), typeof(customer_id), typeof(total_amount), typeof(status), typeof(order_date)"
            f' FROM "{physical}" ORDER BY rowid ASC LIMIT 1;'
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    return {"id": row[0], "customer_id": row[1], "total_amount": row[2], "status": row[3], "order_date": row[4]}


def test_silver_typing_typeof_matches_contract_schema(tmp_path: Path) -> None:
    """L1: TEXT-landed numerics are cast per contract; typeof() is INTEGER/REAL/TEXT."""
    db_path = str(tmp_path / "lakehouse.db")
    storage = LakehouseStorageAdapter(db_path=db_path)
    # Bronze lands everything as TEXT (strings).
    raw = [_order_row(str(i), f"{50.0 + i}", i) for i in range(1, 11)]
    assert storage.write_table("bronze.sales.orders", raw, mode="append") == 10

    uc = CleanseSilverUseCase(
        storage_reader=storage,
        storage_writer=storage,
        quality_engine=ExpectationEngineAdapter(),
        lineage=StubLineage(),
        catalog=FsDomainCatalogAdapter(repo_root=str(REPO_ROOT)),
    )
    res = uc.execute("orders", suite_path=SILVER_ORDERS_SUITE)
    assert res["status"] == "SUCCESS"
    assert res["rows_promoted"] == 10
    assert res["cast_failures"] == 0
    assert res["dropped_null_id"] == 0

    assert _typeof_map(db_path, silver_table("orders")) == {
        "id": "integer",
        "customer_id": "integer",
        "total_amount": "real",
        "status": "text",
        "order_date": "text",
    }

    silver = storage.read_table("silver.sales.orders")
    assert isinstance(silver[0]["id"], int)
    assert isinstance(silver[0]["total_amount"], float)
    assert isinstance(silver[0]["status"], str)


def test_silver_typing_cast_failure_keeps_original_fail_open(tmp_path: Path, caplog: Any) -> None:
    """L1: an unparseable value keeps the original + bumps cast_failures (gate still decides)."""
    db_path = str(tmp_path / "lakehouse.db")
    storage = LakehouseStorageAdapter(db_path=db_path)
    storage.write_table("bronze.sales.orders", [_order_row(1, "not-a-number", 1)], mode="append")

    uc = CleanseSilverUseCase(
        storage_reader=storage,
        storage_writer=storage,
        quality_engine=_PassEngine(),
        lineage=StubLineage(),
        catalog=FsDomainCatalogAdapter(repo_root=str(REPO_ROOT)),
    )
    with caplog.at_level(logging.WARNING, logger="use_case.cleanse_silver"):
        res = uc.execute("orders", suite_path="stub.json")
    assert res["status"] == "SUCCESS"
    assert res["cast_failures"] == 1
    assert "keeping original" in caplog.text

    silver = storage.read_table("silver.sales.orders")
    assert silver[0]["total_amount"] == "not-a-number"


def test_silver_drops_null_id_auditable(tmp_path: Path, caplog: Any) -> None:
    """M2: a NULL-id Bronze row never reaches Silver but is counted + warned about."""
    db_path = str(tmp_path / "lakehouse.db")
    storage = LakehouseStorageAdapter(db_path=db_path)
    raw = [_order_row(1, 10.0, 1), _order_row(None, 20.0, 2), _order_row(3, 30.0, 3)]
    storage.write_table("bronze.sales.orders", raw, mode="append")

    uc = CleanseSilverUseCase(
        storage_reader=storage,
        storage_writer=storage,
        quality_engine=_PassEngine(),
        lineage=StubLineage(),
        catalog=FsDomainCatalogAdapter(repo_root=str(REPO_ROOT)),
    )
    with caplog.at_level(logging.WARNING, logger="use_case.cleanse_silver"):
        res = uc.execute("orders", suite_path="stub.json")
    assert res["status"] == "SUCCESS"
    assert res["dropped_null_id"] == 1
    assert res["rows_promoted"] == 2
    assert "NULL 'id'" in caplog.text

    silver = storage.read_table("silver.sales.orders")
    assert len(silver) == 2
    assert all(row.get("id") is not None for row in silver)
