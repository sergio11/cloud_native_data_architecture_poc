"""Unit tests: Silver latest-snapshot-wins. Hermetic: tmp_path only."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.source.oltp_source import OltpSourceAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.use_cases.cleanse_silver import CleanseSilverUseCase
from tests.helpers import EmptyContractCatalog, StubLineage

REPO_ROOT = Path(__file__).resolve().parents[2]
SILVER_ORDERS_SUITE = str(REPO_ROOT / "quality" / "expectations" / "silver_orders_suite.json")


def _bronze_order_row(id: int, total: float, batch: str, ingested_at: str, **overrides: Any) -> dict[str, Any]:
    row = {
        "id": id,
        "customer_id": 7,
        "total_amount": total,
        "status": "confirmed",
        "order_date": "2026-09-01",
        "created_at": "2026-09-01T10:00:00+00:00",
        "_batch_id": batch,
        "_ingested_at": ingested_at,
    }
    row.update(overrides)
    return row


def test_silver_keeps_latest_snapshot(tmp_path: Path) -> None:
    storage = LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))
    # Two snapshots for id=1 (oldest 100.0, newest 200.0) + ids 2..10 to satisfy suite row-count >= 10.
    raw = [
        _bronze_order_row(1, 100.0, "batch-old", "2026-09-01T00:00:00+00:00"),
        *[_bronze_order_row(i, 50.0 + i, "batch-old", "2026-09-01T00:00:00+00:00") for i in range(2, 11)],
        _bronze_order_row(1, 200.0, "batch-new", "2026-09-02T00:00:00+00:00"),
    ]
    assert storage.write_table("bronze.sales.orders", raw, mode="append") == len(raw)

    uc = CleanseSilverUseCase(
        storage_reader=storage,
        storage_writer=storage,
        quality_engine=ExpectationEngineAdapter(),
        lineage=StubLineage(),
        catalog=EmptyContractCatalog(),
    )
    res = uc.execute("orders", suite_path=SILVER_ORDERS_SUITE)
    assert res["status"] == "SUCCESS"

    silver = storage.read_table("silver.sales.orders")
    by_id = {str(r["id"]): r for r in silver}
    assert len(silver) == 10
    # Explicit typing: Silver holds native numerics (INTEGER/REAL affinity).
    assert by_id["1"]["total_amount"] == 200.0
    assert "_batch_id" not in by_id["1"] and "_ingested_at" not in by_id["1"]


def test_synthetic_source_is_deterministic(monkeypatch) -> None:
    monkeypatch.delenv("PGPASSWORD", raising=False)
    first = OltpSourceAdapter(seed=42).extract_table("orders", limit=10).records
    second = OltpSourceAdapter(seed=42).extract_table("orders", limit=10).records
    assert first == second
    assert len(first) == 10

    # POC_DATA_SEED env override respected.
    monkeypatch.setenv("POC_DATA_SEED", "999")
    monkeypatch.delenv("PGPASSWORD", raising=False)
    env_a = OltpSourceAdapter().extract_table("orders", limit=10).records
    env_b = OltpSourceAdapter().extract_table("orders", limit=10).records
    assert env_a == env_b
    assert OltpSourceAdapter()._seed == 999
    # Explicit seed overrides env.
    assert OltpSourceAdapter(seed=42)._seed == 42


def test_rerun_is_idempotent_and_reflects_same_values(tmp_path: Path) -> None:
    storage = LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))
    records = [
        {
            "order_date": "2026-09-01",
            "status": "confirmed",
            "gross_revenue": 100.0,
            "total_orders": 1,
            "average_order_value": 100.0,
        },
    ]
    assert storage.write_table("gold.sales.daily_metrics", records, mode="append") == 1
    rerun = storage.write_table("gold.sales.daily_metrics", records, mode="append")
    assert rerun == 0
    rows = storage.read_table("gold.sales.daily_metrics")
    assert len(rows) == 1
    # Numerics survive the round-trip as REAL (no TEXT coercion).
    assert rows[0]["gross_revenue"] == 100.0
