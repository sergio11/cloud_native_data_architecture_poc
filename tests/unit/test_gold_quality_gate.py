"""
Modern Data Architecture POC - Unit Tests: Gold Quality Gate (RF-06 fail-closed).

Verifies that AggregateGoldUseCase enforces a per-target quality gate mirroring
the Silver pattern: a breaching target emits a FAIL lineage event and raises
QualityGateBreachError BEFORE any write, while the independent target still
publishes. Fully hermetic: lakehouse SQLite DB and lineage audit JSONL live
under tmp_path, Marquez HTTP is stubbed out, and the use case is constructed
directly (no src.container import needed).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import requests

from src.adapters.lineage import marquez_lineage
from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.domain.exceptions import QualityGateBreachError
from src.use_cases.aggregate_gold import AggregateGoldUseCase

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _hermetic_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keeps every Gold gate test offline and CWD-independent.

    * Marquez HTTP POST fails instantly instead of burning the adapter's
      socket timeout on every lineage emit (the local audit-trail path is
      still exercised on every emit).
    * CWD is pinned to the repo root so the default relative suite paths
      (quality/expectations/...) resolve exactly like production drivers.
    """

    def _unreachable_post(*args: Any, **kwargs: Any) -> Any:
        raise requests.exceptions.ConnectionError(
            "network disabled: unit tests must be hermetic"
        )

    monkeypatch.setattr(marquez_lineage.requests, "post", _unreachable_post)
    monkeypatch.chdir(REPO_ROOT)


@pytest.fixture
def world(tmp_path: Path):
    storage = LakehouseStorageAdapter(db_path=str(tmp_path / "gold_gate_lakehouse.db"))
    audit_file = tmp_path / "lineage.jsonl"
    lineage = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(audit_file),
    )
    use_case = AggregateGoldUseCase(
        storage=storage,
        lineage=lineage,
        quality_engine=ExpectationEngineAdapter(),
    )
    return {
        "storage": storage,
        "audit_file": audit_file,
        "use_case": use_case,
    }


def _audit_events(audit_file: Path) -> list[dict[str, Any]]:
    if not audit_file.exists():
        return []
    return [json.loads(line) for line in audit_file.read_text(encoding="utf-8").splitlines() if line.strip()]


def _job_status(events: list[dict[str, Any]], job_name: str) -> list[str]:
    return [e["eventType"] for e in events if e.get("job", {}).get("name") == job_name]


def _valid_customers() -> list[dict[str, Any]]:
    return [
        {
            "id": 101,
            "name": "Alice Smith",
            "email": "alice.smith101@example.com",
            "segment": "premium",
            "registration_date": "2026-01-15",
            "created_at": "2026-01-15T09:00:00+00:00",
        },
        {
            "id": 102,
            "name": "Bob Jones",
            "email": "bob.jones102@example.com",
            "segment": "standard",
            "registration_date": "2026-02-20",
            "created_at": "2026-02-20T09:00:00+00:00",
        },
    ]


def test_gold_gate_blocks_on_breach(world):
    storage: LakehouseStorageAdapter = world["storage"]
    # Violating silver orders: null status (aggregates to the "None" bucket,
    # outside the allowed status set) and negative amounts (negative
    # gross_revenue, outside [0, 100000000]) on a single business date, so the
    # daily-metrics gate fails on framing AND revenue assertions.
    storage.write_table("silver.sales.orders", [
        {
            "id": i,
            "customer_id": 101,
            "total_amount": -50.0,
            "status": None,
            "order_date": "2026-09-25",
            "created_at": "2026-09-25T10:00:00+00:00",
        }
        for i in range(1, 4)
    ])
    storage.write_table("silver.sales.customers", _valid_customers())

    with pytest.raises(QualityGateBreachError) as exc_info:
        world["use_case"].execute("all")

    assert exc_info.value.pass_rate < exc_info.value.threshold
    # Breaching target blocked BEFORE any write: table never created/empty.
    assert storage.read_table("gold.sales.daily_metrics") == []
    # FAIL lineage event recorded in the offline audit trail.
    events = _audit_events(world["audit_file"])
    assert "FAIL" in _job_status(events, "gold_daily_sales_rollup")
    # The independent customers target still proceeded and published.
    assert len(storage.read_table("gold.sales.customers")) == 2
    assert "COMPLETE" in _job_status(events, "gold_customer_pseudonymization")


def test_gold_gate_passes_happy_path(world):
    storage: LakehouseStorageAdapter = world["storage"]
    storage.write_table("silver.sales.orders", [
        {"id": 1, "customer_id": 101, "total_amount": 100.0, "status": "confirmed",
         "order_date": "2026-09-25", "created_at": "2026-09-25T10:00:00+00:00"},
        {"id": 2, "customer_id": 102, "total_amount": 200.0, "status": "confirmed",
         "order_date": "2026-09-25", "created_at": "2026-09-25T11:00:00+00:00"},
        {"id": 3, "customer_id": 101, "total_amount": 50.0, "status": "pending",
         "order_date": "2026-09-25", "created_at": "2026-09-25T12:00:00+00:00"},
        {"id": 4, "customer_id": 102, "total_amount": 75.5, "status": "shipped",
         "order_date": "2026-09-26", "created_at": "2026-09-26T10:00:00+00:00"},
    ])
    storage.write_table("silver.sales.customers", _valid_customers())

    result = world["use_case"].execute("all")

    assert result["status"] == "SUCCESS"
    # (2026-09-25, confirmed), (2026-09-25, pending), (2026-09-26, shipped).
    assert result["daily_metrics_rows"] == 3
    assert result["customers_masked_rows"] == 2

    metrics = storage.read_table("gold.sales.daily_metrics")
    assert len(metrics) == 3
    assert {(m["order_date"], m["status"]) for m in metrics} == {
        ("2026-09-25", "confirmed"),
        ("2026-09-25", "pending"),
        ("2026-09-26", "shipped"),
    }

    customers = storage.read_table("gold.sales.customers")
    assert len(customers) == 2
    for row in customers:
        assert "email" not in row and "name" not in row  # raw PII removed
        assert len(row["email_hash"]) == 64
        assert row["name_masked"]

    events = _audit_events(world["audit_file"])
    assert "COMPLETE" in _job_status(events, "gold_daily_sales_rollup")
    assert "COMPLETE" in _job_status(events, "gold_customer_pseudonymization")
    assert "FAIL" not in [e["eventType"] for e in events]
