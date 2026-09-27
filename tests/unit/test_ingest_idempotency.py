"""
Modern Data Architecture POC - Unit Tests: RF-01 Idempotent Bronze Ingestion.
Verifies append-only Bronze writes with content-hash idempotency, audit columns,
lineage emission and SQL identifier validation.
Adapters and use cases are constructed directly (never via src.container).
"""

from __future__ import annotations

import pytest

from src.adapters.source.oltp_source import OltpSourceAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.use_cases.ingest_bronze import IngestBronzeUseCase
from tests.helpers import StubLineage

ORDERS_SNAPSHOT = [
    {"id": 1, "customer_id": 10, "total_amount": 100.5, "status": "confirmed",
     "order_date": "2026-09-01", "created_at": "2026-09-01T10:00:00+00:00"},
    {"id": 2, "customer_id": 11, "total_amount": 250.0, "status": "shipped",
     "order_date": "2026-09-02", "created_at": "2026-09-02T11:30:00+00:00"},
    {"id": 3, "customer_id": 12, "total_amount": 75.25, "status": "pending",
     "order_date": "2026-09-03", "created_at": "2026-09-03T09:15:00+00:00"},
]

ORDERS_SNAPSHOT_B = [
    {"id": 4, "customer_id": 13, "total_amount": 42.0, "status": "cancelled",
     "order_date": "2026-09-04", "created_at": "2026-09-04T08:00:00+00:00"},
    {"id": 5, "customer_id": 14, "total_amount": 999.99, "status": "confirmed",
     "order_date": "2026-09-05", "created_at": "2026-09-05T12:45:00+00:00"},
]


class StubOltpSource:
    """Returns one snapshot batch per call; repeats the last batch when exhausted."""

    def __init__(self, batches):
        self._batches = [list(b) for b in batches]
        self.calls = 0

    def extract_table(self, table_name, limit=None):
        batch = self._batches[min(self.calls, len(self._batches) - 1)]
        self.calls += 1
        return [dict(r) for r in batch]


def _storage(tmp_path) -> LakehouseStorageAdapter:
    return LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))


def test_ingest_bronze_idempotent(tmp_path):
    storage = _storage(tmp_path)
    source = StubOltpSource([ORDERS_SNAPSHOT])
    lineage = StubLineage()
    use_case = IngestBronzeUseCase(source=source, storage=storage, lineage=lineage)

    # First run: full snapshot lands
    res1 = use_case.execute("orders")
    assert res1["status"] == "SUCCESS"
    assert res1["rows_ingested"] == len(ORDERS_SNAPSHOT)
    assert res1["rows_new"] == res1["rows_ingested"]
    assert res1["idempotent_hit"] is False

    count1 = len(storage.read_table("bronze.sales.orders"))
    assert count1 == len(ORDERS_SNAPSHOT)
    rows = storage.read_table("bronze.sales.orders")
    assert "_ingested_at" in rows[0]
    assert "_batch_id" in rows[0]
    assert rows[0]["_batch_id"] == res1["batch_id"]

    # Second run over the identical snapshot
    res2 = use_case.execute("orders")
    assert res2["batch_id"] != res1["batch_id"]
    assert len(lineage.events) == 2
    assert lineage.events[0]["job_name"] == "bronze_ingest_orders"
    assert lineage.events[1]["job_name"] == "bronze_ingest_orders"

    count2 = len(storage.read_table("bronze.sales.orders"))
    # RF-01: re-running ingestion over an identical snapshot is idempotent —
    # the content hash excludes audit columns (_batch_id/_ingested_at), so
    # byte-identical business content must write 0 rows (idempotent_hit).
    assert res2["rows_ingested"] == 0, (
        "Bronze re-run over an identical snapshot duplicated rows: "
        "content hash likely includes audit columns (_batch_id/_ingested_at) which differ per run."
    )
    assert res2["idempotent_hit"] is True
    assert count2 == count1


def test_ingest_bronze_new_content_appends(tmp_path):
    storage = _storage(tmp_path)
    source = StubOltpSource([ORDERS_SNAPSHOT, ORDERS_SNAPSHOT_B])
    lineage = StubLineage()
    use_case = IngestBronzeUseCase(source=source, storage=storage, lineage=lineage)

    res1 = use_case.execute("orders")
    assert res1["rows_ingested"] == len(ORDERS_SNAPSHOT)
    assert res1["idempotent_hit"] is False
    count1 = len(storage.read_table("bronze.sales.orders"))
    assert count1 == len(ORDERS_SNAPSHOT)

    res2 = use_case.execute("orders")
    assert res2["rows_ingested"] == len(ORDERS_SNAPSHOT_B)
    assert res2["idempotent_hit"] is False

    count2 = len(storage.read_table("bronze.sales.orders"))
    assert count2 == count1 + len(ORDERS_SNAPSHOT_B)
    assert len(lineage.events) == 2


def test_ingest_bronze_empty_source_reports_empty(tmp_path):
    storage = _storage(tmp_path)
    source = StubOltpSource([[]])
    lineage = StubLineage()
    use_case = IngestBronzeUseCase(source=source, storage=storage, lineage=lineage)

    res = use_case.execute("orders")

    assert res["status"] == "EMPTY"
    assert res["rows_ingested"] == 0
    assert res["target_table"] == "bronze.sales.orders"
    assert storage.read_table("bronze.sales.orders") == []  # nothing written
    assert len(lineage.events) == 1  # lineage still emitted
    assert lineage.events[0]["job_name"] == "bronze_ingest_orders"
    assert lineage.events[0]["status"] == "COMPLETE"


def test_oltp_source_validation():
    source = OltpSourceAdapter()
    with pytest.raises(ValueError):
        source.extract_table("orders; DROP TABLE x")
