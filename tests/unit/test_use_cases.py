"""
Modern Data Architecture POC - Unit Tests: Use Cases & Hexagonal Flow.
Verifies end-to-end data flow: Bronze Ingestion -> Silver Cleansing -> Gold Modeling -> Serving Query.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter
from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.serving.sql_serving import SqlServingAdapter
from src.adapters.source.oltp_source import OltpSourceAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.adapters.streaming.event_stream import EventStreamAdapter
from src.use_cases.aggregate_gold import AggregateGoldUseCase
from src.use_cases.cleanse_silver import CleanseSilverUseCase
from src.use_cases.ingest_bronze import IngestBronzeUseCase
from src.use_cases.process_stream import ProcessStreamUseCase
from src.use_cases.query_serving import QueryServingUseCase
from src.use_cases.validate_contract import ValidateContractUseCase

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def temp_lakehouse(tmp_path: Path):
    db_path = str(tmp_path / "test_lakehouse.db")
    storage = LakehouseStorageAdapter(db_path=db_path)
    source = OltpSourceAdapter(seed=42)
    stream_broker = EventStreamAdapter(db_path=str(tmp_path / "test_events.db"))
    lineage = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(tmp_path / "test_lineage.jsonl"),
    )
    quality = ExpectationEngineAdapter()

    return {
        "db_path": db_path,
        "storage": storage,
        "source": source,
        "stream_broker": stream_broker,
        "lineage": lineage,
        "quality": quality,
    }


def test_medallion_pipeline_flow(temp_lakehouse):
    storage = temp_lakehouse["storage"]
    source = temp_lakehouse["source"]
    lineage = temp_lakehouse["lineage"]
    quality = temp_lakehouse["quality"]

    # 1. Ingest Bronze
    ingest_uc = IngestBronzeUseCase(source=source, storage=storage, lineage=lineage)
    bronze_res = ingest_uc.execute("orders")
    assert bronze_res["status"] == "SUCCESS"
    assert bronze_res["rows_ingested"] > 0

    bronze_records = storage.read_table("bronze.sales.orders")
    assert len(bronze_records) > 0
    assert "_ingested_at" in bronze_records[0]
    assert "_batch_id" in bronze_records[0]

    # 2. Cleanse Silver
    cleanse_uc = CleanseSilverUseCase(storage=storage, quality_engine=quality, lineage=lineage)
    silver_res = cleanse_uc.execute(
        "orders",
        suite_path=str(REPO_ROOT / "quality" / "expectations" / "silver_orders_suite.json"),
    )
    assert silver_res["status"] == "SUCCESS"
    assert silver_res["rows_promoted"] > 0

    silver_records = storage.read_table("silver.sales.orders")
    assert len(silver_records) > 0
    assert "_ingested_at" not in silver_records[0]  # Audit cols stripped for conformed layer

    # 3. Aggregate Gold
    aggregate_uc = AggregateGoldUseCase(storage=storage, lineage=lineage, quality_engine=quality)
    gold_res = aggregate_uc.execute(
        "daily_metrics",
        daily_metrics_suite=str(REPO_ROOT / "quality" / "expectations" / "gold_daily_metrics_suite.json"),
        customers_suite=str(REPO_ROOT / "quality" / "expectations" / "gold_customers_suite.json"),
    )
    assert gold_res["status"] == "SUCCESS"
    assert gold_res["daily_metrics_rows"] > 0

    gold_records = storage.read_table("gold.sales.daily_metrics")
    assert len(gold_records) > 0
    assert "gross_revenue" in gold_records[0]
    assert "average_order_value" in gold_records[0]


def test_kappa_streaming_and_serving(temp_lakehouse):
    storage = temp_lakehouse["storage"]
    broker = temp_lakehouse["stream_broker"]
    lineage = temp_lakehouse["lineage"]

    # 0. Seed deterministic events into the log (adapters never fabricate data).
    # Fixed wall-clock (no datetime.now): flaky at UTC midnight otherwise.
    now = datetime(2026, 9, 26, 10, 0, 0, tzinfo=UTC)
    for i in range(1, 6):
        broker.publish_event(
            topic="sales.orders.events",
            key=str(i),
            payload={
                "order_id": i,
                "total_amount": 100.0 * i,
                "status": "confirmed",
                "order_date": now.strftime("%Y-%m-%d"),
                "timestamp": now.isoformat(),
            },
        )

    # 1. Process Stream Speed Layer (incremental consumption, no duplicates on re-run)
    stream_uc = ProcessStreamUseCase(broker=broker, storage=storage, lineage=lineage)
    res = stream_uc.execute(topic="sales.orders.events", replay=False)
    assert res["status"] == "SUCCESS"
    assert res["windows_written"] > 0

    stream_records = storage.read_table("stream.sales.orders_windowed")
    assert len(stream_records) > 0

    rerun = stream_uc.execute(topic="sales.orders.events", replay=False)
    assert rerun["status"] == "NO_EVENTS"
    assert len(storage.read_table("stream.sales.orders_windowed")) == len(stream_records)

    # 2. Batch Gold tier for the Lambda fusion (RF-11): historical day (yesterday),
    # so today's stream window adds a DISTINCT key instead of colliding.
    storage.write_table(
        "gold.sales.daily_metrics",
        [
            {
                "order_date": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
                "status": "confirmed",
                "gross_revenue": 9999.99,
                "total_orders": 7,
                "average_order_value": 1428.57,
            }
        ],
        mode="append",
    )

    # 3. Unified Serving Layer Query (RF-11) via the outbound QueryExecutorPort
    serving_uc = QueryServingUseCase(query_executor=SqlServingAdapter(db_path=temp_lakehouse["db_path"]))
    unified = serving_uc.execute_unified_revenue_query()
    assert len(unified) > 0
    tiers = {r["source_tier"] for r in unified}
    assert "batch_historical" in tiers
    assert "stream_speed" in tiers


def test_contract_validation_use_case():
    validator = ValidateContractUseCase(catalog=FsDomainCatalogAdapter(str(REPO_ROOT)))
    res = validator.execute()
    assert res["products_evaluated"] >= 6
    assert res["contracts_evaluated"] >= 6
    assert res["passed"] is True
