"""
Modern Data Architecture POC - Integration Test: full demo flow.

Reproduces the whole F1-F6 demo path hermetically inside tmp_path:
Bronze ingest (orders + customers) -> Silver cleanse behind the real quality
suites -> Gold aggregation with GDPR masking -> seed stream log -> speed-layer
processing -> single unified serving query. Verifies stage row counts, the
GDPR column contract, dual-tier serving, Bronze re-ingest idempotency and
whole-chain idempotency.
"""

from __future__ import annotations

import re

STREAM_TOPIC = "sales.orders.events"
STREAM_TARGET = "stream.sales.orders_windowed"

GOLD_METRICS = "gold.sales.daily_metrics"
GOLD_CUSTOMERS = "gold.sales.customers"

STREAM_EVENTS = [
    {
        "order_id": 9001,
        "timestamp": "2026-09-27T10:00:00+00:00",
        "status": "confirmed",
        "total_amount": 150.0,
        "order_date": "2026-09-27",
    },
    {
        "order_id": 9002,
        "timestamp": "2026-09-27T10:02:00+00:00",
        "status": "confirmed",
        "total_amount": 250.0,
        "order_date": "2026-09-27",
    },
    {
        "order_id": 9003,
        "timestamp": "2026-09-27T10:07:00+00:00",
        "status": "confirmed",
        "total_amount": 350.0,
        "order_date": "2026-09-27",
    },
]


def _run_medallion_chain(world) -> dict:
    """Bronze -> Silver -> Gold for both entities; returns stage results."""
    results: dict = {"ingest": {}, "cleanse": {}}
    for entity in ("orders", "customers"):
        results["ingest"][entity] = world.ingest_bronze.execute(entity)
    for entity, suite in (("orders", world.orders_suite), ("customers", world.customers_suite)):
        results["cleanse"][entity] = world.cleanse_silver.execute(entity, quality_threshold=0.95, suite_path=suite)
    results["gold"] = world.aggregate_gold.execute(
        "all",
        daily_metrics_suite=world.gold_daily_metrics_suite,
        customers_suite=world.gold_customers_suite,
    )
    return results


def test_full_demo_flow(world) -> None:
    """End-to-end: every stage produces rows, Gold is GDPR-safe,
    serving unifies batch + stream in one query, Bronze re-ingest is a no-op
    and re-running the entire medallion chain never duplicates Silver/Gold."""
    storage = world.storage

    # --- F1/F2: Bronze ingest (orders + customers), deterministic source ----
    first = _run_medallion_chain(world)
    expected_rows = {"orders": len(world.demo_orders), "customers": len(world.demo_customers)}
    for entity in ("orders", "customers"):
        assert first["ingest"][entity]["status"] == "SUCCESS"
        assert first["ingest"][entity]["rows_ingested"] == expected_rows[entity]
        assert first["cleanse"][entity]["status"] == "SUCCESS"
        assert first["cleanse"][entity]["rows_promoted"] == expected_rows[entity]
    assert first["gold"]["daily_metrics_rows"] > 0
    assert first["gold"]["customers_masked_rows"] > 0

    bronze_orders = storage.read_table("bronze.sales.orders")
    bronze_customers = storage.read_table("bronze.sales.customers")
    assert len(bronze_orders) == len(world.demo_orders)
    assert len(bronze_customers) == len(world.demo_customers)

    # --- Bronze re-ingest idempotency -------------------------------------
    reingest = world.ingest_bronze.execute("orders")
    assert reingest["rows_ingested"] == 0
    assert reingest["idempotent_hit"] is True
    assert len(storage.read_table("bronze.sales.orders")) == len(world.demo_orders)

    # --- Silver row counts -------------------------------------------------
    silver_orders = storage.read_table("silver.sales.orders")
    silver_customers = storage.read_table("silver.sales.customers")
    assert len(silver_orders) == len(world.demo_orders)
    assert len(silver_customers) == len(world.demo_customers)
    assert all(not key.startswith("_") for row in silver_orders for key in row)

    # --- Gold GDPR column contract ----------------------------------------
    gold_customers = storage.read_table(GOLD_CUSTOMERS)
    assert len(gold_customers) == len(world.demo_customers)

    columns = set(storage.get_table_schema(GOLD_CUSTOMERS))
    assert "email" not in columns and "name" not in columns
    assert "email_hash" in columns and "name_masked" in columns

    raw_values = {str(customer["email"]) for customer in world.demo_customers} | {
        str(customer["name"]) for customer in world.demo_customers
    }
    for row in gold_customers:
        assert "email" not in row and "name" not in row
        assert "email_hash" in row and "name_masked" in row
        assert re.fullmatch(r"[0-9a-f]{64}", str(row["email_hash"]))
        assert re.fullmatch(r"[A-Z][a-z]+ [A-Z]\.", str(row["name_masked"]))
        assert str(row["email_hash"]) not in raw_values
        assert str(row["name_masked"]) not in raw_values

    # --- F4/F5: seed stream log + speed layer ------------------------------
    for index, event in enumerate(STREAM_EVENTS, start=1):
        world.broker.publish_event(STREAM_TOPIC, str(index), event)
    stream_result = world.process_stream.execute(topic=STREAM_TOPIC, target_table=STREAM_TARGET)
    assert stream_result["status"] == "SUCCESS"
    assert stream_result["events_consumed"] == len(STREAM_EVENTS)
    assert stream_result["windows_written"] > 0
    stream_rows = storage.read_table(STREAM_TARGET)
    assert len(stream_rows) > 0

    # --- F6: unified serving query - both tiers in one call ---------------
    unified = world.query_serving.execute()
    assert unified, "unified serving query returned no rows"
    assert {row["source_tier"] for row in unified} == {"batch_historical", "stream_speed"}
    assert any(row["order_date"] == "2026-09-27" and row["source_tier"] == "stream_speed" for row in unified)
    assert any(
        row["order_date"] in ("2026-09-25", "2026-09-26") and row["source_tier"] == "batch_historical"
        for row in unified
    )

    # --- Re-running the WHOLE chain duplicates nothing --------------------
    counts_before = {
        "silver_orders": len(storage.read_table("silver.sales.orders")),
        "silver_customers": len(storage.read_table("silver.sales.customers")),
        "gold_metrics": len(storage.read_table(GOLD_METRICS)),
        "gold_customers": len(storage.read_table(GOLD_CUSTOMERS)),
        "silver_orders_versions": len(storage.list_versions("silver.sales.orders")),
        "gold_metrics_versions": len(storage.list_versions(GOLD_METRICS)),
    }

    second = _run_medallion_chain(world)
    for entity in ("orders", "customers"):
        assert second["ingest"][entity]["rows_ingested"] == 0  # Bronze untouched
        assert second["cleanse"][entity]["status"] == "SUCCESS"

    counts_after = {
        "silver_orders": len(storage.read_table("silver.sales.orders")),
        "silver_customers": len(storage.read_table("silver.sales.customers")),
        "gold_metrics": len(storage.read_table(GOLD_METRICS)),
        "gold_customers": len(storage.read_table(GOLD_CUSTOMERS)),
        "silver_orders_versions": len(storage.list_versions("silver.sales.orders")),
        "gold_metrics_versions": len(storage.list_versions(GOLD_METRICS)),
    }
    assert counts_after == counts_before

    # Serving still answers identically after the second pass.
    assert world.query_serving.execute() == unified
