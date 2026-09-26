"""
Modern Data Architecture POC - Integration Tests: SRS §9 Acceptance Criteria.

Hermetic Given/When/Then proofs for RF-06, RF-07, RF-09, RF-10, RF-11, RF-15
and RF-19 run against the REAL adapters (SQLite lakehouse, SQLite event log,
JSONL lineage audit, expectation engine, SQL serving) wired inside tmp_path by
``tests/integration/conftest.py``. No network (Marquez POST is stubbed to fail
instantly), no repository ``data/`` writes, no randomness: every timestamp and
row is fixed so the suite is reproducible.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.domain.exceptions import QualityGateBreachError

CLI_PATH = Path(__file__).resolve().parents[2] / "governance" / "contract_validator.py"

STREAM_TOPIC = "sales.orders.events"
STREAM_TARGET = "stream.sales.orders_windowed"
GOLD_METRICS = "gold.sales.daily_metrics"


# ---------------------------------------------------------------------------
# Deterministic builders
# ---------------------------------------------------------------------------


def _copy_suite(tmp_path: Path, suite_path: str, name: str) -> str:
    """Copies a real quality suite into tmp so gates never depend on CWD."""
    target = tmp_path / name
    shutil.copyfile(suite_path, target)
    return str(target)


def _bronze_orders_with_null_status(null_count: int = 3, total: int = 20) -> list[dict[str, Any]]:
    """Raw Bronze rows where ~15% of ``status`` are null/blank (RF-06 Given).

    Includes ``_``-prefixed audit columns exactly like real Bronze output;
    the cleansing use case must strip them before quality evaluation.
    """
    statuses = ["confirmed", "pending", "shipped", "cancelled"]
    null_variants = [None, "", "   "]
    rows: list[dict[str, Any]] = []
    for index in range(1, total + 1):
        if index <= null_count:
            status: Any = null_variants[(index - 1) % len(null_variants)]
        else:
            status = statuses[(index - 1) % len(statuses)]
        rows.append(
            {
                "id": index,
                "customer_id": 100 + index,
                "total_amount": round(10.0 + index, 2),
                "status": status,
                "order_date": "2026-09-25",
                "created_at": "2026-09-25T10:00:00+00:00",
                "_batch_id": "bronze-batch-1",
                "_ingested_at": "2026-09-25T23:00:00+00:00",
            }
        )
    return rows


def _stream_event(order_id: int, timestamp: str, amount: float, date: str) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "timestamp": timestamp,
        "status": "confirmed",
        "total_amount": amount,
        "order_date": date,
    }


def _stored(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Expected on-disk representation (scalar values are persisted as TEXT)."""
    return [{key: str(value) for key, value in row.items()} for row in rows]


def _window_sort_key(row: dict[str, Any]) -> Any:
    return (row["window_start"], row["status"])


def _field(name: str, field_type: str, required: bool = True) -> dict[str, Any]:
    return {"name": name, "type": field_type, "required": required}


def _write_product(
    root: Path,
    folder: str,
    name: str,
    contract_fields: list[dict[str, Any]],
    schema_fields: list[dict[str, Any]],
) -> Path:
    """Materialises a minimal valid DataProduct trio (product/contract/schema)."""
    product_dir = root / "domains" / "sales" / "data-products" / folder
    product_dir.mkdir(parents=True, exist_ok=True)

    product = {
        "apiVersion": "dataplatform.io/v1alpha1",
        "kind": "DataProduct",
        "metadata": {
            "name": name,
            "domain": "sales",
            "version": "1.0.0",
            "owner": "data-team@company.internal",
        },
        "spec": {
            "description": f"RF-15 acceptance product {name}",
            "classification": "internal",
            "sla": {"freshness": "1h"},
            "assets": [
                {"name": f"gold_{name}", "type": "iceberg_table", "path": f"s3a://gold/{name}/"}
            ],
            "quality": {"suite": "quality/expectations/suite.json"},
            "contract": "./contract.yaml",
        },
    }
    (product_dir / "product.yaml").write_text(yaml.safe_dump(product), encoding="utf-8")

    contract = {
        "apiVersion": "datacontract.com/v1alpha1",
        "kind": "DataContract",
        "metadata": {
            "name": f"{name}-contract",
            "version": "1.0.0",
            "domain": "sales",
            "data_product": name,
            "owner": "data-team@company.internal",
        },
        "spec": {
            "description": f"RF-15 acceptance contract for {name}",
            "compatibility_mode": "BACKWARD",
            "sla": {"freshness": "1h"},
            "schema": {"format": "iceberg", "fields": contract_fields},
            "quality": {"thresholds": {"completeness": 0.95}},
        },
    }
    (product_dir / "contract.yaml").write_text(yaml.safe_dump(contract), encoding="utf-8")

    properties: dict[str, Any] = {}
    required: list[str] = []
    for field_def in schema_fields:
        properties[field_def["name"]] = {"type": field_def["type"]}
        required.append(field_def["name"])
    schema_doc = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }
    (product_dir / "schema.json").write_text(json.dumps(schema_doc, indent=2), encoding="utf-8")
    return product_dir


def _run_governance_cli(repo_root: Path) -> subprocess.CompletedProcess:
    """The exact CLI entry point the Rakefile / demo.sh invoke (RF-15)."""
    return subprocess.run(
        [sys.executable, str(CLI_PATH), "--repo-root", str(repo_root)],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        timeout=120,
    )


# ---------------------------------------------------------------------------
# RF-06 - Quality gate blocks Silver promotion on 15% nulls
# ---------------------------------------------------------------------------


def test_rf06_null_breach_blocks_silver_and_alerts_via_lineage(world, tmp_path: Path) -> None:
    """Given 15% null/blank ``status`` in Bronze, When the gate runs with the
    real silver orders suite at threshold 0.95, Then promotion raises
    QualityGateBreachError, Silver stays untouched and a FAIL lineage event
    (the owner alert trail) is appended to the audit file."""
    suite_path = _copy_suite(tmp_path, world.orders_suite, "silver_orders_suite.json")
    world.storage.write_table(
        "bronze.sales.orders", _bronze_orders_with_null_status(), mode="append"
    )

    with pytest.raises(QualityGateBreachError) as exc_info:
        world.cleanse_silver.execute("orders", quality_threshold=0.95, suite_path=suite_path)

    assert exc_info.value.threshold == 0.95
    assert exc_info.value.pass_rate < 0.95
    assert "silver.sales.orders" in str(exc_info.value)

    # Then: nothing was promoted to Silver.
    assert world.storage.table_exists("silver.sales.orders") is False
    assert world.storage.read_table("silver.sales.orders") == []

    # Then: the FAIL run is on the audit trail (RF-06 alert).
    fail_events = [e for e in world.lineage_events() if e["eventType"] == "FAIL"]
    assert len(fail_events) == 1
    assert fail_events[0]["job"]["name"] == "silver_clean_orders"
    assert [o["name"] for o in fail_events[0]["outputs"]] == ["silver.sales.orders"]


def test_rf06_clean_rows_pass_gate_and_reach_silver(world, tmp_path: Path) -> None:
    """Given clean Bronze rows, When the gate runs at 0.95, Then promotion
    succeeds, Silver holds every row and a COMPLETE lineage event is recorded."""
    suite_path = _copy_suite(tmp_path, world.orders_suite, "silver_orders_suite.json")

    ingest = world.ingest_bronze.execute("orders")
    assert ingest["status"] == "SUCCESS"
    assert ingest["rows_ingested"] == len(world.demo_orders)

    result = world.cleanse_silver.execute("orders", quality_threshold=0.95, suite_path=suite_path)

    assert result["status"] == "SUCCESS"
    assert result["quality_pass_rate"] >= 0.95
    assert result["rows_promoted"] == len(world.demo_orders)

    silver_rows = world.storage.read_table("silver.sales.orders")
    assert len(silver_rows) > 0
    assert len(silver_rows) == len(world.demo_orders)
    assert "_ingested_at" not in silver_rows[0]

    complete = [
        e
        for e in world.lineage_events()
        if e["job"]["name"] == "silver_clean_orders" and e["eventType"] == "COMPLETE"
    ]
    assert len(complete) == 1
    assert [i["name"] for i in complete[0]["inputs"]] == ["bronze.sales.orders"]


# ---------------------------------------------------------------------------
# RF-07 - Gold time travel returns the prior version unaltered
# ---------------------------------------------------------------------------


def test_rf07_time_travel_returns_previous_gold_version_unchanged(world) -> None:
    """Given gold.sales.daily_metrics written as version 1 and then overwritten
    with version 2, When reading version 1, Then the historical snapshot comes
    back byte-for-byte unaltered while the current read returns version 2 and
    list_versions() reports both snapshots with the right row counts."""
    storage = world.storage
    version_1 = [
        {"order_date": "2026-09-20", "status": "confirmed", "total_orders": 5,
         "gross_revenue": 1000.0, "average_order_value": 200.0},
        {"order_date": "2026-09-21", "status": "shipped", "total_orders": 3,
         "gross_revenue": 450.0, "average_order_value": 150.0},
        {"order_date": "2026-09-22", "status": "pending", "total_orders": 2,
         "gross_revenue": 80.0, "average_order_value": 40.0},
    ]
    version_2 = [
        {"order_date": "2026-09-23", "status": "confirmed", "total_orders": 9,
         "gross_revenue": 2000.0, "average_order_value": 222.22},
        {"order_date": "2026-09-24", "status": "cancelled", "total_orders": 1,
         "gross_revenue": 25.0, "average_order_value": 25.0},
    ]

    assert storage.write_table(GOLD_METRICS, version_1, mode="append") == 3
    v1_snapshot = storage.read_table(GOLD_METRICS, version=1)
    assert v1_snapshot == _stored(version_1)
    assert storage.read_table(GOLD_METRICS) == v1_snapshot

    assert storage.write_table(GOLD_METRICS, version_2, mode="overwrite") == 2

    # Then: version 1 is served exactly as it was written (no mutation).
    assert storage.read_table(GOLD_METRICS, version=1) == v1_snapshot
    # Then: the current read reflects version 2 only.
    assert storage.read_table(GOLD_METRICS) == _stored(version_2)

    versions = storage.list_versions(GOLD_METRICS)
    assert [v["snapshot_version"] for v in versions] == [1, 2]
    assert [v["row_count"] for v in versions] == [3, 2]
    assert all(v["created_at"] for v in versions)


# ---------------------------------------------------------------------------
# RF-09 - Event-time tumbling windows land in serving right after processing
# ---------------------------------------------------------------------------


def test_rf09_five_minute_windows_reflect_events_immediately(world) -> None:
    """Given events at 10:00:00, 10:03:00 and 10:07:00 UTC, When
    ProcessStreamUseCase runs, Then they bucket into 10:00-10:05 (2 events) and
    10:05-10:10 (1 event), readable the moment execute() returns."""
    events = [
        _stream_event(1, "2026-09-26T10:00:00+00:00", 100.0, "2026-09-26"),
        _stream_event(2, "2026-09-26T10:03:00+00:00", 200.0, "2026-09-26"),
        _stream_event(3, "2026-09-26T10:07:00+00:00", 400.0, "2026-09-26"),
    ]
    for index, event in enumerate(events, start=1):
        world.broker.publish_event(STREAM_TOPIC, str(index), event)

    result = world.process_stream.execute(topic=STREAM_TOPIC, target_table=STREAM_TARGET)

    assert result["status"] == "SUCCESS"
    assert result["events_consumed"] == 3
    assert result["windows_generated"] == 2
    assert result["windows_written"] == 2

    windows = world.storage.read_table(STREAM_TARGET)
    assert len(windows) == 2

    first, second = windows
    assert first["window_start"] == "2026-09-26T10:00:00+00:00"
    assert first["window_end"] == "2026-09-26T10:05:00+00:00"  # end exclusive
    assert int(first["window_orders"]) == 2
    assert float(first["window_revenue"]) == 300.0
    assert first["order_date"] == "2026-09-26"
    assert first["source_tier"] == "stream_speed"

    assert second["window_start"] == "2026-09-26T10:05:00+00:00"
    assert second["window_end"] == "2026-09-26T10:10:00+00:00"
    assert int(second["window_orders"]) == 1
    assert float(second["window_revenue"]) == 400.0


# ---------------------------------------------------------------------------
# RF-10 - Full replay is deterministic and ignores the consumer cursor
# ---------------------------------------------------------------------------


def test_rf10_replay_is_deterministic_and_ignores_consumer_cursor(world) -> None:
    """Given a seeded log already consumed in incremental mode, When
    execute(replay=True) runs twice, Then both runs reprocess ALL events
    (the speed-layer cursor is ignored) and produce identical table contents."""
    events = [
        _stream_event(1, "2026-09-26T10:00:00+00:00", 100.0, "2026-09-26"),
        _stream_event(2, "2026-09-26T10:03:00+00:00", 200.0, "2026-09-26"),
        _stream_event(3, "2026-09-26T10:07:00+00:00", 400.0, "2026-09-26"),
    ]
    for index, event in enumerate(events, start=1):
        world.broker.publish_event(STREAM_TOPIC, str(index), event)

    # Incremental mode consumes everything and advances the cursor.
    incremental = world.process_stream.execute(
        topic=STREAM_TOPIC, target_table=STREAM_TARGET, replay=False
    )
    assert incremental["status"] == "SUCCESS"
    assert incremental["events_consumed"] == 3
    assert len(world.storage.read_table(STREAM_TARGET)) == 2

    first_replay = world.process_stream.execute(
        topic=STREAM_TOPIC, target_table=STREAM_TARGET, replay=True
    )
    assert first_replay["status"] == "SUCCESS"
    assert first_replay["mode"] == "replay_full"
    # Cursor ignored: already-consumed events are reprocessed from offset 0.
    assert first_replay["events_consumed"] == 3
    snapshot = sorted(world.storage.read_table(STREAM_TARGET), key=_window_sort_key)
    assert len(snapshot) == 2

    second_replay = world.process_stream.execute(
        topic=STREAM_TOPIC, target_table=STREAM_TARGET, replay=True
    )
    assert second_replay["status"] == "SUCCESS"
    assert second_replay["events_consumed"] == 3
    second_snapshot = sorted(world.storage.read_table(STREAM_TARGET), key=_window_sort_key)

    assert second_snapshot == snapshot  # deterministic full replay, no drift
    assert len(second_snapshot) == len(snapshot)  # no row growth across replays


# ---------------------------------------------------------------------------
# RF-11 - One coherent query over batch Gold + stream Speed, no double count
# ---------------------------------------------------------------------------


def test_rf11_batch_wins_and_stream_fills_gaps_in_single_query(world) -> None:
    """Given batch gold rows for D1 (confirmed) and stream rows for D1 (same key,
    different numbers) plus D2, When execute_unified_revenue_query() runs once,
    Then D1/confirmed appears EXACTLY ONCE with the batch values and
    source_tier=batch_historical, D2 comes from stream_speed, all in a single
    coherent list."""
    gold_rows = [
        {
            "order_date": "2026-09-25",
            "status": "confirmed",
            "total_orders": 5,
            "gross_revenue": 1000.0,
            "average_order_value": 200.0,
        }
    ]
    stream_rows = [
        {"order_date": "2026-09-25", "status": "confirmed", "window_orders": 3,
         "window_revenue": 600.0, "avg_order_value": 200.0},
        {"order_date": "2026-09-26", "status": "confirmed", "window_orders": 2,
         "window_revenue": 300.0, "avg_order_value": 150.0},
    ]
    assert world.storage.write_table(GOLD_METRICS, gold_rows, mode="append") == 1
    assert world.storage.write_table(STREAM_TARGET, stream_rows, mode="append") == 2

    rows = world.query_serving.execute_unified_revenue_query()

    assert isinstance(rows, list) and rows == sorted(
        rows, key=lambda r: (r["order_date"], r["status"]), reverse=True
    )

    d1_confirmed = [
        r for r in rows if r["order_date"] == "2026-09-25" and r["status"] == "confirmed"
    ]
    assert len(d1_confirmed) == 1  # exactly once - the batch value wins
    batch = d1_confirmed[0]
    assert batch["source_tier"] == "batch_historical"
    assert batch["total_orders"] == 5
    assert batch["total_revenue"] == 1000.0
    assert batch["avg_order_value"] == 200.0

    d2 = [r for r in rows if r["order_date"] == "2026-09-26"]
    assert len(d2) == 1
    assert d2[0]["source_tier"] == "stream_speed"
    assert d2[0]["total_orders"] == 2
    assert d2[0]["total_revenue"] == 300.0

    assert {r["source_tier"] for r in rows} == {"batch_historical", "stream_speed"}
    assert len(rows) == 2  # batch key not duplicated by its stream twin


# ---------------------------------------------------------------------------
# RF-15 - Governance CLI blocks a protected-field rename in CI
# ---------------------------------------------------------------------------


def test_rf15_governance_cli_blocks_field_rename_and_passes_clean_tree(
    tmp_path: Path,
) -> None:
    """Given a domains tree whose contract renames a protected field (status ->
    status_code) while its sibling schema.json still declares ``status``, When
    the real governance CLI runs, Then the exit code is 1; given a fully
    compatible tree, Then the exit code is 0 (publication allowed)."""
    schema_fields = [
        {"name": "id", "type": "integer"},
        {"name": "status", "type": "string"},
        {"name": "total_amount", "type": "number"},
    ]
    compatible_fields = [
        _field("id", "integer"),
        _field("status", "string"),
        _field("total_amount", "decimal(12,2)"),
    ]

    _write_product(tmp_path, "orders", "orders", compatible_fields, schema_fields)
    passing = _run_governance_cli(tmp_path)
    assert passing.returncode == 0, passing.stderr
    assert "successfully validated" in passing.stderr

    # Producer renames the protected field in the contract only.
    renamed_fields = [
        _field("id", "integer"),
        _field("status_code", "string"),
        _field("total_amount", "decimal(12,2)"),
    ]
    _write_product(tmp_path, "orders", "orders", renamed_fields, schema_fields)

    blocked = _run_governance_cli(tmp_path)
    assert blocked.returncode == 1, blocked.stdout
    assert "status" in blocked.stderr
    assert "failed validation" in blocked.stderr


# ---------------------------------------------------------------------------
# RF-19 - Lineage: connected bronze -> silver -> gold chain
# ---------------------------------------------------------------------------


def test_rf19_lineage_records_connected_bronze_to_gold_chain(world) -> None:
    """Given the medallion flow executed (ingest x2, cleanse x2, aggregate all),
    Then the audit file holds bronze -> silver -> gold job events in order with
    inputs/outputs forming a connected chain (bronze output == silver input,
    silver output == gold input)."""
    assert world.ingest_bronze.execute("orders")["status"] == "SUCCESS"
    assert world.ingest_bronze.execute("customers")["status"] == "SUCCESS"

    for entity, suite in (("orders", world.orders_suite), ("customers", world.customers_suite)):
        result = world.cleanse_silver.execute(entity, quality_threshold=0.95, suite_path=suite)
        assert result["status"] == "SUCCESS"

    assert world.aggregate_gold.execute("all")["status"] == "SUCCESS"

    events = world.lineage_events()
    job_names = [e["job"]["name"] for e in events]
    assert job_names == [
        "bronze_ingest_orders",
        "bronze_ingest_customers",
        "silver_clean_orders",
        "silver_clean_customers",
        "gold_daily_sales_rollup",
        "gold_customer_pseudonymization",
    ]
    assert all(e["eventType"] == "COMPLETE" for e in events)
    assert all(e["outputs"] for e in events)  # every job declares its output

    def _io(event: dict[str, Any]) -> tuple[list[str], list[str]]:
        return (
            [i["name"] for i in event["inputs"]],
            [o["name"] for o in event["outputs"]],
        )

    bronze_orders_out = _io(events[0])[1]
    assert bronze_orders_out == ["bronze.sales.orders"]
    assert _io(events[2]) == (bronze_orders_out, ["silver.sales.orders"])
    assert _io(events[4]) == (["silver.sales.orders"], ["gold.sales.daily_metrics"])

    bronze_customers_out = _io(events[1])[1]
    assert bronze_customers_out == ["bronze.sales.customers"]
    assert _io(events[3]) == (bronze_customers_out, ["silver.sales.customers"])
    assert _io(events[5]) == (["silver.sales.customers"], ["gold.sales.customers"])

    # Bronze jobs come from the source, silver from bronze, gold from silver.
    assert _io(events[0])[0] == ["postgresql.public.orders"]
    assert _io(events[1])[0] == ["postgresql.public.customers"]
    assert events[0]["eventTime"] <= events[2]["eventTime"] <= events[4]["eventTime"]
    assert events[1]["eventTime"] <= events[3]["eventTime"] <= events[5]["eventTime"]
