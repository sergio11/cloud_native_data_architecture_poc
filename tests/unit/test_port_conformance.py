"""Unit tests: inbound port conformance (LSP invocability).

Every inbound port must be invocable through its DECLARED signature alone: the
test instantiates the real ``Container`` (bound to ``tmp_path`` for the
lakehouse/event/audit files and to the repository root for contracts/suites)
and executes each port using only parameters from the port contract - no extra
kwargs smuggled from a concrete implementation.

This catches the S3 defect class: ``CleanseSilverPort.execute`` once declared
``execute(entity)`` while the implementation required ``suite_path`` (raising
ValueError when None), so any caller honouring the port contract crashed. A
static LSP check (implementation must accept every port-declared call) plus a
live execution of every port through the container guards the regression.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from src.container import Container
from src.ports.inbound import (
    AggregateGoldPort,
    CleanseSilverPort,
    IngestBronzePort,
    ProcessStreamPort,
    QueryServingPort,
    RunQualityGatePort,
    ValidateContractPort,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
ORDERS_SUITE = str(REPO_ROOT / "quality" / "expectations" / "silver_orders_suite.json")
CUSTOMERS_SUITE = str(REPO_ROOT / "quality" / "expectations" / "silver_customers_suite.json")
GOLD_DAILY_SUITE = str(REPO_ROOT / "quality" / "expectations" / "gold_daily_metrics_suite.json")
GOLD_CUSTOMERS_SUITE = str(REPO_ROOT / "quality" / "expectations" / "gold_customers_suite.json")
STREAM_TOPIC = "sales.orders.events"
STREAM_TARGET = "stream.sales.orders_windowed"


@pytest.fixture
def container(tmp_path: Path) -> Container:
    """Real composition root bound to tmp paths (never the repository data/ dir)."""
    return Container(
        db_path=str(tmp_path / "lakehouse.db"),
        repo_root=str(REPO_ROOT),
        event_db_path=str(tmp_path / "events.db"),
        lineage_audit_path=str(tmp_path / "lineage.jsonl"),
    )


def _check_lsp(port_method: Callable[..., Any], impl_method: Callable[..., Any]) -> None:
    """Asserts the implementation honours the port contract (Liskov).

    Every port-declared parameter must exist on the implementation (with a
    default when the port makes it optional), and the implementation must not
    REQUIRE parameters the port does not declare - otherwise a caller using
    only the port signature cannot invoke it.
    """
    port_params = [p for p in inspect.signature(port_method).parameters.values() if p.name != "self"]
    impl_by_name = {p.name: p for p in inspect.signature(impl_method).parameters.values() if p.name != "self"}
    port_by_name = {p.name: p for p in port_params}
    for name, port_param in port_by_name.items():
        assert name in impl_by_name, f"implementation is missing port-declared parameter '{name}'"
        if port_param.default is not inspect.Parameter.empty:
            assert impl_by_name[name].default is not inspect.Parameter.empty, (
                f"port parameter '{name}' is optional but the implementation requires it"
            )
    position_kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }
    for name, impl_param in impl_by_name.items():
        if impl_param.kind in position_kinds and impl_param.default is inspect.Parameter.empty:
            assert name in port_by_name, (
                f"implementation requires '{name}' which the port contract does not declare"
            )


def test_port_signatures_conform_to_lsp(container: Container) -> None:
    """Static LSP check for every inbound port against its container wiring."""
    _check_lsp(IngestBronzePort.execute, container.ingest_bronze.execute)
    _check_lsp(CleanseSilverPort.execute, container.cleanse_silver.execute)
    _check_lsp(AggregateGoldPort.execute, container.aggregate_gold.execute)
    _check_lsp(ProcessStreamPort.execute, container.process_stream.execute)
    _check_lsp(RunQualityGatePort.execute, container.run_quality_gate.execute)
    _check_lsp(ValidateContractPort.execute, container.validate_contract.execute)
    _check_lsp(QueryServingPort.execute_unified_revenue_query, container.query_serving.execute_unified_revenue_query)


def test_every_inbound_port_is_invocable_by_its_declared_signature(container: Container) -> None:
    """Live check: each port executes using ONLY its declared signature."""
    # Ingest (entity only).
    ingest_orders = container.ingest_bronze.execute("orders")
    assert ingest_orders["status"] in ("SUCCESS", "EMPTY")
    assert ingest_orders["rows_ingested"] > 0
    ingest_customers = container.ingest_bronze.execute("customers")
    assert ingest_customers["status"] in ("SUCCESS", "EMPTY")

    # Cleanse (entity, suite): positional call proves the port is invocable as declared.
    silver_orders = container.cleanse_silver.execute("orders", ORDERS_SUITE)
    assert silver_orders["status"] == "SUCCESS"
    assert silver_orders["rows_promoted"] > 0
    silver_customers = container.cleanse_silver.execute("customers", CUSTOMERS_SUITE)
    assert silver_customers["status"] == "SUCCESS"
    assert silver_customers["rows_promoted"] > 0

    # Gold (target + required suites).
    gold = container.aggregate_gold.execute(
        "all",
        daily_metrics_suite=GOLD_DAILY_SUITE,
        customers_suite=GOLD_CUSTOMERS_SUITE,
    )
    assert gold["status"] == "SUCCESS"
    assert gold["daily_metrics_rows"] > 0
    assert gold["customers_masked_rows"] > 0

    # Stream (topic/target/replay): seed one deterministic event, then process.
    container.stream_broker.publish_event(
        topic=STREAM_TOPIC,
        key="1",
        payload={
            "order_id": 1,
            "timestamp": "2026-09-26T10:00:00+00:00",
            "status": "confirmed",
            "total_amount": 100.0,
            "order_date": "2026-09-26",
        },
    )
    stream = container.process_stream.execute(topic=STREAM_TOPIC, target_table=STREAM_TARGET, replay=False)
    assert stream["status"] == "SUCCESS"
    assert stream["events_consumed"] == 1

    # Quality gate (table/suite/threshold) against the Silver table just promoted.
    evaluation = container.run_quality_gate.execute(
        table_name="silver.sales.orders",
        suite_path=ORDERS_SUITE,
        threshold=0.95,
    )
    assert evaluation.passed is True

    # Validate (no arguments: the catalog owns the repository root).
    validation = container.validate_contract.execute()
    assert validation["passed"] is True

    # Serving query over the batch + stream tiers just materialised.
    rows = container.query_serving.execute_unified_revenue_query(limit=5)
    assert isinstance(rows, list)
    assert len(rows) > 0
