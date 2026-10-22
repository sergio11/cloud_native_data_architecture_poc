"""Unit tests: quality gate (fail-closed).

Covers the expectation engine, Silver promotion gate and gate runner with
inline stub adapters.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.domain.exceptions import QualityGateBreachError, SuiteNotFoundError
from src.use_cases.cleanse_silver import CleanseSilverUseCase
from src.use_cases.run_quality_gate import RunQualityGateUseCase
from tests.helpers import EmptyContractCatalog, StubLineage


class StubStorage:
    """Minimal in-memory LakehouseStoragePort stub."""

    def __init__(self, tables: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = tables or {}
        self.writes: list[dict[str, Any]] = []

    def write_table(self, table_name: str, records: list[dict[str, Any]], mode: str = "append") -> int:
        self.writes.append({"table": table_name, "rows": len(records), "mode": mode})
        self.tables[table_name] = list(records)
        return len(records)

    def read_table(
        self,
        table_name: str,
        limit: int | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.tables.get(table_name, [])
        return list(rows[:limit] if limit else rows)

    def table_exists(self, table_name: str) -> bool:
        return table_name in self.tables

    def get_table_schema(self, table_name: str) -> list[str]:
        rows = self.tables.get(table_name, [])
        return sorted(rows[0].keys()) if rows else []

    def list_versions(self, table_name: str) -> list[dict[str, Any]]:
        return []


def _write_suite(tmp_path, name: str, expectations: list[dict[str, Any]]) -> str:
    suite_file = tmp_path / f"{name}.json"
    suite_file.write_text(
        json.dumps({"expectation_suite_name": name, "expectations": expectations}),
        encoding="utf-8",
    )
    return str(suite_file)


def test_row_count_expectation_enforced(tmp_path):
    suite_path = _write_suite(
        tmp_path,
        "row_count_suite",
        [
            {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 10}},
        ],
    )
    engine = ExpectationEngineAdapter()

    below = engine.evaluate_suite(
        [{"id": i} for i in range(9)],
        suite_path,
        threshold=1.0,
        table_name="silver.sales.orders",
    )
    assert below.passed is False
    assert below.pass_rate == 0.0
    assert below.table_name == "silver.sales.orders"
    assert below.suite_name == "row_count_suite"
    assert below.failures
    assert below.failures[0].rule_name == "expect_table_row_count_to_be_between"

    exact = engine.evaluate_suite(
        [{"id": i} for i in range(10)],
        suite_path,
        threshold=1.0,
        table_name="silver.sales.orders",
    )
    assert exact.passed is True
    assert exact.pass_rate == 1.0


def test_unknown_expectation_type_fails(tmp_path):
    suite_path = _write_suite(
        tmp_path,
        "unknown_suite",
        [
            {"expectation_type": "expect_column_values_to_be_zip_code", "kwargs": {"column": "zip"}},
        ],
    )
    engine = ExpectationEngineAdapter()

    # threshold=0.0 would pass on arithmetic alone: fail-closed must override it.
    evaluation = engine.evaluate_suite(
        [{"zip": "12345"}],
        suite_path,
        threshold=0.0,
        table_name="silver.sales.orders",
    )
    assert evaluation.passed is False
    assert evaluation.failures
    failure = evaluation.failures[0]
    assert failure.success is False
    assert failure.observed_value == "Unsupported expectation type: expect_column_values_to_be_zip_code"
    assert "cannot verify" in failure.details


def test_empty_records_fail_closed(tmp_path):
    suite_path = _write_suite(
        tmp_path,
        "empty_suite",
        [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
        ],
    )
    engine = ExpectationEngineAdapter()

    evaluation = engine.evaluate_suite([], suite_path, threshold=1.0, table_name="silver.sales.orders")
    assert evaluation.passed is False
    assert evaluation.pass_rate == 0.0
    assert evaluation.failures
    assert "no data / no assertions to evaluate" in evaluation.failures[0].details


def test_missing_suite_raises(tmp_path):
    engine = ExpectationEngineAdapter()
    missing_path = str(tmp_path / "does_not_exist_suite.json")

    with pytest.raises(SuiteNotFoundError):
        engine.evaluate_suite([{"id": 1}], missing_path, threshold=0.95, table_name="silver.sales.orders")


def test_cleanse_silver_blocks_on_breach(tmp_path):
    suite_path = _write_suite(
        tmp_path,
        "silver_orders_breach_suite",
        [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "status"}},
        ],
    )
    # 20 bronze rows with 3 null statuses => 15% nulls exceeds the 5% tolerated
    # by threshold=0.95, so the single expectation fails and the gate must block.
    rows = [
        {"id": i, "status": None if i < 3 else "confirmed", "total_amount": 10.0, "_batch_id": "b1"} for i in range(20)
    ]
    storage = StubStorage(tables={"bronze.sales.orders": rows})
    lineage = StubLineage()
    use_case = CleanseSilverUseCase(
        storage_reader=storage,
        storage_writer=storage,
        quality_engine=ExpectationEngineAdapter(),
        lineage=lineage,
        catalog=EmptyContractCatalog(),
    )

    with pytest.raises(QualityGateBreachError) as exc_info:
        use_case.execute("orders", quality_threshold=0.95, suite_path=suite_path)

    assert exc_info.value.pass_rate == 0.0
    assert exc_info.value.threshold == 0.95
    assert storage.writes == []  # nothing promoted to Silver
    assert len(lineage.events) == 1
    event = lineage.events[0]
    assert event["status"] == "FAIL"
    assert event["job_name"] == "silver_clean_orders"
    assert event["inputs"] == ["bronze.sales.orders"]
    assert event["outputs"] == ["silver.sales.orders"]


def test_cleanse_silver_missing_suite_raises(tmp_path):
    storage = StubStorage(tables={"bronze.sales.orders": [{"id": 1, "status": "confirmed"}]})
    lineage = StubLineage()
    use_case = CleanseSilverUseCase(
        storage_reader=storage,
        storage_writer=storage,
        quality_engine=ExpectationEngineAdapter(),
        lineage=lineage,
        catalog=EmptyContractCatalog(),
    )

    with pytest.raises(SuiteNotFoundError):
        use_case.execute("orders", suite_path=str(tmp_path / "missing_suite.json"))

    assert storage.writes == []
    assert lineage.events == []


def test_run_quality_gate_empty_table_fails(tmp_path):
    suite_path = _write_suite(
        tmp_path,
        "silver_orders_empty_suite",
        [
            {"expectation_type": "expect_table_row_count_to_be_between", "kwargs": {"min_value": 1}},
        ],
    )
    use_case = RunQualityGateUseCase(
        storage=StubStorage(),  # table absent -> read_table returns []
        quality_engine=ExpectationEngineAdapter(),
        catalog=EmptyContractCatalog(),
    )

    # Fail-closed: a failing verdict raises carrying the evaluation.
    with pytest.raises(QualityGateBreachError) as exc_info:
        use_case.execute("silver.sales.orders", suite_path, threshold=0.95)
    evaluation = exc_info.value.evaluation
    assert evaluation is not None
    assert evaluation.passed is False
    assert evaluation.pass_rate == 0.0
    assert evaluation.table_name == "silver.sales.orders"
    assert evaluation.failures
    assert "no data / no assertions to evaluate" in evaluation.failures[0].details


def test_run_quality_gate_records_override(tmp_path):
    suite_path = _write_suite(
        tmp_path,
        "silver_records_suite",
        [
            {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
        ],
    )
    use_case = RunQualityGateUseCase(
        storage=StubStorage(),  # storage is empty: records= must win
        quality_engine=ExpectationEngineAdapter(),
        catalog=EmptyContractCatalog(),
    )

    records = [{"id": i} for i in range(10)]
    evaluation = use_case.execute("silver.sales.orders", suite_path, threshold=1.0, records=records)
    assert evaluation.passed is True
    assert evaluation.table_name == "silver.sales.orders"
