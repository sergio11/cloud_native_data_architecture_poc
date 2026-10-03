"""Unit tests: standalone quality gate honors contract thresholds (W1).

Regression for the split-brain policy: ``RunQualityGateUseCase`` used to apply
only the passed/global threshold while Silver/Gold promotion
(``gate_support.enforce_quality_gate``) enforced the contract thresholds, so
the same table could PASS the standalone gate and FAIL promotion. The use case
now resolves the governing contract through ``ContractCatalogPort`` and assesses
the verdict with the same domain policy (``assess_gate``).

Scenario pinned here: 50 completeness assertions, 48 of which pass, so the
overall pass rate is 0.96 (>= a 0.95 fallback threshold) while the measured
completeness dimension is also 0.96 (< the contract's 0.98).
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.domain.exceptions import QualityGateBreachError
from src.use_cases.run_quality_gate import RunQualityGateUseCase

_TOTAL_COLUMNS = 50
_NULL_COLUMNS = 2  # col48/col49 hold a single null -> those assertions fail
_RECORDS = 100
_ORDERS_CONTRACT_PATH = "domains/sales/data-products/orders/contract.yaml"


class _StubStorage:
    """LakehouseTableReader stub: records= must short-circuit storage reads."""

    def read_table(self, table_name: str, limit: int | None = None, version: int | None = None) -> list[dict[str, Any]]:
        raise AssertionError("records= override must be used; storage must not be read")


class _StubCatalog:
    """ContractCatalogPort stub serving canned (path, contract) pairs."""

    def __init__(self, contracts: list[tuple[str, dict[str, Any]]] | None = None) -> None:
        self.contracts = contracts or []

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        return list(self.contracts)


def _records() -> list[dict[str, Any]]:
    """100 rows x 50 columns; the last two columns hold one null each."""
    rows: list[dict[str, Any]] = []
    for i in range(_RECORDS):
        row: dict[str, Any] = {}
        for c in range(_TOTAL_COLUMNS):
            nulls_here = c >= _TOTAL_COLUMNS - _NULL_COLUMNS and i == 0
            row[f"col{c}"] = None if nulls_here else f"v{i}-{c}"
        rows.append(row)
    return rows


def _suite(tmp_path, name: str) -> str:
    expectations = [
        {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": f"col{c}"}}
        for c in range(_TOTAL_COLUMNS)
    ]
    suite_file = tmp_path / f"{name}.json"
    suite_file.write_text(
        json.dumps({"expectation_suite_name": name, "expectations": expectations}),
        encoding="utf-8",
    )
    return str(suite_file)


def test_contract_thresholds_override_the_fallback_threshold(tmp_path) -> None:
    """Contract completeness 0.98 vs measured 0.96 -> FAIL despite pass rate 0.96."""
    suite_path = _suite(tmp_path, "w1_completeness_suite")
    catalog = _StubCatalog(
        [
            (
                _ORDERS_CONTRACT_PATH,
                {"spec": {"quality": {"thresholds": {"completeness": 0.98}}}},
            )
        ]
    )
    use_case = RunQualityGateUseCase(
        storage=_StubStorage(),
        quality_engine=ExpectationEngineAdapter(),
        catalog=catalog,
    )

    # Old behaviour: threshold-only arithmetic (0.96 >= 0.95) PASSED here while
    # promotion failed the same table. The contract must govern.
    with pytest.raises(QualityGateBreachError) as exc_info:
        use_case.execute(
            "silver.sales.orders",
            suite_path,
            threshold=0.95,
            records=_records(),
        )

    breach = exc_info.value
    assert breach.evaluation is not None
    # The breach carries the FAILED verdict (QA regression: a contract-dimension
    # breach used to attach an evaluation with passed=True because the engine's
    # global-threshold arithmetic was fine — the CLI then exited 0 / printed
    # PASSED while the contract blocked the gate).
    assert breach.evaluation.passed is False
    assert breach.evaluation.pass_rate == 0.96
    assert breach.evaluation.dimension_rates["completeness"] == 0.96
    assert breach.threshold == 0.98
    assert "completeness" in str(breach)


def test_fallback_threshold_applies_when_no_contract_matches(tmp_path) -> None:
    """No matching contract -> the passed threshold governs (pass rate 0.96 >= 0.95)."""
    suite_path = _suite(tmp_path, "w1_no_contract_suite")
    use_case = RunQualityGateUseCase(
        storage=_StubStorage(),
        quality_engine=ExpectationEngineAdapter(),
        catalog=_StubCatalog(),
    )

    evaluation = use_case.execute(
        "silver.sales.orders",
        suite_path,
        threshold=0.95,
        records=_records(),
    )

    assert evaluation.passed is True
    assert evaluation.pass_rate == 0.96


def test_fallback_threshold_still_blocks_below_rate(tmp_path) -> None:
    """Without a contract the fallback threshold is enforced exactly as before."""
    suite_path = _suite(tmp_path, "w1_below_fallback_suite")
    use_case = RunQualityGateUseCase(
        storage=_StubStorage(),
        quality_engine=ExpectationEngineAdapter(),
        catalog=_StubCatalog(),
    )

    with pytest.raises(QualityGateBreachError) as exc_info:
        use_case.execute(
            "silver.sales.orders",
            suite_path,
            threshold=0.97,
            records=_records(),
        )

    assert exc_info.value.evaluation is not None
    assert exc_info.value.evaluation.pass_rate == 0.96
    assert exc_info.value.threshold == 0.97


def test_gold_product_table_resolves_the_orders_contract(tmp_path) -> None:
    """gold.sales.daily_metrics is governed by the orders contract (GOLD_PRODUCTS)."""
    suite_path = _suite(tmp_path, "w1_gold_suite")
    catalog = _StubCatalog(
        [
            (
                _ORDERS_CONTRACT_PATH,
                {"spec": {"quality": {"thresholds": {"completeness": 0.98}}}},
            )
        ]
    )
    use_case = RunQualityGateUseCase(
        storage=_StubStorage(),
        quality_engine=ExpectationEngineAdapter(),
        catalog=catalog,
    )

    with pytest.raises(QualityGateBreachError) as exc_info:
        use_case.execute(
            "gold.sales.daily_metrics",
            suite_path,
            threshold=0.95,
            records=_records(),
        )

    assert exc_info.value.threshold == 0.98
    assert "completeness" in str(exc_info.value)
