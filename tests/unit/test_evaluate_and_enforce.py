"""
Modern Data Architecture POC - Unit Tests: shared fail-closed gate enforcement.

Direct regression tests for the gate collaboration (RF-06): the pure domain
policy (:func:`src.domain.quality_gate.assess_gate`) and the shared
application collaborator (:func:`src.use_cases.gate_support.enforce_quality_gate`)
used by the Silver and Gold promotion paths. In particular these tests pin the
engine fail-closed veto: an unsupported expectation type must block promotion
even when the declared contract dimensions all pass and regardless of the
global pass-rate arithmetic.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.domain.entities import QualityGateEvaluation
from src.domain.exceptions import QualityGateBreachError
from src.domain.quality_gate import assess_gate
from src.use_cases.gate_support import enforce_quality_gate, find_contract_thresholds


class StubEngine:
    """QualityEnginePort stub returning a canned evaluation."""

    def __init__(self, evaluation: QualityGateEvaluation) -> None:
        self.evaluation = evaluation
        self.calls: list[dict[str, Any]] = []

    def evaluate_suite(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        threshold: float = 0.95,
        table_name: str = "",
    ) -> QualityGateEvaluation:
        self.calls.append({"threshold": threshold, "table_name": table_name})
        return self.evaluation


class StubLineage:
    """LineagePublisherPort stub recording emitted events."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit_job_event(
        self,
        job_name: str,
        inputs: list[str],
        outputs: list[str],
        status: str = "COMPLETE",
    ) -> bool:
        self.events.append({"job": job_name, "status": status})
        return True


class StubCatalog:
    """ContractCatalogPort stub serving canned (path, contract) pairs."""

    def __init__(self, contracts: list[tuple[str, dict[str, Any]]] | None = None) -> None:
        self.contracts = contracts or []

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        return list(self.contracts)


def _evaluation(
    *,
    passed: bool,
    pass_rate: float,
    unsupported: bool = False,
    dimension_rates: dict[str, float] | None = None,
) -> QualityGateEvaluation:
    return QualityGateEvaluation(
        suite_name="suite",
        table_name="silver.sales.orders",
        total_assertions=20,
        passed_assertions=int(20 * pass_rate),
        pass_rate=pass_rate,
        threshold=0.95,
        passed=passed,
        failures=[],
        dimension_rates=dimension_rates or {},
        unsupported=unsupported,
    )


_ORDERS_CONTRACT = (
    "domains/sales/data-products/orders/contract.yaml",
    {"spec": {"quality": {"thresholds": {"completeness": 0.99}}}},
)


def _enforce(evaluation: QualityGateEvaluation) -> tuple[float, StubEngine, StubLineage]:
    engine = StubEngine(evaluation)
    lineage = StubLineage()
    pass_rate = enforce_quality_gate(
        quality_engine=engine,
        lineage=lineage,
        catalog=StubCatalog(),
        records=[{"id": 1}],
        suite_path="suite.json",
        table_name="silver.sales.orders",
        job_name="job",
        inputs=["bronze.sales.orders"],
        entity="orders",
        quality_threshold=None,
    )
    return pass_rate, engine, lineage


# ---------------------------------------------------------------------------
# Pure domain policy: assess_gate (no I/O, no lineage, no raising)
# ---------------------------------------------------------------------------


def test_assess_veto_rejects_despite_passing_contract_dims() -> None:
    verdict = assess_gate(
        _evaluation(passed=False, pass_rate=0.90, unsupported=True, dimension_rates={"completeness": 1.0}),
        {"completeness": 0.99},
        0.95,
    )
    assert verdict.approved is False
    assert "veto" in verdict.reason


def test_assess_contract_dims_govern_over_global_arithmetic() -> None:
    verdict = assess_gate(
        _evaluation(passed=False, pass_rate=0.90, unsupported=False, dimension_rates={"completeness": 1.0}),
        {"completeness": 0.99},
        0.95,
    )
    assert verdict.approved is True


def test_assess_dimension_breach_names_dimension() -> None:
    verdict = assess_gate(
        _evaluation(passed=True, pass_rate=1.0, unsupported=False, dimension_rates={"completeness": 0.50}),
        {"completeness": 0.99},
        0.95,
    )
    assert verdict.approved is False
    assert verdict.dimension == "completeness"
    assert verdict.rate == 0.50
    assert verdict.required == 0.99


def test_assess_global_fallback() -> None:
    assert assess_gate(_evaluation(passed=False, pass_rate=0.80), None, 0.95).approved is False
    assert assess_gate(_evaluation(passed=True, pass_rate=1.0), None, 0.95).approved is True


# ---------------------------------------------------------------------------
# Application collaborator: enforce_quality_gate (engine + lineage + raise)
# ---------------------------------------------------------------------------


def test_veto_blocks_when_pass_rate_below_threshold_with_contract_dims_passing() -> None:
    """Regression: veto + pass_rate < threshold + contract dims passing must BLOCK.

    Before the fix the ``pass_rate < threshold`` heuristic silently approved
    this case (no exception, no FAIL lineage event).
    """
    evaluation = _evaluation(
        passed=False,
        pass_rate=0.90,
        unsupported=True,
        dimension_rates={"completeness": 1.0},
    )
    engine = StubEngine(evaluation)
    lineage = StubLineage()
    with pytest.raises(QualityGateBreachError, match="fail-closed veto"):
        enforce_quality_gate(
            quality_engine=engine,
            lineage=lineage,
            catalog=StubCatalog([_ORDERS_CONTRACT]),
            records=[{"id": 1}],
            suite_path="suite.json",
            table_name="silver.sales.orders",
            job_name="job",
            inputs=["bronze.sales.orders"],
            entity="orders",
            quality_threshold=None,
        )
    assert [e["status"] for e in lineage.events] == ["FAIL"]


def test_veto_blocks_when_pass_rate_above_threshold() -> None:
    evaluation = _evaluation(
        passed=False,
        pass_rate=0.96,
        unsupported=True,
        dimension_rates={"completeness": 1.0},
    )
    engine = StubEngine(evaluation)
    lineage = StubLineage()
    with pytest.raises(QualityGateBreachError, match="fail-closed veto"):
        enforce_quality_gate(
            quality_engine=engine,
            lineage=lineage,
            catalog=StubCatalog([_ORDERS_CONTRACT]),
            records=[{"id": 1}],
            suite_path="suite.json",
            table_name="silver.sales.orders",
            job_name="job",
            inputs=["bronze.sales.orders"],
            entity="orders",
            quality_threshold=None,
        )
    assert [e["status"] for e in lineage.events] == ["FAIL"]


def test_dimension_breach_blocks_and_reports_dimension() -> None:
    evaluation = _evaluation(
        passed=True,
        pass_rate=1.0,
        unsupported=False,
        dimension_rates={"completeness": 0.50},
    )
    engine = StubEngine(evaluation)
    lineage = StubLineage()
    with pytest.raises(QualityGateBreachError) as exc_info:
        enforce_quality_gate(
            quality_engine=engine,
            lineage=lineage,
            catalog=StubCatalog([_ORDERS_CONTRACT]),
            records=[{"id": 1}],
            suite_path="suite.json",
            table_name="silver.sales.orders",
            job_name="job",
            inputs=["bronze.sales.orders"],
            entity="orders",
            quality_threshold=None,
        )
    assert "completeness" in str(exc_info.value)
    assert exc_info.value.pass_rate == 0.50
    assert exc_info.value.threshold == 0.99
    assert [e["status"] for e in lineage.events] == ["FAIL"]


def test_approved_run_returns_pass_rate_without_lineage() -> None:
    evaluation = _evaluation(passed=True, pass_rate=1.0)
    pass_rate, engine, lineage = _enforce(evaluation)
    assert pass_rate == 1.0
    assert engine.calls[0]["threshold"] == 0.95
    assert lineage.events == []


def test_explicit_threshold_used_when_contract_silent() -> None:
    evaluation = _evaluation(passed=True, pass_rate=1.0)
    engine = StubEngine(evaluation)
    lineage = StubLineage()
    pass_rate = enforce_quality_gate(
        quality_engine=engine,
        lineage=lineage,
        catalog=StubCatalog(),
        records=[{"id": 1}],
        suite_path="suite.json",
        table_name="silver.sales.orders",
        job_name="job",
        inputs=["bronze.sales.orders"],
        entity="orders",
        quality_threshold=0.80,
    )
    assert pass_rate == 1.0
    assert engine.calls[0]["threshold"] == 0.80


def test_find_contract_thresholds_resolution() -> None:
    catalog = StubCatalog(
        [
            (
                "domains/sales/data-products/orders/contract.yaml",
                {"spec": {"quality": {"thresholds": {"completeness": 0.98}}}},
            ),
            (
                "domains/sales/data-products/customers/contract.yaml",
                {"spec": {"quality": {"thresholds": {"completeness": 0.90}}}},
            ),
        ]
    )
    assert find_contract_thresholds(catalog, "orders") == {"completeness": 0.98}
    assert find_contract_thresholds(catalog, "unknown") is None
    assert find_contract_thresholds(None, "orders") is None
