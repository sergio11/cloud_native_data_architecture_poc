"""
Modern Data Architecture POC - Shared fail-closed quality gate enforcement.

Single collaborator used by the Silver (``CleanseSilverUseCase``) and Gold
(``AggregateGoldUseCase``) tiers so both apply identical promotion rules
(RF-06): evaluate the expectation suite, enforce the global pass-rate
threshold plus (when provided) per-dimension contract thresholds, emit a FAIL
lineage event and raise ``QualityGateBreachError`` on breach, otherwise return
the evaluation. Both use cases stay thin orchestrators delegating to
:func:`evaluate_and_enforce`.

Contract thresholds come from the agreed ``extract_quality_thresholds``
domain API; per-dimension pass ratios come from the evaluation's
``dimension_rates`` attribute filled by the engine.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

from src.domain.contracts import extract_quality_thresholds
from src.domain.entities import QualityGateEvaluation
from src.domain.exceptions import QualityGateBreachError

logger = logging.getLogger("domain.quality_gate")

#: Fallback global threshold when neither the caller nor a contract says otherwise.
DEFAULT_QUALITY_THRESHOLD = 0.95


class QualityEngineLike(Protocol):
    """Structural type for QualityEnginePort (keeps the domain free of port imports)."""

    def evaluate_suite(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        threshold: float = 0.95,
        table_name: str = "",
    ) -> QualityGateEvaluation:
        """Evaluates quality assertions on records against the suite specification."""
        ...


class LineagePublisherLike(Protocol):
    """Structural type for LineagePublisherPort (keeps the domain free of port imports)."""

    def emit_job_event(
        self,
        job_name: str,
        inputs: list[str],
        outputs: list[str],
        status: str = "COMPLETE",
    ) -> bool:
        """Publishes a standardized OpenLineage run event."""
        ...


class DomainCatalogLike(Protocol):
    """Structural type for DomainCatalogPort (keeps the domain free of port imports)."""

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (source_file_path, parsed contract.yaml) pairs, sorted by path."""
        ...


def find_contract_thresholds(
    catalog: DomainCatalogLike | None,
    entity: str,
) -> dict[str, float] | None:
    """Locates the contract governing ``entity`` and returns its quality thresholds.

    The contract is the ``list_contracts()`` entry whose path contains the
    ``data-products/<entity>/`` segment (first match in path order wins).
    Returns None when there is no catalog, no matching contract, or the
    contract declares no thresholds (caller falls back to explicit/default).
    """
    if catalog is None:
        return None
    segment = f"data-products/{entity}/"
    for path, data in catalog.list_contracts():
        if segment not in str(path).replace("\\", "/"):
            continue
        thresholds = extract_quality_thresholds(data)
        if thresholds is not None:
            logger.info("Contract thresholds %s apply for entity '%s' (contract %s)", thresholds, entity, path)
        return thresholds
    return None


def evaluate_and_enforce(
    quality_engine: QualityEngineLike,
    lineage: LineagePublisherLike,
    records: list[dict[str, Any]],
    suite_path: str,
    table_name: str,
    job_name: str,
    inputs: list[str],
    threshold: float,
    contract_thresholds: dict[str, float] | None = None,
    dimension_rates: dict[str, float] | None = None,
) -> QualityGateEvaluation:
    """Evaluates the suite and fail-closes on breach (shared Silver/Gold policy).

    A missing suite raises SuiteNotFoundError from the engine and propagates
    (never a silent pass). When ``contract_thresholds`` are declared and the
    suite measures at least one of those dimensions (via explicit
    ``dimension_rates`` or the evaluation's engine-filled ``dimension_rates``),
    the contract dimensions govern the verdict: any measured
    ``dimension_rates[d] < contract[d]`` emits a FAIL lineage event and raises
    ``QualityGateBreachError`` naming the dimension, ignoring the global
    threshold arithmetic (an engine fail-closed veto still blocks). Contract
    dimensions the suite does not measure are skipped - the engine only
    reports measured dimensions. When no declared dimension is measurable the
    global ``pass_rate >= threshold`` rule applies as a fallback. On breach a
    FAIL lineage event is emitted and ``QualityGateBreachError`` is raised
    BEFORE any write for the target. Returns the evaluation when it passes.
    """
    logger.info("Evaluating Quality Gate suite '%s' (threshold=%.2f)...", suite_path, threshold)
    evaluation: QualityGateEvaluation = quality_engine.evaluate_suite(
        records=records,
        suite_path=suite_path,
        threshold=threshold,
        table_name=table_name,
    )

    rates = dimension_rates if dimension_rates is not None else evaluation.dimension_rates

    if contract_thresholds:
        measurable = {dim: required for dim, required in contract_thresholds.items() if dim in rates}
        if not measurable:
            logger.warning(
                "Contract thresholds %s declared for '%s' but the suite measures none of those "
                "dimensions; falling back to the global threshold (%.2f).",
                contract_thresholds, table_name, threshold,
            )
        else:
            skipped = sorted(set(contract_thresholds) - set(measurable))
            if skipped:
                logger.info(
                    "Contract dimensions %s not measured by suite '%s'; enforcing %s for '%s'.",
                    skipped, suite_path, sorted(measurable), table_name,
                )
            for dimension, required in measurable.items():
                rate = rates[dimension]
                if rate < required:
                    logger.error(
                        "Quality gate FAILED for '%s' on dimension '%s'! Rate: %.4f < contract %.2f. "
                        "Blocking promotion.",
                        table_name, dimension, rate, required,
                    )
                    lineage.emit_job_event(
                        job_name=job_name,
                        inputs=list(inputs),
                        outputs=[table_name],
                        status="FAIL",
                    )
                    raise QualityGateBreachError(
                        f"Quality gate breached for {table_name} on dimension '{dimension}' "
                        f"(rate: {rate:.4f} < contract threshold {required:.2f})",
                        pass_rate=rate,
                        threshold=required,
                    )
            # Contract dimensions govern: the global arithmetic is ignored once
            # they all pass - unless the engine vetoed fail-closed (unsupported
            # expectation types still block even when the arithmetic would pass).
            threshold_failed = evaluation.pass_rate < threshold
            if not evaluation.passed and not threshold_failed:
                logger.error(
                    "Quality gate FAILED for '%s'! Engine fail-closed veto despite contract dimensions passing. "
                    "Blocking promotion.",
                    table_name,
                )
                lineage.emit_job_event(
                    job_name=job_name,
                    inputs=list(inputs),
                    outputs=[table_name],
                    status="FAIL",
                )
                raise QualityGateBreachError(
                    f"Quality gate breached for {table_name} "
                    f"(engine fail-closed veto; pass rate: {evaluation.pass_rate:.2f})",
                    pass_rate=evaluation.pass_rate,
                    threshold=threshold,
                )
            logger.info(
                "Quality gate PASSED for '%s' on contract dimensions %s (global threshold ignored).",
                table_name, sorted(measurable),
            )
            return evaluation

    if not evaluation.passed:
        logger.error(
            "Quality gate FAILED for '%s'! Pass rate: %.2f < %.2f. Blocking promotion.",
            table_name, evaluation.pass_rate, threshold,
        )
        # RF-06 owner notification trail: record the FAILED run before raising.
        lineage.emit_job_event(
            job_name=job_name,
            inputs=list(inputs),
            outputs=[table_name],
            status="FAIL",
        )
        raise QualityGateBreachError(
            f"Quality gate breached for {table_name} "
            f"(Pass rate: {evaluation.pass_rate:.2f} < threshold {threshold:.2f})",
            pass_rate=evaluation.pass_rate,
            threshold=threshold,
        )
    logger.info("Quality gate PASSED for '%s' (Pass rate: %.2f)", table_name, evaluation.pass_rate)
    return evaluation
