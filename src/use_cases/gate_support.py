"""Modern Data Architecture POC - Shared fail-closed gate enforcement.

Used by Silver and Gold so both apply identical promotion rules: resolve
contract thresholds, evaluate, emit FAIL lineage and raise on breach.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

from src.domain.contracts import extract_quality_thresholds
from src.domain.exceptions import QualityGateBreachError
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD, assess_gate
from src.ports.outbound import ContractCatalogPort, LineagePublisherPort, QualityEnginePort

logger = logging.getLogger("use_case.gate_support")


def find_contract_thresholds(
    catalog: ContractCatalogPort | None,
    entity: str,
) -> dict[str, float] | None:
    """Returns the quality thresholds of the contract governing ``entity``.

    Returns None when no catalog, no matching contract, or no declared thresholds.
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


def enforce_quality_gate(
    *,
    quality_engine: QualityEnginePort,
    lineage: LineagePublisherPort,
    catalog: ContractCatalogPort | None,
    records: list[dict[str, Any]],
    suite_path: str,
    table_name: str,
    job_name: str,
    inputs: list[str],
    entity: str,
    quality_threshold: float | None = None,
) -> float:
    """Enforces the shared Silver/Gold promotion policy; returns the pass rate.

    Contract thresholds govern per dimension; the explicit threshold is a fallback.
    """
    contract_thresholds = find_contract_thresholds(catalog, entity)
    if contract_thresholds is not None and quality_threshold is not None:
        logger.info(
            "contract thresholds take precedence over explicit quality_threshold=%.2f for '%s'",
            quality_threshold,
            table_name,
        )
    effective_threshold = quality_threshold if quality_threshold is not None else DEFAULT_QUALITY_THRESHOLD
    logger.info(
        "Evaluating Quality Gate suite '%s' (threshold=%.2f)...",
        suite_path,
        effective_threshold,
    )
    evaluation = quality_engine.evaluate_suite(
        records=records,
        suite_path=suite_path,
        threshold=effective_threshold,
        table_name=table_name,
    )
    verdict = assess_gate(evaluation, contract_thresholds, effective_threshold)
    if not verdict.approved:
        logger.error(
            "Quality gate FAILED for '%s'! %s. Blocking promotion.",
            table_name,
            verdict.reason,
        )
        # Owner notification trail: record the FAILED run before raising.
        lineage.emit_job_event(
            job_name=job_name,
            inputs=list(inputs),
            outputs=[table_name],
            status="FAIL",
        )
        raise QualityGateBreachError(
            f"Quality gate breached for {table_name} ({verdict.reason})",
            pass_rate=verdict.rate if verdict.rate is not None else evaluation.pass_rate,
            threshold=verdict.required if verdict.required is not None else effective_threshold,
            # Breach carries the FAILED verdict (``passed=False``).
            evaluation=replace(evaluation, passed=False),
        )
    logger.info("Quality gate PASSED for '%s' (%s)", table_name, verdict.reason)
    return evaluation.pass_rate
