"""
Modern Data Architecture POC - Shared fail-closed gate enforcement (application layer).

Single collaborator used by the Silver (``CleanseSilverUseCase``) and Gold
(``AggregateGoldUseCase``) tiers so both apply identical promotion rules
(RF-06): resolve the governing contract thresholds, evaluate the expectation
suite through the engine port, assess the verdict through the pure domain
policy (:func:`src.domain.quality_gate.assess_gate`), emit a FAIL lineage
event and raise ``QualityGateBreachError`` on breach — always BEFORE any
write for the target. Both use cases stay thin orchestrators delegating here.

This module owns every application concern the pure domain policy must not:
OpenLineage emission, ``QualityGateBreachError`` raising, and the
``data-products/<entity>/`` contract-location convention (resolved through
the catalog port, never by the domain).
"""

from __future__ import annotations

import logging
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
    """Enforces the shared Silver/Gold promotion policy, returning the approved pass rate.

    Contract thresholds (``data-products/<entity>/``) govern per dimension and
    ignore the global threshold; the explicit threshold (or its
    ``DEFAULT_QUALITY_THRESHOLD`` default) is only a fallback when the
    contract is silent.
    """
    contract_thresholds = find_contract_thresholds(catalog, entity)
    if contract_thresholds is not None and quality_threshold is not None:
        logger.info(
            "contract thresholds take precedence over explicit quality_threshold=%.2f for '%s'",
            quality_threshold, table_name,
        )
    effective_threshold = quality_threshold if quality_threshold is not None else DEFAULT_QUALITY_THRESHOLD
    logger.info(
        "Evaluating Quality Gate suite '%s' (threshold=%.2f)...",
        suite_path, effective_threshold,
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
            table_name, verdict.reason,
        )
        # RF-06 owner notification trail: record the FAILED run before raising.
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
        )
    logger.info("Quality gate PASSED for '%s' (%s)", table_name, verdict.reason)
    return evaluation.pass_rate
