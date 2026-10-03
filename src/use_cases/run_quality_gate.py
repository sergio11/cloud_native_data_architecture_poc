"""
Modern Data Architecture POC - Quality Gate Runner Use Case.
Evaluates data quality assertion suites against lakehouse datasets
and enforces quality thresholds before promotion.

Threshold policy is IDENTICAL to the Silver/Gold promotion gate
(:func:`src.use_cases.gate_support.enforce_quality_gate`): contract-declared
per-dimension thresholds govern, and the caller's ``threshold`` only applies
when the governing contract declares none. The verdict comes from the shared
domain policy (:func:`src.domain.quality_gate.assess_gate`), so a table can
never PASS here and FAIL promotion (or vice versa) on the same records.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from typing import Any

from src.domain.entities import QualityGateEvaluation
from src.domain.exceptions import QualityGateBreachError
from src.domain.naming import contract_entity_for_table
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD, assess_gate
from src.ports.inbound import RunQualityGatePort
from src.ports.outbound import ContractCatalogPort, LakehouseTableReader, QualityEnginePort
from src.use_cases.gate_support import find_contract_thresholds

logger = logging.getLogger("use_case.run_quality_gate")


class RunQualityGateUseCase(RunQualityGatePort):
    """Orchestrates quality assertion execution and pass/fail enforcement."""

    def __init__(
        self,
        storage: LakehouseTableReader,
        quality_engine: QualityEnginePort,
        catalog: ContractCatalogPort,
    ) -> None:
        self._storage = storage
        self._quality = quality_engine
        self._catalog = catalog

    def execute(
        self,
        table_name: str,
        suite_path: str,
        threshold: float = DEFAULT_QUALITY_THRESHOLD,
        records: list[dict[str, Any]] | None = None,
    ) -> QualityGateEvaluation:
        logger.info(
            "Executing Quality Gate on '%s' with suite '%s' (threshold=%.2f)...",
            table_name,
            suite_path,
            threshold,
        )

        # 1. Records override (file-input scenarios) or load from lakehouse storage.
        #    An empty table is NOT short-circuited to passed=True: it is evaluated
        #    as [] so the engine's fail-closed rule (RF-06) produces passed=False.
        #    A missing suite propagates SuiteNotFoundError (fail-closed).
        if records is None:
            records = self._storage.read_table(table_name)
            if not records:
                logger.warning(
                    "Table '%s' contains no records; evaluating empty record set (fail-closed, RF-06).",
                    table_name,
                )

        # 2. Resolve the governing contract thresholds exactly like the promotion
        #    gate: contract thresholds win per dimension; ``threshold`` is only
        #    the fallback when no matching contract declares any.
        entity = contract_entity_for_table(table_name)
        contract_thresholds = find_contract_thresholds(self._catalog, entity)
        if contract_thresholds is not None:
            logger.info(
                "Contract thresholds %s govern '%s'; passed threshold=%.2f is the fallback only",
                contract_thresholds,
                table_name,
                threshold,
            )

        # 3. Evaluate expectations against records (engine arithmetic uses the
        #    fallback threshold; contract dimensions are applied by the policy).
        result = self._quality.evaluate_suite(
            records=records,
            suite_path=suite_path,
            threshold=threshold,
            table_name=table_name,
        )

        # 4. Shared policy verdict - same code path as Silver/Gold promotion.
        verdict = assess_gate(result, contract_thresholds, threshold)
        logger.info(
            "Quality Gate on '%s': %d/%d assertions passed (Pass rate: %.2f | Threshold: %.2f) -> %s (%s)",
            table_name,
            result.passed_assertions,
            result.total_assertions,
            result.pass_rate,
            threshold,
            "PASS" if verdict.approved else "FAIL",
            verdict.reason,
        )
        # 5. Fail closed (RF-06): a failing verdict raises with the evaluation
        #    attached, so programmatic callers cannot ignore a breach and CLIs can
        #    still render results, emit FAIL lineage and persist reports.
        if not verdict.approved:
            raise QualityGateBreachError(
                f"Quality gate breached for {table_name} ({verdict.reason})",
                pass_rate=verdict.rate if verdict.rate is not None else result.pass_rate,
                threshold=verdict.required if verdict.required is not None else threshold,
                evaluation=result,
            )
        if not result.passed:
            # Contract dimensions govern and all passed while the arithmetic
            # against the fallback threshold failed: the gate is approved, so
            # report a passing evaluation (the port promises callers only ever
            # receive passing evaluations back).
            logger.info(
                "Contract thresholds govern '%s': pass rate %.2f below fallback threshold %.2f "
                "but the contract dimensions approve (global threshold ignored).",
                table_name,
                result.pass_rate,
                threshold,
            )
            return replace(result, passed=True)
        return result
