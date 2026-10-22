"""Modern Data Architecture POC - Quality gate runner use case.

Evaluates quality assertion suites against lakehouse datasets and enforces
the same threshold policy as Silver/Gold promotion (fail-closed).
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

        # 1. Use provided records or load from storage; an empty table is
        #    still evaluated so the engine fails closed.
        if records is None:
            records = self._storage.read_table(table_name)
            if not records:
                logger.warning(
                    "Table '%s' contains no records; evaluating empty record set (fail-closed, RF-06).",
                    table_name,
                )

        # 2. Resolve contract thresholds exactly like the promotion gate.
        entity = contract_entity_for_table(table_name)
        contract_thresholds = find_contract_thresholds(self._catalog, entity)
        if contract_thresholds is not None:
            logger.info(
                "Contract thresholds %s govern '%s'; passed threshold=%.2f is the fallback only",
                contract_thresholds,
                table_name,
                threshold,
            )

        # 3. Evaluate expectations (contract dimensions are applied by policy).
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
        # 5. Fail closed: raise with the FAILED evaluation attached so callers
        #    cannot ignore a breach.
        if not verdict.approved:
            raise QualityGateBreachError(
                f"Quality gate breached for {table_name} ({verdict.reason})",
                pass_rate=verdict.rate if verdict.rate is not None else result.pass_rate,
                threshold=verdict.required if verdict.required is not None else threshold,
                evaluation=replace(result, passed=False),
            )
        if not result.passed:
            # Contract dimensions approved despite the fallback arithmetic.
            logger.info(
                "Contract thresholds govern '%s': pass rate %.2f below fallback threshold %.2f "
                "but the contract dimensions approve (global threshold ignored).",
                table_name,
                result.pass_rate,
                threshold,
            )
            return replace(result, passed=True)
        return result
