"""
Modern Data Architecture POC - Quality Gate Runner Use Case.
Evaluates data quality assertion suites against lakehouse datasets
and enforces quality thresholds before promotion.
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.entities import QualityGateEvaluation
from src.ports.inbound import RunQualityGatePort
from src.ports.outbound import LakehouseStoragePort, QualityEnginePort

logger = logging.getLogger("use_case.run_quality_gate")


class RunQualityGateUseCase(RunQualityGatePort):
    """Orchestrates quality assertion execution and pass/fail enforcement."""

    def __init__(
        self,
        storage: LakehouseStoragePort,
        quality_engine: QualityEnginePort,
    ) -> None:
        self._storage = storage
        self._quality = quality_engine

    def execute(
        self,
        table_name: str,
        suite_path: str,
        threshold: float = 0.95,
        records: list[dict[str, Any]] | None = None,
    ) -> QualityGateEvaluation:
        logger.info(
            "Executing Quality Gate on '%s' with suite '%s' (threshold=%.2f)...",
            table_name, suite_path, threshold,
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

        # 2. Evaluate expectations against records
        result = self._quality.evaluate_suite(
            records=records,
            suite_path=suite_path,
            threshold=threshold,
            table_name=table_name,
        )

        logger.info(
            "Quality Gate on '%s': %d/%d assertions passed (Pass rate: %.2f | Threshold: %.2f) -> %s",
            table_name,
            result.passed_assertions,
            result.total_assertions,
            result.pass_rate,
            threshold,
            "PASS" if result.passed else "FAIL",
        )
        return result
