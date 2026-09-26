"""
Modern Data Architecture POC - Silver Layer Cleansing & Quality Gate Use Case.
Applies deduplication, type casting, quality gate assertion checks,
and promotes validated records into Silver lakehouse storage.
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.entities import QualityGateEvaluation
from src.domain.exceptions import QualityGateBreachError
from src.ports.outbound import LakehouseStoragePort, LineagePublisherPort, QualityEnginePort

logger = logging.getLogger("use_case.cleanse_silver")


class CleanseSilverUseCase:
    """Orchestrates Bronze-to-Silver cleansing, deduplication, and quality gate enforcement."""

    def __init__(
        self,
        storage: LakehouseStoragePort,
        quality_engine: QualityEnginePort,
        lineage: LineagePublisherPort,
    ) -> None:
        self._storage = storage
        self._quality = quality_engine
        self._lineage = lineage

    def execute(
        self,
        entity: str,
        quality_threshold: float = 0.95,
        suite_path: str | None = None,
    ) -> dict[str, Any]:
        source_table = f"bronze.sales.{entity}"
        target_table = f"silver.sales.{entity}"
        logger.info("Starting Silver transformation from '%s' to '%s'", source_table, target_table)

        # 1. Read raw records from Bronze
        raw_records = self._storage.read_table(source_table)
        if not raw_records:
            logger.warning("No records found in '%s'. Skipping promotion.", source_table)
            return {"entity": entity, "rows_promoted": 0, "status": "EMPTY"}

        # 2. Deduplicate records based on primary key ('id')
        seen_ids = set()
        deduped: list[dict[str, Any]] = []
        for r in raw_records:
            record_id = r.get("id")
            if record_id is not None and record_id not in seen_ids:
                seen_ids.add(record_id)
                # Filter out internal audit columns
                clean_row = {k: v for k, v in r.items() if not k.startswith("_")}
                deduped.append(clean_row)

        logger.info("Deduplication: %d raw -> %d unique records", len(raw_records), len(deduped))

        # 3. Quality Gate Enforcement (RF-06, fail-closed: a missing suite raises
        #    SuiteNotFoundError which must propagate - never a silent pass).
        actual_suite_path = suite_path or f"quality/expectations/silver_{entity}_suite.json"
        logger.info(
            "Evaluating Quality Gate suite '%s' (threshold=%.2f)...",
            actual_suite_path, quality_threshold
        )
        quality_result: QualityGateEvaluation = self._quality.evaluate_suite(
            records=deduped,
            suite_path=actual_suite_path,
            threshold=quality_threshold,
            table_name=target_table,
        )
        if not quality_result.passed:
            logger.error(
                "Quality gate FAILED for '%s'! Pass rate: %.2f < %.2f. Blocking promotion.",
                target_table, quality_result.pass_rate, quality_threshold
            )
            # RF-06 owner notification trail: record the FAILED run before raising.
            self._lineage.emit_job_event(
                job_name=f"silver_clean_{entity}",
                inputs=[source_table],
                outputs=[target_table],
                status="FAIL",
            )
            raise QualityGateBreachError(
                f"Quality gate breached for {target_table} "
                f"(Pass rate: {quality_result.pass_rate:.2f} < threshold {quality_threshold:.2f})",
                pass_rate=quality_result.pass_rate,
                threshold=quality_threshold,
            )
        logger.info("Quality gate PASSED (Pass rate: %.2f)", quality_result.pass_rate)

        # 4. Write conformed records to Silver tier
        written_count = self._storage.write_table(target_table, deduped, mode="overwrite")
        logger.info("Successfully wrote %d records to '%s'", written_count, target_table)

        # 5. Emit OpenLineage metadata
        self._lineage.emit_job_event(
            job_name=f"silver_clean_{entity}",
            inputs=[source_table],
            outputs=[target_table],
            status="COMPLETE",
        )

        return {
            "entity": entity,
            "target_table": target_table,
            "rows_promoted": written_count,
            "quality_pass_rate": quality_result.pass_rate,
            "status": "SUCCESS",
        }
