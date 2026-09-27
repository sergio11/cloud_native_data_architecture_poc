"""
Modern Data Architecture POC - Silver Layer Cleansing & Quality Gate Use Case.
Applies deduplication, type casting, quality gate assertion checks,
and promotes validated records into Silver lakehouse storage.
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.naming import bronze_table, silver_table
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD, evaluate_and_enforce, find_contract_thresholds
from src.ports.inbound import CleanseSilverPort
from src.ports.outbound import DomainCatalogPort, LakehouseStoragePort, LineagePublisherPort, QualityEnginePort

logger = logging.getLogger("use_case.cleanse_silver")


class CleanseSilverUseCase(CleanseSilverPort):
    """Orchestrates Bronze-to-Silver cleansing, deduplication, and quality gate enforcement."""

    def __init__(
        self,
        storage: LakehouseStoragePort,
        quality_engine: QualityEnginePort,
        lineage: LineagePublisherPort,
        catalog: DomainCatalogPort | None = None,
    ) -> None:
        self._storage = storage
        self._quality = quality_engine
        self._lineage = lineage
        self._catalog = catalog

    def execute(
        self,
        entity: str,
        suite_path: str,
        quality_threshold: float | None = None,
    ) -> dict[str, Any]:
        source_table = bronze_table(entity)
        target_table = silver_table(entity)
        logger.info("Starting Silver transformation from '%s' to '%s'", source_table, target_table)

        # 1. Read raw records from Bronze
        raw_records = self._storage.read_table(source_table)
        if not raw_records:
            logger.warning("No records found in '%s'. Skipping promotion.", source_table)
            return {"entity": entity, "rows_promoted": 0, "status": "EMPTY"}

        # 2. Deduplicate records based on primary key ('id'):
        # latest snapshot wins (RF-01 periodic snapshots). read_table returns
        # rows ordered by rowid ASC (oldest first), so iterating in order and
        # letting each later occurrence overwrite keeps the most recent snapshot.
        latest: dict[object, dict[str, Any]] = {}
        for r in raw_records:
            record_id = r.get("id")
            if record_id is None:
                continue
            # Filter out internal audit columns
            clean_row = {k: v for k, v in r.items() if not k.startswith("_")}
            latest[record_id] = clean_row
        deduped: list[dict[str, Any]] = list(latest.values())

        logger.info("Deduplication: %d raw -> %d unique records", len(raw_records), len(deduped))

        # 3. Quality Gate Enforcement (RF-06, fail-closed: a missing suite raises
        #    SuiteNotFoundError which must propagate - never a silent pass).
        #    Hexagonal note: the core holds no filesystem conventions - the
        #    driver resolves the suite path per entity and passes it explicitly.
        if suite_path is None:
            raise ValueError(
                "suite_path is required: the driver must resolve the "
                "quality/expectations suite path for the entity and pass it explicitly."
            )
        # Contract thresholds (data-products/<entity>/) govern per dimension and
        # ignore the global threshold; the explicit threshold (or its 0.95
        # default) is only a fallback when the contract is silent.
        contract_thresholds = find_contract_thresholds(self._catalog, entity)
        if contract_thresholds is not None and quality_threshold is not None:
            logger.info(
                "contract thresholds take precedence over explicit quality_threshold=%.2f for '%s'",
                quality_threshold, target_table,
            )
        effective_threshold = quality_threshold if quality_threshold is not None else DEFAULT_QUALITY_THRESHOLD
        logger.info(
            "Evaluating Quality Gate suite '%s' (threshold=%.2f)...",
            suite_path, effective_threshold,
        )
        quality_result = evaluate_and_enforce(
            self._quality,
            self._lineage,
            deduped,
            suite_path,
            target_table,
            f"silver_clean_{entity}",
            [source_table],
            effective_threshold,
            contract_thresholds=contract_thresholds,
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
