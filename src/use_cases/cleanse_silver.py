"""
Modern Data Architecture POC - Silver Layer Cleansing & Quality Gate Use Case.
Applies deduplication, type casting, quality gate assertion checks,
and promotes validated records into Silver lakehouse storage.
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.naming import bronze_table, silver_table
from src.ports.inbound import CleanseSilverPort, CleanseSilverResult
from src.ports.outbound import (
    ContractCatalogPort,
    LakehouseTableReader,
    LakehouseTableWriter,
    LineagePublisherPort,
    QualityEnginePort,
)
from src.use_cases.gate_support import enforce_quality_gate

logger = logging.getLogger("use_case.cleanse_silver")


class CleanseSilverUseCase(CleanseSilverPort):
    """Orchestrates Bronze-to-Silver cleansing, deduplication, and quality gate enforcement."""

    def __init__(
        self,
        storage_reader: LakehouseTableReader,
        storage_writer: LakehouseTableWriter,
        quality_engine: QualityEnginePort,
        lineage: LineagePublisherPort,
        catalog: ContractCatalogPort,
    ) -> None:
        self._reader = storage_reader
        self._writer = storage_writer
        self._quality = quality_engine
        self._lineage = lineage
        self._catalog = catalog

    def execute(
        self,
        entity: str,
        suite_path: str,
        quality_threshold: float | None = None,
    ) -> CleanseSilverResult:
        source_table = bronze_table(entity)
        target_table = silver_table(entity)
        logger.info("Starting Silver transformation from '%s' to '%s'", source_table, target_table)

        # 1. Read raw records from Bronze
        raw_records = self._reader.read_table(source_table)
        if not raw_records:
            logger.warning("No records found in '%s'. Skipping promotion.", source_table)
            # The EMPTY shape mirrors the SUCCESS contract (target_table and
            # quality_pass_rate included): drivers index those keys directly.
            return {
                "entity": entity,
                "target_table": target_table,
                "rows_promoted": 0,
                "quality_pass_rate": 0.0,
                "status": "EMPTY",
            }

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
        #    Threshold resolution, lineage emission and breach raising live in
        #    the shared gate_support collaborator (single policy for Silver/Gold).
        quality_pass_rate = enforce_quality_gate(
            quality_engine=self._quality,
            lineage=self._lineage,
            catalog=self._catalog,
            records=deduped,
            suite_path=suite_path,
            table_name=target_table,
            job_name=f"silver_clean_{entity}",
            inputs=[source_table],
            entity=entity,
            quality_threshold=quality_threshold,
        )
        logger.info("Quality gate PASSED (Pass rate: %.2f)", quality_pass_rate)

        # 4. Write conformed records to Silver tier
        written_count = self._writer.write_table(target_table, deduped, mode="overwrite")
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
            "quality_pass_rate": quality_pass_rate,
            "status": "SUCCESS",
        }
