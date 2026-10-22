"""Modern Data Architecture POC - Bronze raw ingestion use case.

Extracts operational records, adds audit columns and writes idempotently to
append-only Bronze storage.
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import Any

from src.domain.entities import ExtractionResult
from src.domain.naming import bronze_table
from src.ports.inbound import IngestBronzePort, IngestBronzeResult
from src.ports.outbound import LakehouseTableWriter, LineagePublisherPort, OltpSourcePort

logger = logging.getLogger("use_case.ingest_bronze")


def provenance_label(extraction: ExtractionResult, entity: str) -> str:
    """Maps extraction origin to the Bronze provenance label.

    Stamped on ``_source`` and used as the OpenLineage input dataset.
    """
    if extraction.origin == "postgresql":
        return f"postgresql.public.{entity}"
    return f"synthetic.local.{entity}"


class IngestBronzeUseCase(IngestBronzePort):
    """Orchestrates extraction of OLTP data into Bronze raw lakehouse storage."""

    def __init__(
        self,
        source: OltpSourcePort,
        storage: LakehouseTableWriter,
        lineage: LineagePublisherPort,
    ) -> None:
        self._source = source
        self._storage = storage
        self._lineage = lineage

    def execute(self, entity: str) -> IngestBronzeResult:
        """Ingests one OLTP snapshot into append-only Bronze storage.

        Content-hash idempotency means a re-run returns 0 rows.
        """
        batch_id = str(uuid.uuid4())
        target_table = bronze_table(entity)
        logger.info("Starting Bronze ingestion for entity '%s' (batch_id=%s)", entity, batch_id)

        # 1. Extract raw records (``origin`` carries honest provenance)
        extraction = self._source.extract_table(entity)
        records = extraction.records
        source_label = provenance_label(extraction, entity)
        logger.info(
            "Extracted %d records for entity '%s' (origin=%s)",
            len(records),
            entity,
            extraction.origin,
        )

        # 1b. Empty source: report EMPTY without writing, still emit lineage.
        if not records:
            logger.warning("Source table '%s' returned 0 rows; reporting EMPTY without writing.", entity)
            self._lineage.emit_job_event(
                job_name=f"bronze_ingest_{entity}",
                inputs=[source_label],
                outputs=[target_table],
                status="COMPLETE",
            )
            return {
                "entity": entity,
                "target_table": target_table,
                "batch_id": batch_id,
                "rows_ingested": 0,
                "idempotent_hit": False,
                "status": "EMPTY",
            }

        # 2. Append technical audit columns
        now_iso = datetime.now(UTC).isoformat()
        enriched_records: list[dict[str, Any]] = []
        for r in records:
            row = dict(r)
            row["_ingested_at"] = now_iso
            row["_source"] = source_label
            row["_batch_id"] = batch_id
            enriched_records.append(row)

        # 3. Append to Bronze tier storage (content-hash idempotent)
        count_written = self._storage.write_table(target_table, enriched_records, mode="append")
        idempotent_hit = count_written == 0 and len(records) > 0
        logger.info(
            "Persisted %d of %d records to '%s' (idempotent_hit=%s)",
            count_written,
            len(records),
            target_table,
            idempotent_hit,
        )

        # 4. Emit OpenLineage metadata
        self._lineage.emit_job_event(
            job_name=f"bronze_ingest_{entity}",
            inputs=[source_label],
            outputs=[target_table],
            status="COMPLETE",
        )

        return {
            "entity": entity,
            "target_table": target_table,
            "batch_id": batch_id,
            "rows_ingested": count_written,
            "idempotent_hit": idempotent_hit,
            "status": "SUCCESS",
        }
