"""
Modern Data Architecture POC - Bronze Layer Raw Ingestion Use Case.
Extracts operational records, attaches audit technical columns,
and writes idempotently to Bronze tier lakehouse storage.

RF-01 append-only Bronze: records are always APPENDED (never overwritten), and
the storage layer deduplicates by content hash, so re-running ingestion over an
identical source snapshot inserts 0 new rows and does not duplicate existing
ones (SRS §8 audit retention keeps prior batches readable).
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
    """Maps the observed extraction origin to the Bronze provenance label.

    The label is stamped on the ``_source`` audit column AND used as the
    OpenLineage input dataset, so both always agree with where the records
    actually came from: ``postgresql.public.<entity>`` for rows queried from a
    live database, ``synthetic.local.<entity>`` for rows fabricated by the
    embedded demo generator (never the other way around).
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

        Append-only + content-hash idempotency (RF-01): the write always uses
        ``mode="append"`` and the storage adapter skips rows whose content hash
        already exists, so a re-run of the same snapshot returns
        ``rows_ingested == 0`` (``idempotent_hit=True``) instead of duplicating
        rows.
        """
        batch_id = str(uuid.uuid4())
        target_table = bronze_table(entity)
        logger.info("Starting Bronze ingestion for entity '%s' (batch_id=%s)", entity, batch_id)

        # 1. Extract raw records from OLTP source (``origin`` carries honest provenance)
        extraction = self._source.extract_table(entity)
        records = extraction.records
        source_label = provenance_label(extraction, entity)
        logger.info(
            "Extracted %d records for entity '%s' (origin=%s)",
            len(records),
            entity,
            extraction.origin,
        )

        # 1b. Honest empty ingestion: 0 source rows short-circuits to EMPTY
        # (no write) while still emitting lineage like every other run.
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

        # 3. Append to Bronze tier storage (RF-01 content-hash idempotent)
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
