"""
Modern Data Architecture POC - Marquez OpenLineage Adapter.
Implements LineagePublisherPort emitting OpenLineage standard JSON events
to Marquez and active metadata webhook listeners (RF-19).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

from src.ports.outbound import LineagePublisherPort

logger = logging.getLogger("adapter.lineage.marquez")


class MarquezLineageAdapter(LineagePublisherPort):
    """Emits OpenLineage JSON run events to Marquez and records local lineage audit trail."""

    def __init__(
        self,
        marquez_url: str | None = None,
        audit_file_path: str = "data/lineage_audit.jsonl",
    ) -> None:
        self._marquez_url: str = marquez_url or os.getenv("MARQUEZ_URL") or "http://localhost:5000"
        self._audit_file_path = audit_file_path
        Path(os.path.dirname(audit_file_path) or ".").mkdir(parents=True, exist_ok=True)

    def emit_job_event(
        self,
        job_name: str,
        inputs: list[str],
        outputs: list[str],
        status: str = "COMPLETE",
    ) -> bool:
        """Emits an OpenLineage run event.

        Precedence (SRS audit retention for this POC): the local JSONL audit
        trail is the source of truth. The method returns True when the local
        audit append succeeded - even if the Marquez HTTP POST failed (the
        failure is logged as a warning, never silenced). It returns False only
        when the local audit trail could not be written either.
        """
        now_iso = datetime.now(UTC).isoformat()
        payload: dict[str, Any] = {
            "eventType": status,
            "eventTime": now_iso,
            "producer": "poc_hexagonal_pipeline",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
            "job": {
                "namespace": "modern_lakehouse",
                "name": job_name,
            },
            "inputs": [{"namespace": "lakehouse", "name": inp} for inp in inputs],
            "outputs": [{"namespace": "lakehouse", "name": out} for out in outputs],
        }

        # 1. Write to local lineage audit log (source of truth for the POC)
        audit_ok = True
        try:
            with open(self._audit_file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload) + "\n")
        except Exception as exc:
            audit_ok = False
            logger.warning(
                "Failed to write local lineage audit trail to '%s': %s",
                self._audit_file_path,
                exc,
            )

        # 2. Post to Marquez API (best effort; local audit takes precedence)
        marquez_ok = False
        try:
            target_url = f"{self._marquez_url.rstrip('/')}/api/v1/lineage"
            resp = requests.post(target_url, json=payload, timeout=2)
            if resp.status_code in (200, 201):
                logger.info("OpenLineage event emitted to Marquez: job='%s' status=%d", job_name, resp.status_code)
                marquez_ok = True
            else:
                logger.warning(
                    "Marquez rejected OpenLineage event for job '%s': HTTP %d %s "
                    "(local audit trail remains the source of truth)",
                    job_name,
                    resp.status_code,
                    resp.text[:200],
                )
        except Exception as exc:
            logger.warning(
                "Marquez HTTP POST failed for job '%s': %s (local audit trail remains the source of truth)",
                job_name,
                exc,
            )

        if marquez_ok:
            return True
        if not audit_ok:
            return False

        logger.info("OpenLineage lineage recorded locally: %s -> %s", inputs, outputs)
        return True
