"""Modern Data Architecture POC - Marquez OpenLineage Adapter.

Implements LineagePublisherPort, emitting OpenLineage events to Marquez
and active metadata webhook listeners.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from typing import Any

import requests

from src.adapters._files import ensure_parent_dir
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
        ensure_parent_dir(audit_file_path)

    def emit_job_event(
        self,
        job_name: str,
        inputs: list[str],
        outputs: list[str],
        status: str = "COMPLETE",
    ) -> bool:
        """Emits an OpenLineage run event.

        The local JSONL audit trail is the source of truth. Returns False only
        when neither the audit trail nor Marquez accepted the event.
        """
        now_iso = datetime.now(UTC).isoformat()
        run_id = uuid.uuid4().hex
        payload: dict[str, Any] = {
            "eventType": status,
            "eventTime": now_iso,
            "producer": "poc_hexagonal_pipeline",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
            "run": {
                "runId": run_id,
            },
            "job": {
                "namespace": "modern_lakehouse",
                "name": job_name,
            },
            "inputs": [{"namespace": "lakehouse", "name": inp} for inp in inputs],
            "outputs": [{"namespace": "lakehouse", "name": out} for out in outputs],
        }

        # 1. Write to the local lineage audit log
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

        # 2. Post to Marquez (best effort)
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
