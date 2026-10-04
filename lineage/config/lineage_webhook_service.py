"""
lineage_webhook_service.py - Active Metadata & OpenLineage Webhook Microservice.

FastAPI microservice that:
1. Ingests OpenLineage RunEvents at /api/v1/lineage
2. Evaluates active metadata rules from alert-rules.yaml (RF-20):
   - Rule 1: Schema change alert on Silver/Gold tables
   - Rule 2: Quality threshold breach alert
   - Rule 3: Pipeline failure alert
   - Rule 4: SLA freshness breach alert
3. Persists events, datasets, schema history, and alerts in SQLite (with WAL mode).
4. Exposes REST endpoints for querying active alerts, dataset lineage graphs, and health metrics.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.request
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

# Try importing YAML, fallback to embedded parser if missing
try:
    import yaml
except ImportError:
    yaml = None

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("lineage_webhook_service")

# -----------------------------------------------------------------------------
# Configuration & Paths
# -----------------------------------------------------------------------------
DATABASE_PATH = os.getenv("LINEAGE_DB_PATH", "lineage_events.db")
ALERT_RULES_PATH = os.getenv(
    "ALERT_RULES_PATH",
    str(Path(__file__).parent / "alert-rules.yaml"),
)

# Outbound alert webhook delivery (best-effort; never blocks ingestion on failure).
WEBHOOK_DELIVERY_ENABLED = os.getenv("LINEAGE_WEBHOOK_DELIVERY", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
WEBHOOK_TIMEOUT_SECONDS = float(os.getenv("LINEAGE_WEBHOOK_TIMEOUT", "3"))

# CORS: explicit origins (comma-separated) enable credentials; otherwise the
# wildcard origin is used WITHOUT credentials (the CORS spec forbids
# ``Access-Control-Allow-Origin: *`` together with
# ``Access-Control-Allow-Credentials: true``).
CORS_ORIGINS = [origin.strip() for origin in os.getenv("CORS_ALLOW_ORIGINS", "").split(",") if origin.strip()]


def _like_escape(value: str) -> str:
    r"""Escape LIKE wildcards so user-supplied input matches literally.

    Must be paired with ``ESCAPE '\'`` in the SQL statement.
    """
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# -----------------------------------------------------------------------------
# Pydantic Schemas for OpenLineage Spec (Simplified & Extensible)
# -----------------------------------------------------------------------------
class Job(BaseModel):
    namespace: str = "default"
    name: str
    facets: dict[str, Any] | None = Field(default_factory=dict)


class Run(BaseModel):
    runId: str
    facets: dict[str, Any] | None = Field(default_factory=dict)


class DatasetFacet(BaseModel):
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
    datasource: dict[str, Any] | None = None
    dataQualityAssertions: dict[str, Any] | None = None
    version: dict[str, Any] | None = None
    extra_facets: dict[str, Any] | None = Field(default_factory=dict)

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class Dataset(BaseModel):
    namespace: str
    name: str
    facets: dict[str, Any] | None = Field(default_factory=dict)


class OpenLineageEvent(BaseModel):
    eventType: str = "COMPLETE"  # START, RUNNING, COMPLETE, FAIL, ABORT, OTHER
    eventTime: str
    run: Run
    job: Job
    inputs: list[Dataset] | None = Field(default_factory=list)
    outputs: list[Dataset] | None = Field(default_factory=list)
    producer: str | None = "openlineage-client"
    schemaURL: str | None = None

    model_config = ConfigDict(extra="allow")


class AlertItem(BaseModel):
    alert_id: str
    rule_id: str
    rule_name: str
    severity: str
    dataset_name: str | None = None
    job_name: str | None = None
    summary: str
    description: str
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    status: str = "ACTIVE"


class IngestionResponse(BaseModel):
    status: str
    event_id: str
    job_name: str
    event_type: str
    alerts_triggered: int
    alerts: list[dict[str, Any]]


# -----------------------------------------------------------------------------
# Database Manager (SQLite with WAL mode)
# -----------------------------------------------------------------------------
class StorageManager:
    """Manages thread-safe SQLite persistence for OpenLineage events, datasets,
    schema history, and generated alerts."""

    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        # RLock: insert paths hold the lock while ``_connection()`` lazily
        # initializes the schema on first use.
        self._lock = threading.RLock()
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Create the database file/schema on FIRST use (never at import)."""
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            self._init_db()
            self._initialized = True

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Yields a managed connection that is always closed (no handle leaks).

        Shares the lifecycle contract of ``src.adapters._sqlite``: commit on
        clean exit, rollback on error, ``close()`` in all cases.
        """
        self._ensure_initialized()
        conn = self._get_connection()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        """Create tables/indexes. Idempotent; the caller must hold ``self._lock``."""
        conn = self._get_connection()
        try:
            with conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT NOT NULL,
                        event_type TEXT NOT NULL,
                        event_time TEXT NOT NULL,
                        job_namespace TEXT NOT NULL,
                        job_name TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        inputs_json TEXT,
                        outputs_json TEXT,
                        raw_payload TEXT NOT NULL,
                        received_at TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id);
                    CREATE INDEX IF NOT EXISTS idx_events_job ON events(job_name);
                    CREATE INDEX IF NOT EXISTS idx_events_time ON events(event_time);

                    CREATE TABLE IF NOT EXISTS datasets (
                        dataset_key TEXT PRIMARY KEY,
                        namespace TEXT NOT NULL,
                        name TEXT NOT NULL,
                        current_schema_json TEXT,
                        schema_hash TEXT,
                        last_updated_at TEXT NOT NULL,
                        last_job_name TEXT,
                        last_run_id TEXT,
                        total_events INTEGER DEFAULT 1
                    );

                    CREATE TABLE IF NOT EXISTS schema_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dataset_key TEXT NOT NULL,
                        schema_hash TEXT NOT NULL,
                        schema_json TEXT NOT NULL,
                        captured_at TEXT NOT NULL,
                        run_id TEXT NOT NULL,
                        FOREIGN KEY (dataset_key) REFERENCES datasets(dataset_key)
                    );

                    CREATE TABLE IF NOT EXISTS alerts (
                        alert_id TEXT PRIMARY KEY,
                        rule_id TEXT NOT NULL,
                        rule_name TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        dataset_name TEXT,
                        job_name TEXT,
                        summary TEXT NOT NULL,
                        description TEXT NOT NULL,
                        details_json TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'ACTIVE'
                    );

                    CREATE INDEX IF NOT EXISTS idx_alerts_rule ON alerts(rule_id);
                    CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts(severity);
                    CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts(status);
                    CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at);
                    """
                )
        finally:
            conn.close()

    def insert_event(self, event: OpenLineageEvent, raw_json: str) -> None:
        received_at = datetime.now(UTC).isoformat()
        inputs_json = json.dumps([d.model_dump() for d in (event.inputs or [])])
        outputs_json = json.dumps([d.model_dump() for d in (event.outputs or [])])

        with self._lock:
            with self._connection() as conn:
                conn.execute(
                    """
                    INSERT INTO events (
                        event_id, event_type, event_time, job_namespace, job_name,
                        run_id, inputs_json, outputs_json, raw_payload, received_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.run.runId,
                        event.eventType,
                        event.eventTime,
                        event.job.namespace,
                        event.job.name,
                        event.run.runId,
                        inputs_json,
                        outputs_json,
                        raw_json,
                        received_at,
                    ),
                )

    def upsert_dataset_schema(
        self,
        namespace: str,
        name: str,
        schema_dict: dict[str, Any] | None,
        run_id: str,
        job_name: str,
        event_time: str,
    ) -> tuple[bool, dict[str, Any] | None, dict[str, Any] | None]:
        """Upsert dataset and detect schema changes.

        Returns (is_new_or_changed, previous_schema, new_schema).
        """
        dataset_key = f"{namespace}.{name}"
        schema_json = json.dumps(schema_dict, sort_keys=True) if schema_dict else "{}"
        schema_hash = hashlib.sha256(schema_json.encode("utf-8")).hexdigest()

        with self._lock:
            with self._connection() as conn:
                existing = conn.execute(
                    "SELECT current_schema_json, schema_hash FROM datasets WHERE dataset_key = ?",
                    (dataset_key,),
                ).fetchone()

                if not existing:
                    # New dataset record
                    conn.execute(
                        """
                        INSERT INTO datasets (
                            dataset_key, namespace, name, current_schema_json, schema_hash,
                            last_updated_at, last_job_name, last_run_id, total_events
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                        """,
                        (
                            dataset_key,
                            namespace,
                            name,
                            schema_json,
                            schema_hash,
                            event_time,
                            job_name,
                            run_id,
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO schema_history (dataset_key, schema_hash, schema_json, captured_at, run_id)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (dataset_key, schema_hash, schema_json, event_time, run_id),
                    )
                    return True, None, schema_dict

                old_hash = existing["schema_hash"]
                old_schema_json = existing["current_schema_json"]

                if old_hash != schema_hash and schema_dict:
                    # Schema changed!
                    old_schema = json.loads(old_schema_json) if old_schema_json else None
                    conn.execute(
                        """
                        UPDATE datasets SET
                            current_schema_json = ?,
                            schema_hash = ?,
                            last_updated_at = ?,
                            last_job_name = ?,
                            last_run_id = ?,
                            total_events = total_events + 1
                        WHERE dataset_key = ?
                        """,
                        (
                            schema_json,
                            schema_hash,
                            event_time,
                            job_name,
                            run_id,
                            dataset_key,
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO schema_history (dataset_key, schema_hash, schema_json, captured_at, run_id)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (dataset_key, schema_hash, schema_json, event_time, run_id),
                    )
                    return True, old_schema, schema_dict
                else:
                    # Update metadata only
                    conn.execute(
                        """
                        UPDATE datasets SET
                            last_updated_at = ?,
                            last_job_name = ?,
                            last_run_id = ?,
                            total_events = total_events + 1
                        WHERE dataset_key = ?
                        """,
                        (event_time, job_name, run_id, dataset_key),
                    )
                    return False, None, schema_dict

    _ALERT_INSERT_SQL = """
        INSERT INTO alerts (
            alert_id, rule_id, rule_name, severity, dataset_name,
            job_name, summary, description, details_json, created_at, status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    @staticmethod
    def _alert_row(alert: AlertItem) -> tuple[Any, ...]:
        return (
            alert.alert_id,
            alert.rule_id,
            alert.rule_name,
            alert.severity,
            alert.dataset_name,
            alert.job_name,
            alert.summary,
            alert.description,
            json.dumps(alert.details),
            alert.created_at,
            alert.status,
        )

    @staticmethod
    def _within_dedup_window(created_at: Any, window_seconds: int) -> bool:
        """True when ``created_at`` is inside the throttle window.

        An unparsable timestamp is logged and treated as "not a duplicate"
        (allow the alert) rather than silently swallowing the parse failure.
        """
        try:
            alert_time = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
        except (ValueError, TypeError) as exc:
            logger.warning(
                "Dedup check: stored alert created_at=%r is unparsable (%s); treating as outside the window",
                created_at,
                exc,
            )
            return False
        if alert_time.tzinfo is None:
            alert_time = alert_time.replace(tzinfo=UTC)
        return (datetime.now(UTC) - alert_time).total_seconds() < window_seconds

    def insert_alert_if_new(self, alert: AlertItem, window_seconds: int = 300) -> bool:
        """Atomically persist ``alert`` unless a recent duplicate exists.

        The duplicate check and the INSERT share a single IMMEDIATE
        transaction, so concurrent evaluators (threads or processes) cannot
        both pass the check before either one writes (no check-then-insert
        TOCTOU). Returns True only when the row was actually persisted.

        Identity is the tuple (rule_id, dataset_name, job_name) compared per
        column against the event's own fields: a job_name never matches
        another alert's dataset_name. ``COALESCE(..., 'unknown')`` maps NULL
        to the "unknown" sentinel per column because in SQLite NULL never
        equals NULL, so without the mapping two alerts with NULL fields
        would never deduplicate. Accepted limitation: two alerts with
        neither dataset nor job share the "unknown/unknown" identity and
        deduplicate against each other.
        """
        dataset = alert.dataset_name or "unknown"
        job = alert.job_name or "unknown"
        with self._lock:
            with self._connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    """
                    SELECT created_at FROM alerts
                    WHERE rule_id = ?
                      AND COALESCE(dataset_name, 'unknown') = ?
                      AND COALESCE(job_name, 'unknown') = ?
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (alert.rule_id, dataset, job),
                ).fetchone()
                if row is not None and self._within_dedup_window(row["created_at"], window_seconds):
                    return False
                conn.execute(self._ALERT_INSERT_SQL, self._alert_row(alert))
                return True

    def query_alerts(
        self,
        severity: str | None = None,
        rule_id: str | None = None,
        status: str | None = None,
        dataset: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM alerts WHERE 1=1"
        params: list[Any] = []

        if severity:
            query += " AND severity = ?"
            params.append(severity.upper())
        if rule_id:
            query += " AND rule_id = ?"
            params.append(rule_id)
        if status:
            query += " AND status = ?"
            params.append(status.upper())
        if dataset:
            # Documented substring filter, but wildcards in the input are
            # escaped so "%"/"_" in user input match literally (ESCAPE '\').
            query += " AND dataset_name LIKE ? ESCAPE '\\'"
            params.append(f"%{_like_escape(dataset)}%")

        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._connection() as conn:
            rows = conn.execute(query, params).fetchall()
            result = []
            for r in rows:
                item = dict(r)
                item["details"] = json.loads(item["details_json"])
                del item["details_json"]
                result.append(item)
            return result

    def get_alert_by_id(self, alert_id: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            row = conn.execute("SELECT * FROM alerts WHERE alert_id = ?", (alert_id,)).fetchone()
            if not row:
                return None
            res = dict(row)
            res["details"] = json.loads(res["details_json"])
            del res["details_json"]
            return res

    def set_alert_status(self, alert_id: str, new_status: str) -> bool:
        with self._lock:
            with self._connection() as conn:
                cur = conn.execute(
                    "UPDATE alerts SET status = ? WHERE alert_id = ?",
                    (new_status.upper(), alert_id),
                )
                return cur.rowcount > 0

    def get_dataset_lineage(self, dataset_name: str) -> dict[str, Any]:
        """Fetch lineage connections: all jobs consuming or producing this dataset.

        Dataset identity is matched EXACTLY (either the bare ``name`` or the
        ``namespace.name`` key), never by substring, so ``silver.orders`` no
        longer matches ``silver.orders_archive``.
        """
        with self._connection() as conn:
            # Producers (where dataset in outputs)
            producers = conn.execute(
                """
                SELECT DISTINCT job_namespace, job_name, event_type, event_time, run_id
                FROM events
                WHERE EXISTS (
                    SELECT 1 FROM json_each(events.outputs_json) je
                    WHERE json_extract(je.value, '$.name') = ?
                       OR json_extract(je.value, '$.namespace') || '.' ||
                          json_extract(je.value, '$.name') = ?
                )
                ORDER BY event_time DESC LIMIT 20
                """,
                (dataset_name, dataset_name),
            ).fetchall()

            # Consumers (where dataset in inputs)
            consumers = conn.execute(
                """
                SELECT DISTINCT job_namespace, job_name, event_type, event_time, run_id
                FROM events
                WHERE EXISTS (
                    SELECT 1 FROM json_each(events.inputs_json) je
                    WHERE json_extract(je.value, '$.name') = ?
                       OR json_extract(je.value, '$.namespace') || '.' ||
                          json_extract(je.value, '$.name') = ?
                )
                ORDER BY event_time DESC LIMIT 20
                """,
                (dataset_name, dataset_name),
            ).fetchall()

            # Schema history (exact "namespace.name" key or bare-name suffix)
            schemas = conn.execute(
                """
                SELECT schema_hash, schema_json, captured_at, run_id
                FROM schema_history
                WHERE dataset_key = ? OR dataset_key LIKE ? ESCAPE '\\'
                ORDER BY captured_at DESC LIMIT 10
                """,
                (dataset_name, f"%.{_like_escape(dataset_name)}"),
            ).fetchall()

            return {
                "dataset": dataset_name,
                "producers": [dict(p) for p in producers],
                "consumers": [dict(c) for c in consumers],
                "schema_versions": [
                    {
                        "hash": s["schema_hash"],
                        "captured_at": s["captured_at"],
                        "run_id": s["run_id"],
                        "schema": json.loads(s["schema_json"]),
                    }
                    for s in schemas
                ],
            }

    def list_datasets(self) -> list[dict[str, Any]]:
        with self._connection() as conn:
            rows = conn.execute(
                "SELECT dataset_key, namespace, name, schema_hash, last_updated_at, "
                "last_job_name, total_events FROM datasets ORDER BY last_updated_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]

    def get_stats(self) -> dict[str, Any]:
        with self._connection() as conn:
            total_events = conn.execute("SELECT COUNT(*) as c FROM events").fetchone()["c"]
            total_datasets = conn.execute("SELECT COUNT(*) as c FROM datasets").fetchone()["c"]
            total_alerts = conn.execute("SELECT COUNT(*) as c FROM alerts").fetchone()["c"]
            active_alerts = conn.execute("SELECT COUNT(*) as c FROM alerts WHERE status = 'ACTIVE'").fetchone()["c"]
            critical_alerts = conn.execute(
                "SELECT COUNT(*) as c FROM alerts WHERE severity = 'CRITICAL' AND status = 'ACTIVE'"
            ).fetchone()["c"]
            return {
                "total_events": total_events,
                "total_datasets": total_datasets,
                "total_alerts": total_alerts,
                "active_alerts": active_alerts,
                "critical_alerts": critical_alerts,
            }


# -----------------------------------------------------------------------------
# Outbound Alert Webhook Delivery (best-effort, stdlib-only: urllib.request)
# -----------------------------------------------------------------------------
def _post_alert_webhook(endpoint: str, payload: dict[str, Any], attempts: int = 1) -> bool:
    """POST ``payload`` as JSON to ``endpoint``. Never raises.

    Gated by the ``LINEAGE_WEBHOOK_DELIVERY`` env switch (read at call time)
    and bounded by ``LINEAGE_WEBHOOK_TIMEOUT`` per attempt. Every failure is
    logged and swallowed so webhook delivery can never crash ingestion.
    ``attempts`` is the total number of POSTs (1 + ``actions.webhook.retry_count``).
    """
    if not WEBHOOK_DELIVERY_ENABLED:
        return False
    body = json.dumps(payload, default=str).encode("utf-8")
    total = max(1, int(attempts))
    for attempt in range(1, total + 1):
        request = urllib.request.Request(
            endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=WEBHOOK_TIMEOUT_SECONDS) as response:
                status_code = getattr(response, "status", None) or response.getcode()
            if 200 <= int(status_code) < 300:
                logger.info("Alert webhook delivered to %s (HTTP %s)", endpoint, status_code)
                return True
            logger.warning(
                "Alert webhook POST %s returned HTTP %s (attempt %d/%d)",
                endpoint,
                status_code,
                attempt,
                total,
            )
        except Exception as exc:
            logger.warning(
                "Alert webhook POST %s failed (attempt %d/%d): %s",
                endpoint,
                attempt,
                total,
                exc,
            )
        if attempt < total:
            time.sleep(0.1)
    return False


# -----------------------------------------------------------------------------
# Active Metadata Rule Evaluator (RF-20)
# -----------------------------------------------------------------------------
class RuleEngine:
    """Evaluates the four active metadata rules defined in alert-rules.yaml."""

    def __init__(self, storage: StorageManager, rules_path: str = ALERT_RULES_PATH):
        self.storage = storage
        self.rules_path = rules_path
        self._raw_config: dict[str, Any] = {}
        self.rules = self._load_rules()
        self.rules_by_id: dict[str, dict[str, Any]] = {
            r["id"]: r for r in self.rules if isinstance(r, dict) and r.get("id")
        }

    def _rule(self, rule_id: str) -> dict[str, Any]:
        """Return the loaded YAML rule dict for rule_id ({} if absent)."""
        rule = self.rules_by_id.get(rule_id)
        return rule if isinstance(rule, dict) else {}

    def _rule_enabled(self, rule_id: str) -> bool:
        """A rule fires only when its YAML `enabled` flag is true.

        Unknown rule ids default to True to preserve legacy behaviour
        when alert-rules.yaml is missing/unparseable.
        """
        rule = self._rule(rule_id)
        if not rule:
            return True
        return bool(rule.get("enabled", True))

    def _dedup_window_seconds(self) -> int:
        """Dedup window from YAML defaults.throttling (fallback: 300s)."""
        try:
            return int(
                self._raw_config.get("defaults", {}).get("throttling", {}).get("deduplication_window_seconds", 300)
            )
        except (ValueError, TypeError, AttributeError):
            return 300

    # -- alert-rules.yaml target/actions wiring helpers --------------------

    def _target(self, rule_id: str) -> dict[str, Any]:
        target = self._rule(rule_id).get("target")
        return target if isinstance(target, dict) else {}

    def _event_type_allowed(self, rule_id: str, event_type: Any, default: set[str] | None = None) -> bool:
        """Gate on YAML ``target.event_types`` (case-insensitive).

        An absent/empty key (or ``default=None``) preserves the legacy
        unfiltered behaviour of the rule.
        """
        raw = self._target(rule_id).get("event_types")
        allowed = {str(s).upper() for s in raw} if isinstance(raw, list) and raw else default
        return allowed is None or str(event_type).upper() in allowed

    def _namespace_allowed(self, rule_id: str, namespace: Any) -> bool:
        """Gate on YAML ``target.namespace`` regex (absent/invalid -> allow all)."""
        pattern = self._target(rule_id).get("namespace")
        if not isinstance(pattern, str) or not pattern:
            return True
        try:
            return re.search(pattern, str(namespace)) is not None
        except re.error:
            logger.warning("Invalid target.namespace regex %r in %s; ignoring", pattern, rule_id)
            return True

    def _dataset_allowed(self, rule_id: str, dataset_name: Any) -> bool:
        """Gate on YAML ``target.dataset_pattern`` regex (absent/invalid -> allow all)."""
        pattern = self._target(rule_id).get("dataset_pattern")
        if not isinstance(pattern, str) or not pattern:
            return True
        try:
            return re.search(pattern, str(dataset_name)) is not None
        except re.error:
            logger.warning("Invalid target.dataset_pattern regex %r in %s; ignoring", pattern, rule_id)
            return True

    def _webhook_config(self, rule_id: str) -> dict[str, Any]:
        actions = self._rule(rule_id).get("actions")
        webhook = actions.get("webhook") if isinstance(actions, dict) else None
        return webhook if isinstance(webhook, dict) else {}

    def _webhook_endpoint(self, rule_id: str) -> str | None:
        """YAML ``actions.webhook.endpoint`` for rule_id (None when absent)."""
        endpoint = self._webhook_config(rule_id).get("endpoint")
        if isinstance(endpoint, str) and endpoint.strip():
            return endpoint.strip()
        return None

    def _webhook_attempts(self, rule_id: str) -> int:
        """Total POSTs = 1 + YAML ``actions.webhook.retry_count`` (retries after first)."""
        try:
            retries = int(self._webhook_config(rule_id).get("retry_count", 0))
        except (TypeError, ValueError):
            retries = 0
        return 1 + max(0, retries)

    def _deliver_alert_webhook(self, alert: AlertItem) -> bool:
        """Best-effort webhook delivery for a persisted alert. Never raises."""
        try:
            endpoint = self._webhook_endpoint(alert.rule_id)
            if not endpoint:
                return False
            return _post_alert_webhook(
                endpoint,
                alert.model_dump(mode="json"),
                self._webhook_attempts(alert.rule_id),
            )
        except Exception as exc:
            logger.warning("Alert webhook delivery skipped for %s: %s", alert.alert_id, exc)
            return False

    def _load_rules(self) -> list[dict[str, Any]]:
        """Load rules from YAML file or return fallback default rules."""
        if Path(self.rules_path).exists() and yaml is not None:
            try:
                with open(self.rules_path, encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)
                    if isinstance(cfg, dict):
                        self._raw_config = cfg
                    rules = cfg.get("rules", []) if isinstance(cfg, dict) else []
                    logger.info(
                        "Successfully loaded %d active metadata rules from %s",
                        len(rules),
                        self.rules_path,
                    )
                    return rules
            except Exception as e:
                logger.error("Failed to parse alert-rules.yaml: %s. Using default rules.", e)

        logger.info("Initializing fallback default active metadata rules")
        return [
            {
                "id": "RF20-RULE-001",
                "name": "SchemaChangeAlert",
                "enabled": True,
                "dataset_pattern": "^(silver|gold)\\..*",
            },
            {
                "id": "RF20-RULE-002",
                "name": "QualityThresholdBreachAlert",
                "enabled": True,
                "min_pass_rate": 0.95,
            },
            {
                "id": "RF20-RULE-003",
                "name": "PipelineFailureAlert",
                "enabled": True,
                "terminal_states": ["FAIL", "ABORT"],
            },
            {
                "id": "RF20-RULE-004",
                "name": "SLAFreshnessBreachAlert",
                "enabled": True,
            },
        ]

    def evaluate(self, event: OpenLineageEvent) -> list[AlertItem]:
        alerts: list[AlertItem] = []

        # Evaluate Rule 1: Schema Change Alert on Silver/Gold tables
        schema_alerts = self._check_schema_changes(event)
        alerts.extend(schema_alerts)

        # Evaluate Rule 2: Quality threshold breach alert
        quality_alerts = self._check_quality_breaches(event)
        alerts.extend(quality_alerts)

        # Evaluate Rule 3: Pipeline failure alert
        failure_alerts = self._check_pipeline_failures(event)
        alerts.extend(failure_alerts)

        # Evaluate Rule 4: SLA freshness breach alert
        sla_alerts = self._check_sla_freshness(event)
        alerts.extend(sla_alerts)

        # Persist triggered alerts atomically (dedup inside a single IMMEDIATE
        # transaction) and deliver webhooks best-effort. Only alerts that were
        # actually persisted are returned, so suppressed duplicates never
        # inflate ``alerts_triggered``.
        dedup_window = self._dedup_window_seconds()
        persisted: list[AlertItem] = []
        for alert in alerts:
            if not self.storage.insert_alert_if_new(alert, dedup_window):
                continue
            target_key = alert.dataset_name or alert.job_name or "unknown"
            logger.warning(
                "TRIGGERED ALERT [%s - %s]: %s (target=%s)",
                alert.severity,
                alert.rule_name,
                alert.summary,
                target_key,
            )
            self._deliver_alert_webhook(alert)
            persisted.append(alert)

        return persisted

    def _check_schema_changes(self, event: OpenLineageEvent) -> list[AlertItem]:
        """Rule 1: Detect schema modifications in Silver and Gold tables."""
        alerts: list[AlertItem] = []
        if not self._rule_enabled("RF20-RULE-001"):
            return alerts
        if not self._event_type_allowed("RF20-RULE-001", event.eventType):
            return alerts
        rule = self._rule("RF20-RULE-001")
        target = rule.get("target", {})
        if not isinstance(target, dict):
            target = {}
        criteria = rule.get("criteria", {})
        if not isinstance(criteria, dict):
            criteria = {}
        sev_map = rule.get("severity_mapping", {})
        if not isinstance(sev_map, dict):
            sev_map = {}
        pattern = target.get("dataset_pattern", r"^(silver|gold)\..*")
        try:
            silver_gold_regex = re.compile(str(pattern), re.IGNORECASE)
        except re.error:
            silver_gold_regex = re.compile(r"^(silver|gold)\..*", re.IGNORECASE)
        removed_sev = str(sev_map.get("removed_column", "CRITICAL"))
        altered_sev = str(sev_map.get("type_altered", "CRITICAL"))
        added_sev = str(sev_map.get("added_column", "WARNING"))
        # Nullability detection is opt-in: without the YAML key the legacy
        # (pre-wiring) behaviour of ignoring nullability is preserved.
        detect_nullability = bool(criteria.get("detect_nullability_changes", False))
        nullability_sev = str(sev_map.get("nullability_changed", "WARNING"))
        detect_added = bool(criteria.get("detect_added_columns", True))
        detect_removed = bool(criteria.get("detect_removed_columns", True))
        detect_types = bool(criteria.get("detect_type_changes", True))
        strict_mode = bool(criteria.get("strict_mode", False))
        datasets_to_check = (event.outputs or []) + (event.inputs or [])

        for ds in datasets_to_check:
            # Check namespace + pattern match
            if not self._namespace_allowed("RF20-RULE-001", ds.namespace):
                continue
            if not silver_gold_regex.search(ds.name) and not silver_gold_regex.search(f"{ds.namespace}.{ds.name}"):
                continue

            schema_facet = ds.facets.get("schema") if ds.facets else None
            if not schema_facet:
                continue

            fields = schema_facet.get("fields", [])
            is_changed, old_schema, new_schema = self.storage.upsert_dataset_schema(
                namespace=ds.namespace,
                name=ds.name,
                schema_dict=schema_facet,
                run_id=event.run.runId,
                job_name=event.job.name,
                event_time=event.eventTime,
            )

            if is_changed and old_schema:
                # Compare fields
                old_field_entries: Any = old_schema.get("fields", [])
                new_field_entries: Any = fields
                old_fields = {f.get("name"): f.get("type") for f in old_field_entries}
                new_fields = {f.get("name"): f.get("type") for f in new_field_entries}
                old_meta = {f.get("name"): f for f in old_field_entries if isinstance(f, dict)}
                new_meta = {f.get("name"): f for f in new_field_entries if isinstance(f, dict)}

                added = set(new_fields.keys()) - set(old_fields.keys())
                removed = set(old_fields.keys()) - set(new_fields.keys())
                altered = {
                    col: {"old": old_fields[col], "new": new_fields[col]}
                    for col in set(old_fields.keys()) & set(new_fields.keys())
                    if old_fields[col] != new_fields[col]
                }
                # Only flag explicit nullability flips (both schemas must carry
                # the key) so a producer newly emitting `nullable` metadata does
                # not raise a false drift alert.
                nullability_altered = {
                    col: {"old": old_meta[col].get("nullable"), "new": new_meta[col].get("nullable")}
                    for col in old_meta.keys() & new_meta.keys()
                    if old_meta[col].get("nullable") is not None
                    and new_meta[col].get("nullable") is not None
                    and old_meta[col].get("nullable") != new_meta[col].get("nullable")
                }

                diff_parts = []
                severity = "WARNING"
                if removed and detect_removed:
                    severity = removed_sev
                    diff_parts.append(f"Removed columns: {list(removed)}")
                if altered and detect_types:
                    severity = altered_sev
                    diff_parts.append(f"Type changed: {altered}")
                if nullability_altered and detect_nullability:
                    if severity == "WARNING":
                        severity = nullability_sev
                    diff_parts.append(f"Nullability changed: {nullability_altered}")
                if added and detect_added:
                    if strict_mode:
                        severity = "CRITICAL"
                    elif severity == "WARNING":
                        severity = added_sev
                    diff_parts.append(f"Added columns: {list(added)}")
                if not diff_parts:
                    continue

                summary = f"Schema drift detected on {ds.name}"
                description = f"Dataset {ds.name} schema updated by job {event.job.name}. Details: " + "; ".join(
                    diff_parts
                )

                alerts.append(
                    AlertItem(
                        alert_id=str(uuid.uuid4()),
                        rule_id="RF20-RULE-001",
                        rule_name="SchemaChangeAlert",
                        severity=severity,
                        dataset_name=ds.name,
                        job_name=event.job.name,
                        summary=summary,
                        description=description,
                        details={
                            "dataset": ds.name,
                            "namespace": ds.namespace,
                            "job": event.job.name,
                            "runId": event.run.runId,
                            "added_columns": list(added),
                            "removed_columns": list(removed),
                            "type_changes": altered,
                            "nullability_changes": nullability_altered,
                            "total_fields": len(fields),
                        },
                        created_at=datetime.now(UTC).isoformat(),
                    )
                )

        return alerts

    def _check_quality_breaches(self, event: OpenLineageEvent) -> list[AlertItem]:
        """Rule 2: Detect quality threshold breach in dataQuality / dataQualityAssertions facets."""
        alerts: list[AlertItem] = []
        if not self._rule_enabled("RF20-RULE-002"):
            return alerts
        if not self._event_type_allowed("RF20-RULE-002", event.eventType):
            return alerts
        rule = self._rule("RF20-RULE-002")
        criteria = rule.get("criteria", {})
        if not isinstance(criteria, dict):
            criteria = {}
        sev_map = rule.get("severity_mapping", {})
        if not isinstance(sev_map, dict):
            sev_map = {}
        try:
            min_pass_rate = float(criteria.get("min_pass_rate", 0.95))
        except (ValueError, TypeError):
            min_pass_rate = 0.95
        raw_crit = criteria.get("critical_severities", ["critical", "high", "fatal"])
        if isinstance(raw_crit, list):
            critical_sevs = {str(s).lower() for s in raw_crit}
        else:
            critical_sevs = {"critical", "high", "fatal"}
        fail_on_critical = bool(criteria.get("fail_on_critical_assertion", True))
        crit_pass_sev = str(sev_map.get("pass_rate_below_0_90", "CRITICAL"))
        warn_pass_sev = str(sev_map.get("pass_rate_below_0_95", "WARNING"))
        crit_assert_sev = str(sev_map.get("critical_assertion_failed", "CRITICAL"))

        # 1. Run-level dataQuality facet
        run_facets = event.run.facets or {}
        metrics_facet_key = criteria.get("metrics_facet_key")
        if isinstance(metrics_facet_key, str) and metrics_facet_key:
            dq_facet = run_facets.get(metrics_facet_key) or run_facets.get("dataQualityMetrics")
        else:
            dq_facet = run_facets.get("dataQuality") or run_facets.get("dataQualityMetrics")

        if dq_facet and self._namespace_allowed("RF20-RULE-002", event.job.namespace):
            # Explicit None checks: a pass rate of 0.0 (total quality failure)
            # is falsy but MUST still trigger the alert.
            pass_rate = dq_facet.get("passRate")
            if pass_rate is None:
                pass_rate = dq_facet.get("pass_rate")
            suite_name = dq_facet.get("suiteName", "unknown_suite")
            failed_count = dq_facet.get("failedExpectations", 0)

            if pass_rate is not None and pass_rate < min_pass_rate:
                severity = crit_pass_sev if pass_rate < 0.90 else warn_pass_sev
                alerts.append(
                    AlertItem(
                        alert_id=str(uuid.uuid4()),
                        rule_id="RF20-RULE-002",
                        rule_name="QualityThresholdBreachAlert",
                        severity=severity,
                        job_name=event.job.name,
                        summary=f"Quality pass rate breach: {pass_rate:.2%} (Suite: {suite_name})",
                        description=(
                            f"Validation suite {suite_name} scored {pass_rate:.2%}, below 95% threshold. "
                            f"Failed expectations: {failed_count}."
                        ),
                        details=dq_facet,
                        created_at=datetime.now(UTC).isoformat(),
                    )
                )

        # 2. Dataset-level dataQualityAssertions facets
        facet_key = criteria.get("facet_key")
        if not isinstance(facet_key, str) or not facet_key:
            facet_key = "dataQualityAssertions"
        all_datasets = (event.inputs or []) + (event.outputs or [])
        for ds in all_datasets:
            if not ds.facets:
                continue
            if not self._namespace_allowed("RF20-RULE-002", ds.namespace):
                continue
            if not self._dataset_allowed("RF20-RULE-002", ds.name):
                continue
            assertions_facet = ds.facets.get(facet_key)
            if not assertions_facet:
                continue

            assertions = assertions_facet.get("assertions", [])
            failed_assertions = [a for a in assertions if a.get("success") is False]

            if failed_assertions:
                critical_failed = [
                    a
                    for a in failed_assertions
                    if str((a.get("meta") or {}).get("severity", "")).lower() in critical_sevs
                ]
                if critical_failed and fail_on_critical:
                    severity = crit_assert_sev
                else:
                    severity = "WARNING"
                alerts.append(
                    AlertItem(
                        alert_id=str(uuid.uuid4()),
                        rule_id="RF20-RULE-002",
                        rule_name="QualityThresholdBreachAlert",
                        severity=severity,
                        dataset_name=ds.name,
                        job_name=event.job.name,
                        summary=f"Data quality assertion failures on {ds.name} ({len(failed_assertions)} failed)",
                        description=(
                            f"Dataset {ds.name} had {len(failed_assertions)} failed quality checks. "
                            f"Critical failures: {len(critical_failed)}."
                        ),
                        details={
                            "dataset": ds.name,
                            "total_assertions": len(assertions),
                            "failed_count": len(failed_assertions),
                            "failed_assertions": failed_assertions[:10],
                        },
                        created_at=datetime.now(UTC).isoformat(),
                    )
                )

        return alerts

    def _check_pipeline_failures(self, event: OpenLineageEvent) -> list[AlertItem]:
        """Rule 3: Pipeline failure alert on FAIL or ABORT."""
        alerts: list[AlertItem] = []
        if not self._rule_enabled("RF20-RULE-003"):
            return alerts
        if not self._event_type_allowed("RF20-RULE-003", event.eventType):
            return alerts
        if not self._namespace_allowed("RF20-RULE-003", event.job.namespace):
            return alerts
        rule = self._rule("RF20-RULE-003")
        criteria = rule.get("criteria", {})
        if not isinstance(criteria, dict):
            criteria = {}
        raw_states = criteria.get("terminal_states", ["FAIL", "ABORT"])
        terminal_states = [str(s).upper() for s in raw_states] if isinstance(raw_states, list) else ["FAIL", "ABORT"]
        raw_excl = criteria.get("exclude_job_patterns", [])
        exclude_patterns = [str(p) for p in raw_excl] if isinstance(raw_excl, list) else []

        if str(event.eventType).upper() not in terminal_states:
            return alerts
        for pat in exclude_patterns:
            try:
                if re.search(pat, event.job.name):
                    return alerts
            except re.error:
                continue

        # Extract error details (reached only for terminal states above)
        run_facets = event.run.facets or {}
        error_facet = run_facets.get("errorMessage") or run_facets.get("spark_unknown_operator")
        error_msg = "Pipeline execution failed or aborted"
        if isinstance(error_facet, dict):
            error_msg = error_facet.get("message", error_msg)

        severity = "CRITICAL"
        alerts.append(
            AlertItem(
                alert_id=str(uuid.uuid4()),
                rule_id="RF20-RULE-003",
                rule_name="PipelineFailureAlert",
                severity=severity,
                job_name=event.job.name,
                summary=f"Pipeline job execution failed: {event.job.name}",
                description=(
                    f"Job {event.job.name} (runId: {event.run.runId}) failed with state {event.eventType}. {error_msg}"
                ),
                details={
                    "job": event.job.name,
                    "namespace": event.job.namespace,
                    "runId": event.run.runId,
                    "eventType": event.eventType,
                    "eventTime": event.eventTime,
                },
                created_at=datetime.now(UTC).isoformat(),
            )
        )

        return alerts

    def _sla_lookup(self, dataset_name: str) -> tuple[float, str]:
        """Resolve (max_staleness_seconds, sla_tier) for a dataset.

        Uses YAML criteria.sla_definitions when present; otherwise falls
        back to the legacy hardcoded medallion table.
        """
        rule = self._rule("RF20-RULE-004")
        criteria = rule.get("criteria", {})
        sla_defs = criteria.get("sla_definitions") if isinstance(criteria, dict) else None
        if isinstance(sla_defs, list) and sla_defs:
            for entry in sla_defs:
                if not isinstance(entry, dict):
                    continue
                pat = str(entry.get("pattern", ".*"))
                try:
                    if re.search(pat, dataset_name):
                        return (
                            float(entry.get("max_staleness_seconds", 3600)),
                            str(entry.get("sla_tier", "tier_2")),
                        )
                except re.error:
                    continue
            return 3600.0, "tier_2"
        # Legacy fallback table (pre-RF-20-wiring behaviour)
        if dataset_name.startswith("stream."):
            return 5.0, "real_time"
        elif dataset_name.startswith("bronze.") or dataset_name.startswith("silver."):
            return 3600.0, "tier_2"
        elif dataset_name.startswith("gold."):
            return 86400.0, "tier_1"
        return 3600.0, "tier_2"

    def _sla_severity(self, sla_tier: str, max_staleness: float, is_stream: bool) -> str:
        """Map an SLA tier breach to a severity via YAML severity_mapping."""
        rule = self._rule("RF20-RULE-004")
        sev_map = rule.get("severity_mapping", {})
        if not isinstance(sev_map, dict):
            sev_map = {}
        if not rule:  # no YAML loaded -> preserve legacy severity behaviour
            return "CRITICAL" if is_stream or max_staleness <= 3600 else "WARNING"
        defaults = {"tier_1": "CRITICAL", "tier_2": "WARNING", "real_time": "CRITICAL"}
        return str(sev_map.get(f"{sla_tier}_breach", defaults.get(sla_tier, "WARNING")))

    def _check_sla_freshness(self, event: OpenLineageEvent) -> list[AlertItem]:
        """Rule 4: SLA freshness breach alert.

        eventTime handling is FAIL-CLOSED: freshness is the whole contract of
        this rule, so a timestamp we cannot parse cannot prove the SLA was met
        and is treated as a breach candidate (details carry
        ``event_time_error``) instead of the legacy fail-open ``lag=0`` that
        silently swallowed corrupt timestamps. Naive (offset-less) timestamps
        parse but violate the OpenLineage RFC3339 requirement; they are
        interpreted as UTC so a missing offset alone never zeroes the lag.
        """
        alerts: list[AlertItem] = []
        if not self._rule_enabled("RF20-RULE-004"):
            return alerts
        if not self._event_type_allowed("RF20-RULE-004", event.eventType, default={"COMPLETE", "CHECK"}):
            return alerts

        event_time_error: str | None = None
        lag_seconds: float | None = None
        try:
            event_dt = datetime.fromisoformat(event.eventTime.replace("Z", "+00:00"))
        except (ValueError, TypeError, AttributeError) as exc:
            event_time_error = f"unparsable eventTime {event.eventTime!r}: {exc}"
        else:
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=UTC)
            lag_seconds = (datetime.now(UTC) - event_dt).total_seconds()

        # Check outputs SLA against SLA table
        for ds in event.outputs or []:
            if not self._namespace_allowed("RF20-RULE-004", ds.namespace):
                continue
            if not self._dataset_allowed("RF20-RULE-004", ds.name):
                continue
            max_staleness, sla_tier = self._sla_lookup(ds.name)
            is_stream = sla_tier == "real_time" or ds.name.startswith("stream.")

            if not (event_time_error is not None or (lag_seconds is not None and lag_seconds > max_staleness)):
                continue

            severity = self._sla_severity(sla_tier, max_staleness, is_stream)
            if lag_seconds is not None:
                summary = f"SLA Freshness breach on {ds.name} (lag: {lag_seconds:.1f}s)"
                description = (
                    f"Dataset {ds.name} produced with lag of {lag_seconds:.1f}s, "
                    f"exceeding maximum SLA threshold of {max_staleness}s."
                )
            else:
                summary = f"SLA Freshness breach on {ds.name} (unparsable eventTime)"
                description = (
                    f"Dataset {ds.name} has unparsable eventTime {event.eventTime!r}; "
                    f"freshness cannot be verified, so the {max_staleness}s SLA is "
                    f"treated as breached (fail-closed)."
                )
            alerts.append(
                AlertItem(
                    alert_id=str(uuid.uuid4()),
                    rule_id="RF20-RULE-004",
                    rule_name="SLAFreshnessBreachAlert",
                    severity=severity,
                    dataset_name=ds.name,
                    job_name=event.job.name,
                    summary=summary,
                    description=description,
                    details={
                        "dataset": ds.name,
                        "job": event.job.name,
                        "lag_seconds": lag_seconds,
                        "max_staleness_seconds": max_staleness,
                        "eventTime": event.eventTime,
                        "event_time_error": event_time_error,
                    },
                    created_at=datetime.now(UTC).isoformat(),
                )
            )

        return alerts


# -----------------------------------------------------------------------------
# FastAPI App Definition & Dependency Injection
# -----------------------------------------------------------------------------
storage = StorageManager()
rule_engine = RuleEngine(storage)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info("Lineage Webhook Service started. Connected to DB at %s", storage.db_path)
    yield
    logger.info("Lineage Webhook Service shutting down.")


app = FastAPI(
    title="OpenLineage Webhook & Active Metadata Service",
    description="Microservice for OpenLineage event ingestion and real-time active metadata alerts (RF-20)",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    # Explicit CORS_ALLOW_ORIGINS -> those origins WITH credentials; no config
    # -> wildcard WITHOUT credentials (the CORS spec forbids "*" together
    # with Access-Control-Allow-Credentials: true).
    allow_origins=CORS_ORIGINS or ["*"],
    allow_credentials=bool(CORS_ORIGINS),
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------
@app.post(
    "/api/v1/lineage",
    status_code=status.HTTP_201_CREATED,
    response_model=IngestionResponse,
    summary="Ingest OpenLineage event",
)
async def ingest_lineage_event(event: OpenLineageEvent) -> IngestionResponse:
    """Ingest OpenLineage RunEvent, store in SQLite, and evaluate active metadata rules."""
    raw_json = event.model_dump_json()

    try:
        # 1. Store event
        storage.insert_event(event, raw_json)

        # 2. Evaluate active metadata alert rules
        triggered_alerts = rule_engine.evaluate(event)

        return IngestionResponse(
            status="accepted",
            event_id=event.run.runId,
            job_name=event.job.name,
            event_type=event.eventType,
            alerts_triggered=len(triggered_alerts),
            alerts=[asdict_alert(a) for a in triggered_alerts],
        )
    except Exception as exc:
        logger.exception("Failed to process OpenLineage event: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Lineage event ingestion failed: {str(exc)}",
        ) from exc


def asdict_alert(a: AlertItem) -> dict[str, Any]:
    return {
        "alert_id": a.alert_id,
        "rule_id": a.rule_id,
        "rule_name": a.rule_name,
        "severity": a.severity,
        "dataset_name": a.dataset_name,
        "job_name": a.job_name,
        "summary": a.summary,
        "description": a.description,
        "details": a.details,
        "created_at": a.created_at,
        "status": a.status,
    }


@app.get("/api/v1/alerts", summary="Get active metadata alerts")
async def get_alerts(
    severity: str | None = Query(None, description="Filter by severity (CRITICAL, WARNING, INFO)"),
    rule_id: str | None = Query(None, description="Filter by rule ID (e.g. RF20-RULE-001)"),
    status: str | None = Query("ACTIVE", description="Filter by status (ACTIVE, ACKNOWLEDGED, RESOLVED)"),
    dataset: str | None = Query(None, description="Filter by dataset substring"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Retrieve active metadata alerts with optional filtering."""
    alerts = storage.query_alerts(
        severity=severity,
        rule_id=rule_id,
        status=status,
        dataset=dataset,
        limit=limit,
        offset=offset,
    )
    return {
        "count": len(alerts),
        "limit": limit,
        "offset": offset,
        "alerts": alerts,
    }


@app.get("/api/v1/alerts/{alert_id}", summary="Get alert by ID")
async def get_alert(alert_id: str) -> dict[str, Any]:
    """Retrieve details for a single alert."""
    alert = storage.get_alert_by_id(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    return alert


@app.post("/api/v1/alerts/{alert_id}/acknowledge", summary="Acknowledge alert")
async def acknowledge_alert(alert_id: str) -> dict[str, Any]:
    """Mark an active alert as ACKNOWLEDGED."""
    updated = storage.set_alert_status(alert_id, "ACKNOWLEDGED")
    if not updated:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    return {"status": "success", "alert_id": alert_id, "new_status": "ACKNOWLEDGED"}


@app.get("/api/v1/lineage/{dataset:path}", summary="Get lineage for dataset")
async def get_dataset_lineage(dataset: str) -> dict[str, Any]:
    """Get lineage graph and producers/consumers for a dataset."""
    lineage_data = storage.get_dataset_lineage(dataset)
    return lineage_data


@app.get("/api/v1/datasets", summary="List known datasets")
async def list_datasets() -> dict[str, Any]:
    """List all registered datasets and their latest schemas."""
    return {"datasets": storage.list_datasets()}


@app.get("/api/v1/stats", summary="Platform metadata statistics")
async def get_stats() -> dict[str, Any]:
    """Get summary counts of lineage events and alert states."""
    return storage.get_stats()


@app.get("/health", summary="Healthcheck")
async def health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "service": "lineage_webhook_service",
        "timestamp": datetime.now(UTC).isoformat(),
    }


@app.get("/metrics", summary="Prometheus metrics")
async def prometheus_metrics() -> Response:
    """Prometheus exposition format metrics for scrapers."""
    stats = storage.get_stats()
    lines = [
        "# HELP lineage_events_total Total OpenLineage events ingested",
        "# TYPE lineage_events_total counter",
        f"lineage_events_total {stats['total_events']}",
        "# HELP lineage_datasets_tracked Total unique datasets tracked in lineage graph",
        "# TYPE lineage_datasets_tracked gauge",
        f"lineage_datasets_tracked {stats['total_datasets']}",
        "# HELP lineage_active_alerts Current active metadata alerts count",
        "# TYPE lineage_active_alerts gauge",
        f"lineage_active_alerts {stats['active_alerts']}",
        "# HELP lineage_critical_alerts Current active critical alerts count",
        "# TYPE lineage_critical_alerts gauge",
        f"lineage_critical_alerts {stats['critical_alerts']}",
    ]
    return Response(content="\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    host = os.getenv("HOST", "0.0.0.0")
    logger.info("Starting Lineage Webhook Service on %s:%d", host, port)
    uvicorn.run(app, host=host, port=port)
