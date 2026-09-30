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
import uuid
from collections.abc import Iterator
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
    yaml = None  # type: ignore

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
        self._lock = threading.Lock()
        self._init_db()

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
        conn = self._get_connection()
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._lock:
            with self._connection() as conn:
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

    def get_dataset(self, dataset_key: str) -> dict[str, Any] | None:
        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT * FROM datasets WHERE dataset_key = ?",
                (dataset_key,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def upsert_dataset_schema(
        self,
        namespace: str,
        name: str,
        schema_dict: dict[str, Any] | None,
        run_id: str,
        job_name: str,
        event_time: str,
    ) -> tuple[bool, str | None, dict[str, Any] | None]:
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

    def insert_alert(self, alert: AlertItem) -> None:
        with self._lock:
            with self._connection() as conn:
                conn.execute(
                    """
                    INSERT INTO alerts (
                        alert_id, rule_id, rule_name, severity, dataset_name,
                        job_name, summary, description, details_json, created_at, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
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
                    ),
                )

    def is_recent_alert_duplicate(
        self, rule_id: str, identifier: str, window_seconds: int = 300
    ) -> bool:
        """Check if an identical alert was fired recently to avoid spam."""
        with self._connection() as conn:
            cursor = conn.execute(
                """
                SELECT created_at FROM alerts
                WHERE rule_id = ? AND (dataset_name = ? OR job_name = ?)
                ORDER BY created_at DESC LIMIT 1
                """,
                (rule_id, identifier, identifier),
            )
            row = cursor.fetchone()
            if not row:
                return False

            try:
                alert_time = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
                diff = (datetime.now(UTC) - alert_time).total_seconds()
                return diff < window_seconds
            except Exception:
                return False

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
            query += " AND dataset_name LIKE ?"
            params.append(f"%{dataset}%")

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
        """Fetch lineage connections: all jobs consuming or producing this dataset."""
        with self._connection() as conn:
            # Producers (where dataset in outputs)
            producers = conn.execute(
                """
                SELECT DISTINCT job_namespace, job_name, event_type, event_time, run_id
                FROM events
                WHERE outputs_json LIKE ?
                ORDER BY event_time DESC LIMIT 20
                """,
                (f"%{dataset_name}%",),
            ).fetchall()

            # Consumers (where dataset in inputs)
            consumers = conn.execute(
                """
                SELECT DISTINCT job_namespace, job_name, event_type, event_time, run_id
                FROM events
                WHERE inputs_json LIKE ?
                ORDER BY event_time DESC LIMIT 20
                """,
                (f"%{dataset_name}%",),
            ).fetchall()

            # Schema history
            schemas = conn.execute(
                """
                SELECT schema_hash, schema_json, captured_at, run_id
                FROM schema_history
                WHERE dataset_key LIKE ?
                ORDER BY captured_at DESC LIMIT 10
                """,
                (f"%{dataset_name}%",),
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
            active_alerts = conn.execute(
                "SELECT COUNT(*) as c FROM alerts WHERE status = 'ACTIVE'"
            ).fetchone()["c"]
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
                self._raw_config.get("defaults", {}).get("throttling", {}).get(
                    "deduplication_window_seconds", 300
                )
            )
        except (ValueError, TypeError, AttributeError):
            return 300

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

        # Persist triggered alerts (respecting deduplication)
        dedup_window = self._dedup_window_seconds()
        for alert in alerts:
            target_key = alert.dataset_name or alert.job_name or "unknown"
            if not self.storage.is_recent_alert_duplicate(alert.rule_id, target_key, dedup_window):
                self.storage.insert_alert(alert)
                logger.warning(
                    "TRIGGERED ALERT [%s - %s]: %s (target=%s)",
                    alert.severity,
                    alert.rule_name,
                    alert.summary,
                    target_key,
                )

        return alerts

    def _check_schema_changes(self, event: OpenLineageEvent) -> list[AlertItem]:
        """Rule 1: Detect schema modifications in Silver and Gold tables."""
        alerts: list[AlertItem] = []
        if not self._rule_enabled("RF20-RULE-001"):
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
        detect_added = bool(criteria.get("detect_added_columns", True))
        detect_removed = bool(criteria.get("detect_removed_columns", True))
        detect_types = bool(criteria.get("detect_type_changes", True))
        strict_mode = bool(criteria.get("strict_mode", False))
        datasets_to_check = (event.outputs or []) + (event.inputs or [])

        for ds in datasets_to_check:
            # Check pattern match
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
                old_fields = {f.get("name"): f.get("type") for f in old_schema.get("fields", [])}
                new_fields = {f.get("name"): f.get("type") for f in fields}

                added = set(new_fields.keys()) - set(old_fields.keys())
                removed = set(old_fields.keys()) - set(new_fields.keys())
                altered = {
                    col: {"old": old_fields[col], "new": new_fields[col]}
                    for col in set(old_fields.keys()) & set(new_fields.keys())
                    if old_fields[col] != new_fields[col]
                }

                diff_parts = []
                severity = "WARNING"
                if removed and detect_removed:
                    severity = removed_sev
                    diff_parts.append(f"Removed columns: {list(removed)}")
                if altered and detect_types:
                    severity = altered_sev
                    diff_parts.append(f"Type changed: {altered}")
                if added and detect_added:
                    if strict_mode:
                        severity = "CRITICAL"
                    elif severity == "WARNING":
                        severity = added_sev
                    diff_parts.append(f"Added columns: {list(added)}")
                if not diff_parts:
                    continue

                summary = f"Schema drift detected on {ds.name}"
                description = (
                    f"Dataset {ds.name} schema updated by job {event.job.name}. "
                    f"Details: " + "; ".join(diff_parts)
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
        dq_facet = run_facets.get("dataQuality") or run_facets.get("dataQualityMetrics")

        if dq_facet:
            pass_rate = dq_facet.get("passRate") or dq_facet.get("pass_rate")
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
        all_datasets = (event.inputs or []) + (event.outputs or [])
        for ds in all_datasets:
            if not ds.facets:
                continue
            assertions_facet = ds.facets.get("dataQualityAssertions")
            if not assertions_facet:
                continue

            assertions = assertions_facet.get("assertions", [])
            failed_assertions = [a for a in assertions if a.get("success") is False]

            if failed_assertions:
                critical_failed = [
                    a for a in failed_assertions
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
                    f"Job {event.job.name} (runId: {event.run.runId}) "
                    f"failed with state {event.eventType}. {error_msg}"
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
        """Rule 4: SLA freshness breach alert."""
        alerts: list[AlertItem] = []
        if not self._rule_enabled("RF20-RULE-004"):
            return alerts
        if event.eventType not in ("COMPLETE", "CHECK"):
            return alerts

        try:
            event_dt = datetime.fromisoformat(event.eventTime.replace("Z", "+00:00"))
            now_dt = datetime.now(UTC)
            lag_seconds = (now_dt - event_dt).total_seconds()
        except Exception:
            lag_seconds = 0.0

        # Check outputs SLA against SLA table
        for ds in (event.outputs or []):
            max_staleness, sla_tier = self._sla_lookup(ds.name)
            is_stream = sla_tier == "real_time" or ds.name.startswith("stream.")

            if lag_seconds > max_staleness:
                severity = self._sla_severity(sla_tier, max_staleness, is_stream)
                alerts.append(
                    AlertItem(
                        alert_id=str(uuid.uuid4()),
                        rule_id="RF20-RULE-004",
                        rule_name="SLAFreshnessBreachAlert",
                        severity=severity,
                        dataset_name=ds.name,
                        job_name=event.job.name,
                        summary=f"SLA Freshness breach on {ds.name} (lag: {lag_seconds:.1f}s)",
                        description=(
                            f"Dataset {ds.name} produced with lag of {lag_seconds:.1f}s, "
                            f"exceeding maximum SLA threshold of {max_staleness}s."
                        ),
                        details={
                            "dataset": ds.name,
                            "job": event.job.name,
                            "lag_seconds": lag_seconds,
                            "max_staleness_seconds": max_staleness,
                            "eventTime": event.eventTime,
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
async def lifespan(app: FastAPI):
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
    allow_origins=["*"],
    allow_credentials=True,
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
):
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
async def get_alert(alert_id: str):
    """Retrieve details for a single alert."""
    alert = storage.get_alert_by_id(alert_id)
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    return alert


@app.post("/api/v1/alerts/{alert_id}/acknowledge", summary="Acknowledge alert")
async def acknowledge_alert(alert_id: str):
    """Mark an active alert as ACKNOWLEDGED."""
    updated = storage.set_alert_status(alert_id, "ACKNOWLEDGED")
    if not updated:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")
    return {"status": "success", "alert_id": alert_id, "new_status": "ACKNOWLEDGED"}


@app.get("/api/v1/lineage/{dataset:path}", summary="Get lineage for dataset")
async def get_dataset_lineage(dataset: str):
    """Get lineage graph and producers/consumers for a dataset."""
    lineage_data = storage.get_dataset_lineage(dataset)
    return lineage_data


@app.get("/api/v1/datasets", summary="List known datasets")
async def list_datasets():
    """List all registered datasets and their latest schemas."""
    return {"datasets": storage.list_datasets()}


@app.get("/api/v1/stats", summary="Platform metadata statistics")
async def get_stats():
    """Get summary counts of lineage events and alert states."""
    return storage.get_stats()


@app.get("/health", summary="Healthcheck")
async def health():
    return {
        "status": "healthy",
        "service": "lineage_webhook_service",
        "timestamp": datetime.now(UTC).isoformat(),
    }


@app.get("/metrics", summary="Prometheus metrics")
async def prometheus_metrics():
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
