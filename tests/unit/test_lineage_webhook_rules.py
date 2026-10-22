"""Hermetic tests proving alert-rules.yaml drives RuleEngine decisions.

No network, no repo data: YAML rules and SQLite DBs live in tmp_path.
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOD_NAME = "lineage.config.lineage_webhook_service"


def _load_service(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # contain module-level lineage_events.db
    return importlib.import_module(MOD_NAME)


def _write_rules(tmp_path: Path, body: str) -> str:
    p = tmp_path / "alert-rules.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


def _make_event(svc, **kwargs):
    now = "2026-09-27T00:00:00+00:00"
    base = {
        "eventType": "COMPLETE",
        "eventTime": now,
        "run": {"runId": "run-1", "facets": {}},
        "job": {"namespace": "default", "name": "job_a"},
        "inputs": [],
        "outputs": [],
    }
    base.update(kwargs)
    return svc.OpenLineageEvent(**base)


def test_yaml_min_pass_rate_drives_quality_alert(monkeypatch, tmp_path):
    """A passRate below the YAML threshold still alerts."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(
        tmp_path,
        """\
        version: "1.0.0"
        defaults:
          throttling:
            deduplication_window_seconds: 300
        rules:
          - id: "RF20-RULE-001"
            name: "SchemaChangeAlert"
            enabled: false
          - id: "RF20-RULE-002"
            name: "QualityThresholdBreachAlert"
            enabled: true
            criteria:
              min_pass_rate: 0.99
              fail_on_critical_assertion: true
              critical_severities: ["critical"]
            severity_mapping:
              pass_rate_below_0_90: "CRITICAL"
              pass_rate_below_0_95: "WARNING"
              critical_assertion_failed: "CRITICAL"
          - id: "RF20-RULE-003"
            name: "PipelineFailureAlert"
            enabled: false
          - id: "RF20-RULE-004"
            name: "SLAFreshnessBreachAlert"
            enabled: false
        """,
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "t1.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    assert engine._rule_enabled("RF20-RULE-002") is True

    event = _make_event(
        svc,
        run={"runId": "run-1", "facets": {"dataQuality": {"passRate": 0.97, "suiteName": "s"}}},
    )
    alerts = engine.evaluate(event)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-002"]


def test_disabled_rule_does_not_alert(monkeypatch, tmp_path):
    """A FAIL event produces no alert when the pipeline-failure rule is disabled in YAML."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(
        tmp_path,
        """\
        version: "1.0.0"
        defaults:
          throttling:
            deduplication_window_seconds: 300
        rules:
          - id: "RF20-RULE-001"
            name: "SchemaChangeAlert"
            enabled: false
          - id: "RF20-RULE-002"
            name: "QualityThresholdBreachAlert"
            enabled: false
          - id: "RF20-RULE-003"
            name: "PipelineFailureAlert"
            enabled: false
            criteria:
              terminal_states: ["FAIL", "ABORT"]
          - id: "RF20-RULE-004"
            name: "SLAFreshnessBreachAlert"
            enabled: false
        """,
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "t2.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    assert engine._rule_enabled("RF20-RULE-003") is False

    event = _make_event(svc, eventType="FAIL")
    assert engine.evaluate(event) == []


def test_yaml_exclude_job_pattern_suppresses_failure_alert(monkeypatch, tmp_path):
    """exclude_job_patterns loaded from YAML suppresses the failure alert."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(
        tmp_path,
        """\
        version: "1.0.0"
        rules:
          - id: "RF20-RULE-001"
            name: "SchemaChangeAlert"
            enabled: false
          - id: "RF20-RULE-002"
            name: "QualityThresholdBreachAlert"
            enabled: false
          - id: "RF20-RULE-003"
            name: "PipelineFailureAlert"
            enabled: true
            criteria:
              terminal_states: ["FAIL", "ABORT"]
              exclude_job_patterns: ["^test_.*"]
          - id: "RF20-RULE-004"
            name: "SLAFreshnessBreachAlert"
            enabled: false
        """,
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "t3.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    excluded = _make_event(svc, eventType="FAIL", job={"namespace": "default", "name": "test_smoke"})
    assert engine.evaluate(excluded) == []

    real = _make_event(svc, eventType="FAIL", job={"namespace": "default", "name": "nightly_build"})
    alerts = engine.evaluate(real)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-003"]


def _dedup_alert(svc: Any, alert_id: str, rule_id: str = "RF20-RULE-002", **fields: Any) -> Any:
    base: dict[str, Any] = {
        "alert_id": alert_id,
        "rule_id": rule_id,
        "rule_name": "QualityThresholdBreachAlert",
        "severity": "WARNING",
        "dataset_name": None,
        "job_name": None,
        "summary": f"alert {alert_id}",
        "description": "d",
        "details": {},
        "created_at": datetime.now(UTC).isoformat(),
    }
    base.update(fields)
    return svc.AlertItem(**base)


def test_dedup_identity_is_rule_plus_dataset_plus_job_tuple(monkeypatch: Any, tmp_path: Path) -> None:
    """Dedup identity is the (rule_id, dataset_name, job_name) tuple, per column.

    Guards the M4 fix: a job_name matching ANOTHER alert's dataset_name must
    not silence it (no cross-identity ``OR``), while an exact-tuple repeat
    still deduplicates. Alerts with neither dataset nor job share the
    "unknown/unknown" identity (accepted limitation, documented in
    ``StorageManager.insert_alert_if_new``).
    """
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "dedup_tuple.db"))

    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-1", dataset_name="shared", job_name="job_1")) is True
    # Same rule, job_name == first alert's dataset_name: different tuple -> NOT a duplicate.
    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-2", dataset_name="other", job_name="shared")) is True
    # Exact tuple repeat -> duplicate, suppressed.
    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-3", dataset_name="shared", job_name="job_1")) is False
    # Same tuple but different rule -> NOT a duplicate.
    assert (
        storage.insert_alert_if_new(
            _dedup_alert(svc, "m4-4", rule_id="RF20-RULE-003", dataset_name="shared", job_name="job_1")
        )
        is True
    )
    # Job-only alerts deduplicate against each other (NULL dataset -> "unknown" per column).
    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-5", job_name="job_x")) is True
    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-6", job_name="job_x")) is False
    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-7", job_name="job_y")) is True
    # Neither dataset nor job: shared "unknown/unknown" identity (accepted limitation).
    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-8")) is True
    assert storage.insert_alert_if_new(_dedup_alert(svc, "m4-9")) is False
    assert storage.get_stats()["total_alerts"] == 6
