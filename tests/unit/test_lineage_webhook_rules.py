"""Hermetic tests proving alert-rules.yaml drives RuleEngine decisions (RF-20).

No network, no repo data: YAML rules and SQLite DBs live in tmp_path.
Module import is deferred until after chdir to tmp_path so the service's
module-level StorageManager() side effect never touches the repo.
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from pathlib import Path

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
    """A passRate BELOW the YAML threshold but ABOVE the old hardcoded 0.95 alerts."""
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
    """A FAIL event produces no alert when RF20-RULE-003 is disabled in YAML."""
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
