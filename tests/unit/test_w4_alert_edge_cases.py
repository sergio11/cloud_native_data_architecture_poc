"""W4 alert edge cases: zero passRate and malformed eventTime.

A pass rate of 0.0 is a TOTAL quality failure and must alert (CRITICAL per
severity_mapping); an unparsable eventTime must not fail open with lag 0.
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOD_NAME = "lineage.config.lineage_webhook_service"


def _load_service(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    return importlib.import_module(MOD_NAME)


def _write_rules(tmp_path: Path, body: str) -> str:
    p = tmp_path / "alert-rules.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


def _make_event(svc, **kwargs):
    base = {
        "eventType": "COMPLETE",
        "eventTime": "2026-09-27T00:00:00+00:00",
        "run": {"runId": "run-1", "facets": {}},
        "job": {"namespace": "default", "name": "job_a"},
        "inputs": [],
        "outputs": [],
    }
    base.update(kwargs)
    return svc.OpenLineageEvent(**base)


_QUALITY_RULES = """\
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
      min_pass_rate: 0.95
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
"""

_SLA_RULES = """\
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
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: true
    target:
      event_types: ["COMPLETE", "CHECK"]
    criteria:
      sla_definitions:
        - pattern: "^silver\\\\..*"
          max_staleness_seconds: 3600
          sla_tier: "tier_1"
    severity_mapping:
      tier_1_breach: "CRITICAL"
      tier_2_breach: "WARNING"
"""


def test_zero_pass_rate_triggers_critical_alert(monkeypatch, tmp_path):
    """passRate: 0.0 (falsy!) must still breach -> CRITICAL per severity_mapping."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(tmp_path, _QUALITY_RULES)
    storage = svc.StorageManager(db_path=str(tmp_path / "t1.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    event = _make_event(
        svc,
        run={"runId": "run-1", "facets": {"dataQuality": {"passRate": 0.0, "suiteName": "s"}}},
    )
    alerts = engine.evaluate(event)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-002"]
    assert alerts[0].severity == "CRITICAL"
    assert "0.00%" in alerts[0].summary


def test_zero_pass_rate_via_snake_case_alias_triggers(monkeypatch, tmp_path):
    """The ``pass_rate`` alias with 0.0 must breach too (None-chain, not or-chain)."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(tmp_path, _QUALITY_RULES)
    storage = svc.StorageManager(db_path=str(tmp_path / "t2.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    event = _make_event(
        svc,
        run={"runId": "run-2", "facets": {"dataQuality": {"pass_rate": 0.0, "suiteName": "s"}}},
    )
    alerts = engine.evaluate(event)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-002"]
    assert alerts[0].severity == "CRITICAL"


def test_missing_pass_rate_does_not_alert(monkeypatch, tmp_path):
    """A facet without any pass-rate key stays suppressed (None, not 0)."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(tmp_path, _QUALITY_RULES)
    storage = svc.StorageManager(db_path=str(tmp_path / "t3.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    event = _make_event(
        svc,
        run={"runId": "run-3", "facets": {"dataQuality": {"suiteName": "s"}}},
    )
    assert engine.evaluate(event) == []


def test_unparsable_event_time_fails_closed(monkeypatch, tmp_path):
    """Malformed eventTime => breach candidate (NOT silent lag=0 pass)."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(tmp_path, _SLA_RULES)
    storage = svc.StorageManager(db_path=str(tmp_path / "t4.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    event = _make_event(
        svc,
        eventTime="not-a-timestamp",
        outputs=[{"namespace": "default", "name": "silver.orders", "facets": {}}],
    )
    alerts = engine.evaluate(event)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-004"]
    assert alerts[0].severity == "CRITICAL"
    assert alerts[0].details["event_time_error"].startswith("unparsable eventTime")
    assert alerts[0].details["lag_seconds"] is None
    assert "fail-closed" in alerts[0].description


def test_naive_event_time_is_treated_as_utc(monkeypatch, tmp_path):
    """Offset-less timestamps no longer zero the lag: interpreted as UTC."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(tmp_path, _SLA_RULES)
    storage = svc.StorageManager(db_path=str(tmp_path / "t5.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    event = _make_event(
        svc,
        eventTime="2020-01-01T00:00:00",  # naive timestamp, no offset
        outputs=[{"namespace": "default", "name": "silver.orders", "facets": {}}],
    )
    alerts = engine.evaluate(event)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-004"]
    assert alerts[0].details["lag_seconds"] > 3600
    assert alerts[0].details["event_time_error"] is None


def test_fresh_event_time_does_not_alert(monkeypatch, tmp_path):
    """A valid, recent eventTime inside the SLA window stays quiet (fail-closed
    only applies when freshness cannot be proven)."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(tmp_path, _SLA_RULES)
    storage = svc.StorageManager(db_path=str(tmp_path / "t6.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    now = datetime.now(UTC).isoformat()
    event = _make_event(
        svc,
        eventTime=now,
        outputs=[{"namespace": "default", "name": "silver.orders", "facets": {}}],
    )
    assert engine.evaluate(event) == []
