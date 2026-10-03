"""W4 alert-rules.yaml wiring: target gates, nullability detection, honest config.

Every behavioural key the YAML declares must actually steer RuleEngine
decisions; keys that no code reads must not exist in the shipped file.
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
REAL_RULES = REPO_ROOT / "lineage" / "config" / "alert-rules.yaml"


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


def _ds(svc, name: str, fields: list[dict]):
    return svc.Dataset(
        namespace="default",
        name=name,
        facets={"schema": {"fields": fields}},
    )


def _engine(svc, tmp_path, body: str, db_name: str):
    rules_path = _write_rules(tmp_path, body)
    storage = svc.StorageManager(db_path=str(tmp_path / db_name))
    return svc.RuleEngine(storage, rules_path=rules_path)


def _schema_rules(criteria_extra: str = "", sev_extra: str = "", target_extra: str = ""):
    return f"""\
version: "1.0.0"
defaults:
  throttling:
    deduplication_window_seconds: 300
rules:
  - id: "RF20-RULE-001"
    name: "SchemaChangeAlert"
    enabled: true
    target:
{target_extra}      dataset_pattern: "^(silver|gold)\\\\..*"
    criteria:
{criteria_extra}      detect_added_columns: true
      detect_removed_columns: true
      detect_type_changes: true
    severity_mapping:
{sev_extra}      removed_column: "CRITICAL"
      type_altered: "CRITICAL"
      added_column: "WARNING"
  - id: "RF20-RULE-002"
    name: "QualityThresholdBreachAlert"
    enabled: false
  - id: "RF20-RULE-003"
    name: "PipelineFailureAlert"
    enabled: false
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: false
"""


def test_rule1_event_types_gate_blocks_untargeted_events(monkeypatch, tmp_path):
    """target.event_types steers Rule 1: a FAIL event neither alerts nor
    consumes the schema change."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(
        svc,
        tmp_path,
        _schema_rules(target_extra='      event_types: ["COMPLETE"]\n'),
        "c1.db",
    )

    established = _make_event(svc, outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "INTEGER"}])])
    assert engine.evaluate(established) == []

    gated = _make_event(
        svc,
        eventType="FAIL",
        run={"runId": "run-2", "facets": {}},
        outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "BIGINT"}])],
    )
    assert engine.evaluate(gated) == []  # FAIL not in target.event_types

    detected = _make_event(
        svc,
        run={"runId": "run-3", "facets": {}},
        outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "BIGINT"}])],
    )
    alerts = engine.evaluate(detected)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-001"]
    assert alerts[0].severity == "CRITICAL"  # type_altered


def test_rule1_event_types_absent_keeps_legacy_behaviour(monkeypatch, tmp_path):
    """Without target.event_types the rule behaves as before (any event type)."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(svc, tmp_path, _schema_rules(), "c2.db")

    established = _make_event(svc, outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "INTEGER"}])])
    assert engine.evaluate(established) == []

    gated = _make_event(
        svc,
        eventType="FAIL",
        run={"runId": "run-2", "facets": {}},
        outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "BIGINT"}])],
    )
    alerts = engine.evaluate(gated)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-001"]


def test_nullability_change_uses_severity_mapping(monkeypatch, tmp_path):
    """detect_nullability_changes + severity_mapping.nullability_changed wire up."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(
        svc,
        tmp_path,
        _schema_rules(
            criteria_extra="      detect_nullability_changes: true\n",
            sev_extra='      nullability_changed: "CRITICAL"\n',
        ),
        "c3.db",
    )

    established = _make_event(
        svc,
        outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "INTEGER", "nullable": True}])],
    )
    assert engine.evaluate(established) == []

    flipped = _make_event(
        svc,
        run={"runId": "run-2", "facets": {}},
        outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "INTEGER", "nullable": False}])],
    )
    alerts = engine.evaluate(flipped)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-001"]
    assert alerts[0].severity == "CRITICAL"  # from severity_mapping.nullability_changed
    assert alerts[0].details["nullability_changes"] == {"id": {"old": True, "new": False}}


def test_nullability_change_skipped_when_disabled(monkeypatch, tmp_path):
    """detect_nullability_changes: false suppresses a nullability-only drift."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(
        svc,
        tmp_path,
        _schema_rules(criteria_extra="      detect_nullability_changes: false\n"),
        "c4.db",
    )

    established = _make_event(
        svc,
        outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "INTEGER", "nullable": True}])],
    )
    assert engine.evaluate(established) == []

    flipped = _make_event(
        svc,
        run={"runId": "run-2", "facets": {}},
        outputs=[_ds(svc, "silver.orders", [{"name": "id", "type": "INTEGER", "nullable": False}])],
    )
    assert engine.evaluate(flipped) == []


def test_rule4_event_types_gate_and_default(monkeypatch, tmp_path):
    """Rule 4 reads target.event_types; without it the legacy COMPLETE/CHECK
    default applies (CHECK allowed, START blocked)."""
    svc = _load_service(monkeypatch, tmp_path)
    gated_rules = """\
version: "1.0.0"
rules:
  - id: "RF20-RULE-001"
    enabled: false
  - id: "RF20-RULE-002"
    enabled: false
  - id: "RF20-RULE-003"
    enabled: false
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: true
    target:
      event_types: ["COMPLETE"]
    criteria:
      sla_definitions:
        - pattern: "^silver\\\\..*"
          max_staleness_seconds: 3600
          sla_tier: "tier_1"
    severity_mapping:
      tier_1_breach: "CRITICAL"
"""
    engine = _engine(svc, tmp_path, gated_rules, "c5.db")
    stale = dict(eventTime="2020-01-01T00:00:00+00:00")
    outputs = [{"namespace": "default", "name": "silver.orders", "facets": {}}]

    check_event = _make_event(svc, eventType="CHECK", run={"runId": "r1", "facets": {}}, outputs=outputs, **stale)
    assert engine.evaluate(check_event) == []  # CHECK not in target.event_types

    complete_event = _make_event(svc, eventType="COMPLETE", run={"runId": "r2", "facets": {}}, outputs=outputs, **stale)
    alerts = engine.evaluate(complete_event)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-004"]

    # Legacy default (no target.event_types): CHECK allowed, START blocked.
    default_rules = gated_rules.replace('    target:\n      event_types: ["COMPLETE"]\n', "")
    engine2 = _engine(svc, tmp_path, default_rules, "c5b.db")
    check2 = _make_event(svc, eventType="CHECK", run={"runId": "r3", "facets": {}}, outputs=outputs, **stale)
    assert [a.rule_id for a in engine2.evaluate(check2)] == ["RF20-RULE-004"]
    start2 = _make_event(svc, eventType="START", run={"runId": "r4", "facets": {}}, outputs=outputs, **stale)
    assert engine2.evaluate(start2) == []


def test_shipped_alert_rules_yaml_is_honest(monkeypatch, tmp_path):
    """Wired keys exist; dead keys were deleted from the shipped config."""
    import yaml

    svc = _load_service(monkeypatch, tmp_path)  # noqa: F841  (ensures import side effects)
    cfg = yaml.safe_load(REAL_RULES.read_text(encoding="utf-8"))
    rules = {r["id"]: r for r in cfg["rules"]}

    # Wired keys
    for rule_id, rule in rules.items():
        assert "event_types" in rule.get("target", {}), rule_id
        assert "namespace" in rule.get("target", {}), rule_id
        webhook = rule.get("actions", {}).get("webhook", {})
        assert webhook.get("endpoint"), rule_id
        assert set(webhook) <= {"endpoint", "retry_count"}, rule_id
    criteria1 = rules["RF20-RULE-001"]["criteria"]
    assert criteria1["detect_nullability_changes"] is True
    assert "nullability_changed" in rules["RF20-RULE-001"]["severity_mapping"]
    criteria3 = rules["RF20-RULE-003"]["criteria"]
    assert criteria3["terminal_states"] == ["FAIL", "ABORT"]
    assert criteria3["exclude_job_patterns"]

    # Deleted keys (no code reads them)
    assert "evaluation_interval_seconds" not in cfg
    assert "notification_channels" not in cfg.get("defaults", {})
    assert "environment" not in cfg.get("defaults", {})
    for rule in rules.values():
        assert "type" not in rule.get("criteria", {}), rule["id"]
        assert "annotations" not in rule.get("actions", {}), rule["id"]
    assert "dataset_pattern" not in rules["RF20-RULE-003"]["target"]
    assert "severity_mapping" not in rules["RF20-RULE-003"]  # medallion_core/streaming_core/auxiliary
    assert "medallion_core" not in REAL_RULES.read_text(encoding="utf-8")
