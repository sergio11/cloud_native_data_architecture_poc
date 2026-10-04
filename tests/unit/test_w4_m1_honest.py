"""M1 honest measurement: unit coverage for lineage_webhook_service uncovered routes.

Follows tests/unit/test_w4_*.py patterns: hermetic (tmp_path DB + rules),
module import deferred until after chdir so the module-level
StorageManager() never touches the repo.
"""

from __future__ import annotations

import importlib
import sys
import textwrap
import urllib.error
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

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


_ALL_DISABLED = """\
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
    enabled: false
"""


def _alert(svc, alert_id: str, **kw: Any):
    base: dict[str, Any] = {
        "alert_id": alert_id,
        "rule_id": "RF20-RULE-002",
        "rule_name": "QualityThresholdBreachAlert",
        "severity": "WARNING",
        "dataset_name": None,
        "job_name": "job_a",
        "summary": f"alert {alert_id}",
        "description": "d",
        "details": {},
        "created_at": datetime.now(UTC).isoformat(),
    }
    base.update(kw)
    return svc.AlertItem(**base)


def _event(svc, **kw: Any):
    base: dict[str, Any] = {
        "eventType": "COMPLETE",
        "eventTime": "2026-09-27T00:00:00+00:00",
        "run": {"runId": "run-1", "facets": {}},
        "job": {"namespace": "default", "name": "job_a"},
        "inputs": [],
        "outputs": [],
    }
    base.update(kw)
    return svc.OpenLineageEvent(**base)


# --------------------------------------------------------------------------
# StorageManager query paths
# --------------------------------------------------------------------------
def test_m1_like_escape_backslash(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    assert svc._like_escape("a\\b%c_d") == "a\\\\b\\%c\\_d"


def test_m1_upsert_metadata_only_path(monkeypatch, tmp_path):
    """Second upsert with identical schema hits the metadata-only UPDATE."""
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "m1.db"))
    changed, _, _ = storage.upsert_dataset_schema(
        namespace="default",
        name="silver.orders",
        schema_dict={"fields": [{"name": "id", "type": "INTEGER"}]},
        run_id="r1",
        job_name="j",
        event_time="2026-09-27T00:00:00+00:00",
    )
    assert changed is True
    changed2, old2, _ = storage.upsert_dataset_schema(
        namespace="default",
        name="silver.orders",
        schema_dict={"fields": [{"name": "id", "type": "INTEGER"}]},
        run_id="r2",
        job_name="j",
        event_time="2026-09-27T00:00:00+00:00",
    )
    assert changed2 is False
    assert old2 is None
    # Empty schema_dict path (schema_json "{}")
    changed3, _, _ = storage.upsert_dataset_schema(
        namespace="default",
        name="bronze.raw",
        schema_dict=None,
        run_id="r3",
        job_name="j",
        event_time="2026-09-27T00:00:00+00:00",
    )
    assert changed3 is True


def test_m1_dedup_window_unparsable_and_naive(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    # Unparsable timestamp -> treated as outside window (allow insert)
    assert svc.StorageManager._within_dedup_window("not-a-date", 300) is False
    assert svc.StorageManager._within_dedup_window(None, 300) is False
    # Naive timestamp gets UTC assumed; recent naive -> inside window
    naive = datetime.now(UTC).replace(tzinfo=None).isoformat()
    assert svc.StorageManager._within_dedup_window(naive, 300) is True
    old = (datetime.now(UTC) - timedelta(hours=2)).isoformat()
    assert svc.StorageManager._within_dedup_window(old, 300) is False


def test_m1_query_alerts_all_filters(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "q.db"))
    storage.insert_alert_if_new(
        _alert(
            svc,
            "q-1",
            severity="CRITICAL",
            rule_id="RF20-RULE-003",
            dataset_name="silver.orders",
            job_name="job_a",
            status="ACTIVE",
        )
    )
    storage.insert_alert_if_new(
        _alert(
            svc,
            "q-2",
            severity="WARNING",
            rule_id="RF20-RULE-002",
            dataset_name="gold.sales",
            job_name="job_b",
            status="ACKNOWLEDGED",
        )
    )
    assert len(storage.query_alerts(severity="critical")) == 1
    assert len(storage.query_alerts(rule_id="RF20-RULE-002")) == 1
    assert len(storage.query_alerts(status="active")) == 1
    assert len(storage.query_alerts(dataset="gold")) == 1
    combo = storage.query_alerts(
        severity="WARNING", rule_id="RF20-RULE-002", status="ACKNOWLEDGED", dataset="gold", limit=10, offset=0
    )
    assert [a["alert_id"] for a in combo] == ["q-2"]
    assert storage.query_alerts(limit=1, offset=1)
    # details JSON round-trips
    assert combo[0]["details"] == {}


def test_m1_get_alert_and_set_status(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "a.db"))
    assert storage.get_alert_by_id("missing") is None
    assert storage.set_alert_status("missing", "ACKNOWLEDGED") is False
    storage.insert_alert_if_new(_alert(svc, "a-1"))
    got = storage.get_alert_by_id("a-1")
    assert got is not None and got["alert_id"] == "a-1"
    assert storage.set_alert_status("a-1", "acknowledged") is True
    assert storage.get_alert_by_id("a-1")["status"] == "ACKNOWLEDGED"


def test_m1_list_datasets_and_stats(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "s.db"))
    assert storage.list_datasets() == []
    storage.upsert_dataset_schema(
        namespace="default",
        name="silver.orders",
        schema_dict={"fields": []},
        run_id="r1",
        job_name="j",
        event_time="2026-09-27T00:00:00+00:00",
    )
    assert len(storage.list_datasets()) == 1
    storage.insert_alert_if_new(_alert(svc, "s-1", severity="CRITICAL", status="ACTIVE"))
    storage.insert_alert_if_new(_alert(svc, "s-2", severity="WARNING", status="RESOLVED", job_name="other"))
    stats = storage.get_stats()
    assert stats["total_datasets"] == 1
    assert stats["total_alerts"] == 2
    assert stats["active_alerts"] == 1
    assert stats["critical_alerts"] == 1


def test_m1_insert_event_persists(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "e.db"))
    ev = _event(svc)
    storage.insert_event(ev, ev.model_dump_json())
    assert storage.get_stats()["total_events"] == 1


# --------------------------------------------------------------------------
# Outbound webhook helper
# --------------------------------------------------------------------------
def test_m1_post_webhook_success_and_failure(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)

    class _Resp:
        def __init__(self, status):
            self.status = status

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def getcode(self):
            return self.status

    calls: list[str] = []

    def ok(request, timeout=None):
        calls.append(request.full_url)
        return _Resp(200)

    monkeypatch.setattr(svc.urllib.request, "urlopen", ok)
    assert svc._post_alert_webhook("http://x.test/hook", {"a": 1}) is True

    def bad_status(request, timeout=None):
        return _Resp(500)

    monkeypatch.setattr(svc.urllib.request, "urlopen", bad_status)
    assert svc._post_alert_webhook("http://x.test/hook", {"a": 1}, attempts=2) is False

    def boom(request, timeout=None):
        raise urllib.error.URLError("down")

    monkeypatch.setattr(svc.urllib.request, "urlopen", boom)
    assert svc._post_alert_webhook("http://x.test/hook", {"a": 1}, attempts=2) is False
    # attempts=0 normalizes to 1 attempt
    assert svc._post_alert_webhook("http://x.test/hook", {"a": 1}, attempts=0) is False


# --------------------------------------------------------------------------
# RuleEngine wiring / fallbacks
# --------------------------------------------------------------------------
def test_m1_load_rules_fallback_and_bad_yaml(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "f.db"))
    # Missing file -> fallback defaults
    engine = svc.RuleEngine(storage, rules_path=str(tmp_path / "nope.yaml"))
    assert len(engine.rules) == 4
    assert engine._rule_enabled("UNKNOWN-RULE") is True
    assert engine._dedup_window_seconds() == 300
    # Unparsable YAML -> fallback defaults
    bad = tmp_path / "bad.yaml"
    bad.write_text("::: not yaml ::: [", encoding="utf-8")
    engine2 = svc.RuleEngine(storage, rules_path=str(bad))
    assert len(engine2.rules) == 4
    # Bad dedup window type -> 300
    rules_path = _write_rules(
        tmp_path, _ALL_DISABLED.replace("deduplication_window_seconds: 300", "deduplication_window_seconds: [bad]")
    )
    engine3 = svc.RuleEngine(storage, rules_path=rules_path)
    assert engine3._dedup_window_seconds() == 300


def test_m1_target_gates_invalid_regex(monkeypatch, tmp_path):
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
    enabled: true
    target:
      namespace: "[invalid"
      dataset_pattern: "[invalid"
      event_types: ["COMPLETE"]
  - id: "RF20-RULE-002"
    name: "QualityThresholdBreachAlert"
    enabled: false
  - id: "RF20-RULE-003"
    name: "PipelineFailureAlert"
    enabled: true
    target:
      namespace: "[bad"
    criteria:
      terminal_states: ["FAIL"]
      exclude_job_patterns: ["[bad", "^skip_.*"]
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: false
""",
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "g.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    # Invalid regexes are ignored (allow all)
    assert engine._namespace_allowed("RF20-RULE-001", "anything") is True
    assert engine._dataset_allowed("RF20-RULE-001", "anything") is True
    # Invalid terminal-state handling + invalid exclude pattern skipped
    ev = _event(svc, eventType="FAIL", job={"namespace": "default", "name": "job_a"})
    alerts = engine.evaluate(ev)
    assert [a.rule_id for a in alerts] == ["RF20-RULE-003"]
    skipped = _event(svc, eventType="FAIL", job={"namespace": "default", "name": "skip_me"})
    assert engine.evaluate(skipped) == []


def test_m1_webhook_attempts_and_missing_endpoint(monkeypatch, tmp_path):
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
    enabled: true
    actions:
      webhook:
        retry_count: "not-a-number"
  - id: "RF20-RULE-003"
    name: "PipelineFailureAlert"
    enabled: false
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: false
""",
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "w.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    assert engine._webhook_attempts("RF20-RULE-002") == 1
    assert engine._webhook_endpoint("RF20-RULE-002") is None
    alert = _alert(svc, "w-1", rule_id="RF20-RULE-002")
    assert engine._deliver_alert_webhook(alert) is False


def test_m1_sla_lookup_and_severity(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "sla.db"))
    # No YAML -> legacy table
    engine = svc.RuleEngine(storage, rules_path=str(tmp_path / "missing.yaml"))
    assert engine._sla_lookup("stream.orders") == (5.0, "real_time")
    assert engine._sla_lookup("bronze.raw") == (3600.0, "tier_2")
    assert engine._sla_lookup("gold.mart") == (86400.0, "tier_1")
    assert engine._sla_lookup("other") == (3600.0, "tier_2")
    assert engine._sla_severity("real_time", 5.0, True) == "CRITICAL"
    assert engine._sla_severity("tier_1", 86400.0, False) == "CRITICAL"
    # YAML sla_definitions incl. invalid regex entry (skipped)
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
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: true
    criteria:
      sla_definitions:
        - pattern: "[bad"
          max_staleness_seconds: 10
          sla_tier: "real_time"
        - pattern: "^silver\\\\."
          max_staleness_seconds: 60
          sla_tier: "tier_2"
    severity_mapping:
      tier_2_breach: "CRITICAL"
""",
    )
    engine2 = svc.RuleEngine(storage, rules_path=rules_path)
    assert engine2._sla_lookup("silver.orders") == (60.0, "tier_2")
    assert engine2._sla_lookup("nope") == (3600.0, "tier_2")
    assert engine2._sla_severity("tier_2", 60.0, False) == "CRITICAL"


def test_m1_sla_unparsable_event_time_is_breach(monkeypatch, tmp_path):
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
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: true
""",
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "slab.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    ev = _event(svc, eventTime="not-a-date", outputs=[{"namespace": "default", "name": "silver.orders", "facets": {}}])
    alerts = engine.evaluate(ev)
    assert len(alerts) == 1
    assert alerts[0].rule_id == "RF20-RULE-004"
    assert alerts[0].details["event_time_error"] is not None


def test_m1_pipeline_failure_error_facet(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(
        tmp_path,
        _ALL_DISABLED.replace(
            '"PipelineFailureAlert"\n    enabled: false', '"PipelineFailureAlert"\n    enabled: true'
        ),
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "pf.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    ev = _event(svc, eventType="FAIL", run={"runId": "r1", "facets": {"errorMessage": {"message": "boom"}}})
    alerts = engine.evaluate(ev)
    assert len(alerts) == 1 and "boom" in alerts[0].description


def test_m1_quality_zero_pass_rate_and_assertions(monkeypatch, tmp_path):
    """passRate 0.0 is falsy but must still alert; assertion path covered."""
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
      min_pass_rate: 0.95
  - id: "RF20-RULE-003"
    name: "PipelineFailureAlert"
    enabled: false
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: false
""",
    )
    storage = svc.StorageManager(db_path=str(tmp_path / "qq.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    ev = _event(svc, run={"runId": "r1", "facets": {"dataQuality": {"passRate": 0.0, "suiteName": "s"}}})
    assert len(engine.evaluate(ev)) == 1
    ev2 = _event(
        svc,
        run={"runId": "r2", "facets": {}},
        outputs=[
            {
                "namespace": "default",
                "name": "silver.t",
                "facets": {
                    "dataQualityAssertions": {
                        "assertions": [
                            {"success": False, "meta": {"severity": "critical"}},
                            {"success": True},
                        ]
                    }
                },
            }
        ],
    )
    alerts2 = engine.evaluate(ev2)
    assert len(alerts2) == 1 and alerts2[0].severity == "CRITICAL"


# --------------------------------------------------------------------------
# FastAPI endpoints (covers ingest/asdict/health/metrics/stats/datasets)
# --------------------------------------------------------------------------
def test_m1_endpoints_health_metrics_stats(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    from fastapi.testclient import TestClient

    storage = svc.StorageManager(db_path=str(tmp_path / "api.db"))
    monkeypatch.setattr(svc, "storage", storage)
    monkeypatch.setattr(svc, "rule_engine", svc.RuleEngine(storage, rules_path=str(tmp_path / "no.yaml")))
    client = TestClient(svc.app)
    assert client.get("/health").status_code == 200
    assert client.get("/metrics").status_code == 200
    assert "lineage_events_total" in client.get("/metrics").text
    assert client.get("/api/v1/stats").status_code == 200
    assert client.get("/api/v1/datasets").status_code == 200
    assert client.get("/api/v1/lineage/silver.orders").status_code == 200
    assert client.get("/api/v1/alerts/missing-id").status_code == 404
    assert client.post("/api/v1/alerts/missing-id/acknowledge").status_code == 404


def test_m1_endpoints_ingest_and_alert_flow(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    from fastapi.testclient import TestClient

    rules_path = _write_rules(tmp_path, _ALL_DISABLED)
    storage = svc.StorageManager(db_path=str(tmp_path / "ing.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)
    monkeypatch.setattr(svc, "storage", storage)
    monkeypatch.setattr(svc, "rule_engine", engine)
    client = TestClient(svc.app)
    payload = {
        "eventType": "COMPLETE",
        "eventTime": "2026-09-27T00:00:00+00:00",
        "run": {"runId": "run-web-1", "facets": {}},
        "job": {"namespace": "default", "name": "job_web"},
        "inputs": [],
        "outputs": [],
    }
    resp = client.post("/api/v1/lineage", json=payload)
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "accepted"
    assert svc.asdict_alert(_alert(svc, "cover-1"))["alert_id"] == "cover-1"
    # alerts listing + acknowledge round-trip
    storage.insert_alert_if_new(_alert(svc, "web-1"))
    got = client.get("/api/v1/alerts")
    assert got.status_code == 200 and got.json()["count"] >= 1
    ack = client.post("/api/v1/alerts/web-1/acknowledge")
    assert ack.status_code == 200
    one = client.get("/api/v1/alerts/web-1")
    assert one.status_code == 200 and one.json()["status"] == "ACKNOWLEDGED"


def test_m1_lifespan_runs(monkeypatch, tmp_path):
    import asyncio

    svc = _load_service(monkeypatch, tmp_path)
    asyncio.run(svc.lifespan(svc.app).__aenter__())
