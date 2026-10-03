"""W4 outbound alert webhook delivery: POST payload, failure isolation, env gate.

Delivery is best-effort: every path must leave the alert itself persisted and
must never raise into the ingestion flow.
"""

from __future__ import annotations

import importlib
import json
import sys
import textwrap
import urllib.error
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOD_NAME = "lineage.config.lineage_webhook_service"

ENDPOINT = "http://alerts.example.test/api/v1/alerts"

_RULES_WITH_WEBHOOK = """\
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
    severity_mapping:
      pass_rate_below_0_90: "CRITICAL"
      pass_rate_below_0_95: "WARNING"
      critical_assertion_failed: "CRITICAL"
    actions:
      webhook:
        endpoint: "http://alerts.example.test/api/v1/alerts"
        retry_count: 0
  - id: "RF20-RULE-003"
    name: "PipelineFailureAlert"
    enabled: false
  - id: "RF20-RULE-004"
    name: "SLAFreshnessBreachAlert"
    enabled: false
"""

_RULES_NO_WEBHOOK = _RULES_WITH_WEBHOOK.replace(
    """    actions:
      webhook:
        endpoint: "http://alerts.example.test/api/v1/alerts"
        retry_count: 0
""",
    "",
)


def _load_service(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    return importlib.import_module(MOD_NAME)


def _write_rules(tmp_path: Path, body: str) -> str:
    p = tmp_path / "alert-rules.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


def _quality_event(svc, run_id: str = "run-1"):
    return svc.OpenLineageEvent(
        eventType="COMPLETE",
        eventTime="2026-09-27T00:00:00+00:00",
        run={"runId": run_id, "facets": {"dataQuality": {"passRate": 0.5, "suiteName": "s"}}},
        job={"namespace": "default", "name": "job_a"},
        inputs=[],
        outputs=[],
    )


class _FakeResponse:
    def __init__(self, status: int = 200):
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def getcode(self):
        return self.status

    def close(self):
        pass


def _record_urlopen(calls, status: int = 200):
    def fake_urlopen(request, timeout=None):
        calls.append(
            {
                "url": request.full_url,
                "method": request.get_method(),
                "data": request.data,
                "timeout": timeout,
                "content_type": request.get_header("Content-type"),
            }
        )
        return _FakeResponse(status)

    return fake_urlopen


def _engine(svc, tmp_path, rules_body: str, db_name: str):
    rules_path = _write_rules(tmp_path, rules_body)
    storage = svc.StorageManager(db_path=str(tmp_path / db_name))
    return svc.RuleEngine(storage, rules_path=rules_path)


def test_webhook_posted_with_alert_payload(monkeypatch, tmp_path):
    """A fired alert is POSTed as JSON to the rule's actions.webhook.endpoint."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(svc, tmp_path, _RULES_WITH_WEBHOOK, "w1.db")

    calls: list[dict] = []
    monkeypatch.setattr(svc.urllib.request, "urlopen", _record_urlopen(calls))

    alerts = engine.evaluate(_quality_event(svc))
    assert len(alerts) == 1
    assert len(calls) == 1

    call = calls[0]
    assert call["method"] == "POST"
    assert call["url"] == ENDPOINT
    assert call["timeout"] == svc.WEBHOOK_TIMEOUT_SECONDS
    assert call["content_type"] == "application/json"

    payload = json.loads(call["data"])
    assert payload["alert_id"] == alerts[0].alert_id
    assert payload["rule_id"] == "RF20-RULE-002"
    assert payload["rule_name"] == "QualityThresholdBreachAlert"
    assert payload["severity"] == "CRITICAL"
    assert payload["job_name"] == "job_a"
    assert payload["summary"]
    assert payload["details"]["passRate"] == 0.5


def test_webhook_failure_never_raises(monkeypatch, tmp_path):
    """Endpoint failure is logged and swallowed; the alert is still persisted."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(svc, tmp_path, _RULES_WITH_WEBHOOK, "w2.db")

    def failing_urlopen(request, timeout=None):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(svc.urllib.request, "urlopen", failing_urlopen)

    alerts = engine.evaluate(_quality_event(svc))  # must not raise
    assert [a.rule_id for a in alerts] == ["RF20-RULE-002"]
    assert engine.storage.get_stats()["total_alerts"] == 1


def test_webhook_disabled_means_no_post(monkeypatch, tmp_path):
    """LINEAGE_WEBHOOK_DELIVERY=0 gates delivery; alerting itself continues."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(svc, tmp_path, _RULES_WITH_WEBHOOK, "w3.db")

    calls: list[dict] = []
    monkeypatch.setattr(svc, "WEBHOOK_DELIVERY_ENABLED", False)
    monkeypatch.setattr(svc.urllib.request, "urlopen", _record_urlopen(calls))

    alerts = engine.evaluate(_quality_event(svc))
    assert len(alerts) == 1
    assert calls == []
    assert engine.storage.get_stats()["total_alerts"] == 1


def test_rule_without_webhook_endpoint_posts_nothing(monkeypatch, tmp_path):
    """No actions.webhook.endpoint configured -> no outbound call at all."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(svc, tmp_path, _RULES_NO_WEBHOOK, "w4.db")

    calls: list[dict] = []
    monkeypatch.setattr(svc.urllib.request, "urlopen", _record_urlopen(calls))

    alerts = engine.evaluate(_quality_event(svc))
    assert len(alerts) == 1
    assert calls == []


def test_suppressed_duplicate_is_not_redelivered(monkeypatch, tmp_path):
    """Dedup-suppressed alerts are neither returned nor re-POSTed."""
    svc = _load_service(monkeypatch, tmp_path)
    engine = _engine(svc, tmp_path, _RULES_WITH_WEBHOOK, "w5.db")

    calls: list[dict] = []
    monkeypatch.setattr(svc.urllib.request, "urlopen", _record_urlopen(calls))

    first = engine.evaluate(_quality_event(svc, run_id="run-1"))
    second = engine.evaluate(_quality_event(svc, run_id="run-2"))
    assert len(first) == 1
    assert second == []
    assert len(calls) == 1
    assert engine.storage.get_stats()["total_alerts"] == 1
