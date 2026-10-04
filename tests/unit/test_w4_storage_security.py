"""W4 storage/security hardening: lazy DB init, atomic dedup, anchored SQL, CORS.

Also guards the pruned lineage requirements pins (confirmed-unused deps).
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from datetime import UTC, datetime
from pathlib import Path

from fastapi.middleware.cors import CORSMiddleware

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOD_NAME = "lineage.config.lineage_webhook_service"

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


def _load_service(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    return importlib.import_module(MOD_NAME)


def _write_rules(tmp_path: Path, body: str) -> str:
    p = tmp_path / "alert-rules.yaml"
    p.write_text(textwrap.dedent(body), encoding="utf-8")
    return str(p)


def _alert(svc, alert_id: str, dataset_name: str | None = None, job_name: str | None = None):
    return svc.AlertItem(
        alert_id=alert_id,
        rule_id="RF20-RULE-002",
        rule_name="QualityThresholdBreachAlert",
        severity="CRITICAL",
        dataset_name=dataset_name,
        job_name=job_name,
        summary=f"alert {alert_id}",
        description="d",
        details={},
        created_at=datetime.now(UTC).isoformat(),
    )


def _event(svc, run_id: str, job_name: str = "job_a"):
    return svc.OpenLineageEvent(
        eventType="COMPLETE",
        eventTime="2026-09-27T00:00:00+00:00",
        run={"runId": run_id, "facets": {"dataQuality": {"passRate": 0.5, "suiteName": "s"}}},
        job={"namespace": "default", "name": job_name},
        inputs=[],
        outputs=[],
    )


def _find_cors(app):
    mw = app.build_middleware_stack()
    while mw is not None:
        if isinstance(mw, CORSMiddleware):
            return mw
        mw = getattr(mw, "app", None)
    raise AssertionError("CORSMiddleware not installed")


# --------------------------------------------------------------------------
# Lazy DB init
# --------------------------------------------------------------------------
def test_import_reloads_without_creating_any_files(monkeypatch, tmp_path):
    """Reloading the service must not create the DB (or anything else) on disk."""
    monkeypatch.chdir(tmp_path)
    svc = importlib.import_module(MOD_NAME)
    svc = importlib.reload(svc)
    assert svc.storage._initialized is False
    assert list(tmp_path.iterdir()) == []


def test_first_db_use_initializes_lazily(monkeypatch, tmp_path):
    """The schema is created on first use, not at construction time."""
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "lazy.db"))
    assert not (tmp_path / "lazy.db").exists()
    storage.get_stats()  # first use
    assert (tmp_path / "lazy.db").exists()
    assert storage._initialized is True


# --------------------------------------------------------------------------
# Dedup atomicity / honest alert counts
# --------------------------------------------------------------------------
def test_insert_alert_if_new_is_atomic_dedup(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "d1.db"))

    first = _alert(svc, "a-1", job_name="job_a")
    dup = _alert(svc, "a-2", job_name="job_a")
    other = _alert(svc, "a-3", job_name="job_b")

    assert storage.insert_alert_if_new(first, window_seconds=300) is True
    assert storage.insert_alert_if_new(dup, window_seconds=300) is False
    assert storage.insert_alert_if_new(other, window_seconds=300) is True
    assert storage.insert_alert_if_new(_alert(svc, "a-4", job_name="job_a"), window_seconds=0) is True
    assert storage.get_stats()["total_alerts"] == 3


def test_evaluate_returns_only_persisted_alerts(monkeypatch, tmp_path):
    """Suppressed duplicates must not be returned (no alerts_triggered inflation)."""
    svc = _load_service(monkeypatch, tmp_path)
    rules_path = _write_rules(tmp_path, _QUALITY_RULES)
    storage = svc.StorageManager(db_path=str(tmp_path / "d2.db"))
    engine = svc.RuleEngine(storage, rules_path=rules_path)

    first = engine.evaluate(_event(svc, "run-1"))
    second = engine.evaluate(_event(svc, "run-2"))
    assert len(first) == 1
    assert second == []
    assert storage.get_stats()["total_alerts"] == 1


# --------------------------------------------------------------------------
# Anchored / exact SQL lookups
# --------------------------------------------------------------------------
def test_lineage_dataset_identity_is_exact(monkeypatch, tmp_path):
    """silver.orders must not match silver.orders_archive."""
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "d3.db"))

    for run_id, job, name in [
        ("r1", "j_orders", "silver.orders"),
        ("r2", "j_archive", "silver.orders_archive"),
        ("r3", "j_consumer", "silver.orders"),
    ]:
        event = svc.OpenLineageEvent(
            eventType="COMPLETE",
            eventTime="2026-09-27T00:00:00+00:00",
            run={"runId": run_id, "facets": {}},
            job={"namespace": "default", "name": job},
            inputs=[{"namespace": "default", "name": name, "facets": {}}] if run_id == "r3" else [],
            outputs=[{"namespace": "default", "name": name, "facets": {}}] if run_id != "r3" else [],
        )
        storage.insert_event(event, event.model_dump_json())

    storage.upsert_dataset_schema(
        namespace="default",
        name="silver.orders",
        schema_dict={"fields": [{"name": "id", "type": "INTEGER"}]},
        run_id="r1",
        job_name="j_orders",
        event_time="2026-09-27T00:00:00+00:00",
    )
    storage.upsert_dataset_schema(
        namespace="default",
        name="silver.orders_archive",
        schema_dict={"fields": [{"name": "id", "type": "TEXT"}]},
        run_id="r2",
        job_name="j_archive",
        event_time="2026-09-27T00:00:00+00:00",
    )

    lineage = storage.get_dataset_lineage("silver.orders")
    assert {p["job_name"] for p in lineage["producers"]} == {"j_orders"}
    assert {c["job_name"] for c in lineage["consumers"]} == {"j_consumer"}
    assert len(lineage["schema_versions"]) == 1  # not orders_archive


def test_query_alerts_filter_escapes_like_wildcards(monkeypatch, tmp_path):
    """`_`/`%` in the dataset filter match literally, not as LIKE wildcards."""
    svc = _load_service(monkeypatch, tmp_path)
    storage = svc.StorageManager(db_path=str(tmp_path / "d4.db"))
    assert storage.insert_alert_if_new(_alert(svc, "b-1", dataset_name="silver.sales_x")) is True
    assert storage.insert_alert_if_new(_alert(svc, "b-2", dataset_name="silver.salesaxx")) is True

    hit = storage.query_alerts(dataset="sales_x")
    assert [a["dataset_name"] for a in hit] == ["silver.sales_x"]

    assert storage.insert_alert_if_new(_alert(svc, "b-3", dataset_name="discount_100%")) is True
    pct = storage.query_alerts(dataset="100%")
    assert [a["dataset_name"] for a in pct] == ["discount_100%"]


# --------------------------------------------------------------------------
# CORS
# --------------------------------------------------------------------------
def test_cors_wildcard_without_credentials_by_default(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    svc = importlib.reload(svc)
    mw = _find_cors(svc.app)
    assert mw.allow_origins == ["*"]
    assert mw.allow_credentials is False


def test_cors_explicit_origins_with_credentials(monkeypatch, tmp_path):
    svc = _load_service(monkeypatch, tmp_path)
    monkeypatch.setenv("CORS_ALLOW_ORIGINS", "https://a.example, https://b.example")
    svc = importlib.reload(svc)
    mw = _find_cors(svc.app)
    assert mw.allow_origins == ["https://a.example", "https://b.example"]
    assert mw.allow_credentials is True
    # leave the module in its default configuration for other tests
    monkeypatch.delenv("CORS_ALLOW_ORIGINS", raising=False)
    importlib.reload(svc)


# --------------------------------------------------------------------------
# Requirements pruning
# --------------------------------------------------------------------------
def test_lineage_requirements_only_pin_used_deps():
    req_text = (REPO_ROOT / "lineage" / "config" / "requirements.txt").read_text(encoding="utf-8")
    for unused in ("requests", "python-multipart"):
        assert unused not in req_text, f"{unused} is not imported anywhere under lineage/"
