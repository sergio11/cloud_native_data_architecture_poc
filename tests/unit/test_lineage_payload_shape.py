"""A1: the Marquez adapter payload must satisfy the webhook service model.

The service validates ``OpenLineageEvent`` with a mandatory ``run: Run``
(``Run.runId: str``). This test builds the adapter on a temp audit path,
captures the HTTP POST body, emits one event, and asserts BOTH the local
JSONL line and the POST body carry a non-empty ``run.runId`` plus the keys
the service requires (``eventType``, ``eventTime``, ``run.runId``,
``job.name``). It also round-trips the payload through the service's
pydantic model as end-to-end evidence.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

MOD_NAME = "lineage.config.lineage_webhook_service"

REQUIRED_TOP_LEVEL_KEYS = ("eventType", "eventTime", "run", "job")


def _load_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    monkeypatch.chdir(tmp_path)  # contain module-level lineage_events.db
    return importlib.import_module(MOD_NAME)


def _assert_service_shape(payload: dict[str, Any]) -> None:
    for key in REQUIRED_TOP_LEVEL_KEYS:
        assert key in payload, f"payload missing required key {key!r}: {sorted(payload)}"
    assert isinstance(payload["run"], dict), "payload['run'] must be an object"
    run_id = payload["run"].get("runId")
    assert isinstance(run_id, str) and run_id.strip() != "", "payload['run']['runId'] must be a non-empty string"
    assert isinstance(payload["job"], dict), "payload['job'] must be an object"
    assert isinstance(payload["job"].get("name"), str) and payload["job"]["name"].strip() != ""
    assert isinstance(payload["eventType"], str) and payload["eventType"].strip() != ""
    assert isinstance(payload["eventTime"], str) and payload["eventTime"].strip() != ""


def test_emit_payload_includes_run_run_id_in_jsonl_and_post(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    svc = _load_service(monkeypatch, tmp_path)
    captured: dict[str, Any] = {}

    def _fake_post(url: str, json: dict[str, Any] | None = None, timeout: float = 2) -> Any:
        captured["url"] = url
        captured["body"] = json
        return SimpleNamespace(status_code=201, text="created")

    monkeypatch.setattr("src.adapters.lineage.marquez_lineage.requests.post", _fake_post)

    audit_path = tmp_path / "lineage_audit.jsonl"
    adapter = MarquezLineageAdapter(marquez_url="http://127.0.0.1:1", audit_file_path=str(audit_path))

    emitted = adapter.emit_job_event(job_name="shape_job", inputs=["silver.in"], outputs=["gold.out"])

    assert emitted is True

    # 1. Local JSONL audit trail carries run.runId.
    lines = audit_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    jsonl_payload = json.loads(lines[0])
    _assert_service_shape(jsonl_payload)

    # 2. HTTP POST body carries the SAME non-empty run.runId.
    assert captured["body"] is not None, "adapter must POST a JSON body to Marquez"
    _assert_service_shape(captured["body"])
    assert captured["body"]["run"]["runId"] == jsonl_payload["run"]["runId"]

    # 3. End-to-end: the service model accepts the payload (run.runId mandatory).
    event = svc.OpenLineageEvent.model_validate(captured["body"])
    assert event.run.runId == jsonl_payload["run"]["runId"]
    assert event.job.name == "shape_job"


def test_emit_run_ids_are_unique_per_event(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "src.adapters.lineage.marquez_lineage.requests.post",
        lambda *args: SimpleNamespace(status_code=201, text="created"),
    )
    audit_path = tmp_path / "lineage_audit.jsonl"
    adapter = MarquezLineageAdapter(marquez_url="http://127.0.0.1:1", audit_file_path=str(audit_path))

    assert adapter.emit_job_event(job_name="shape_job", inputs=["a"], outputs=["b"]) is True
    assert adapter.emit_job_event(job_name="shape_job", inputs=["a"], outputs=["b"]) is True

    run_ids = [json.loads(line)["run"]["runId"] for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert len(run_ids) == 2
    assert all(isinstance(rid, str) and rid.strip() != "" for rid in run_ids)
    assert run_ids[0] != run_ids[1], "each emitted event must carry a fresh runId"
