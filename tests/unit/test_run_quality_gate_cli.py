"""Unit tests: quality-gate CLI flag/env routing (A2) and threshold panel (M3).

Hermetic: records travel through ``--input-file`` (storage is never read) and
``requests.post`` is stubbed, so no Marquez server is needed; the lineage audit
trail is redirected under ``tmp_path`` and the global container's cached
lineage adapter is evicted before/after each run.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from typing import Any

from click.testing import CliRunner

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_PATH = REPO_ROOT / "quality" / "gates" / "run_quality_gate.py"
SUITE_PATH = str(REPO_ROOT / "quality" / "expectations" / "silver_orders_suite.json")
TABLE = "silver.sales.orders"


def _load_cli() -> Any:
    spec = importlib.util.spec_from_file_location("run_quality_gate_cli", CLI_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _passing_records() -> list[dict[str, object]]:
    statuses = ["confirmed", "pending", "shipped", "cancelled"]
    return [
        {
            "id": index,
            "customer_id": 100 + index,
            "total_amount": 50.0 + index,
            "status": statuses[index % len(statuses)],
            "order_date": "2026-09-25",
            "created_at": "2026-09-25T10:00:00+00:00",
        }
        for index in range(1, 11)
    ]


def _write_records(tmp_path: Path) -> str:
    input_file = tmp_path / "records.json"
    input_file.write_text(json.dumps(_passing_records()), encoding="utf-8")
    return str(input_file)


def test_marquez_url_flag_routes_each_invocation_to_its_url(tmp_path: Path, monkeypatch: Any) -> None:
    """Two invocations with different --marquez-url must POST to their own URL.

    Regression (A2): the flag used to be a mere on/off switch while the POST
    always left for the default URL.
    """
    cli = _load_cli()
    posted_urls: list[str] = []

    class _FakeResponse:
        status_code = 200

    def _fake_post(url: str, *args: Any, **kwargs: Any) -> _FakeResponse:
        posted_urls.append(url)
        return _FakeResponse()

    monkeypatch.setattr("src.adapters.lineage.marquez_lineage.requests.post", _fake_post)
    # Contain side effects of the shared container singleton: redirect the
    # audit trail and guarantee teardown restores the environment even though
    # the CLI writes os.environ directly.
    monkeypatch.setenv("MARQUEZ_URL", "http://initial-env:5000")
    monkeypatch.setattr(cli.container, "_lineage_audit_path", str(tmp_path / "audit.jsonl"))
    cli.container.__dict__.pop("lineage", None)
    try:
        input_file = _write_records(tmp_path)
        runner = CliRunner()
        for url in ("http://marquez-one:5000", "http://marquez-two:5000"):
            result = runner.invoke(
                cli.main,
                [
                    "--suite",
                    SUITE_PATH,
                    "--table",
                    TABLE,
                    "--input-file",
                    input_file,
                    "--marquez-url",
                    url,
                ],
            )
            assert result.exit_code == 0, f"stdout:\n{result.output}"
    finally:
        cli.container.__dict__.pop("lineage", None)

    assert posted_urls == [
        "http://marquez-one:5000/api/v1/lineage",
        "http://marquez-two:5000/api/v1/lineage",
    ]


def test_summary_panel_shows_effective_contract_thresholds(tmp_path: Path, monkeypatch: Any) -> None:
    """The panel renders the governing contract thresholds, not the fallback.

    silver.sales.orders is governed by the orders contract
    (completeness 0.98, uniqueness 1.00) while the CLI fallback is 0.95.
    """
    cli = _load_cli()
    monkeypatch.delenv("MARQUEZ_URL", raising=False)
    monkeypatch.setattr(cli.container, "_lineage_audit_path", str(tmp_path / "audit.jsonl"))
    cli.container.__dict__.pop("lineage", None)
    try:
        result = CliRunner().invoke(
            cli.main,
            ["--suite", SUITE_PATH, "--table", TABLE, "--input-file", _write_records(tmp_path)],
        )
    finally:
        cli.container.__dict__.pop("lineage", None)

    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert "Effective Thresholds: completeness=0.98, uniqueness=1.00" in result.output


def test_summary_panel_shows_fallback_when_no_contract_governs(tmp_path: Path, monkeypatch: Any) -> None:
    """Without a governing contract the panel renders the fallback per dimension."""
    cli = _load_cli()
    suite_file = tmp_path / "not_null_suite.json"
    suite_file.write_text(
        json.dumps(
            {
                "expectation_suite_name": "not_null_suite",
                "expectations": [
                    {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}}
                ],
            }
        ),
        encoding="utf-8",
    )
    records_file = tmp_path / "records.json"
    records_file.write_text(json.dumps([{"id": 1}, {"id": 2}]), encoding="utf-8")
    monkeypatch.delenv("MARQUEZ_URL", raising=False)
    monkeypatch.setattr(cli.container, "_lineage_audit_path", str(tmp_path / "audit.jsonl"))
    cli.container.__dict__.pop("lineage", None)
    try:
        result = CliRunner().invoke(
            cli.main,
            [
                "--suite",
                str(suite_file),
                "--table",
                "silver.unknown.no_contract_table",
                "--input-file",
                str(records_file),
            ],
        )
    finally:
        cli.container.__dict__.pop("lineage", None)

    assert result.exit_code == 0, f"stdout:\n{result.output}"
    assert "Effective Thresholds: completeness=0.95" in result.output
