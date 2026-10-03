"""
RF-22 (W5) - Local dispatch for the Dagster orchestration resources.

Covers ``pipelines/orchestration/local_dispatch.py`` (no dagster import
required): subprocess execution of pipeline scripts, submit-path -> local
script resolution, and a dagster-guarded smoke test that the repository
definitions still load when the optional ``orchestration`` extra is installed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pipelines.orchestration.local_dispatch import (
    ScriptExecutionError,
    resolve_local_script,
    run_script,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

# Submit paths passed by the five Dagster assets (dagster_repository.py) and
# by the K8s Spark job template, mapped to the local pipeline that must exist.
SUBMIT_PATH_CASES = [
    ("pipelines/medallion/bronze_ingestion.py", "pipelines/medallion/bronze_ingestion.py"),
    ("pipelines/medallion/silver_transform.py", "pipelines/medallion/silver_transform.py"),
    ("pipelines/medallion/gold_aggregation.py", "pipelines/medallion/gold_aggregation.py"),
    (
        "local:///opt/spark/work-dir/pipelines/medallion/bronze_ingestion.py",
        "pipelines/medallion/bronze_ingestion.py",
    ),
    ("pipelines/stream/flink_orders_stream.py", "pipelines/stream/flink_orders_stream.py"),
]


def _write_script(tmp_path: Path, source: str) -> Path:
    script = tmp_path / "script_under_test.py"
    script.write_text(source, encoding="utf-8")
    return script


def test_run_script_success(tmp_path: Path) -> None:
    script = _write_script(tmp_path, "print('ok')\n")
    result = run_script(str(script))
    assert result.returncode == 0
    assert "ok" in result.stdout


def test_run_script_failure_raises_clear_error(tmp_path: Path) -> None:
    script = _write_script(tmp_path, "import sys\nprint('boom', file=sys.stderr)\nsys.exit(3)\n")
    with pytest.raises(ScriptExecutionError) as excinfo:
        run_script(str(script))
    message = str(excinfo.value)
    assert "exited with code 3" in message
    assert "boom" in message


def test_run_script_passes_args_as_list_without_shell(tmp_path: Path) -> None:
    script = _write_script(tmp_path, "import json, sys\nprint(json.dumps(sys.argv[1:]))\n")
    result = run_script(str(script), ["--entity", "orders; rm -rf /", "--flag"])
    assert result.returncode == 0
    assert '"--entity"' in result.stdout
    assert "orders; rm -rf /" in result.stdout


def test_run_script_missing_local_equivalent_raises() -> None:
    with pytest.raises(ScriptExecutionError, match="No local equivalent"):
        run_script("pipelines/does_not_exist/ghost.py", ["--x"])


def test_run_script_timeout_raises(tmp_path: Path) -> None:
    script = _write_script(tmp_path, "import time\ntime.sleep(30)\n")
    with pytest.raises(ScriptExecutionError, match="timed out"):
        run_script(str(script), timeout=0.5)


@pytest.mark.parametrize(("submit_path", "relative"), SUBMIT_PATH_CASES)
def test_submit_paths_resolve_to_existing_local_pipeline(submit_path: str, relative: str) -> None:
    resolved = resolve_local_script(submit_path)
    assert resolved is not None, f"{submit_path} must resolve to a local script"
    assert resolved == (REPO_ROOT / relative).resolve()
    assert resolved.is_file()


def test_resolve_returns_none_for_unknown_path() -> None:
    assert resolve_local_script("jobs/unknown_k8s_job.py") is None


def test_dagster_repository_definitions_load() -> None:
    pytest.importorskip("dagster", reason="dagster is optional (pyproject 'orchestration' extra)")
    from pipelines.orchestration import dagster_repository

    asset_keys = {a.key.to_user_string() for a in dagster_repository.defs.assets}
    assert asset_keys == {
        "bronze_orders",
        "bronze_customers",
        "silver_orders",
        "silver_customers",
        "gold_daily_metrics",
    }
    assert {"spark", "flink", "marquez", "ge"} <= set(dagster_repository.defs.resources)
