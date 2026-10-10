"""
Local job dispatch for the batch orchestration resources (RF-22).

The K8s profile submits Spark/Flink work to the cluster; the LOCAL profile must
still be able to materialize the Dagster batch assets, so
``SparkResource.submit_job`` delegates here to run the equivalent pipeline
scripts in a subprocess with ``sys.executable`` (the same scripts the
``rake medallion:*`` tasks invoke).

The streaming speed layer is NOT dispatched here: it is a real Apache Flink job
with no local equivalent (see :data:`LOCAL_SCRIPT_MAP`).

This module deliberately contains NO dagster imports: the dispatch logic is
unit-testable in environments where the optional ``orchestration`` extra
(``dagster``) is not installed.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path

logger = logging.getLogger("dagster.resources.local_dispatch")

REPO_ROOT: Path = Path(__file__).resolve().parents[2]

DEFAULT_TIMEOUT_SECONDS: float = 600.0

K8S_SPARK_WORKDIR_PREFIX = "local:///opt/spark/work-dir/"

# Documented submit path -> repo-relative local pipeline script mapping.
# Values are the scripts actually executed by the local profile (the ones the
# Rakefile tasks run); keys are the paths the K8s profile / Dagster assets use.
#
# There is deliberately NO entry for pipelines/stream/flink_*.py: the speed layer
# is a real Apache Flink job submitted by the Flink Kubernetes Operator and has
# no local equivalent. Kappa replay is driven against the cluster by
# pipelines/orchestration/resources.py :: FlinkResource, not by a local script.
LOCAL_SCRIPT_MAP: dict[str, str] = {
    # Dagster asset submit paths (RF-22) -> same repo-relative script locally.
    "pipelines/medallion/bronze_ingestion.py": "pipelines/medallion/bronze_ingestion.py",
    "pipelines/medallion/silver_transform.py": "pipelines/medallion/silver_transform.py",
    "pipelines/medallion/gold_aggregation.py": "pipelines/medallion/gold_aggregation.py",
    # Spark driver submit path used by the helm s3-batch ScheduledSparkApplication.
    "local:///opt/spark/work-dir/pipelines/medallion/bronze_ingestion.py": "pipelines/medallion/bronze_ingestion.py",
}


class ScriptExecutionError(RuntimeError):
    """Raised when a dispatched pipeline script cannot be run or exits non-zero."""


def _candidate_paths(normalized: str) -> list[Path]:
    mapped = LOCAL_SCRIPT_MAP.get(normalized)
    if mapped is not None:
        return [REPO_ROOT / mapped]
    if normalized.startswith(K8S_SPARK_WORKDIR_PREFIX):
        return [REPO_ROOT / normalized[len(K8S_SPARK_WORKDIR_PREFIX) :]]
    path = Path(normalized)
    if path.is_absolute():
        return [path]
    # Repo-relative first (Dagster assets pass repo-relative paths), then
    # cwd-relative so tests can dispatch scripts outside the repository.
    return [REPO_ROOT / normalized, Path(normalized)]


def resolve_local_script(script_path: str) -> Path | None:
    """Resolve a submit path to an existing local pipeline script.

    Returns the absolute path of the local equivalent, or ``None`` when the
    path has no local equivalent in this repository.
    """
    normalized = script_path.replace("\\", "/").strip()
    for candidate in _candidate_paths(normalized):
        if candidate.is_file():
            return candidate.resolve()
    return None


def _tail(text: str, limit: int = 2000) -> str:
    stripped = text.strip()
    return stripped if len(stripped) <= limit else "..." + stripped[-limit:]


def run_script(
    script_path: str,
    args: list[str] | None = None,
    *,
    cwd: str | Path | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> subprocess.CompletedProcess[str]:
    """Run a pipeline script locally with ``sys.executable`` (no shell).

    Args:
        script_path: Submit path (asset/K8s style); resolved through
            :data:`LOCAL_SCRIPT_MAP` to a real local script.
        args: Extra CLI arguments passed as a list (never shell-interpolated).
        cwd: Working directory; defaults to the repository root so relative
            data paths (``data/lakehouse.db``) resolve like the Rake tasks.
        timeout: Kill the subprocess after this many seconds.

    Returns:
        The completed process (exit code 0).

    Raises:
        ScriptExecutionError: No local equivalent for ``script_path``, the
            script timed out, or it exited non-zero (so Dagster marks the
            asset failed).
    """
    resolved = resolve_local_script(script_path)
    if resolved is None:
        raise ScriptExecutionError(
            f"No local equivalent for script path {script_path!r}: the LOCAL profile can only "
            "dispatch scripts that exist in this repository "
            "(see pipelines/orchestration/local_dispatch.py:LOCAL_SCRIPT_MAP)."
        )
    command = [sys.executable, str(resolved), *(args or [])]
    run_cwd = Path(cwd) if cwd is not None else REPO_ROOT
    logger.info("LOCAL dispatch: %s (cwd=%s)", command, run_cwd)
    try:
        result = subprocess.run(
            command,
            cwd=str(run_cwd),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ScriptExecutionError(f"Script {resolved} timed out after {timeout}s (args={args or []}).") from exc
    if result.returncode != 0:
        detail = _tail(result.stderr) or _tail(result.stdout)
        raise ScriptExecutionError(
            f"Script {resolved} exited with code {result.returncode} (args={args or []}).\n{detail}"
        )
    logger.info("LOCAL dispatch OK: %s", resolved)
    return result
