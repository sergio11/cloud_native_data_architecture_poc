"""W8 CLI smoke tests: quality-gate exit codes 0 / 1 / 2.

Runs ``quality/gates/run_quality_gate.py`` with records through ``--input-file``
so the lakehouse storage and repository ``data/`` are never touched.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_PATH = REPO_ROOT / "quality" / "gates" / "run_quality_gate.py"
SUITE_PATH = "quality/expectations/silver_orders_suite.json"
TABLE = "silver.sales.orders"


def _passing_records() -> list[dict[str, object]]:
    """10 rows satisfying every expectation of silver_orders_suite.json."""
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


def _write_json(path: Path, payload: object) -> str:
    # encoding="utf-8" (not utf-8-sig): no BOM, which the CLI's json.load rejects.
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _run(input_file: str, suite: str = SUITE_PATH) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(CLI_PATH),
            "--suite",
            suite,
            "--table",
            TABLE,
            "--threshold",
            "0.95",
            "--input-file",
            input_file,
        ],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=120,
    )


def test_cli_exit_0_when_gate_passes(tmp_path: Path) -> None:
    input_file = _write_json(tmp_path / "records.json", _passing_records())

    result = _run(input_file)

    assert result.returncode == 0, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "PASSED" in result.stdout


def test_cli_exit_1_on_empty_records_fail_closed(tmp_path: Path) -> None:
    """Empty array => fail-closed evaluation => gate FAILED => exit 1."""
    input_file = _write_json(tmp_path / "empty.json", [])

    result = _run(input_file)

    assert result.returncode == 1, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "No records to evaluate" in result.stdout


def test_cli_exit_2_on_nonexistent_suite(tmp_path: Path) -> None:
    input_file = _write_json(tmp_path / "records.json", _passing_records())

    result = _run(input_file, suite=str(tmp_path / "missing_suite.json"))

    assert result.returncode == 2, f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    assert "Execution Error" in result.stdout
