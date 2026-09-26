"""
Modern Data Architecture POC - Integration Test: Governance CLI on the real repo.

Guards the exact entry point the Rakefile (``governance:validate``) and
``scripts/demo.sh`` invoke: ``python governance/contract_validator.py
--repo-root .`` run from the repository root. One subprocess, read-only against
the checked-in domains/ tree (the repo's 6 data products must validate).
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_PATH = REPO_ROOT / "governance" / "contract_validator.py"


def test_repo_data_products_validate_via_governance_cli() -> None:
    """When the governance CLI runs against the real repository exactly as the
    Rakefile/demo.sh do, Then it exits 0 and reports every data product and
    contract as valid (6 products)."""
    result = subprocess.run(
        [sys.executable, str(CLI_PATH), "--repo-root", "."],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=120,
    )

    assert result.returncode == 0, result.stderr
    assert "successfully validated" in result.stderr

    found = re.search(r"Found (\d+) DataProduct manifests and (\d+) DataContract files", result.stderr)
    assert found, result.stderr
    assert int(found.group(1)) >= 6
    assert int(found.group(2)) >= 6
