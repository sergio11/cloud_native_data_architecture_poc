#!/usr/bin/env python3
"""
Modern Data Architecture POC - Data Contract & Manifest Validator (CLI driver).
Thin delegator: every validation rule lives in the application core
(src.use_cases.validate_contract / src.domain.contracts). This file is kept
at its original path because the Rakefile (governance:validate task) and
scripts/demo.sh invoke it as ``python governance/contract_validator.py
--repo-root .``; the legacy ``--file/--prev-file`` two-file comparison mode
is preserved as well.

Exit codes: 0 = all validations passed, 1 = one or more violations.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any

import yaml

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter  # noqa: E402
from src.domain.contracts import (  # noqa: E402
    ContractCompatibilityEvaluator,
    extract_contract_fields,
)
from src.use_cases.validate_contract import ValidateContractUseCase  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("contract_validator")


def _load_yaml(file_path: str) -> dict[str, Any]:
    with open(file_path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data if isinstance(data, dict) else {}


def run_legacy_comparison(new_file: str, prev_file: str) -> int:
    """Two-file BACKWARD compatibility comparison (legacy ``--file/--prev-file``).

    Delegates straight to the domain evaluator; no validation logic here.
    """
    prev_fields = extract_contract_fields(_load_yaml(prev_file))
    new_fields = extract_contract_fields(_load_yaml(new_file))
    _, violations = ContractCompatibilityEvaluator.evaluate_backward_compatibility(
        prev_fields=prev_fields,
        new_fields=new_fields,
    )
    if violations:
        logger.error(
            "BACKWARD COMPATIBILITY VIOLATION in %s:\n%s",
            new_file,
            "\n".join(f" - {message}" for message in violations),
        )
        return 1
    logger.info("BACKWARD compatibility check passed successfully.")
    return 0


def run_repo_validation(repo_root: str) -> int:
    """Full RF-15 repository validation via the use case (container-free wiring)."""
    use_case = ValidateContractUseCase(catalog=FsDomainCatalogAdapter(repo_root))
    results: dict[str, Any] = use_case.execute(repo_root=repo_root)

    logger.info(
        "Found %d DataProduct manifests and %d DataContract files",
        results["products_evaluated"],
        results["contracts_evaluated"],
    )
    for detail in results["details"]:
        kind = detail.get("kind", "DataProduct")
        label = detail.get("entity") or detail.get("contract") or ""
        paired = detail.get("backward_compatible_with")
        if paired:
            logger.info(
                "PASSED %s: %s [%s] (backward-compatible with %s)",
                kind,
                detail["file"],
                label,
                paired,
            )
        else:
            logger.info("PASSED %s: %s [%s]", kind, detail["file"], label)
    for entry in results["errors"]:
        kind = entry.get("kind", "Validation")
        messages: list[str] = entry.get("errors", [])
        logger.error("FAILED %s: %s => %s", kind, entry["file"], "; ".join(messages))

    if not results["passed"]:
        logger.error("One or more contracts/manifests failed validation.")
        return 1
    logger.info("All Data Products and Data Contracts successfully validated!")
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Modern Data Architecture Data Contract Validator")
    parser.add_argument("--repo-root", default=".", help="Root directory of the repository")
    parser.add_argument("--file", help="Specific file to validate")
    parser.add_argument("--prev-file", help="Previous contract file to check backward compatibility")

    args = parser.parse_args()

    if args.file and args.prev_file:
        sys.exit(run_legacy_comparison(args.file, args.prev_file))
    sys.exit(run_repo_validation(args.repo_root))


if __name__ == "__main__":
    main()
