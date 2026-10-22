#!/usr/bin/env python3
"""Data contract and manifest validator CLI driver.

Delegates validation to ``src.use_cases.validate_contract`` and adds the
product.yaml <-> contract.yaml quality-threshold cross-check.
Exit codes: 0 = all validations passed, 1 = one or more violations.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from src.container import Container  # noqa: E402
from src.domain.contracts import (  # noqa: E402
    ContractCompatibilityEvaluator,
    extract_contract_fields,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("contract_validator")


def _load_yaml(file_path: str) -> dict[str, Any]:
    with open(file_path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data if isinstance(data, dict) else {}


def _mapping(value: Any) -> dict[str, Any]:
    """Return ``value`` when it is a mapping, otherwise an empty mapping."""
    return value if isinstance(value, dict) else {}


def _numeric_thresholds(block: Any) -> dict[str, float]:
    """Extract numeric threshold dimensions (bools are not thresholds)."""
    mapping = _mapping(block)
    return {
        str(key): float(value)
        for key, value in mapping.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }


def cross_check_quality_thresholds(repo_root: str) -> list[str]:
    """Compare each manifest's ``spec.quality`` with its contract's thresholds.

    The contract is authoritative: the quality gate reads only
    ``spec.quality.thresholds`` from ``contract.yaml``. Whenever the sibling
    ``product.yaml`` re-declares the SAME dimension with a different number,
    the two documents have drifted and validation must fail. Dimensions only
    present on one side (e.g. a manifest-only ``timeliness`` target, or the
    ``suite`` pointer) are informational and are not compared.

    Returns:
        One human-readable violation message per mismatched dimension
        (empty list when every shared dimension agrees).
    """
    violations: list[str] = []
    domains_root = Path(repo_root) / "domains"
    for product_path in sorted(domains_root.glob("*/data-products/*/product.yaml")):
        spec = _mapping(_load_yaml(str(product_path)).get("spec"))
        contract_ref = spec.get("contract")
        ref_name = (
            str(contract_ref).removeprefix("./") if isinstance(contract_ref, str) and contract_ref else "contract.yaml"
        )
        contract_path = product_path.parent / ref_name
        if not contract_path.is_file():
            # Missing/unreadable contracts are already reported by the use case.
            continue
        contract_spec = _mapping(_load_yaml(str(contract_path)).get("spec"))
        contract_quality = _mapping(contract_spec.get("quality"))
        contract_thresholds = _numeric_thresholds(contract_quality.get("thresholds"))
        if not contract_thresholds:
            continue
        product_thresholds = _numeric_thresholds(spec.get("quality"))
        for dimension in sorted(product_thresholds.keys() & contract_thresholds.keys()):
            manifest_value = product_thresholds[dimension]
            contract_value = contract_thresholds[dimension]
            if manifest_value != contract_value:
                violations.append(
                    f"{product_path}: spec.quality.{dimension}={manifest_value} diverges from "
                    f"{contract_path}: spec.quality.thresholds.{dimension}={contract_value} "
                    "(contract.yaml is authoritative)"
                )
    return violations


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
    """Full repository validation via the use case (single composition root)."""
    use_case = Container(repo_root=repo_root).validate_contract
    results = use_case.execute()

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

    threshold_violations = cross_check_quality_thresholds(repo_root)
    for violation in threshold_violations:
        logger.error("FAILED QualityThresholdCrossCheck => %s", violation)

    if not results["passed"] or threshold_violations:
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
