#!/usr/bin/env python3
"""
Modern Data Architecture POC - Data Contract & Manifest Validator
Validates DataProduct manifests and DataContract YAML files against JSON schemas.
Enforces BACKWARD compatibility and federated governance standards across domains.
"""

import sys
import os
import glob
import json
import logging
import argparse
from typing import Dict, Any, List, Tuple
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("contract_validator")


def load_yaml(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_structure(data: Dict[str, Any], doc_type: str, file_path: str) -> Tuple[bool, List[str]]:
    errors = []
    if not isinstance(data, dict):
        return False, ["Root document must be a dictionary/mapping"]

    meta = data.get("metadata", {})
    spec = data.get("spec", data)

    if doc_type == "DataProduct":
        if not meta.get("name"):
            errors.append("Missing metadata.name")
        if not meta.get("domain"):
            errors.append("Missing metadata.domain")
        if not meta.get("owner"):
            errors.append("Missing metadata.owner")
        if not spec.get("classification"):
            errors.append("Missing spec.classification")
        if not spec.get("assets"):
            errors.append("Missing spec.assets list")
        # Check contract reference or sibling contract.yaml file
        contract_ref = spec.get("contract")
        sibling_contract = os.path.join(os.path.dirname(file_path), "contract.yaml")
        if not contract_ref and not os.path.exists(sibling_contract):
            errors.append("Missing spec.contract reference or sibling contract.yaml")

    elif doc_type == "DataContract":
        product_ref = meta.get("data_product") or meta.get("product")
        if not product_ref:
            errors.append("Missing metadata.data_product or metadata.product")

        compat = (
            spec.get("compatibility_mode")
            or spec.get("compatibility", {}).get("policy")
            or spec.get("compatibility", {}).get("mode")
            or data.get("compatibility", {}).get("policy")
            or data.get("compatibility", {}).get("mode")
        )
        if not compat:
            errors.append("Missing compatibility policy (compatibility_mode, compatibility.policy, or compatibility.mode)")

        # Schema fields check (supports root or spec nested)
        schema = spec.get("schema") or data.get("schema", {})
        if not schema.get("fields"):
            errors.append("Missing schema.fields definition")

    return len(errors) == 0, errors


def check_backward_compatibility(prev_contract: Dict[str, Any], new_contract: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Enforces BACKWARD compatibility:
    - No existing field can be removed or renamed.
    - Required fields cannot be added without backward allowance.
    """
    errors = []

    def extract_fields(c: Dict[str, Any]) -> Dict[str, Any]:
        schema = c.get("spec", {}).get("schema") or c.get("schema", {})
        fields = schema.get("fields", [])
        return {f["name"]: f for f in fields if isinstance(f, dict) and "name" in f}

    old_fields = extract_fields(prev_contract)
    new_fields = extract_fields(new_contract)

    # Check for removed fields
    for field_name in old_fields:
        if field_name not in new_fields:
            errors.append(f"Breaking change: Field '{field_name}' was removed or renamed in new contract version.")

    # Check for newly added required fields
    for field_name, f_def in new_fields.items():
        if field_name not in old_fields and f_def.get("required", False):
            errors.append(
                f"Breaking change: New field '{field_name}' is declared as required=true "
                "(must be optional for BACKWARD compatibility)."
            )

    return len(errors) == 0, errors


def validate_all_domains(repo_root: str) -> bool:
    logger.info("Scanning all domain data products and contracts in %s", repo_root)
    domains_dir = os.path.join(repo_root, "domains")
    if not os.path.exists(domains_dir):
        logger.error("Domains directory not found: %s", domains_dir)
        return False

    all_passed = True
    product_files = glob.glob(os.path.join(domains_dir, "**", "product.yaml"), recursive=True)
    contract_files = glob.glob(os.path.join(domains_dir, "**", "contract.yaml"), recursive=True)

    logger.info("Found %d DataProduct manifests and %d DataContract files", len(product_files), len(contract_files))

    for p_file in product_files:
        try:
            doc = load_yaml(p_file)
            valid, errs = validate_structure(doc, "DataProduct", p_file)
            if not valid:
                logger.error("FAILED DataProduct: %s => %s", p_file, errs)
                all_passed = False
            else:
                logger.info("PASSED DataProduct: %s [%s.%s]", p_file, doc["metadata"].get("domain"), doc["metadata"].get("name"))
        except Exception as e:
            logger.error("ERROR parsing %s: %s", p_file, e)
            all_passed = False

    for c_file in contract_files:
        try:
            doc = load_yaml(c_file)
            valid, errs = validate_structure(doc, "DataContract", c_file)
            if not valid:
                logger.error("FAILED DataContract: %s => %s", c_file, errs)
                all_passed = False
            else:
                logger.info("PASSED DataContract: %s [%s]", c_file, doc["metadata"].get("name"))
        except Exception as e:
            logger.error("ERROR parsing %s: %s", c_file, e)
            all_passed = False

    return all_passed


def main():
    parser = argparse.ArgumentParser(description="Modern Data Architecture Data Contract Validator")
    parser.add_argument("--repo-root", default=".", help="Root directory of the repository")
    parser.add_argument("--file", help="Specific file to validate")
    parser.add_argument("--prev-file", help="Previous contract file to check backward compatibility")

    args = parser.parse_args()

    if args.file and args.prev_file:
        old_c = load_yaml(args.prev_file)
        new_c = load_yaml(args.file)
        compat, errs = check_backward_compatibility(old_c, new_c)
        if not compat:
            logger.error("BACKWARD COMPATIBILITY VIOLATION in %s:\n%s", args.file, "\n".join(f" - {e}" for e in errs))
            sys.exit(1)
        logger.info("BACKWARD compatibility check passed successfully.")
        sys.exit(0)

    success = validate_all_domains(args.repo_root)
    if not success:
        logger.error("One or more contracts/manifests failed validation.")
        sys.exit(1)

    logger.info("All Data Products and Data Contracts successfully validated!")
    sys.exit(0)


if __name__ == "__main__":
    main()
