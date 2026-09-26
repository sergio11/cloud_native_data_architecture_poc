"""
Modern Data Architecture POC - Data Contract Validation Use Case (RF-15).
Validates DataProduct manifests and DataContracts discovered through the
DomainCatalogPort: structure, governance JSON schemas, declared compatibility
mode, and BACKWARD schema evolution against the sibling schema.json snapshot.
Publication-blocking rules live here (and in src.domain.contracts) only - the
governance CLI is a thin delegator to this use case.

All errors are aggregated (never early-return) and reported in deterministic
path order. ``jsonschema`` is used opportunistically: when the library is not
installed a warning is logged and the structural checks still run (dependency
concern, no requirements files are managed here).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from src.domain.contracts import (
    ContractCompatibilityEvaluator,
    extract_contract_fields,
    extract_schema_fields,
    resolve_compatibility_mode,
    validate_compatibility_mode,
)
from src.ports.outbound import DomainCatalogPort

try:
    import jsonschema
except ImportError:  # pragma: no cover - optional dependency
    jsonschema = None

logger = logging.getLogger("use_case.validate_contract")


class ValidateContractUseCase:
    """Orchestrates validation of Bitol ODCS contracts and data products.

    ``catalog`` is the DomainCatalogPort used to discover manifests. When no
    catalog is injected (composition-time default for drivers such as the
    CLI delegator or ``Container`` calling ``ValidateContractUseCase()``),
    ``execute`` lazily builds ``FsDomainCatalogAdapter(repo_root)``; the
    adapter import lives inside the method so the use case keeps no hard
    module-level dependency on infrastructure (the coordinator injects the
    adapter through the container). ``repo_root`` therefore always drives the
    governance-schema lookup and, for the default catalog, the scan root.
    """

    def __init__(
        self,
        catalog: DomainCatalogPort | None = None,
        evaluator: ContractCompatibilityEvaluator | None = None,
    ) -> None:
        self._catalog = catalog
        self._evaluator = evaluator or ContractCompatibilityEvaluator()
        self._warned_jsonschema = False

    def execute(self, repo_root: str = ".") -> dict[str, Any]:
        catalog = self._catalog
        if catalog is None:
            from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter

            catalog = FsDomainCatalogAdapter(repo_root)

        products = sorted(catalog.list_products(), key=lambda item: item[0])
        contracts = sorted(catalog.list_contracts(), key=lambda item: item[0])
        schemas = sorted(catalog.list_schemas(), key=lambda item: item[0])

        results: dict[str, Any] = {
            "products_evaluated": len(products),
            "contracts_evaluated": len(contracts),
            "passed": True,
            "errors": [],
            "details": [],
        }

        if not products and not contracts:
            results["passed"] = False
            results["errors"].append(
                {
                    "file": os.path.join(repo_root, "domains"),
                    "kind": "Discovery",
                    "errors": ["No data product manifests or data contracts discovered under domains/"],
                }
            )
            return results

        product_paths = {os.path.normpath(p) for p, _ in products}
        contract_paths = {os.path.normpath(p) for p, _ in contracts}
        schemas_by_dir = {
            os.path.normpath(os.path.dirname(p)): (p, data) for p, data in schemas
        }

        # 1. Validate DataProduct manifests (structure + governance JSON schema)
        for path, data in products:
            errs = self._validate_product(data, path, contract_paths, repo_root)
            self._record(results, errs, path, kind="DataProduct")
            if not errs:
                meta = data.get("metadata") or {}
                results["details"].append(
                    {
                        "file": path,
                        "status": "PASSED",
                        "kind": "DataProduct",
                        "entity": f"{meta.get('domain')}.{meta.get('name')}",
                    }
                )

        # 2-4. Validate DataContracts: structure, compatibility mode,
        #      governance JSON schema, and BACKWARD evolution vs schema.json.
        for path, data in contracts:
            errs = self._validate_contract(
                data, path, product_paths, schemas_by_dir, repo_root
            )
            self._record(results, errs, path, kind="DataContract")
            if not errs:
                meta = data.get("metadata") or {}
                paired = schemas_by_dir.get(os.path.normpath(os.path.dirname(path)))
                detail: dict[str, Any] = {
                    "file": path,
                    "status": "PASSED",
                    "kind": "DataContract",
                    "contract": meta.get("name"),
                    "compatibility_mode": resolve_compatibility_mode(data),
                    "backward_compatible_with": paired[0] if paired else None,
                }
                results["details"].append(detail)

        results["passed"] = not results["errors"]
        return results

    # ------------------------------------------------------------------
    # Product manifest rules
    # ------------------------------------------------------------------
    def _validate_product(
        self,
        data: dict[str, Any],
        path: str,
        contract_paths: set,
        repo_root: str,
    ) -> list[str]:
        errs: list[str] = []
        meta_raw = data.get("metadata")
        meta: dict[str, Any] = meta_raw if isinstance(meta_raw, dict) else {}
        spec_raw = data.get("spec")
        spec: dict[str, Any] = spec_raw if isinstance(spec_raw, dict) else {}

        if not meta.get("name"):
            errs.append("Missing metadata.name")
        if not meta.get("domain"):
            errs.append("Missing metadata.domain")
        if not meta.get("owner"):
            errs.append("Missing metadata.owner")
        if not spec.get("classification"):
            errs.append("Missing spec.classification")
        if not spec.get("assets"):
            errs.append("Missing spec.assets list")

        contract_ref = spec.get("contract")
        sibling = os.path.join(os.path.dirname(path), "contract.yaml")
        if not contract_ref and not os.path.exists(sibling):
            errs.append("Missing spec.contract reference or sibling contract.yaml")
        else:
            target = (
                os.path.normpath(os.path.join(os.path.dirname(path), str(contract_ref)))
                if contract_ref
                else os.path.normpath(sibling)
            )
            if target not in contract_paths:
                errs.append(f"Referenced contract.yaml missing or unreadable: {target}")

        errs.extend(self._jsonschema_errors(data, "product-schema.json", path, repo_root))
        return errs

    # ------------------------------------------------------------------
    # Contract rules (structure + mode + schema + backward compatibility)
    # ------------------------------------------------------------------
    def _validate_contract(
        self,
        data: dict[str, Any],
        path: str,
        product_paths: set,
        schemas_by_dir: dict[str, Any],
        repo_root: str,
    ) -> list[str]:
        errs: list[str] = []
        meta_raw = data.get("metadata")
        meta: dict[str, Any] = meta_raw if isinstance(meta_raw, dict) else {}
        spec_raw = data.get("spec")
        spec: dict[str, Any] = spec_raw if isinstance(spec_raw, dict) else {}

        if not (meta.get("data_product") or meta.get("product")):
            errs.append("Missing metadata.data_product")

        schema_raw = spec.get("schema")
        schema: dict[str, Any] = schema_raw if isinstance(schema_raw, dict) else {}
        schema_fields_raw = schema.get("fields")
        if not (isinstance(schema_fields_raw, list) and schema_fields_raw):
            root_raw = data.get("schema")
            root_schema: dict[str, Any] = root_raw if isinstance(root_raw, dict) else {}
            root_fields_raw = root_schema.get("fields")
            if not (isinstance(root_fields_raw, list) and root_fields_raw):
                errs.append("Missing schema.fields definition")

        _, mode_errs = validate_compatibility_mode(data)
        errs.extend(mode_errs)

        errs.extend(self._jsonschema_errors(data, "contract-schema.json", path, repo_root))

        sibling_product = os.path.normpath(
            os.path.join(os.path.dirname(path), "product.yaml")
        )
        if sibling_product not in product_paths:
            errs.append(f"Missing or unreadable sibling product.yaml: {sibling_product}")

        errs.extend(self._backward_compatibility_errors(data, path, schemas_by_dir))
        return errs

    def _backward_compatibility_errors(
        self,
        data: dict[str, Any],
        path: str,
        schemas_by_dir: dict[str, Any],
    ) -> list[str]:
        """RF-15 key rule: contract fields vs the sibling schema.json snapshot."""
        paired = schemas_by_dir.get(os.path.normpath(os.path.dirname(path)))
        if paired is None:
            expected = os.path.join(os.path.dirname(path), "schema.json")
            return [
                f"Missing sibling schema.json for backward compatibility check: {expected}"
            ]

        schema_path, schema_data = paired
        prev_fields = extract_schema_fields(schema_data)
        new_fields = extract_contract_fields(data)
        if not prev_fields:
            return [f"Schema {schema_path} defines no fields; cannot verify backward compatibility"]

        _, violations = self._evaluator.evaluate_backward_compatibility(
            prev_fields=prev_fields,
            new_fields=new_fields,
        )
        return [
            f"Backward compatibility violation against {schema_path}: {message}"
            for message in violations
        ]

    # ------------------------------------------------------------------
    # Governance JSON schema validation (optional dependency)
    # ------------------------------------------------------------------
    def _jsonschema_errors(
        self,
        data: dict[str, Any],
        schema_filename: str,
        file_path: str,
        repo_root: str,
    ) -> list[str]:
        if jsonschema is None:
            if not self._warned_jsonschema:
                logger.warning(
                    "jsonschema library not available; skipping governance "
                    "JSON-schema validation (structural checks still apply)"
                )
                self._warned_jsonschema = True
            return []
        schema_path = os.path.join(repo_root, "governance", "schemas", schema_filename)
        if not os.path.isfile(schema_path):
            logger.warning("Governance schema not found: %s; skipping JSON-schema validation", schema_path)
            return []
        try:
            with open(schema_path, encoding="utf-8") as handle:
                schema = json.load(handle)
            validator = jsonschema.validators.validator_for(schema)(schema)
            messages = []
            for error in sorted(validator.iter_errors(data), key=lambda e: list(e.path)):
                location = "/".join(str(part) for part in error.path) or "(root)"
                messages.append(f"JSON-schema violation at '{location}': {error.message}")
            return messages
        except Exception as exc:
            logger.warning("JSON-schema validation could not run for %s: %s", file_path, exc)
            return []

    # ------------------------------------------------------------------
    # Result bookkeeping
    # ------------------------------------------------------------------
    @staticmethod
    def _record(
        results: dict[str, Any],
        errs: list[str],
        path: str,
        kind: str,
    ) -> None:
        if errs:
            results["passed"] = False
            results["errors"].append({"file": path, "kind": kind, "errors": errs})
