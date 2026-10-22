"""Modern Data Architecture POC - Data contract validation use case.

Validates DataProduct manifests and DataContracts: structure, governance
schemas, compatibility mode and BACKWARD evolution. All errors are aggregated.
"""

from __future__ import annotations

import logging
from pathlib import PurePath
from typing import Any

from src.domain.contracts import (
    ContractCompatibilityEvaluator,
    extract_contract_fields,
    extract_schema_fields,
    resolve_compatibility_mode,
    validate_compatibility_mode,
)
from src.ports.inbound import ValidateContractPort, ValidateContractResult
from src.ports.outbound import DomainCatalogPort

logger = logging.getLogger("use_case.validate_contract")


def _norm_path(path: str) -> str:
    """Normalises a catalog path lexically (pure computation, no filesystem access)."""
    return str(PurePath(path))


def _parent_dir(path: str) -> str:
    """Returns the parent directory of a catalog path (no filesystem access)."""
    return str(PurePath(path).parent)


class ValidateContractUseCase(ValidateContractPort):
    """Orchestrates validation of ODCS contracts and data products.

    ``catalog`` is required for manifest discovery and governance schemas.
    """

    def __init__(
        self,
        catalog: DomainCatalogPort,
        evaluator: ContractCompatibilityEvaluator | None = None,
    ) -> None:
        self._catalog = catalog
        self._evaluator = evaluator or ContractCompatibilityEvaluator()
        self._warned_jsonschema = False

    def execute(self) -> ValidateContractResult:
        catalog = self._catalog

        products = sorted(catalog.list_products(), key=lambda item: item[0])
        contracts = sorted(catalog.list_contracts(), key=lambda item: item[0])
        schemas = sorted(catalog.list_schemas(), key=lambda item: item[0])

        results: ValidateContractResult = {
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
                    "file": "domains",
                    "kind": "Discovery",
                    "errors": ["No data product manifests or data contracts discovered under domains/"],
                }
            )
            return results

        product_paths = {_norm_path(p) for p, _ in products}
        contract_paths = {_norm_path(p) for p, _ in contracts}
        schemas_by_dir = {_norm_path(_parent_dir(p)): (p, data) for p, data in schemas}

        # 1. Validate DataProduct manifests (structure + governance JSON schema)
        for path, data in products:
            errs = self._validate_product(data, path, contract_paths)
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
            errs = self._validate_contract(data, path, product_paths, schemas_by_dir)
            self._record(results, errs, path, kind="DataContract")
            if not errs:
                meta = data.get("metadata") or {}
                paired = schemas_by_dir.get(_norm_path(_parent_dir(path)))
                detail: dict[str, Any] = {
                    "file": path,
                    "status": "PASSED",
                    "kind": "DataContract",
                    "contract": meta.get("name"),
                    "compatibility_mode": resolve_compatibility_mode(data),
                    "backward_compatible_with": paired[0] if paired else None,
                }
                results["details"].append(detail)

        return results

    # Product manifest rules
    def _validate_product(
        self,
        data: dict[str, Any],
        path: str,
        contract_paths: set[str],
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
        parent = _parent_dir(path)
        # A sibling contract in discovery is valid; otherwise report missing.
        sibling = _norm_path(str(PurePath(parent, "contract.yaml")))
        if not contract_ref and sibling not in contract_paths:
            errs.append("Missing spec.contract reference or sibling contract.yaml")
        else:
            target = _norm_path(str(PurePath(parent, str(contract_ref)))) if contract_ref else sibling
            if target not in contract_paths:
                errs.append(f"Referenced contract.yaml missing or unreadable: {target}")

        errs.extend(self._jsonschema_errors(data, "product-schema.json", path))
        return errs

    # Contract rules (structure + mode + schema + backward compatibility)
    def _validate_contract(
        self,
        data: dict[str, Any],
        path: str,
        product_paths: set[str],
        schemas_by_dir: dict[str, Any],
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

        errs.extend(self._jsonschema_errors(data, "contract-schema.json", path))

        sibling_product = _norm_path(str(PurePath(_parent_dir(path), "product.yaml")))
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
        """Key rule: contract fields vs the sibling schema.json snapshot."""
        paired = schemas_by_dir.get(_norm_path(_parent_dir(path)))
        if paired is None:
            expected = str(PurePath(_parent_dir(path), "schema.json"))
            return [f"Missing sibling schema.json for backward compatibility check: {expected}"]

        schema_path, schema_data = paired
        prev_fields = extract_schema_fields(schema_data)
        new_fields = extract_contract_fields(data)
        if not prev_fields:
            return [f"Schema {schema_path} defines no fields; cannot verify backward compatibility"]

        _, violations = self._evaluator.evaluate_backward_compatibility(
            prev_fields=prev_fields,
            new_fields=new_fields,
        )
        return [f"Backward compatibility violation against {schema_path}: {message}" for message in violations]

    # Governance JSON schema validation (optional dependency)
    def _jsonschema_errors(
        self,
        data: dict[str, Any],
        schema_filename: str,
        file_path: str,
    ) -> list[str]:
        try:
            import jsonschema
            from jsonschema import exceptions as jsonschema_exceptions
        except ImportError:
            if not self._warned_jsonschema:
                logger.warning(
                    "jsonschema library not available; skipping governance "
                    "JSON-schema validation (structural checks still apply)"
                )
                self._warned_jsonschema = True
            return []
        # Governance schema comes from the catalog port: no file I/O here.
        schema = self._catalog.load_governance_schema(schema_filename)
        if schema is None:
            return []
        try:
            validator = jsonschema.validators.validator_for(schema)(schema)
            messages = []
            for error in sorted(validator.iter_errors(data), key=lambda e: list(e.path)):
                location = "/".join(str(part) for part in error.path) or "(root)"
                messages.append(f"JSON-schema violation at '{location}': {error.message}")
            return messages
        except jsonschema_exceptions.SchemaError as exc:
            # Broken governance schema: fail closed by reporting it.
            logger.error(
                "Governance JSON-schema '%s' is invalid; failing closed for %s: %s",
                schema_filename,
                file_path,
                exc,
            )
            return [f"Governance JSON-schema '{schema_filename}' is invalid: {exc}"]
        except Exception as exc:
            # Any other validation failure must also block.
            logger.error("JSON-schema validation could not run for %s: %s", file_path, exc)
            return [f"JSON-schema validation could not run for {file_path}: {exc}"]

    # Result bookkeeping
    @staticmethod
    def _record(
        results: ValidateContractResult,
        errs: list[str],
        path: str,
        kind: str,
    ) -> None:
        if errs:
            results["passed"] = False
            results["errors"].append({"file": path, "kind": kind, "errors": errs})
