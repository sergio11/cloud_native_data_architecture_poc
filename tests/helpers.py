"""
Modern Data Architecture POC - Shared test helpers (deduplicated builders).

Single home for the contract/product builders copied between
``tests/unit/test_contract_validation.py`` and
``tests/integration/test_rf_acceptance.py``, and for the minimal in-memory
``LineagePublisherPort`` stub copied across ``test_quality_gate.py``,
``test_ingest_idempotency.py`` and ``test_silver_latest_wins.py``.

Semantics are the SUPERSET of the copies (owner/domain/format handling from
the unit variant, which is backward compatible with the integration call
sites), so existing assertions behave identically.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml


def contract_field(name: str, field_type: str, required: bool = True) -> dict[str, Any]:
    """Builds a single contract field entry."""
    return {"name": name, "type": field_type, "required": required}


def write_contract_product(
    root: Path,
    folder: str,
    name: str,
    contract_fields: list[dict[str, Any]],
    schema_fields: list[dict[str, Any]],
    *,
    owner: str | None = "data-team@company.internal",
    domain: str = "sales",
) -> Path:
    """Materialises a minimal valid DataProduct trio (product/contract/schema)."""
    product_dir = root / "domains" / domain / "data-products" / folder
    product_dir.mkdir(parents=True, exist_ok=True)

    metadata: dict[str, Any] = {"name": name, "domain": domain, "version": "1.0.0"}
    if owner is not None:
        metadata["owner"] = owner
    product = {
        "apiVersion": "dataplatform.io/v1alpha1",
        "kind": "DataProduct",
        "metadata": metadata,
        "spec": {
            "description": f"Test data product {name}",
            "classification": "internal",
            "sla": {"freshness": "1h"},
            "assets": [
                {"name": f"gold_{name}", "type": "iceberg_table", "path": f"s3a://gold/{name}/"}
            ],
            "quality": {"suite": "quality/expectations/suite.json"},
            "contract": "./contract.yaml",
        },
    }
    (product_dir / "product.yaml").write_text(yaml.safe_dump(product), encoding="utf-8")

    contract = {
        "apiVersion": "datacontract.com/v1alpha1",
        "kind": "DataContract",
        "metadata": {
            "name": f"{name}-contract",
            "version": "1.0.0",
            "domain": domain,
            "data_product": name,
            "owner": owner or "unknown@company.internal",
        },
        "spec": {
            "description": f"Test contract for {name}",
            "compatibility_mode": "BACKWARD",
            "sla": {"freshness": "1h"},
            "schema": {"format": "iceberg", "fields": contract_fields},
            "quality": {"thresholds": {"completeness": 0.95}},
        },
    }
    (product_dir / "contract.yaml").write_text(yaml.safe_dump(contract), encoding="utf-8")

    properties: dict[str, Any] = {}
    required: list[str] = []
    for field_def in schema_fields:
        prop: dict[str, Any] = {"type": field_def["type"]}
        if field_def.get("format"):
            prop["format"] = field_def["format"]
        properties[field_def["name"]] = prop
        if field_def.get("required", True):
            required.append(field_def["name"])
    schema_doc = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }
    (product_dir / "schema.json").write_text(json.dumps(schema_doc, indent=2), encoding="utf-8")
    return product_dir


class StubLineage:
    """Minimal in-memory LineagePublisherPort stub recording emitted events."""

    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []

    def emit_job_event(
        self,
        job_name: str,
        inputs: list[str],
        outputs: list[str],
        status: str = "COMPLETE",
    ) -> bool:
        self.events.append({
            "job_name": job_name,
            "inputs": list(inputs),
            "outputs": list(outputs),
            "status": status,
        })
        return True


class EmptyContractCatalog:
    """Minimal ContractCatalogPort stub advertising no contracts.

    Quality gating then falls back to the explicit/global threshold, which is
    what tests that do not exercise contract-threshold precedence need.
    """

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        return []


def check_lsp(port_method: Callable[..., Any], impl_method: Callable[..., Any]) -> None:
    """Asserts the implementation honours the port contract (Liskov).

    Every port-declared parameter must exist on the implementation (with a
    default when the port makes it optional), and the implementation must not
    REQUIRE parameters the port does not declare - otherwise a caller using
    only the port signature cannot invoke it.
    """
    port_params = [p for p in inspect.signature(port_method).parameters.values() if p.name != "self"]
    impl_by_name = {p.name: p for p in inspect.signature(impl_method).parameters.values() if p.name != "self"}
    port_by_name = {p.name: p for p in port_params}
    for name, port_param in port_by_name.items():
        assert name in impl_by_name, f"implementation is missing port-declared parameter '{name}'"
        if port_param.default is not inspect.Parameter.empty:
            assert impl_by_name[name].default is not inspect.Parameter.empty, (
                f"port parameter '{name}' is optional but the implementation requires it"
            )
    position_kinds = {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }
    for name, impl_param in impl_by_name.items():
        if impl_param.kind in position_kinds and impl_param.default is inspect.Parameter.empty:
            assert name in port_by_name, (
                f"implementation requires '{name}' which the port contract does not declare"
            )
