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

import json
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
