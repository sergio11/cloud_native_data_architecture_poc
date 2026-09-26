"""
Modern Data Architecture POC - Unit Tests: RF-15 Data Contract Validation.
Hermetic: every test builds its own domains/ tree under tmp_path and never
scans the real repository. Covers structure rules, BACKWARD compatibility
blocking (removed field / new required field / type change), the dual-shape
field normaliser, and the governance CLI delegator exit codes.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml

from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter
from src.domain.contracts import (
    SUPPORTED_COMPATIBILITY_MODES,
    ContractCompatibilityEvaluator,
    extract_contract_fields,
    extract_schema_fields,
    validate_compatibility_mode,
)
from src.use_cases.validate_contract import ValidateContractUseCase

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_PATH = REPO_ROOT / "governance" / "contract_validator.py"


def _field(name: str, field_type: str, required: bool = True) -> dict[str, Any]:
    return {"name": name, "type": field_type, "required": required}


def _write_product(
    root: Path,
    folder: str,
    name: str,
    contract_fields: list[dict[str, Any]],
    schema_fields: list[dict[str, Any]],
    *,
    owner: str | None = "data-team@company.internal",
    domain: str = "sales",
) -> Path:
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


def _validate(root: Path) -> dict[str, Any]:
    use_case = ValidateContractUseCase(catalog=FsDomainCatalogAdapter(str(root)))
    return use_case.execute(repo_root=str(root))


def _errors_for(results: dict[str, Any], fragment: str) -> list[str]:
    messages: list[str] = []
    for entry in results["errors"]:
        if fragment.replace("\\", "/") in entry["file"].replace("\\", "/"):
            messages.extend(entry["errors"])
    return messages


def _base_fields() -> list[dict[str, Any]]:
    return [
        _field("id", "integer"),
        _field("status", "string"),
        _field("total_amount", "decimal(12,2)"),
        _field("created_at", "timestamp"),
    ]


def _base_schema_fields() -> list[dict[str, Any]]:
    return [
        {"name": "id", "type": "integer"},
        {"name": "status", "type": "string"},
        {"name": "total_amount", "type": "number"},
        {"name": "created_at", "type": "string", "format": "date-time"},
    ]


def _run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(CLI_PATH), *args],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=120,
    )


# ---------------------------------------------------------------------------
# Use case: structure + backward compatibility
# ---------------------------------------------------------------------------


def test_valid_product_set_passes(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "beta",
        "beta",
        [_field("id", "integer"), _field("name", "string")],
        [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}],
    )

    results = _validate(tmp_path)

    assert results["products_evaluated"] == 2
    assert results["contracts_evaluated"] == 2
    assert results["errors"] == []
    assert results["passed"] is True
    details = {d["file"].replace("\\", "/"): d for d in results["details"]}
    alpha_contract = next(p for p in details if p.endswith("alpha/contract.yaml"))
    assert details[alpha_contract]["compatibility_mode"] == "BACKWARD"
    paired = details[alpha_contract]["backward_compatible_with"].replace("\\", "/")
    assert paired.endswith("alpha/schema.json")


def test_broken_product_missing_owner_reported(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "broken",
        "broken",
        [_field("id", "integer")],
        [{"name": "id", "type": "integer"}],
        owner=None,
    )

    results = _validate(tmp_path)

    assert results["passed"] is False
    messages = _errors_for(results, "broken/product.yaml")
    assert any("Missing metadata.owner" in m for m in messages)


def test_removed_field_blocks_publication(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "removed",
        "removed",
        [_field("id", "integer"), _field("status", "string")],
        [
            {"name": "id", "type": "integer"},
            {"name": "status", "type": "string"},
            {"name": "legacy_note", "type": "string"},
        ],
    )

    results = _validate(tmp_path)

    assert results["passed"] is False
    messages = _errors_for(results, "removed/contract.yaml")
    assert any("legacy_note" in m and "removed" in m for m in messages)
    assert any("schema.json" in m for m in messages)


def test_new_required_field_blocks_publication(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "added",
        "added",
        _base_fields() + [_field("country", "string", required=True)],
        _base_schema_fields(),
    )

    results = _validate(tmp_path)

    assert results["passed"] is False
    messages = _errors_for(results, "added/contract.yaml")
    assert any("country" in m and "required=true" in m for m in messages)


def test_new_optional_field_is_allowed(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "extended",
        "extended",
        _base_fields() + [_field("comment", "string", required=False)],
        _base_schema_fields(),
    )

    results = _validate(tmp_path)

    assert results["passed"] is True, results["errors"]


def test_type_change_blocks_publication(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "retyped",
        "retyped",
        [
            _field("id", "integer"),
            _field("status", "integer"),
            _field("total_amount", "decimal(12,2)"),
            _field("created_at", "timestamp"),
        ],
        _base_schema_fields(),
    )

    results = _validate(tmp_path)

    assert results["passed"] is False
    messages = _errors_for(results, "retyped/contract.yaml")
    assert any("status" in m and "changed type" in m for m in messages)


def test_missing_schema_json_blocks_backward_check(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    product_dir = _write_product(
        tmp_path,
        "noschema",
        "noschema",
        [_field("id", "integer")],
        [{"name": "id", "type": "integer"}],
    )
    (product_dir / "schema.json").unlink()

    results = _validate(tmp_path)

    assert results["passed"] is False
    messages = _errors_for(results, "noschema/contract.yaml")
    assert any("Missing sibling schema.json" in m for m in messages)


def test_adapter_skips_malformed_files(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    broken_dir = tmp_path / "domains" / "sales" / "data-products" / "corrupt"
    broken_dir.mkdir(parents=True, exist_ok=True)
    (broken_dir / "product.yaml").write_text("metadata: [::", encoding="utf-8")

    adapter = FsDomainCatalogAdapter(str(tmp_path))
    products = adapter.list_products()

    assert len(products) == 1
    assert products[0][0].replace("\\", "/").endswith("alpha/product.yaml")


# ---------------------------------------------------------------------------
# Domain: normaliser shapes and compatibility mode validation
# ---------------------------------------------------------------------------


def test_evaluator_accepts_contract_and_jsonschema_shapes():
    canonical = [
        {"name": "id", "type": "integer", "required": True},
        {"name": "amount", "type": "decimal(18,2)", "required": True},
        {"name": "created_at", "type": "timestamp", "required": False},
    ]
    alternative = [
        {"name": "id", "data_type": "integer", "nullable": False},
        {"name": "amount", "data_type": "number", "nullable": False},
        {"name": "created_at", "data_type": ["string", "null"], "format": "date-time", "nullable": True},
    ]

    passed, errors = ContractCompatibilityEvaluator.evaluate_backward_compatibility(
        prev_fields=canonical, new_fields=alternative
    )
    assert passed is True, errors
    assert errors == []


def test_evaluator_flags_type_change_in_alternative_shape():
    passed, errors = ContractCompatibilityEvaluator.evaluate_backward_compatibility(
        prev_fields=[{"name": "id", "type": "string", "required": True}],
        new_fields=[{"name": "id", "data_type": "integer", "nullable": False}],
    )
    assert passed is False
    assert any("changed type from 'string' to 'integer'" in e for e in errors)


def test_extract_helpers_navigate_spec_and_root():
    contract = {"spec": {"schema": {"fields": [{"name": "id", "type": "integer", "required": True}]}}}
    assert extract_contract_fields(contract) == [{"name": "id", "type": "integer", "required": True}]
    assert extract_contract_fields({"schema": {"fields": [{"name": "id"}]}}) == [{"name": "id"}]
    assert extract_contract_fields({}) == []

    schema = {"required": ["id"], "properties": {"id": {"type": "integer"}}}
    assert extract_schema_fields(schema) == [{"name": "id", "type": "integer", "required": True}]
    assert extract_schema_fields({"spec": {"schema": {"fields": [{"name": "id"}]}}}) == [
        {"name": "id"}
    ]
    assert extract_schema_fields({}) == []


def test_validate_compatibility_mode_rules():
    assert SUPPORTED_COMPATIBILITY_MODES == ["BACKWARD", "FORWARD", "FULL", "NONE"]

    ok, errors = validate_compatibility_mode({"spec": {"compatibility_mode": "BACKWARD"}})
    assert ok is True and errors == []

    ok, errors = validate_compatibility_mode(
        {"spec": {"compatibility": {"policy": "FULL"}}}
    )
    assert ok is True and errors == []

    ok, errors = validate_compatibility_mode({"spec": {}})
    assert ok is False
    assert any("Missing compatibility policy" in e for e in errors)

    ok, errors = validate_compatibility_mode({"spec": {"compatibility_mode": "Sideways"}})
    assert ok is False
    assert any("Unsupported compatibility mode 'SIDEWAYS'" in e for e in errors)


# ---------------------------------------------------------------------------
# Governance CLI delegator (subprocess, exit codes)
# ---------------------------------------------------------------------------


def test_cli_repo_mode_exit_codes(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "beta",
        "beta",
        [_field("id", "integer")],
        [{"name": "id", "type": "integer"}],
    )
    passing = _run_cli("--repo-root", str(tmp_path))
    assert passing.returncode == 0, passing.stderr
    assert "successfully validated" in passing.stderr

    _write_product(
        tmp_path,
        "broken",
        "broken",
        [_field("id", "integer")],
        [{"name": "id", "type": "integer"}],
        owner=None,
    )
    failing = _run_cli("--repo-root", str(tmp_path))
    assert failing.returncode == 1
    assert "failed validation" in failing.stderr
    assert "Missing metadata.owner" in failing.stderr


def test_cli_repo_mode_blocks_incompatible_contract(tmp_path):
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(
        tmp_path,
        "retyped",
        "retyped",
        [_field("id", "string"), _field("status", "string")],
        [{"name": "id", "type": "integer"}, {"name": "status", "type": "string"}],
    )

    result = _run_cli("--repo-root", str(tmp_path))

    assert result.returncode == 1
    assert "changed type" in result.stderr


def test_cli_legacy_two_file_mode(tmp_path):
    prev_fields = [_field("id", "integer"), _field("status", "string")]
    new_fields_removed = [_field("id", "integer")]
    base_contract = {"spec": {"schema": {"fields": prev_fields}}}

    prev_file = tmp_path / "prev_contract.yaml"
    prev_file.write_text(yaml.safe_dump(base_contract), encoding="utf-8")

    compatible = tmp_path / "new_contract.yaml"
    compatible.write_text(
        yaml.safe_dump({"spec": {"schema": {"fields": prev_fields}}}), encoding="utf-8"
    )
    breaking = tmp_path / "breaking_contract.yaml"
    breaking.write_text(
        yaml.safe_dump({"spec": {"schema": {"fields": new_fields_removed}}}), encoding="utf-8"
    )

    ok = _run_cli("--file", str(compatible), "--prev-file", str(prev_file))
    assert ok.returncode == 0, ok.stderr
    assert "BACKWARD compatibility check passed successfully." in ok.stderr

    bad = _run_cli("--file", str(breaking), "--prev-file", str(prev_file))
    assert bad.returncode == 1
    assert "BACKWARD COMPATIBILITY VIOLATION" in bad.stderr
    assert "status" in bad.stderr
