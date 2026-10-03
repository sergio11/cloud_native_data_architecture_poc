"""
Modern Data Architecture POC - Unit Tests: RF-15 Data Contract Validation.
Hermetic: every test builds its own domains/ tree under tmp_path and never
scans the real repository. Covers structure rules, BACKWARD compatibility
blocking (removed field / new required field / type change), the dual-shape
field normaliser, and the governance CLI delegator exit codes.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
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
from tests.helpers import contract_field as _field
from tests.helpers import write_contract_product as _write_product

REPO_ROOT = Path(__file__).resolve().parents[2]
CLI_PATH = REPO_ROOT / "governance" / "contract_validator.py"


def _validate(root: Path) -> dict[str, Any]:
    use_case = ValidateContractUseCase(catalog=FsDomainCatalogAdapter(str(root)))
    return use_case.execute()


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


@pytest.mark.parametrize(
    ("folder", "contract_fields", "schema_fields", "expected_groups"),
    [
        pytest.param(
            "removed",
            [_field("id", "integer"), _field("status", "string")],
            [
                {"name": "id", "type": "integer"},
                {"name": "status", "type": "string"},
                {"name": "legacy_note", "type": "string"},
            ],
            [("legacy_note", "removed"), ("schema.json",)],
            id="removed_field",
        ),
        pytest.param(
            "added",
            _base_fields() + [_field("country", "string", required=True)],
            _base_schema_fields(),
            [("country", "required=true")],
            id="new_required_field",
        ),
        pytest.param(
            "retyped",
            [
                _field("id", "integer"),
                _field("status", "integer"),
                _field("total_amount", "decimal(12,2)"),
                _field("created_at", "timestamp"),
            ],
            _base_schema_fields(),
            [("status", "changed type")],
            id="type_change",
        ),
    ],
)
def test_backward_incompatible_changes_block_publication(
    tmp_path: Path,
    folder: str,
    contract_fields: list[dict[str, Any]],
    schema_fields: list[dict[str, Any]],
    expected_groups: list[tuple[str, ...]],
) -> None:
    _write_product(tmp_path, "alpha", "alpha", _base_fields(), _base_schema_fields())
    _write_product(tmp_path, folder, folder, contract_fields, schema_fields)

    results = _validate(tmp_path)

    assert results["passed"] is False
    messages = _errors_for(results, f"{folder}/contract.yaml")
    for group in expected_groups:
        assert any(all(fragment in m for fragment in group) for m in messages), messages


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


@pytest.mark.parametrize(
    ("prev_fields", "new_fields", "expected_passed", "expected_fragment"),
    [
        pytest.param(
            [
                {"name": "id", "type": "integer", "required": True},
                {"name": "amount", "type": "decimal(18,2)", "required": True},
                {"name": "created_at", "type": "timestamp", "required": False},
            ],
            [
                {"name": "id", "data_type": "integer", "nullable": False},
                {"name": "amount", "data_type": "number", "nullable": False},
                {"name": "created_at", "data_type": ["string", "null"], "format": "date-time", "nullable": True},
            ],
            True,
            None,
            id="contract_and_jsonschema_shapes_compatible",
        ),
        pytest.param(
            [{"name": "id", "type": "string", "required": True}],
            [{"name": "id", "data_type": "integer", "nullable": False}],
            False,
            "changed type from 'string' to 'integer'",
            id="alternative_shape_type_change",
        ),
    ],
)
def test_evaluator_backward_compatibility_shapes(
    prev_fields: list[dict[str, Any]],
    new_fields: list[dict[str, Any]],
    expected_passed: bool,
    expected_fragment: str | None,
) -> None:
    passed, errors = ContractCompatibilityEvaluator.evaluate_backward_compatibility(
        prev_fields=prev_fields, new_fields=new_fields
    )
    assert passed is expected_passed
    if expected_fragment is None:
        assert errors == []
    else:
        assert any(expected_fragment in e for e in errors)


@pytest.mark.parametrize(
    ("contract", "expected"),
    [
        pytest.param(
            {"spec": {"schema": {"fields": [{"name": "id", "type": "integer", "required": True}]}}},
            [{"name": "id", "type": "integer", "required": True}],
            id="spec_shape",
        ),
        pytest.param(
            {"schema": {"fields": [{"name": "id"}]}},
            [{"name": "id"}],
            id="root_shape",
        ),
        pytest.param({}, [], id="missing"),
    ],
)
def test_extract_contract_fields_cases(contract: dict[str, Any], expected: list[dict[str, Any]]) -> None:
    assert extract_contract_fields(contract) == expected


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        pytest.param(
            {"required": ["id"], "properties": {"id": {"type": "integer"}}},
            [{"name": "id", "type": "integer", "required": True}],
            id="jsonschema_shape",
        ),
        pytest.param(
            {"spec": {"schema": {"fields": [{"name": "id"}]}}},
            [{"name": "id"}],
            id="spec_shape",
        ),
        pytest.param({}, [], id="missing"),
    ],
)
def test_extract_schema_fields_cases(schema: dict[str, Any], expected: list[dict[str, Any]]) -> None:
    assert extract_schema_fields(schema) == expected


def test_supported_compatibility_modes() -> None:
    assert SUPPORTED_COMPATIBILITY_MODES == ["BACKWARD", "FORWARD", "FULL", "NONE"]


@pytest.mark.parametrize(
    ("contract", "expected_ok", "expected_fragment"),
    [
        pytest.param({"spec": {"compatibility_mode": "BACKWARD"}}, True, None, id="explicit_backward"),
        pytest.param({"spec": {"compatibility": {"policy": "FULL"}}}, True, None, id="nested_policy"),
        pytest.param({"spec": {}}, False, "Missing compatibility policy", id="missing_policy"),
        pytest.param(
            {"spec": {"compatibility_mode": "Sideways"}},
            False,
            "Unsupported compatibility mode 'SIDEWAYS'",
            id="unsupported_mode",
        ),
    ],
)
def test_validate_compatibility_mode_rules(
    contract: dict[str, Any], expected_ok: bool, expected_fragment: str | None
) -> None:
    ok, errors = validate_compatibility_mode(contract)
    assert ok is expected_ok
    if expected_fragment is None:
        assert errors == []
    else:
        assert any(expected_fragment in e for e in errors)


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


@pytest.mark.parametrize(
    ("breaking", "expected_code", "expected_fragments"),
    [
        pytest.param(False, 0, ["BACKWARD compatibility check passed successfully."], id="compatible"),
        pytest.param(True, 1, ["BACKWARD COMPATIBILITY VIOLATION", "status"], id="breaking_removed_field"),
    ],
)
def test_cli_legacy_two_file_mode(
    tmp_path: Path, breaking: bool, expected_code: int, expected_fragments: list[str]
) -> None:
    prev_fields = [_field("id", "integer"), _field("status", "string")]
    new_fields = [_field("id", "integer")] if breaking else list(prev_fields)
    base_contract = {"spec": {"schema": {"fields": prev_fields}}}

    prev_file = tmp_path / "prev_contract.yaml"
    prev_file.write_text(yaml.safe_dump(base_contract), encoding="utf-8")

    new_file = tmp_path / "new_contract.yaml"
    new_file.write_text(yaml.safe_dump({"spec": {"schema": {"fields": new_fields}}}), encoding="utf-8")

    result = _run_cli("--file", str(new_file), "--prev-file", str(prev_file))

    assert result.returncode == expected_code, result.stderr
    for fragment in expected_fragments:
        assert fragment in result.stderr


# ---------------------------------------------------------------------------
# Governance JSON-schema validation is fail-closed (RF-15)
# ---------------------------------------------------------------------------


def _install_fake_jsonschema(monkeypatch: pytest.MonkeyPatch, validator_for) -> None:
    """Registers a stub ``jsonschema`` module exposing ``validators``/``exceptions``."""
    import types

    class FakeSchemaError(Exception):
        pass

    fake_exceptions = types.ModuleType("jsonschema.exceptions")
    fake_exceptions.SchemaError = FakeSchemaError  # type: ignore[attr-defined]
    fake_validators = types.ModuleType("jsonschema.validators")
    fake_validators.validator_for = validator_for  # type: ignore[attr-defined]
    fake_jsonschema = types.ModuleType("jsonschema")
    fake_jsonschema.exceptions = fake_exceptions  # type: ignore[attr-defined]
    fake_jsonschema.validators = fake_validators  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "jsonschema", fake_jsonschema)
    monkeypatch.setitem(sys.modules, "jsonschema.exceptions", fake_exceptions)
    monkeypatch.setitem(sys.modules, "jsonschema.validators", fake_validators)


def _use_case_with_schema(schema: dict[str, Any] | None) -> ValidateContractUseCase:
    from types import SimpleNamespace

    catalog = SimpleNamespace(load_governance_schema=lambda name: schema)
    return ValidateContractUseCase(catalog=catalog)  # type: ignore[arg-type]


def test_invalid_governance_schema_blocks_instead_of_silently_passing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: a broken governance schema must surface as an error (RF-15),
    never degrade to 'no errors' with a mere warning."""

    def explode(schema: dict[str, Any]):
        import sys as _sys

        raise _sys.modules["jsonschema"].exceptions.SchemaError("bad schema")

    _install_fake_jsonschema(monkeypatch, explode)
    use_case = _use_case_with_schema({"type": "nonsense"})

    errs = use_case._jsonschema_errors({"name": "x"}, "product-schema.json", "domains/d/product.yaml")

    assert len(errs) == 1
    assert "invalid" in errs[0].lower()


def test_jsonschema_runtime_failure_blocks_instead_of_silently_passing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Any validation-time failure (unresolvable $ref, library error, ...) blocks."""

    class BoomValidator:
        def __init__(self, schema: dict[str, Any]) -> None:
            pass

        def iter_errors(self, data: dict[str, Any]):
            raise RuntimeError("cannot resolve $ref")

    _install_fake_jsonschema(monkeypatch, lambda schema: BoomValidator)
    use_case = _use_case_with_schema({"type": "object"})

    errs = use_case._jsonschema_errors({"name": "x"}, "product-schema.json", "domains/d/product.yaml")

    assert len(errs) == 1
    assert "could not run" in errs[0]


def test_jsonschema_violations_still_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    """The happy path is unchanged: violations are collected, not raised."""

    class FakeError:
        def __init__(self, path: list[str], message: str) -> None:
            self.path = path
            self.message = message

    class OkValidator:
        def __init__(self, schema: dict[str, Any]) -> None:
            pass

        def iter_errors(self, data: dict[str, Any]):
            yield FakeError(["metadata", "owner"], "required")

    _install_fake_jsonschema(monkeypatch, lambda schema: OkValidator)
    use_case = _use_case_with_schema({"type": "object"})

    errs = use_case._jsonschema_errors({"name": "x"}, "product-schema.json", "domains/d/product.yaml")

    assert errs == ["JSON-schema violation at 'metadata/owner': required"]
