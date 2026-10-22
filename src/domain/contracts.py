"""Modern Data Architecture POC - Data contracts and data products.

Implements ODCS verification and strict BACKWARD compatibility rules.
Pure domain: no I/O.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class CompatibilityMode(StrEnum):
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD"
    FULL = "FULL"
    NONE = "NONE"


SUPPORTED_COMPATIBILITY_MODES: list[str] = [mode.value for mode in CompatibilityMode]

# Maps raw type spellings to canonical families for cross-vocabulary comparison.
_TYPE_ALIASES: dict[str, str] = {
    "int": "integer",
    "integer": "integer",
    "int2": "integer",
    "int4": "integer",
    "int8": "integer",
    "long": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "tinyint": "integer",
    "serial": "integer",
    "bigserial": "integer",
    "number": "number",
    "numeric": "number",
    "decimal": "number",
    "float": "number",
    "float4": "number",
    "float8": "number",
    "double": "number",
    "real": "number",
    "money": "number",
    "str": "string",
    "string": "string",
    "varchar": "string",
    "character": "string",
    "character varying": "string",
    "char": "string",
    "text": "string",
    "uuid": "string",
    "guid": "string",
    "json": "string",
    "jsonb": "string",
    "bool": "boolean",
    "boolean": "boolean",
    "date": "string",
    "datetime": "string",
    "timestamp": "string",
    "timestamptz": "string",
    "time": "string",
    "array": "array",
    "list": "array",
    "object": "object",
    "map": "object",
    "struct": "object",
    "record": "object",
    "bytes": "bytes",
    "binary": "bytes",
    "varbinary": "bytes",
    "blob": "bytes",
}

# Sub-type qualifier for temporal types (canonical form ``family:subtype``).
_TYPE_SUBTYPES: dict[str, str] = {
    "date": "date",
    "datetime": "datetime",
    "timestamp": "datetime",
    "timestamptz": "datetime",
    "time": "time",
}

_FORMAT_SUBTYPES: dict[str, str] = {
    "date": "date",
    "date-time": "datetime",
    "datetime": "datetime",
    "time": "time",
}


def _canonical_type(raw_type: Any, fmt: Any = None) -> str:
    """Normalises a raw type spelling to ``family`` or ``family:subtype``."""
    if isinstance(raw_type, (list, tuple, set)):
        candidates = [t for t in raw_type if isinstance(t, str) and t.strip().lower() != "null"]
        raw_type = candidates[0] if candidates else ""
    text = str(raw_type).strip().lower() if raw_type is not None else ""
    if ":" in text:
        return text
    base = text.split("(", 1)[0].strip()
    if not base:
        return ""
    family = _TYPE_ALIASES.get(base, base)
    subtype = _TYPE_SUBTYPES.get(base, "")
    if not subtype and family == "string" and fmt is not None:
        subtype = _FORMAT_SUBTYPES.get(str(fmt).strip().lower(), "")
    return f"{family}:{subtype}" if subtype else family


def _display_type(raw_type: Any) -> str:
    if isinstance(raw_type, (list, tuple, set)):
        candidates = [t for t in raw_type if isinstance(t, str) and t.strip().lower() != "null"]
        return candidates[0] if candidates else ""
    if raw_type is None:
        return ""
    return str(raw_type)


def _normalise_fields(fields: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Produces canonical ``{name, type, required, raw_type}`` dicts.

    Accepts contract and JSON-Schema field shapes; entries without a name are
    dropped. Idempotent.
    """
    normalised: list[dict[str, Any]] = []
    for field_def in fields or []:
        if not isinstance(field_def, dict):
            continue
        name = field_def.get("name")
        if not name:
            continue
        raw_type = field_def.get("type")
        if raw_type is None:
            raw_type = field_def.get("data_type", field_def.get("dataType"))
        required = field_def.get("required")
        if required is None and "nullable" in field_def:
            required = not field_def.get("nullable")
        normalised.append(
            {
                "name": str(name),
                "type": _canonical_type(raw_type, field_def.get("format")),
                "required": bool(required),
                "raw_type": _display_type(raw_type),
            }
        )
    return normalised


def _types_compatible(old_type: str, new_type: str) -> bool:
    """Family equality with optional sub-type refinement.

    A bare family is compatible with any sub-type; two distinct sub-types are not.
    """
    old_family, _, old_sub = old_type.partition(":")
    new_family, _, new_sub = new_type.partition(":")
    if old_family != new_family:
        return False
    if old_sub and new_sub:
        return old_sub == new_sub
    return True


def _nested_fields(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Navigates ``spec.schema.fields`` -> ``schema.fields`` -> ``fields``."""
    spec_raw = document.get("spec")
    spec: dict[str, Any] = spec_raw if isinstance(spec_raw, dict) else {}
    schema_raw = spec.get("schema")
    schema: dict[str, Any] = schema_raw if isinstance(schema_raw, dict) else {}
    if not schema:
        root_raw = document.get("schema")
        schema = root_raw if isinstance(root_raw, dict) else {}
    fields_raw = schema.get("fields")
    fields: list[Any] = fields_raw if isinstance(fields_raw, list) else []
    if not fields:
        top_raw = document.get("fields")
        fields = top_raw if isinstance(top_raw, list) else []
    return [f for f in fields if isinstance(f, dict)]


def extract_contract_fields(contract_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extracts raw field dicts from a contract.yaml document (pure navigation)."""
    if not isinstance(contract_data, dict):
        return []
    return _nested_fields(contract_data)


def resolve_contract_field_types(contract_data: dict[str, Any]) -> dict[str, str]:
    """Returns field name -> canonical type family for a contract document.

    Keeps only the family prefix so Silver treats dates as TEXT. Unknown types
    map to ``""`` and are left untouched.
    """
    families: dict[str, str] = {}
    for field_def in _normalise_fields(extract_contract_fields(contract_data)):
        family = str(field_def.get("type", "")).split(":")[0]
        families[str(field_def["name"])] = family
    return families


def extract_schema_fields(schema_data: dict[str, Any]) -> list[dict[str, Any]]:
    """Extracts raw field dicts from a schema.json document.

    Supports JSON-Schema ``properties`` + ``required`` and the field-list shape.
    """
    if not isinstance(schema_data, dict):
        return []
    properties = schema_data.get("properties")
    if isinstance(properties, dict) and properties:
        required_names = schema_data.get("required")
        if isinstance(required_names, str):
            required_names = [required_names]
        required_set = set(required_names) if isinstance(required_names, list) else set()
        fields: list[dict[str, Any]] = []
        for name, definition in properties.items():
            entry: dict[str, Any] = {"name": name}
            if isinstance(definition, dict):
                entry.update({k: v for k, v in definition.items() if k != "name"})
            entry["required"] = (name in required_set) or (entry.get("required") is True)
            fields.append(entry)
        return fields
    return _nested_fields(schema_data)


def resolve_compatibility_mode(contract_data: dict[str, Any]) -> str | None:
    """Returns the declared compatibility mode (upper-cased) or None.

    Searches ``spec.compatibility_mode``, ``spec.compatibility`` and root forms.
    """
    if not isinstance(contract_data, dict):
        return None
    spec_raw = contract_data.get("spec")
    spec: dict[str, Any] = spec_raw if isinstance(spec_raw, dict) else {}
    compat_raw = spec.get("compatibility")
    compat: dict[str, Any] = compat_raw if isinstance(compat_raw, dict) else {}
    root_compat_raw = contract_data.get("compatibility")
    root_compat: dict[str, Any] = root_compat_raw if isinstance(root_compat_raw, dict) else {}
    raw = (
        spec.get("compatibility_mode")
        or compat.get("mode")
        or compat.get("policy")
        or root_compat.get("mode")
        or root_compat.get("policy")
    )
    if raw is None:
        return None
    return str(raw).strip().upper() or None


def validate_compatibility_mode(contract_data: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validates the declared compatibility mode against SUPPORTED modes."""
    mode = resolve_compatibility_mode(contract_data)
    if mode is None:
        return False, [
            "Missing compatibility policy (spec.compatibility_mode, "
            "spec.compatibility.mode/policy, or compatibility.mode/policy)"
        ]
    if mode not in SUPPORTED_COMPATIBILITY_MODES:
        return False, [
            f"Unsupported compatibility mode '{mode}'; supported modes: {', '.join(SUPPORTED_COMPATIBILITY_MODES)}"
        ]
    return True, []


def extract_quality_thresholds(contract_data: dict[str, Any]) -> dict[str, float] | None:
    """Extracts declared per-dimension quality thresholds from a contract.

    Returns completeness/uniqueness/validity rates in [0, 1], or None when
    absent. Invalid values raise ``ValueError`` (fail-closed).
    """
    if not isinstance(contract_data, dict):
        return None
    spec_raw = contract_data.get("spec")
    if not isinstance(spec_raw, dict):
        return None
    quality_raw = spec_raw.get("quality")
    if not isinstance(quality_raw, dict):
        return None
    thresholds_raw = quality_raw.get("thresholds")
    if thresholds_raw is None:
        return None
    if not isinstance(thresholds_raw, dict):
        raise ValueError(
            f"Invalid contract quality thresholds: expected a mapping of dimension to rate, got {thresholds_raw!r}"
        )
    extracted: dict[str, float] = {}
    for dimension in ("completeness", "uniqueness", "validity"):
        if dimension not in thresholds_raw:
            continue
        value = thresholds_raw[dimension]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(
                f"Invalid quality threshold for dimension '{dimension}': expected a number in [0, 1], got {value!r}"
            )
        rate = float(value)
        if not 0.0 <= rate <= 1.0:
            raise ValueError(
                f"Invalid quality threshold for dimension '{dimension}': expected a number in [0, 1], got {value!r}"
            )
        extracted[dimension] = rate
    return extracted or None


class ContractCompatibilityEvaluator:
    """Domain service for verifying schema evolution and contract compatibility."""

    @staticmethod
    def evaluate_backward_compatibility(
        prev_fields: list[dict[str, Any]],
        new_fields: list[dict[str, Any]],
    ) -> tuple[bool, list[str]]:
        """Enforces BACKWARD compatibility.

        Existing fields cannot be deleted or renamed, new fields must be
        optional, and existing types cannot change incompatibly.
        """
        violations: list[str] = []
        old_map = {f["name"]: f for f in _normalise_fields(prev_fields)}
        new_map = {f["name"]: f for f in _normalise_fields(new_fields)}

        # Rule 1: Check for deleted or renamed fields
        for field_name in old_map:
            if field_name not in new_map:
                violations.append(
                    f"Breaking change: Field '{field_name}' was removed or renamed in new contract version."
                )

        # Rule 2: Check newly added fields for required=true
        for field_name, f_def in new_map.items():
            if field_name not in old_map and f_def.get("required", False):
                violations.append(
                    f"Breaking change: New field '{field_name}' is declared as required=true "
                    "(must be optional for BACKWARD compatibility)."
                )

        # Rule 3: Check for type modifications
        for field_name, old_def in old_map.items():
            if field_name in new_map:
                new_def = new_map[field_name]
                old_raw = old_def.get("raw_type", "")
                new_raw = new_def.get("raw_type", "")
                if old_raw and new_raw and not _types_compatible(old_def["type"], new_def["type"]):
                    violations.append(
                        f"Breaking change: Field '{field_name}' changed type from '{old_raw}' to '{new_raw}'."
                    )

        return len(violations) == 0, violations
