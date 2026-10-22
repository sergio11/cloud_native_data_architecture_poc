"""W6 governance/domain/infra consistency tests.

Covers the quality-threshold cross-check, manifest URI schemes and the
classification/naming consistency across schema, policies and CRD.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

from governance.contract_validator import cross_check_quality_thresholds

REPO_ROOT = Path(__file__).resolve().parents[2]
DOMAINS = REPO_ROOT / "domains"


def _write_product_contract(
    root: Path,
    *,
    product_quality: dict,
    contract_thresholds: dict,
) -> Path:
    product_dir = root / "domains" / "sales" / "data-products" / "demo"
    product_dir.mkdir(parents=True, exist_ok=True)
    product_dir.joinpath("product.yaml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "dataplatform.io/v1alpha1",
                "kind": "DataProduct",
                "metadata": {"name": "demo", "domain": "sales"},
                "spec": {
                    "description": "demo",
                    "classification": "internal",
                    "assets": [{"name": "gold_demo", "type": "iceberg_table", "path": "s3://x/"}],
                    "quality": product_quality,
                    "contract": "./contract.yaml",
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    product_dir.joinpath("contract.yaml").write_text(
        yaml.safe_dump(
            {
                "apiVersion": "datacontract.com/v1alpha1",
                "kind": "DataContract",
                "metadata": {"name": "demo-contract", "data_product": "demo"},
                "spec": {
                    "compatibility_mode": "BACKWARD",
                    "schema": {"fields": [{"name": "id", "type": "integer"}]},
                    "quality": {"thresholds": contract_thresholds},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return product_dir


# ---------------------------------------------------------------------------
# T1: quality-threshold cross-check
# ---------------------------------------------------------------------------
def test_cross_check_flags_mismatched_threshold(tmp_path: Path) -> None:
    product_dir = _write_product_contract(
        tmp_path,
        product_quality={"completeness": 0.98, "validity": 0.995, "suite": "s.json"},
        contract_thresholds={"completeness": 0.98, "validity": 0.99},
    )

    violations = cross_check_quality_thresholds(str(tmp_path))

    assert len(violations) == 1, violations
    assert "validity" in violations[0]
    assert "0.995" in violations[0] and "0.99" in violations[0]
    assert product_dir.name in violations[0]


def test_cross_check_passes_when_thresholds_match(tmp_path: Path) -> None:
    _write_product_contract(
        tmp_path,
        # Matching shared dimensions; product-only (timeliness) and non-numeric
        # (suite) entries are informational and must not be compared.
        product_quality={"completeness": 1.0, "validity": 1.0, "timeliness": 0.99, "suite": "s.json"},
        contract_thresholds={"completeness": 1.0, "validity": 1.0},
    )

    assert cross_check_quality_thresholds(str(tmp_path)) == []


def test_cross_check_ignores_products_without_contract_thresholds(tmp_path: Path) -> None:
    _write_product_contract(
        tmp_path,
        product_quality={"validity": 0.9},
        contract_thresholds={},
    )

    assert cross_check_quality_thresholds(str(tmp_path)) == []


def test_repo_has_no_threshold_divergence() -> None:
    assert cross_check_quality_thresholds(str(REPO_ROOT)) == []


# ---------------------------------------------------------------------------
# T2: URI scheme + planned assets
# ---------------------------------------------------------------------------
def _manifests() -> list[Path]:
    return sorted(DOMAINS.glob("*/data-products/*/product.yaml"))


def test_manifests_declare_s3_uri_scheme_only() -> None:
    assert _manifests(), "expected checked-in data product manifests"
    for manifest in _manifests():
        data = yaml.safe_load(manifest.read_text(encoding="utf-8"))
        for asset in data["spec"]["assets"]:
            path = str(asset["path"])
            if "://" in path:
                assert path.startswith("s3://"), f"{manifest}: non s3:// asset path {path!r}"
                assert not path.startswith("s3a://"), f"{manifest}: legacy s3a:// path {path!r}"


def test_planned_assets_are_explicitly_marked_and_schema_allows_it() -> None:
    schema = json.loads((REPO_ROOT / "governance" / "schemas" / "product-schema.json").read_text(encoding="utf-8"))
    asset_props = schema["properties"]["spec"]["properties"]["assets"]["items"]["properties"]
    assert "planned" in asset_props["status"]["enum"], "schema must allow status: planned"

    planned = {
        (manifest.parent.parent.parent.name, asset["name"])
        for manifest in _manifests()
        for asset in yaml.safe_load(manifest.read_text(encoding="utf-8"))["spec"]["assets"]
        if asset.get("status") == "planned"
    }
    # Assets no pipeline produces (marketing/finance + funnel/leads silver) must
    # be declared planned; produced sales assets are never marked planned.
    assert planned == {
        ("finance", "gold_budgets"),
        ("finance", "gold_financial_closings"),
        ("finance", "silver_financial_closings"),
        ("marketing", "gold_campaigns"),
        ("marketing", "silver_campaigns"),
        ("marketing", "silver_leads"),
        ("marketing", "stream_funnel_metrics"),
    }


# ---------------------------------------------------------------------------
# T3: classification enum + naming/owner policy consistency
# ---------------------------------------------------------------------------
def test_classification_enum_consistent_everywhere() -> None:
    schema = json.loads((REPO_ROOT / "governance" / "schemas" / "product-schema.json").read_text(encoding="utf-8"))
    schema_levels = schema["properties"]["spec"]["properties"]["classification"]["enum"]

    policy = yaml.safe_load(
        (REPO_ROOT / "governance" / "policies" / "require-pii-classification.yaml").read_text(encoding="utf-8")
    )
    pattern = policy["spec"]["rules"][0]["validate"]["pattern"]["spec"]["classification"]
    policy_levels = [token.strip() for token in pattern.split("|")]

    federation = yaml.safe_load(
        (REPO_ROOT / "governance" / "federation" / "global-standards.yaml").read_text(encoding="utf-8")
    )
    federation_levels = [level["level"] for level in federation["spec"]["standards"]["classification"]["levels"]]

    crd_text = (REPO_ROOT / "infra" / "helm" / "s6-mesh" / "crds" / "dataproduct-crd.yaml").read_text(encoding="utf-8")
    crd_enum = re.search(r"enum:\s*\[([^\]]+)\]", crd_text)
    assert crd_enum, "DataProduct CRD classification enum not found"
    crd_levels = [token.strip().strip('"') for token in crd_enum.group(1).split(",")]

    assert schema_levels == policy_levels == crd_levels == federation_levels
    assert "restricted" in schema_levels and "pii" in schema_levels


def test_naming_policy_accepts_repo_product_names() -> None:
    text = (REPO_ROOT / "governance" / "policies" / "naming-convention.yaml").read_text(encoding="utf-8")
    match = re.search(r"regex_match\('([^']+)'", text)
    assert match, "naming policy must enforce the name rule via a regex deny condition"
    pattern = re.compile(match.group(1))

    names = [yaml.safe_load(manifest.read_text(encoding="utf-8"))["metadata"]["name"] for manifest in _manifests()]
    assert names, "expected data product manifests"
    for name in names:
        assert pattern.match(name), f"{name!r} rejected by the naming policy regex"

    # Underscores are allowed, but a leading hyphen (or uppercase/dots) is not.
    assert pattern.match("financial_closings")
    assert not pattern.match("-leading-hyphen")
    assert not pattern.match("UPPERCASE")
    assert not pattern.match("has.dot")


def test_owner_policy_message_matches_enforced_patterns() -> None:
    policy = yaml.safe_load((REPO_ROOT / "governance" / "policies" / "require-owner.yaml").read_text(encoding="utf-8"))
    validate = policy["spec"]["rules"][0]["validate"]
    message = validate["message"]

    assert "spec.owner" not in message
    assert "metadata.owner" in message
    # The patterns really check metadata.owner (what product.yaml provides)...
    assert validate["anyPattern"][0]["metadata"]["owner"]
    # ...or the documented annotation alternative (key carries the equality anchor).
    annotation_keys = validate["anyPattern"][1]["metadata"]["annotations"]
    assert any("dataplatform.io/owner" in key for key in annotation_keys)

    for manifest in _manifests():
        metadata = yaml.safe_load(manifest.read_text(encoding="utf-8"))["metadata"]
        assert metadata.get("owner"), f"{manifest} must declare metadata.owner"
