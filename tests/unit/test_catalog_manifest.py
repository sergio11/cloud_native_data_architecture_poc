"""
Modern Data Architecture POC - Unit Tests: Catalog Manifests & Quality Resource.

Hermetic tests (tmp_path fixtures only).  The Dagster quality resource is
verified against a stubbed ``src.container`` module injected into
``sys.modules`` — the real container (and its lakehouse adapters) is never
imported here.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import yaml

from pipelines.serving.catalog_registration import (
    GOLD_TABLES,
    STREAM_TABLES,
    ManifestUpdater,
)

# Tables the hexagonal core actually produces today:
#   AggregateGoldUseCase  -> gold.sales.daily_metrics, gold.sales.customers
#   ProcessStreamUseCase  -> stream.sales.orders_windowed (flink_orders_stream)
#                          -> stream.marketing.clickstream_windowed (flink_clickstream_stream)
KNOWN_PRODUCED_TABLES = {
    "gold.sales.daily_metrics",
    "gold.sales.customers",
    "stream.sales.orders_windowed",
    "stream.marketing.clickstream_windowed",
}


def _write_manifest(
    root: Path,
    domain: str,
    product: str,
    spec_assets: list[dict],
    top_level_assets: list[dict] | None = None,
) -> Path:
    """Create a ``domains/<domain>/data-products/<product>/product.yaml`` tree."""
    product_dir = root / "domains" / domain / "data-products" / product
    product_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict = {
        "apiVersion": "dataplatform.io/v1alpha1",
        "kind": "DataProduct",
        "metadata": {"name": product, "domain": domain},
        "spec": {
            "description": "test product",
            "classification": "internal",
            "assets": spec_assets,
        },
    }
    if top_level_assets is not None:
        manifest["assets"] = top_level_assets
    path = product_dir / "product.yaml"
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    return path


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _logical_name(table: dict) -> str:
    """Derive the logical table name from the warehouse location URI.

    ``s3://datalake/warehouse/gold/sales/daily_metrics`` -> ``gold.sales.daily_metrics``.
    """
    return ".".join(table["location"].split("/")[4:])


def test_manifest_updater_finds_manifest(tmp_path: Path) -> None:
    manifest_path = _write_manifest(
        tmp_path,
        "sales",
        "orders",
        spec_assets=[{"name": "gold_orders", "path": "s3a://old/path/"}],
    )

    # Repo-root style tree: <root>/domains/<domain>/data-products/<product>/product.yaml
    updater = ManifestUpdater(manifests_dir=tmp_path)
    assert updater.known_products == [("sales", "orders")]

    new_path = "s3://datalake/warehouse/gold/sales/daily_metrics"
    updater.update_table_path("sales", "orders", "gold_orders", new_path)

    data = _load(manifest_path)
    assert data["spec"]["assets"][0]["path"] == new_path, (
        "manifest was not located/updated for ('sales', 'orders')"
    )

    # domains-directory style tree: <root>/<domain>/data-products/<product>/product.yaml
    sub_updater = ManifestUpdater(manifests_dir=tmp_path / "domains")
    assert sub_updater.known_products == [("sales", "orders")]


def test_update_table_path_targets_spec_assets(tmp_path: Path) -> None:
    manifest_path = _write_manifest(
        tmp_path,
        "sales",
        "orders",
        spec_assets=[
            {"name": "gold_orders", "path": "s3a://old/", "type": "iceberg_table"},
            {"name": "silver_orders", "path": "s3a://silver/keep/"},
        ],
        top_level_assets=[{"name": "stale", "path": "s3a://top-level-old/"}],
    )

    updater = ManifestUpdater(manifests_dir=tmp_path)
    updater.update_table_path("sales", "orders", "gold_orders", "s3://new/path/")

    data = _load(manifest_path)
    spec_assets = data["spec"]["assets"]
    # Matching spec asset updated in place, other fields preserved...
    assert len(spec_assets) == 2
    assert spec_assets[0] == {
        "name": "gold_orders",
        "path": "s3://new/path/",
        "type": "iceberg_table",
    }
    # ...sibling spec assets untouched...
    assert spec_assets[1]["path"] == "s3a://silver/keep/"
    # ...and any top-level ``assets`` key is left alone.
    assert data["assets"] == [{"name": "stale", "path": "s3a://top-level-old/"}]


def test_registry_has_no_phantom_tables() -> None:
    gold_names = {f"gold.{t['namespace']}.{t['name']}" for t in GOLD_TABLES}
    assert gold_names == {
        "gold.sales.daily_metrics",
        "gold.sales.customers",
    }
    assert not any(t["namespace"] in {"marketing", "finance"} for t in GOLD_TABLES)

    # Every registered location must correspond to a table the core produces.
    assert {_logical_name(t) for t in GOLD_TABLES + STREAM_TABLES} == KNOWN_PRODUCED_TABLES

    # No dim_customers drift anywhere in the registry or the source file.
    serialized = " ".join(
        f"{t['namespace']}.{t['name']} {t['location']}"
        for t in GOLD_TABLES + STREAM_TABLES
    )
    assert "dim_customers" not in serialized
    assert "funnel" not in serialized

    import pipelines.serving.catalog_registration as catalog_module

    source = Path(catalog_module.__file__).read_text(encoding="utf-8")
    assert "dim_customers" not in source


def test_quality_resource_delegates(monkeypatch) -> None:
    from pipelines.orchestration.resources import GreatExpectationsResource
    from src.domain.exceptions import SuiteNotFoundError

    calls: list[dict] = []
    state: dict = {"passed": True, "raise": None}

    class _FakeQualityGate:
        def execute(self, **kwargs):
            calls.append(kwargs)
            if state["raise"] is not None:
                raise state["raise"]
            return types.SimpleNamespace(passed=state["passed"])

    fake_container_module = types.ModuleType("src.container")
    fake_container_module.container = types.SimpleNamespace(
        run_quality_gate=_FakeQualityGate()
    )
    monkeypatch.setitem(sys.modules, "src.container", fake_container_module)

    resource = GreatExpectationsResource()

    # Verdict propagates on success...
    assert resource.run_suite("silver_orders_suite.json", "silver.sales.orders") is True
    # ...and on gate failure (the old stub always returned True)...
    state["passed"] = False
    assert resource.run_suite("silver_orders_suite.json", "silver.sales.orders") is False
    # ...and execution errors (e.g. SuiteNotFoundError) are converted to False.
    state["raise"] = SuiteNotFoundError("Quality expectation suite not found: nope.json")
    assert resource.run_suite("nope.json", "silver.sales.orders") is False

    assert calls[0]["table_name"] == "silver.sales.orders"
    assert calls[0]["suite_path"].endswith("silver_orders_suite.json")
    assert calls[0]["threshold"] == 0.95
    assert calls[1]["table_name"] == "silver.sales.orders"
