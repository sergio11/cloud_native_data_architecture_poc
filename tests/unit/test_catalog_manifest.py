"""
Modern Data Architecture POC - Unit Tests: Catalog Manifests & Quality Resource.

Hermetic tests (tmp_path fixtures only, no network: HTTP calls are monkeypatched
at the ``_request`` boundary).  The Dagster quality resource is verified against
a stubbed ``src.container`` module injected into ``sys.modules`` - the real
container (and its lakehouse adapters) is never imported here.
"""

from __future__ import annotations

import re
import sys
import types
from pathlib import Path

import pytest
import requests
import yaml

import pipelines.serving.catalog_registration as catalog_module
from pipelines.serving.catalog_registration import (
    DATASET_IDS,
    GOLD_TABLES,
    NAMESPACES,
    PIPELINE_JOBS,
    STREAM_TABLES,
    ManifestUpdater,
    MarquezClient,
    PolarisClient,
    _register_marquez_datasets,
    _register_marquez_jobs,
    _register_marquez_namespaces,
    _register_polaris_namespaces,
    _register_polaris_tables,
    _replace_asset_path,
    _update_manifests,
    dataset_identity,
    lineage_datasets,
    main,
    marquez_namespaces,
    run_registration,
    unknown_job_dataset_ids,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

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

# Explicit (namespace, name) identifiers, derived from unified_view.sql and
# src/domain/naming.py. Trino exposes the tier as a CATALOG alias (gold /
# stream / iceberg all point at the same Polaris REST catalog), so the Polaris
# namespace is the domain (schema) part: stream.sales.orders_windowed must be
# registered as ("sales", "orders_windowed"), NOT ("stream",
# "sales_orders_windowed").
EXPECTED_GOLD_IDENTIFIERS = {
    ("sales", "daily_metrics"),
    ("sales", "customers"),
}
EXPECTED_STREAM_IDENTIFIERS = {
    ("sales", "orders_windowed"),
    ("marketing", "clickstream_windowed"),
}

# Asset names that exist in NO domains/*/data-products/*/product.yaml.
PHANTOM_ASSET_NAMES = {
    "daily_metrics",
    "orders_windowed",
    "customers",
    "dim_campaigns",
    "daily_funnel",
    "financial_closings",
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


# ---------------------------------------------------------------------------
# HTTP stubs (no network)
# ---------------------------------------------------------------------------
def _boom(*_args, **_kwargs):
    """Simulated outage: every Polaris/Marquez HTTP call fails."""
    raise requests.HTTPError("simulated outage")


class _FakeResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"identifiers": []}


def _fake_ok(*_args, **_kwargs) -> _FakeResponse:
    return _FakeResponse()


@pytest.fixture
def serving_env(monkeypatch) -> None:
    monkeypatch.setenv("POLARIS_URL", "http://polaris:8181")
    monkeypatch.setenv("POLARIS_TOKEN", "test-token")
    monkeypatch.setenv("MARQUEZ_URL", "http://marquez:5000")
    monkeypatch.delenv("MANIFESTS_DIR", raising=False)


# ===========================================================================
# ManifestUpdater
# ===========================================================================
def test_manifest_updater_finds_manifest(tmp_path: Path) -> None:
    manifest_path = _write_manifest(
        tmp_path,
        "sales",
        "orders",
        spec_assets=[{"name": "gold_orders", "path": "s3a://old/path/", "type": "iceberg_table"}],
    )

    # Repo-root style tree: <root>/domains/<domain>/data-products/<product>/product.yaml
    updater = ManifestUpdater(manifests_dir=tmp_path)
    assert updater.known_products == [("sales", "orders")]

    new_path = "s3://datalake/warehouse/gold/sales/daily_metrics"
    assert updater.update_table_path("sales", "orders", "gold_orders", new_path) is True

    data = _load(manifest_path)
    assert data["spec"]["assets"][0]["path"] == new_path, "manifest was not located/updated for ('sales', 'orders')"

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


def test_update_table_path_never_appends(tmp_path: Path, caplog) -> None:
    manifest_path = _write_manifest(
        tmp_path,
        "sales",
        "orders",
        spec_assets=[{"name": "gold_orders", "path": "s3a://old/", "type": "iceberg_table"}],
    )
    updater = ManifestUpdater(manifests_dir=tmp_path)

    # daily_metrics exists in NO manifest: it must be skipped, never appended.
    assert (
        updater.update_table_path(
            "sales", "orders", "daily_metrics", "s3://datalake/warehouse/gold/sales/daily_metrics"
        )
        is False
    )

    data = _load(manifest_path)
    assert [a["name"] for a in data["spec"]["assets"]] == ["gold_orders"]
    assert "never appended" in caplog.text


def test_update_table_path_refuses_asset_without_type(tmp_path: Path, caplog) -> None:
    manifest_path = _write_manifest(
        tmp_path,
        "sales",
        "orders",
        spec_assets=[{"name": "gold_orders", "path": "s3a://old/"}],
    )
    updater = ManifestUpdater(manifests_dir=tmp_path)

    assert updater.update_table_path("sales", "orders", "gold_orders", "s3://new/") is False

    data = _load(manifest_path)
    assert data["spec"]["assets"][0]["path"] == "s3a://old/", "typeless asset must not be written"
    assert "no 'type'" in caplog.text


def test_update_table_path_preserves_comments_and_quotes(tmp_path: Path) -> None:
    product_dir = tmp_path / "domains" / "sales" / "data-products" / "orders"
    product_dir.mkdir(parents=True)
    manifest_path = product_dir / "product.yaml"
    original = (
        "# governance: owned by sales-data-team\n"
        "apiVersion: dataplatform.io/v1alpha1\n"
        "kind: DataProduct\n"
        "metadata:\n"
        "  name: orders\n"
        "spec:\n"
        "  description: 'kept: verbatim'\n"
        "  assets:\n"
        "    # primary gold asset\n"
        "    - name: gold_orders\n"
        "      type: iceberg_table\n"
        '      path: "s3a://old/"  # do not lose me\n'
        "    - name: silver_orders\n"
        "      type: iceberg_table\n"
        "      path: s3a://silver/\n"
        "  contract: './contract.yaml'\n"
    )
    manifest_path.write_text(original, encoding="utf-8", newline="")

    updater = ManifestUpdater(manifests_dir=tmp_path)
    assert (
        updater.update_table_path("sales", "orders", "gold_orders", "s3://datalake/warehouse/gold/sales/daily_metrics")
        is True
    )

    updated = manifest_path.read_text(encoding="utf-8", newline="")
    assert "# governance: owned by sales-data-team" in updated
    assert "# primary gold asset" in updated
    assert 'path: "s3://datalake/warehouse/gold/sales/daily_metrics"  # do not lose me' in updated
    assert "description: 'kept: verbatim'" in updated
    assert "path: s3a://silver/" in updated
    assert updated.count("- name:") == 2


def test_replace_asset_path_rejects_unknown_asset(tmp_path: Path) -> None:
    text = "spec:\n  assets:\n    - name: gold_orders\n      type: iceberg_table\n      path: s3a://old/\n"
    assert _replace_asset_path(text, "silver_orders", "s3://new/") is None
    assert _replace_asset_path(text, "gold_orders", "s3://new/") is not None


def test_update_manifests_uses_real_asset_names(tmp_path: Path, caplog) -> None:
    _write_manifest(
        tmp_path,
        "sales",
        "orders",
        spec_assets=[{"name": "gold_orders", "path": "s3a://old/", "type": "iceberg_table"}],
    )
    _write_manifest(
        tmp_path,
        "sales",
        "customers",
        spec_assets=[{"name": "gold_customers", "path": "s3a://old/", "type": "iceberg_table"}],
    )
    updater = ManifestUpdater(manifests_dir=tmp_path)
    _update_manifests(updater)

    orders = _load(tmp_path / "domains" / "sales" / "data-products" / "orders" / "product.yaml")
    customers = _load(tmp_path / "domains" / "sales" / "data-products" / "customers" / "product.yaml")
    assert orders["spec"]["assets"] == [
        {
            "name": "gold_orders",
            "path": "s3://datalake/warehouse/gold/sales/daily_metrics",
            "type": "iceberg_table",
        }
    ]
    assert customers["spec"]["assets"] == [
        {
            "name": "gold_customers",
            "path": "s3://datalake/warehouse/gold/sales/customers",
            "type": "iceberg_table",
        }
    ]
    # Stream tables have no manifest asset today -> warned, nothing appended.
    assert "No manifest asset represents stream.sales.orders_windowed" in caplog.text
    assert "No manifest asset represents stream.marketing.clickstream_windowed" in caplog.text


def test_manifest_asset_targets_exist_in_repo() -> None:
    targets = []
    for table in GOLD_TABLES + STREAM_TABLES:
        target = table.get("manifest")
        if target is None:
            continue
        targets.append(target["asset"])
        manifest_path = REPO_ROOT / "domains" / target["domain"] / "data-products" / target["product"] / "product.yaml"
        data = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
        assets = {a["name"]: a for a in data["spec"]["assets"]}
        assert target["asset"] in assets, (
            f"{target['asset']} missing from {manifest_path} (real spec.assets names: {sorted(assets)})"
        )
        assert assets[target["asset"]].get("type"), "manifest assets must carry a type"
    assert targets, "expected the gold tables to declare manifest targets"
    assert not PHANTOM_ASSET_NAMES & set(targets)
    # No manifest asset represents the stream speed tables today.
    assert all("manifest" not in table for table in STREAM_TABLES)


# ===========================================================================
# Registry identifiers (namespace + name asserted explicitly)
# ===========================================================================
def test_registry_identifiers_match_pipeline_reality() -> None:
    assert {(t["namespace"], t["name"]) for t in GOLD_TABLES} == EXPECTED_GOLD_IDENTIFIERS
    assert {(t["namespace"], t["name"]) for t in STREAM_TABLES} == EXPECTED_STREAM_IDENTIFIERS

    # Gold ids reconstruct as gold.<domain>.<table>.
    assert {f"gold.{t['namespace']}.{t['name']}" for t in GOLD_TABLES} == {
        "gold.sales.daily_metrics",
        "gold.sales.customers",
    }

    # Logical ids are the single source of truth; locations are derived, not hand-written.
    assert {t["logical"] for t in GOLD_TABLES + STREAM_TABLES} == KNOWN_PRODUCED_TABLES
    for table in GOLD_TABLES + STREAM_TABLES:
        assert table["location"] == ("s3://datalake/warehouse/" + table["logical"].replace(".", "/"))

    assert not any(t["namespace"] in {"marketing", "finance"} for t in GOLD_TABLES)

    # No dim_customers / funnel drift anywhere in the registry.
    serialized = " ".join(f"{t['namespace']}.{t['name']} {t['location']}" for t in GOLD_TABLES + STREAM_TABLES)
    assert "dim_customers" not in serialized
    assert "funnel" not in serialized


def test_registry_matches_unified_view_selects() -> None:
    sql = (REPO_ROOT / "pipelines" / "serving" / "unified_view.sql").read_text(encoding="utf-8")
    referenced = {
        ".".join(parts)
        for parts in re.findall(r"\b(?:FROM|JOIN)\s+(gold|stream)\.(\w+)\.(\w+)", sql, flags=re.IGNORECASE)
    }
    registered = {t["logical"] for t in GOLD_TABLES + STREAM_TABLES}
    assert referenced, "unified_view.sql must read gold/stream tables"
    assert referenced <= registered, (
        f"unified_view.sql reads {sorted(referenced - registered)} which are not registered"
    )
    assert "stream.sales.orders_windowed" in referenced


def test_gold_customers_schema_matches_aggregate_gold() -> None:
    customers = next(t for t in GOLD_TABLES if t["name"] == "customers")
    fields = {f["name"] for f in customers["schema"]}
    # gold_sales_customers = silver customers with PII masked:
    # id, segment, registration_date, created_at, email_hash, name_masked.
    assert fields == {
        "id",
        "name_masked",
        "email_hash",
        "segment",
        "registration_date",
        "created_at",
    }
    assert not ({"total_orders", "lifetime_value", "is_active"} & fields), (
        "columns Gold never writes must not be advertised"
    )


def test_stream_schemas_match_tumbling_window_rows() -> None:
    orders = next(t for t in STREAM_TABLES if t["logical"] == "stream.sales.orders_windowed")
    fields = {f["name"] for f in orders["schema"]}
    expected = {
        "window_start",
        "window_end",
        "order_date",
        "status",
        "window_revenue",
        "window_orders",
        "avg_order_value",
        "source_tier",
    }
    assert fields == expected
    # order_date is selected by unified_view.sql:60 and must be registered.
    assert "order_date" in fields
    order_date_field = next(f for f in orders["schema"] if f["name"] == "order_date")
    assert order_date_field["required"] is True

    # The clickstream speed layer aggregates a different measure per funnel
    # stage, so it must NOT reuse the orders schema: registering the orders
    # columns here described a table no job can produce.
    click = next(t for t in STREAM_TABLES if t["logical"] == "stream.marketing.clickstream_windowed")
    click_fields = {f["name"] for f in click["schema"]}
    assert click_fields == {
        "window_start",
        "window_end",
        "order_date",
        "event_type",
        "event_count",
        "unique_sessions",
        "source_tier",
    }
    assert click_fields != expected


# ===========================================================================
# Lineage: datasets and job I/O share one source of truth
# ===========================================================================
def test_pipeline_jobs_reference_registered_datasets() -> None:
    assert unknown_job_dataset_ids() == []
    known = set(DATASET_IDS)
    created = {(ds["namespace"], ds["name"]) for ds in lineage_datasets()}

    for job in PIPELINE_JOBS:
        for ref in job["inputs"] + job["outputs"]:
            assert ref in known, f"{job['name']} references unregistered dataset {ref}"
            ident = dataset_identity(ref)
            assert (ident["namespace"], ident["name"]) in created

    # No pseudo-namespace references: ids reconstruct to dataset identities.
    all_refs = [ref for job in PIPELINE_JOBS for ref in job["inputs"] + job["outputs"]]
    assert not any(ref.startswith("polaris.") for ref in all_refs)
    assert [ref for ref in all_refs if ref.startswith("iceberg.")] == ["iceberg.serving.sales_unified_revenue"]


def test_dataset_identities_reconstruct_logical_ids() -> None:
    by_id = {ds["logical"]: (ds["namespace"], ds["name"]) for ds in lineage_datasets()}
    assert by_id["stream.sales.orders_windowed"] == ("stream", "sales.orders_windowed")
    assert by_id["stream.marketing.clickstream_windowed"] == (
        "stream",
        "marketing.clickstream_windowed",
    )
    assert by_id["gold.sales.daily_metrics"] == ("gold", "sales.daily_metrics")
    assert by_id["gold.sales.customers"] == ("gold", "sales.customers")
    assert by_id["kafka.sales.orders.events"] == ("kafka", "sales.orders.events")
    # The serving view output is the ACTUAL view id from unified_view.sql:35.
    assert by_id["iceberg.serving.sales_unified_revenue"] == (
        "iceberg",
        "serving.sales_unified_revenue",
    )
    for logical, (namespace, name) in by_id.items():
        assert f"{namespace}.{name}" == logical


def test_marquez_jobs_reject_unknown_dataset_refs(monkeypatch) -> None:
    created: list[dict] = []
    monkeypatch.setattr(
        MarquezClient,
        "create_job",
        lambda self, **kwargs: created.append(kwargs),
    )
    monkeypatch.setattr(
        catalog_module,
        "PIPELINE_JOBS",
        [
            {
                "namespace": "flink",
                "name": "dangling_job",
                "inputs": ["kafka.never.registered"],
                "outputs": ["stream.never.registered"],
                "description": "d",
                "location": "l",
            }
        ],
    )
    client = MarquezClient(base_url="http://marquez:5000", retry_count=1, retry_delay=0.0)
    assert _register_marquez_jobs(client) == 1
    assert created == [], "jobs with dangling dataset refs must not be registered"


# ===========================================================================
# Fail-closed exit behaviour (all HTTP calls monkeypatched)
# ===========================================================================
def test_step_functions_count_failures(monkeypatch) -> None:
    monkeypatch.setattr(PolarisClient, "_request", _boom)
    monkeypatch.setattr(MarquezClient, "_request", _boom)
    polaris = PolarisClient("http://polaris:8181", "token", retry_count=1, retry_delay=0.0)
    marquez = MarquezClient("http://marquez:5000", retry_count=1, retry_delay=0.0)

    assert _register_polaris_namespaces(polaris) == len(NAMESPACES)
    assert _register_polaris_tables(polaris) == len(GOLD_TABLES) + len(STREAM_TABLES)
    assert _register_marquez_namespaces(marquez) == len(marquez_namespaces())
    assert _register_marquez_datasets(marquez) == len(lineage_datasets())
    assert _register_marquez_jobs(marquez) == len(PIPELINE_JOBS)


def test_run_registration_reports_zero_failures_on_success(monkeypatch) -> None:
    monkeypatch.setattr(PolarisClient, "_request", _fake_ok)
    monkeypatch.setattr(MarquezClient, "_request", _fake_ok)
    polaris = PolarisClient("http://polaris:8181", "token", retry_count=1, retry_delay=0.0)
    marquez = MarquezClient("http://marquez:5000", retry_count=1, retry_delay=0.0)

    failures = run_registration(polaris, marquez, None)
    assert set(failures) == {
        "polaris_namespaces",
        "polaris_tables",
        "marquez_namespaces",
        "marquez_datasets",
        "marquez_jobs",
        "polaris_summary",
    }
    assert sum(failures.values()) == 0


def test_main_exits_nonzero_when_any_step_fails(serving_env, monkeypatch) -> None:
    monkeypatch.setattr(PolarisClient, "_request", _boom)
    monkeypatch.setattr(MarquezClient, "_request", _boom)

    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1


def test_main_succeeds_when_every_call_succeeds(serving_env, monkeypatch) -> None:
    monkeypatch.setattr(PolarisClient, "_request", _fake_ok)
    monkeypatch.setattr(MarquezClient, "_request", _fake_ok)

    assert main() is None, "successful run must return without SystemExit (exit code 0)"


def test_main_exits_nonzero_when_env_vars_missing(monkeypatch) -> None:
    for var in ("POLARIS_URL", "POLARIS_TOKEN", "MARQUEZ_URL", "MANIFESTS_DIR"):
        monkeypatch.delenv(var, raising=False)

    with pytest.raises(SystemExit) as excinfo:
        main()
    assert excinfo.value.code == 1


# ===========================================================================
# Dagster quality resource
# ===========================================================================
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
    fake_container_module.container = types.SimpleNamespace(run_quality_gate=_FakeQualityGate())
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
