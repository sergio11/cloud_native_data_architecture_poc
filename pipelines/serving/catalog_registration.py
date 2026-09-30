"""
catalog_registration.py - Catalog registration and metadata publishing.

Registers all serving views in Polaris catalog,
publishes metadata to Marquez/OpenLineage,
updates data product manifests with actual table paths.

Usage:
    python catalog_registration.py

Environment variables:
    POLARIS_URL      - Polaris REST catalog base URL (e.g. http://polaris:8181)
    POLARIS_TOKEN    - Bearer token for Polaris authentication
    MARQUEZ_URL      - Marquez base URL (e.g. http://marquez:5000)
    MANIFESTS_DIR    - Root directory containing domain product.yaml files

Requirements:
    requests>=2.28.0
    PyYAML>=6.0
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests
import yaml

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("catalog_registration")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_DELAY_S = 2.0
HTTP_TIMEOUT = 30

# All Iceberg namespaces to register
NAMESPACES = ["sales", "marketing", "finance", "stream"]

# Gold Iceberg tables to register in Polaris.
# Only tables actually produced by the core use cases are listed:
#   AggregateGoldUseCase -> gold.sales.daily_metrics, gold.sales.customers.
GOLD_TABLES: list[dict[str, Any]] = [
    {
        "namespace": "sales",
        "name": "daily_metrics",
        "location": "s3://datalake/warehouse/gold/sales/daily_metrics",
        "schema": [
            {"name": "order_date", "type": "date", "required": True},
            {"name": "status", "type": "string", "required": True},
            {"name": "gross_revenue", "type": "double", "required": False},
            {"name": "total_orders", "type": "long", "required": False},
            {"name": "average_order_value", "type": "double", "required": False},
        ],
    },
    {
        "namespace": "sales",
        "name": "customers",
        "location": "s3://datalake/warehouse/gold/sales/customers",
        "schema": [
            {"name": "customer_id", "type": "int", "required": True},
            {"name": "name_masked", "type": "string", "required": False},
            {"name": "email_hash", "type": "string", "required": False},
            {"name": "segment", "type": "string", "required": False},
            {"name": "registration_date", "type": "date", "required": False},
            {"name": "total_orders", "type": "long", "required": False},
            {"name": "lifetime_value", "type": "double", "required": False},
            {"name": "is_active", "type": "boolean", "required": False},
        ],
    },
]

# Stream Iceberg tables to register in Polaris.
# Only tables actually written by the streaming drivers are listed:
#   flink_orders_stream     -> stream.sales.orders_windowed
#   flink_clickstream_stream -> stream.marketing.clickstream_windowed
STREAM_TABLES: list[dict[str, Any]] = [
    {
        "namespace": "stream",
        "name": "sales_orders_windowed",
        "location": "s3://datalake/warehouse/stream/sales/orders_windowed",
        "schema": [
            {"name": "window_start", "type": "timestamp", "required": True},
            {"name": "window_end", "type": "timestamp", "required": True},
            {"name": "window_orders", "type": "long", "required": False},
            {"name": "window_revenue", "type": "double", "required": False},
            {"name": "status", "type": "string", "required": False},
        ],
    },
    {
        "namespace": "stream",
        "name": "marketing_clickstream_windowed",
        "location": "s3://datalake/warehouse/stream/marketing/clickstream_windowed",
        "schema": [
            {"name": "order_date", "type": "date", "required": True},
            {"name": "status", "type": "string", "required": False},
            {"name": "window_revenue", "type": "double", "required": False},
            {"name": "window_orders", "type": "long", "required": False},
            {"name": "avg_order_value", "type": "double", "required": False},
            {"name": "source_tier", "type": "string", "required": False},
        ],
    },
]

# Pipeline job definitions for Marquez lineage
PIPELINE_JOBS: list[dict[str, Any]] = [
    {
        "namespace": "flink",
        "name": "flink_orders_stream",
        "inputs": [{"namespace": "kafka", "name": "sales.orders.events"}],
        "outputs": [{"namespace": "iceberg", "name": "polaris.sales.orders_windowed"}],
        "description": "PyFlink streaming job: tumbling 5-min and sliding 1h/5m revenue aggregates per orders event.",
        "location": "pipelines/stream/flink_orders_stream.py",
    },
    {
        "namespace": "flink",
        "name": "flink_clickstream_stream",
        "inputs": [{"namespace": "kafka", "name": "marketing.clickstream.raw"}],
        "outputs": [
            {"namespace": "iceberg", "name": "polaris.marketing.clickstream_windowed"},
        ],
        "description": (
            "PyFlink streaming job: tumbling window aggregation over the clickstream topic "
            "(funnel metrics out of POC scope, SRS 1.2)."
        ),
        "location": "pipelines/stream/flink_clickstream_stream.py",
    },
    {
        "namespace": "spark",
        "name": "spark_sales_gold_etl",
        "inputs": [{"namespace": "iceberg", "name": "bronze.sales.orders"}],
        "outputs": [
            {"namespace": "iceberg", "name": "gold.sales.daily_metrics"},
            {"namespace": "iceberg", "name": "gold.sales.customers"},
        ],
        "description": "Daily Spark batch ETL: bronze -> silver -> gold for sales domain.",
        "location": "pipelines/medallion/gold_aggregation.py",
    },
    {
        "namespace": "trino",
        "name": "unified_serving_views",
        "inputs": [
            {"namespace": "iceberg", "name": "gold.sales.daily_metrics"},
            {"namespace": "iceberg", "name": "gold.sales.customers"},
            {"namespace": "iceberg", "name": "stream.sales.orders_windowed"},
            {"namespace": "iceberg", "name": "stream.marketing.clickstream_windowed"},
        ],
        "outputs": [
            {"namespace": "trino", "name": "serving.sales.unified_revenue"},
        ],
        "description": (
            "Serving layer unified query interface (RF-11). Only tables actually "
            "produced by a pipeline in this POC are listed as inputs/outputs; "
            "marketing/finance analytical views are PLANNED, not produced."
        ),
        "location": "pipelines/serving/unified_view.sql",
    },
]


# ===========================================================================
# PolarisClient
# ===========================================================================

class PolarisClient:
    """REST client for the Polaris Iceberg catalog API.

    Wraps the Polaris REST endpoints for namespace and table management,
    with automatic retry on transient HTTP errors.

    Args:
        base_url: Polaris base URL, e.g. ``http://polaris:8181``.
        token: Bearer token for authentication.
        retry_count: Number of retry attempts on failure.
        retry_delay: Seconds to wait between retries.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        retry_count: int = DEFAULT_RETRY_COUNT,
        retry_delay: float = DEFAULT_RETRY_DELAY_S,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        logger.info("PolarisClient initialised (url=%s)", self.base_url)

    def _request(
        self, method: str, path: str, **kwargs: Any
    ) -> requests.Response:
        """Execute an HTTP request with exponential-back-off retry.

        Args:
            method: HTTP verb (GET, POST, DELETE, etc.).
            path: URL path relative to ``base_url``.
            **kwargs: Extra arguments forwarded to ``requests.Session.request``.

        Returns:
            The successful ``requests.Response``.

        Raises:
            requests.HTTPError: After exhausting all retries.
        """
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(1, self.retry_count + 1):
            try:
                resp = self.session.request(
                    method, url, timeout=HTTP_TIMEOUT, **kwargs
                )
                resp.raise_for_status()
                return resp
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                if status == 409:
                    # Already exists — treat as success (idempotent)
                    logger.debug("Resource already exists at %s (409); skipping.", url)
                    return exc.response  # type: ignore[return-value]
                logger.warning(
                    "HTTP %s on %s %s (attempt %d/%d): %s",
                    status,
                    method,
                    url,
                    attempt,
                    self.retry_count,
                    exc,
                )
                last_exc = exc
            except requests.RequestException as exc:
                logger.warning(
                    "Request error on %s %s (attempt %d/%d): %s",
                    method,
                    url,
                    attempt,
                    self.retry_count,
                    exc,
                )
                last_exc = exc
            time.sleep(self.retry_delay * attempt)
        raise requests.HTTPError(
            f"All {self.retry_count} attempts failed for {method} {url}"
        ) from last_exc

    def register_namespace(self, namespace: str) -> None:
        """Create a namespace (database) in Polaris if it does not exist.

        Args:
            namespace: Namespace name (e.g. ``sales``).
        """
        payload = {
            "namespace": [namespace],
            "properties": {"owner": "data-platform", "managed-by": "catalog_registration.py"},
        }
        self._request("POST", "/api/catalog/v1/namespaces", json=payload)
        logger.info("Namespace registered: %s", namespace)

    def register_table(
        self,
        namespace: str,
        table_name: str,
        location: str,
        schema: list[dict[str, Any]],
    ) -> None:
        """Register an Iceberg table in a Polaris namespace.

        Args:
            namespace: Target namespace (must already exist).
            table_name: Table name (may include dots for nested namespaces).
            location: S3/GCS/ADLS base location of the table data.
            schema: List of field descriptors with ``name``, ``type``, ``required``.
        """
        iceberg_fields = [
            {
                "id": idx + 1,
                "name": field["name"],
                "required": field.get("required", False),
                "type": field["type"],
            }
            for idx, field in enumerate(schema)
        ]
        payload = {
            "name": table_name,
            "location": location,
            "schema": {
                "schema-id": 0,
                "identifier-field-ids": [],
                "fields": iceberg_fields,
            },
            "properties": {
                "format-version": "2",
                "write.format.default": "parquet",
            },
        }
        self._request(
            "POST",
            f"/api/catalog/v1/namespaces/{namespace}/tables",
            json=payload,
        )
        logger.info("Table registered: %s.%s → %s", namespace, table_name, location)

    def list_tables(self, namespace: str) -> list[str]:
        """Return the list of table names registered in a namespace.

        Args:
            namespace: Namespace to query.

        Returns:
            List of table name strings.
        """
        resp = self._request("GET", f"/api/catalog/v1/namespaces/{namespace}/tables")
        data = resp.json()
        identifiers = data.get("identifiers", [])
        names = [ident.get("name", "") for ident in identifiers]
        logger.info("Listed %d tables in namespace '%s'.", len(names), namespace)
        return names


# ===========================================================================
# MarquezClient
# ===========================================================================

class MarquezClient:
    """REST client for the Marquez lineage API (OpenLineage-compatible).

    Provides methods for managing namespaces, datasets, jobs, and quality runs
    in Marquez (https://marquezproject.ai).

    Args:
        base_url: Marquez base URL, e.g. ``http://marquez:5000``.
        retry_count: Number of retry attempts on transient failures.
        retry_delay: Base delay in seconds between retries.
    """

    def __init__(
        self,
        base_url: str,
        retry_count: int = DEFAULT_RETRY_COUNT,
        retry_delay: float = DEFAULT_RETRY_DELAY_S,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.session = requests.Session()
        self.session.headers.update(
            {"Content-Type": "application/json", "Accept": "application/json"}
        )
        logger.info("MarquezClient initialised (url=%s)", self.base_url)

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """Execute an HTTP request with retry logic.

        Args:
            method: HTTP verb.
            path: URL path relative to ``base_url``.
            **kwargs: Extra arguments forwarded to ``requests.Session.request``.

        Returns:
            Successful ``requests.Response``.

        Raises:
            requests.HTTPError: After exhausting retries.
        """
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(1, self.retry_count + 1):
            try:
                resp = self.session.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
                resp.raise_for_status()
                return resp
            except requests.RequestException as exc:
                status = (
                    exc.response.status_code
                    if hasattr(exc, "response") and exc.response is not None
                    else "?"
                )
                logger.warning(
                    "Marquez HTTP %s on %s %s (attempt %d/%d): %s",
                    status,
                    method,
                    url,
                    attempt,
                    self.retry_count,
                    exc,
                )
                last_exc = exc
            time.sleep(self.retry_delay * attempt)
        raise requests.HTTPError(
            f"All {self.retry_count} attempts failed for {method} {url}"
        ) from last_exc

    def create_namespace(
        self, namespace: str, owner: str, description: str
    ) -> None:
        """Create or update a Marquez namespace.

        Args:
            namespace: Namespace name.
            owner: Owning team or user.
            description: Human-readable description.
        """
        payload = {"ownerName": owner, "description": description}
        self._request("PUT", f"/api/v1/namespaces/{namespace}", json=payload)
        logger.info("Marquez namespace created/updated: %s", namespace)

    def create_dataset(
        self,
        namespace: str,
        name: str,
        fields: list[dict[str, Any]],
        source_name: str,
        description: str,
        tags: list[str] | None = None,
    ) -> None:
        """Register a dataset in Marquez.

        Args:
            namespace: Marquez namespace for the dataset.
            name: Dataset name (table path).
            fields: List of field descriptors with ``name`` and ``type``.
            source_name: Name of the source connection in Marquez.
            description: Human-readable description.
            tags: Optional list of tag strings.
        """
        marquez_fields = [
            {"name": f["name"], "type": f["type"].upper(), "description": ""}
            for f in fields
        ]
        payload: dict[str, Any] = {
            "type": "DB_TABLE",
            "physicalName": name,
            "sourceName": source_name,
            "fields": marquez_fields,
            "description": description,
            "tags": tags or [],
        }
        self._request(
            "PUT",
            f"/api/v1/namespaces/{namespace}/datasets/{name}",
            json=payload,
        )
        logger.info("Marquez dataset registered: %s/%s", namespace, name)

    def create_job(
        self,
        namespace: str,
        name: str,
        inputs: list[dict[str, str]],
        outputs: list[dict[str, str]],
        description: str,
        location: str,
    ) -> None:
        """Create or update a job definition in Marquez.

        Args:
            namespace: Marquez namespace for the job.
            name: Job name.
            inputs: List of ``{namespace, name}`` dicts representing input datasets.
            outputs: List of ``{namespace, name}`` dicts representing output datasets.
            description: Human-readable description.
            location: Source file / repository path.
        """
        payload: dict[str, Any] = {
            "type": "BATCH",
            "inputs": [{"namespace": i["namespace"], "name": i["name"]} for i in inputs],
            "outputs": [{"namespace": o["namespace"], "name": o["name"]} for o in outputs],
            "description": description,
            "location": location,
        }
        self._request(
            "PUT",
            f"/api/v1/namespaces/{namespace}/jobs/{name}",
            json=payload,
        )
        logger.info("Marquez job registered: %s/%s", namespace, name)

    def add_quality_run(
        self,
        namespace: str,
        dataset_name: str,
        run_id: str,
        pass_rate: float,
        stats: dict[str, Any],
    ) -> None:
        """Post a data quality metric run for a dataset.

        Args:
            namespace: Marquez namespace containing the dataset.
            dataset_name: Target dataset name.
            run_id: Unique identifier for this quality check run.
            pass_rate: Fraction of records passing quality checks (0.0–1.0).
            stats: Dict of metric names to numeric values.
        """
        payload: dict[str, Any] = {
            "runId": run_id,
            "passRate": pass_rate,
            "stats": stats,
            "checkedAt": datetime.now(UTC).isoformat(),
        }
        self._request(
            "POST",
            f"/api/v1/namespaces/{namespace}/datasets/{dataset_name}/quality",
            json=payload,
        )
        logger.info(
            "Quality run posted for %s/%s: pass_rate=%.2f",
            namespace,
            dataset_name,
            pass_rate,
        )


# ===========================================================================
# ManifestUpdater
# ===========================================================================

class ManifestUpdater:
    """Scans and updates data product manifest YAML files.

    Looks for ``product.yaml`` files under ``manifests_dir`` and provides
    methods to patch asset paths and timestamps in-place, preserving all
    other YAML content.

    Args:
        manifests_dir: Root directory to scan for ``product.yaml`` files.
    """

    def __init__(self, manifests_dir: str | Path) -> None:
        self.manifests_dir = Path(manifests_dir)
        if not self.manifests_dir.exists():
            raise FileNotFoundError(
                f"Manifests directory not found: {self.manifests_dir}"
            )
        self._manifest_map: dict[tuple[str, str], Path] = {}
        self._scan()

    def _scan(self) -> None:
        """Walk ``manifests_dir`` and index all ``product.yaml`` files.

        The index key is ``(domain, product)`` matching the real repository
        layout ``domains/<domain>/data-products/<product>/product.yaml`` (the
        root itself may be the repo root, the ``domains`` directory, or any
        ancestor of the ``data-products`` folder).  For flat
        ``<domain>/<product>/product.yaml`` trees the first path component is
        used as the domain.
        """
        count = 0
        for yaml_path in self.manifests_dir.rglob("product.yaml"):
            parts = yaml_path.relative_to(self.manifests_dir).parts
            if len(parts) < 2:
                continue
            product = parts[-2]
            if "data-products" in parts:
                idx = parts.index("data-products")
                domain = parts[idx - 1] if idx > 0 else self.manifests_dir.name
            else:
                domain = parts[0]
            self._manifest_map[(domain, product)] = yaml_path
            count += 1
        logger.info("ManifestUpdater: indexed %d product.yaml files.", count)

    def _load(self, path: Path) -> dict[str, Any]:
        """Load YAML file into a dict.

        Args:
            path: Path to the YAML file.

        Returns:
            Parsed dictionary.
        """
        with path.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}

    def _save(self, path: Path, data: dict[str, Any]) -> None:
        """Write a dict back to a YAML file, preserving UTF-8 encoding.

        Args:
            path: Destination file path.
            data: Dictionary to serialise.
        """
        with path.open("w", encoding="utf-8") as fh:
            yaml.dump(
                data, fh, default_flow_style=False, allow_unicode=True, sort_keys=False
            )

    def update_table_path(
        self,
        domain: str,
        product: str,
        asset_name: str,
        actual_path: str,
    ) -> None:
        """Update the ``path`` field of a named asset inside a product manifest.

        Manifests keep their assets under ``spec.assets`` (see
        ``domains/sales/data-products/orders/product.yaml``), so the asset list
        is read from and written back to ``data["spec"]["assets"]``; any
        top-level ``assets`` key is left untouched.  Locates the asset by name
        within the list and sets its ``path`` key to ``actual_path``, preserving
        all other fields.  If the asset is not found it is appended as a new
        entry.

        Args:
            domain: Domain name used to locate the manifest.
            product: Product name used to locate the manifest.
            asset_name: Name of the asset entry inside the YAML ``spec.assets`` list.
            actual_path: New physical path (e.g. S3 URI) to write.
        """
        key = (domain, product)
        if key not in self._manifest_map:
            logger.warning(
                "No product.yaml found for domain=%s product=%s; skipping path update.",
                domain,
                product,
            )
            return
        yaml_path = self._manifest_map[key]
        data = self._load(yaml_path)
        spec = data.get("spec")
        if not isinstance(spec, dict):
            spec = {}
            data["spec"] = spec
        assets: list[dict[str, Any]] = spec.setdefault("assets", [])
        if not isinstance(assets, list):
            assets = []
            spec["assets"] = assets
        for asset in assets:
            if isinstance(asset, dict) and asset.get("name") == asset_name:
                asset["path"] = actual_path
                logger.info(
                    "Updated asset '%s' path in %s → %s", asset_name, yaml_path, actual_path
                )
                break
        else:
            assets.append({"name": asset_name, "path": actual_path})
            logger.info(
                "Appended asset '%s' to %s with path=%s", asset_name, yaml_path, actual_path
            )
        self._save(yaml_path, data)

    def update_last_processed(
        self, domain: str, product: str, timestamp: str
    ) -> None:
        """Set the ``last_processed`` top-level field in a product manifest.

        Args:
            domain: Domain name used to locate the manifest.
            product: Product name used to locate the manifest.
            timestamp: ISO-8601 timestamp string to write.
        """
        key = (domain, product)
        if key not in self._manifest_map:
            logger.warning(
                "No product.yaml found for domain=%s product=%s; skipping last_processed update.",
                domain,
                product,
            )
            return
        yaml_path = self._manifest_map[key]
        data = self._load(yaml_path)
        data["last_processed"] = timestamp
        self._save(yaml_path, data)
        logger.info(
            "Updated last_processed in %s → %s", yaml_path, timestamp
        )

    @property
    def known_products(self) -> list[tuple[str, str]]:
        """Return all (domain, product) pairs discovered during scan.

        Returns:
            Sorted list of (domain, product) tuples.
        """
        return sorted(self._manifest_map.keys())


# ===========================================================================
# Main orchestrator
# ===========================================================================

def _build_polaris_client() -> PolarisClient:
    """Construct PolarisClient from environment variables.

    Returns:
        Initialised PolarisClient.

    Raises:
        SystemExit: If required env vars are missing.
    """
    polaris_url = os.getenv("POLARIS_URL", "").strip()
    polaris_token = os.getenv("POLARIS_TOKEN", "").strip()
    if not polaris_url:
        logger.error("POLARIS_URL environment variable is required.")
        sys.exit(1)
    if not polaris_token:
        logger.error("POLARIS_TOKEN environment variable is required.")
        sys.exit(1)
    return PolarisClient(base_url=polaris_url, token=polaris_token)


def _build_marquez_client() -> MarquezClient:
    """Construct MarquezClient from environment variables.

    Returns:
        Initialised MarquezClient.

    Raises:
        SystemExit: If required env vars are missing.
    """
    marquez_url = os.getenv("MARQUEZ_URL", "").strip()
    if not marquez_url:
        logger.error("MARQUEZ_URL environment variable is required.")
        sys.exit(1)
    return MarquezClient(base_url=marquez_url)


def _build_manifest_updater() -> ManifestUpdater | None:
    """Construct ManifestUpdater from environment variables.

    Returns:
        Initialised ManifestUpdater, or ``None`` if MANIFESTS_DIR is not set.
    """
    manifests_dir = os.getenv("MANIFESTS_DIR", "").strip()
    if not manifests_dir:
        logger.warning(
            "MANIFESTS_DIR not set; skipping manifest updates."
        )
        return None
    try:
        return ManifestUpdater(manifests_dir=manifests_dir)
    except FileNotFoundError as exc:
        logger.warning("ManifestUpdater init failed: %s; skipping.", exc)
        return None


def _register_polaris_namespaces(client: PolarisClient) -> None:
    """Register all known namespaces in Polaris.

    Args:
        client: Initialised PolarisClient.
    """
    logger.info("--- Registering Polaris namespaces ---")
    for ns in NAMESPACES:
        try:
            client.register_namespace(ns)
        except requests.HTTPError as exc:
            logger.error("Failed to register namespace '%s': %s", ns, exc)


def _register_polaris_tables(client: PolarisClient) -> None:
    """Register all gold and stream Iceberg tables in Polaris.

    Args:
        client: Initialised PolarisClient.
    """
    logger.info("--- Registering gold Iceberg tables in Polaris ---")
    for tbl in GOLD_TABLES:
        try:
            client.register_table(
                namespace=tbl["namespace"],
                table_name=tbl["name"],
                location=tbl["location"],
                schema=tbl["schema"],
            )
        except requests.HTTPError as exc:
            logger.error(
                "Failed to register table %s.%s: %s", tbl["namespace"], tbl["name"], exc
            )

    logger.info("--- Registering stream Iceberg tables in Polaris ---")
    for tbl in STREAM_TABLES:
        try:
            client.register_table(
                namespace=tbl["namespace"],
                table_name=tbl["name"],
                location=tbl["location"],
                schema=tbl["schema"],
            )
        except requests.HTTPError as exc:
            logger.error(
                "Failed to register table %s.%s: %s", tbl["namespace"], tbl["name"], exc
            )


def _register_marquez_namespaces(client: MarquezClient) -> None:
    """Register all lineage namespaces in Marquez.

    Args:
        client: Initialised MarquezClient.
    """
    logger.info("--- Registering Marquez namespaces ---")
    ns_meta = {
        "sales":     ("data-platform", "Sales domain: orders, customers, revenue"),
        "marketing": ("data-platform", "Marketing domain: campaigns, clickstream, funnels"),
        "finance":   ("data-platform", "Finance domain: financial closings, budgets, revenue reconciliation"),
        "stream":    ("data-platform", "Real-time stream outputs from Flink jobs"),
        "flink":     ("data-platform", "Flink streaming pipeline jobs"),
        "spark":     ("data-platform", "Spark batch pipeline jobs"),
        "trino":     ("data-platform", "Trino serving layer views"),
        "kafka":     ("data-platform", "Kafka event topics"),
        "iceberg":   ("data-platform", "Iceberg tables (physical datasets)"),
    }
    for ns, (owner, desc) in ns_meta.items():
        try:
            client.create_namespace(namespace=ns, owner=owner, description=desc)
        except requests.HTTPError as exc:
            logger.error("Failed to create Marquez namespace '%s': %s", ns, exc)


def _register_marquez_datasets(client: MarquezClient) -> None:
    """Register all datasets in Marquez.

    Args:
        client: Initialised MarquezClient.
    """
    logger.info("--- Registering Marquez datasets ---")
    all_tables = GOLD_TABLES + STREAM_TABLES
    for tbl in all_tables:
        ns = tbl["namespace"]
        try:
            client.create_dataset(
                namespace=ns,
                name=tbl["name"],
                fields=tbl["schema"],
                source_name="polaris-iceberg",
                description=f"Iceberg table at {tbl['location']}",
                tags=["iceberg", ns],
            )
        except requests.HTTPError as exc:
            logger.error(
                "Failed to register Marquez dataset %s/%s: %s", ns, tbl["name"], exc
            )


def _register_marquez_jobs(client: MarquezClient) -> None:
    """Register all pipeline job definitions in Marquez.

    Args:
        client: Initialised MarquezClient.
    """
    logger.info("--- Registering Marquez jobs ---")
    for job in PIPELINE_JOBS:
        try:
            client.create_job(
                namespace=job["namespace"],
                name=job["name"],
                inputs=job["inputs"],
                outputs=job["outputs"],
                description=job["description"],
                location=job["location"],
            )
        except requests.HTTPError as exc:
            logger.error(
                "Failed to register Marquez job %s/%s: %s",
                job["namespace"],
                job["name"],
                exc,
            )


def _update_manifests(updater: ManifestUpdater) -> None:
    """Update all discovered product.yaml manifests with actual table paths.

    Iterates all known (domain, product) pairs, then patches:
    - The primary output table path for each product.
    - The ``last_processed`` timestamp to the current UTC time.

    Args:
        updater: Initialised ManifestUpdater.
    """
    logger.info("--- Updating product.yaml manifests ---")
    now_iso = datetime.now(UTC).isoformat()

    # Domain→product→asset→path mapping derived from table definitions
    asset_path_map: dict[str, dict[str, list[tuple[str, str]]]] = {
        "sales": {
            "orders": [
                ("daily_metrics", "s3://datalake/warehouse/gold/sales/daily_metrics"),
                ("orders_windowed", "s3://datalake/warehouse/stream/sales/orders_windowed"),
            ],
            "customers": [
                ("customers", "s3://datalake/warehouse/gold/sales/customers"),
                ("customer_metrics", "s3://datalake/warehouse/gold/sales/customer_metrics"),
            ],
        },
        "marketing": {
            "campaigns": [
                ("dim_campaigns", "s3://datalake/warehouse/gold/marketing/dim_campaigns"),
                ("daily_attribution", "s3://datalake/warehouse/gold/marketing/daily_attribution"),
            ],
            "leads": [
                ("daily_funnel", "s3://datalake/warehouse/gold/marketing/daily_funnel"),
                ("funnel_metrics", "s3://datalake/warehouse/stream/marketing/funnel_metrics"),
                ("abandoned_carts", "s3://datalake/warehouse/stream/marketing/abandoned_carts"),
            ],
        },
        "finance": {
            "financial_closings": [
                ("financial_closings", "s3://datalake/warehouse/gold/finance/financial_closings"),
            ],
        },
    }

    for domain, products in asset_path_map.items():
        for product, assets in products.items():
            for asset_name, actual_path in assets:
                updater.update_table_path(domain, product, asset_name, actual_path)
            updater.update_last_processed(domain, product, now_iso)


def _print_summary(
    polaris_client: PolarisClient,
) -> None:
    """Print a summary report of registered namespaces and tables.

    Args:
        polaris_client: PolarisClient used for the registration run.
    """
    print("\n" + "=" * 70)
    print("  CATALOG REGISTRATION SUMMARY")
    print("=" * 70)
    print(f"  Timestamp : {datetime.now(UTC).isoformat()}")
    print(f"  Polaris   : {polaris_client.base_url}")
    print()

    for ns in NAMESPACES:
        try:
            tables = polaris_client.list_tables(ns)
            print(f"  Namespace '{ns}': {len(tables)} table(s)")
            for tbl in sorted(tables):
                print(f"    - {tbl}")
        except requests.HTTPError as exc:
            print(f"  Namespace '{ns}': ERROR listing tables — {exc}")

    gold_count = len(GOLD_TABLES)
    stream_count = len(STREAM_TABLES)
    job_count = len(PIPELINE_JOBS)
    print()
    print(f"  Gold tables registered  : {gold_count}")
    print(f"  Stream tables registered: {stream_count}")
    print(f"  Pipeline jobs registered: {job_count}")
    print("=" * 70 + "\n")


def main() -> None:
    """Orchestrate full catalog registration and manifest update.

    Steps:
    1. Read config from environment variables.
    2. Register all namespaces and tables in Polaris.
    3. Register namespaces, datasets, and jobs in Marquez.
    4. Update product.yaml manifests with actual table paths.
    5. Print summary report.
    """
    logger.info("Starting catalog_registration.py")

    polaris_client = _build_polaris_client()
    marquez_client = _build_marquez_client()
    manifest_updater = _build_manifest_updater()

    # Polaris registrations
    _register_polaris_namespaces(polaris_client)
    _register_polaris_tables(polaris_client)

    # Marquez lineage
    _register_marquez_namespaces(marquez_client)
    _register_marquez_datasets(marquez_client)
    _register_marquez_jobs(marquez_client)

    # Manifest patching
    if manifest_updater is not None:
        _update_manifests(manifest_updater)
    else:
        logger.info("Skipping manifest updates (MANIFESTS_DIR not configured).")

    # Summary
    _print_summary(polaris_client)
    logger.info("catalog_registration.py completed successfully.")


if __name__ == "__main__":
    main()
