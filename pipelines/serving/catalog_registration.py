"""
catalog_registration.py - Catalog registration and metadata publishing.

Registers every serving Iceberg table in the Polaris catalog,
publishes datasets and jobs (lineage) to Marquez/OpenLineage,
and updates data product manifests with the actual table paths.

Usage:
    python catalog_registration.py

Environment variables:
    POLARIS_URL      - Polaris REST catalog base URL (e.g. http://polaris:8181)
    POLARIS_TOKEN    - Bearer token for Polaris authentication
    MARQUEZ_URL      - Marquez base URL (e.g. http://marquez:5000)
    MANIFESTS_DIR    - Root directory containing domain product.yaml files

Exit codes:
    0  every Polaris/Marquez call succeeded (manifest updates are best-effort
       warnings by design and never silently append invalid entries)
    1  a required environment variable is missing, or at least one
       catalog/lineage call failed - the script is fail-closed and never
       reports success while a step failed

Requirements:
    requests>=2.28.0
    PyYAML>=6.0
"""

from __future__ import annotations

import logging
import os
import re
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

WAREHOUSE_ROOT = "s3://datalake/warehouse"


# ---------------------------------------------------------------------------
# Table registry (single source of truth)
# ---------------------------------------------------------------------------
# Every logical id follows src/domain/naming.py: <tier>.<domain>.<table>
# (e.g. ``gold.sales.daily_metrics``).  Two identities are derived from each
# logical id, one per target system:
#
#   * Polaris / Trino -> (domain, table): the Trino catalogs ``iceberg``,
#     ``gold`` and ``stream`` are aliases of the SAME Polaris REST catalog
#     (infra/helm/s5-serving/templates/trino-configmap.yaml), so the tier is a
#     catalog name and the Polaris namespace is the middle (schema) part.
#     ``stream.sales.orders_windowed`` is therefore registered as namespace
#     ``sales`` + table ``orders_windowed`` - exactly the identifier
#     pipelines/serving/unified_view.sql queries.
#   * Marquez -> (tier, domain.table): OpenLineage convention (namespace =
#     source system, name = schema.table).  Concatenating both parts
#     reconstructs the logical id, so lineage references are never invented.
def _logical_location(logical: str) -> str:
    """Map a logical id to its warehouse location (derived, never hand-written)."""
    return f"{WAREHOUSE_ROOT}/{logical.replace('.', '/')}"


def _table(
    logical: str,
    schema: list[dict[str, Any]],
    manifest: tuple[str, str, str] | None = None,
) -> dict[str, Any]:
    """Build a registry entry from a logical id.

    Args:
        logical: ``<tier>.<domain>.<table>`` id (naming.py vocabulary).
        schema: Iceberg field descriptors (``name``, ``type``, ``required``).
        manifest: Optional ``(domain, product, asset_name)`` target identifying
            the real ``spec.assets[].name`` in the product manifest that holds
            this table's path. ``None`` means no manifest asset represents it.

    Returns:
        Registry entry with ``logical``, Polaris ``namespace``/``name``,
        ``location`` and ``schema`` (plus ``manifest`` when provided).

    Raises:
        ValueError: If ``logical`` is not a 3-part id.
    """
    parts = logical.split(".")
    if len(parts) != 3:
        raise ValueError(f"logical table id must be '<tier>.<domain>.<table>': {logical!r}")
    _tier, domain, name = parts
    entry: dict[str, Any] = {
        "logical": logical,
        "namespace": domain,
        "name": name,
        "location": _logical_location(logical),
        "schema": schema,
    }
    if manifest is not None:
        entry["manifest"] = {"domain": manifest[0], "product": manifest[1], "asset": manifest[2]}
    return entry


# Gold Iceberg tables to register in Polaris.
# Only tables actually produced by the core use cases are listed:
#   AggregateGoldUseCase -> gold.sales.daily_metrics, gold.sales.customers.
# Columns mirror what aggregate_gold.py actually writes (verified against
# data/lakehouse.db: gold_sales_customers = id, segment, registration_date,
# created_at, email_hash, name_masked - no total_orders/lifetime_value/is_active).
GOLD_TABLES: list[dict[str, Any]] = [
    _table(
        "gold.sales.daily_metrics",
        schema=[
            {"name": "order_date", "type": "date", "required": True},
            {"name": "status", "type": "string", "required": True},
            {"name": "gross_revenue", "type": "double", "required": False},
            {"name": "total_orders", "type": "long", "required": False},
            {"name": "average_order_value", "type": "double", "required": False},
        ],
        manifest=("sales", "orders", "gold_orders"),
    ),
    _table(
        "gold.sales.customers",
        schema=[
            {"name": "id", "type": "int", "required": True},
            {"name": "name_masked", "type": "string", "required": False},
            {"name": "email_hash", "type": "string", "required": False},
            {"name": "segment", "type": "string", "required": False},
            {"name": "registration_date", "type": "date", "required": False},
            {"name": "created_at", "type": "timestamp", "required": False},
        ],
        manifest=("sales", "customers", "gold_customers"),
    ),
]

# Stream Iceberg tables to register in Polaris.
# Only tables actually written by the streaming drivers are listed:
#   flink_orders_stream     -> stream.sales.orders_windowed
#   flink_clickstream_stream -> stream.marketing.clickstream_windowed
# Both are produced by BusinessAggregator.aggregate_tumbling_window, whose
# row shape is window_start/window_end/order_date/status/window_revenue/
# window_orders/avg_order_value/source_tier (order_date is required by
# unified_view.sql). Neither table is represented by a manifest asset today,
# so no ``manifest`` target is declared and the manifest step warns instead.
STREAM_TABLES: list[dict[str, Any]] = [
    _table(
        "stream.sales.orders_windowed",
        schema=[
            {"name": "window_start", "type": "timestamp", "required": True},
            {"name": "window_end", "type": "timestamp", "required": True},
            {"name": "order_date", "type": "date", "required": True},
            {"name": "status", "type": "string", "required": False},
            {"name": "window_revenue", "type": "double", "required": False},
            {"name": "window_orders", "type": "long", "required": False},
            {"name": "avg_order_value", "type": "double", "required": False},
            {"name": "source_tier", "type": "string", "required": False},
        ],
    ),
    _table(
        "stream.marketing.clickstream_windowed",
        schema=[
            {"name": "window_start", "type": "timestamp", "required": True},
            {"name": "window_end", "type": "timestamp", "required": True},
            {"name": "order_date", "type": "date", "required": True},
            {"name": "status", "type": "string", "required": False},
            {"name": "window_revenue", "type": "double", "required": False},
            {"name": "window_orders", "type": "long", "required": False},
            {"name": "avg_order_value", "type": "double", "required": False},
            {"name": "source_tier", "type": "string", "required": False},
        ],
    ),
]

ALL_TABLES: list[dict[str, Any]] = GOLD_TABLES + STREAM_TABLES

# Polaris namespaces == Trino schemas: the domain namespaces actually used by
# registered tables plus ``finance`` (bootstrapped by unified_view.sql).
# ``stream``/``gold`` are Trino catalog aliases, NOT schemas, so they are
# deliberately not registered here.
NAMESPACES: list[str] = sorted({tbl["namespace"] for tbl in ALL_TABLES} | {"finance"})


# ---------------------------------------------------------------------------
# Lineage datasets (Marquez) - single source of truth for dataset identities
# ---------------------------------------------------------------------------
def dataset_identity(logical: str) -> dict[str, str]:
    """Split a full dataset id into its Marquez ``(namespace, name)`` pair.

    The split happens on the FIRST dot so that ``namespace + "." + name``
    reconstructs the id (e.g. ``stream.sales.orders_windowed`` ->
    ``stream`` / ``sales.orders_windowed``).

    Args:
        logical: Full dotted dataset id.

    Returns:
        ``{"namespace": ..., "name": ...}``.

    Raises:
        ValueError: If the id contains no dot.
    """
    namespace, sep, name = logical.partition(".")
    if not sep or not name:
        raise ValueError(f"dataset id must be '<namespace>.<name>': {logical!r}")
    return {"namespace": namespace, "name": name}


def _dataset(
    logical: str,
    schema: list[dict[str, Any]],
    source: str,
    description: str,
    tags: list[str],
) -> dict[str, Any]:
    """Build a Marquez dataset descriptor from a full dotted id."""
    ident = dataset_identity(logical)
    return {
        "logical": logical,
        "namespace": ident["namespace"],
        "name": ident["name"],
        "schema": schema,
        "source": source,
        "description": description,
        "tags": tags,
    }


# Datasets that are NOT Iceberg warehouse tables but are read/written by the
# pipeline jobs (Kafka sources, Silver inputs, the serving view output).
SUPPORTING_DATASETS: list[dict[str, Any]] = [
    _dataset(
        "kafka.sales.orders.events",
        schema=[
            {"name": "event_id", "type": "string", "required": False},
            {"name": "event_type", "type": "string", "required": False},
            {"name": "order_id", "type": "long", "required": False},
            {"name": "customer_id", "type": "long", "required": False},
            {"name": "total_amount", "type": "double", "required": False},
            {"name": "status", "type": "string", "required": False},
            {"name": "order_date", "type": "date", "required": False},
            {"name": "timestamp", "type": "timestamp", "required": False},
            {"name": "source", "type": "string", "required": False},
        ],
        source="kafka",
        description="Kafka topic: real-time CDC order events (data-generator/event_producer.py)",
        tags=["kafka", "sales"],
    ),
    _dataset(
        "kafka.marketing.clickstream.raw",
        schema=[
            {"name": "event_id", "type": "string", "required": False},
            {"name": "session_id", "type": "string", "required": False},
            {"name": "customer_id", "type": "long", "required": False},
            {"name": "page_url", "type": "string", "required": False},
            {"name": "event_type", "type": "string", "required": False},
            {"name": "referrer", "type": "string", "required": False},
            {"name": "timestamp", "type": "timestamp", "required": False},
        ],
        source="kafka",
        description="Kafka topic: raw digital clickstream events (data-generator/event_producer.py)",
        tags=["kafka", "marketing"],
    ),
    _dataset(
        "silver.sales.orders",
        schema=[
            {"name": "id", "type": "long", "required": False},
            {"name": "customer_id", "type": "long", "required": False},
            {"name": "total_amount", "type": "double", "required": False},
            {"name": "status", "type": "string", "required": False},
            {"name": "order_date", "type": "date", "required": False},
            {"name": "created_at", "type": "timestamp", "required": False},
        ],
        source="polaris-iceberg",
        description="Silver conformed orders (input to the Gold aggregation job)",
        tags=["iceberg", "silver"],
    ),
    _dataset(
        "silver.sales.customers",
        schema=[
            {"name": "id", "type": "long", "required": False},
            {"name": "name", "type": "string", "required": False},
            {"name": "email", "type": "string", "required": False},
            {"name": "segment", "type": "string", "required": False},
            {"name": "registration_date", "type": "date", "required": False},
            {"name": "created_at", "type": "timestamp", "required": False},
        ],
        source="polaris-iceberg",
        description="Silver conformed customers (input to the GDPR masking job; PII present)",
        tags=["iceberg", "silver"],
    ),
    _dataset(
        "iceberg.serving.sales_unified_revenue",
        schema=[
            {"name": "order_date", "type": "date", "required": False},
            {"name": "status", "type": "string", "required": False},
            {"name": "total_orders", "type": "long", "required": False},
            {"name": "total_revenue", "type": "double", "required": False},
            {"name": "avg_order_value", "type": "double", "required": False},
            {"name": "source_tier", "type": "string", "required": False},
        ],
        source="trino",
        description="Trino serving view iceberg.serving.sales_unified_revenue (RF-11)",
        tags=["trino", "serving"],
    ),
]


def lineage_datasets() -> list[dict[str, Any]]:
    """Return every Marquez dataset to register (tables + supporting datasets)."""
    table_datasets = [
        _dataset(
            tbl["logical"],
            tbl["schema"],
            source="polaris-iceberg",
            description=f"Iceberg table at {tbl['location']}",
            tags=["iceberg", tbl["namespace"]],
        )
        for tbl in ALL_TABLES
    ]
    return table_datasets + SUPPORTING_DATASETS


DATASET_IDS: list[str] = [ds["logical"] for ds in lineage_datasets()]

# Pipeline job definitions for Marquez lineage.
# ``inputs``/``outputs`` are FULL dataset ids; they are split with
# ``dataset_identity`` when registered and validated against DATASET_IDS, so a
# job can never reference a dataset that was never created (no dangling I/O).
PIPELINE_JOBS: list[dict[str, Any]] = [
    {
        "namespace": "flink",
        "name": "flink_orders_stream",
        "inputs": ["kafka.sales.orders.events"],
        "outputs": ["stream.sales.orders_windowed"],
        "description": "PyFlink streaming job: tumbling 5-min and sliding 1h/5m revenue aggregates per orders event.",
        "location": "pipelines/stream/flink_orders_stream.py",
    },
    {
        "namespace": "flink",
        "name": "flink_clickstream_stream",
        "inputs": ["kafka.marketing.clickstream.raw"],
        "outputs": ["stream.marketing.clickstream_windowed"],
        "description": (
            "PyFlink streaming job: tumbling window aggregation over the clickstream topic "
            "(funnel metrics out of POC scope, SRS 1.2)."
        ),
        "location": "pipelines/stream/flink_clickstream_stream.py",
    },
    {
        "namespace": "spark",
        "name": "spark_sales_gold_etl",
        "inputs": ["silver.sales.orders", "silver.sales.customers"],
        "outputs": ["gold.sales.daily_metrics", "gold.sales.customers"],
        "description": (
            "Daily Spark batch ETL: Silver -> Gold for the sales domain "
            "(daily order metric rollups + GDPR-masked customers)."
        ),
        "location": "pipelines/medallion/gold_aggregation.py",
    },
    {
        "namespace": "trino",
        "name": "unified_serving_views",
        "inputs": ["gold.sales.daily_metrics", "stream.sales.orders_windowed"],
        "outputs": ["iceberg.serving.sales_unified_revenue"],
        "description": (
            "Serving layer unified query interface (RF-11): reads exactly the tables "
            "unified_view.sql VIEW 1 selects; marketing/finance analytical views are "
            "PLANNED, not produced."
        ),
        "location": "pipelines/serving/unified_view.sql",
    },
]


def unknown_job_dataset_ids() -> list[str]:
    """Return sorted job input/output ids that no registered dataset defines."""
    known = set(DATASET_IDS)
    referenced = {ref for job in PIPELINE_JOBS for ref in job["inputs"] + job["outputs"]}
    return sorted(referenced - known)


#: Owner/description per Marquez namespace actually used by datasets/jobs.
MARQUEZ_NAMESPACE_META: dict[str, tuple[str, str]] = {
    "gold": ("data-platform", "Gold tier analytical tables (lakehouse output)"),
    "silver": ("data-platform", "Silver tier conformed tables"),
    "stream": ("data-platform", "Real-time stream outputs from Flink jobs"),
    "kafka": ("data-platform", "Kafka event topics"),
    "iceberg": ("data-platform", "Iceberg catalog datasets (tables and serving views)"),
    "flink": ("data-platform", "Flink streaming pipeline jobs"),
    "spark": ("data-platform", "Spark batch pipeline jobs"),
    "trino": ("data-platform", "Trino serving layer jobs"),
}


def marquez_namespaces() -> list[str]:
    """Return every Marquez namespace used by datasets or jobs (derived, not hand-listed)."""
    used = {ds["namespace"] for ds in lineage_datasets()}
    used.update(job["namespace"] for job in PIPELINE_JOBS)
    return sorted(used)


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

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
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
                resp = self.session.request(method, url, timeout=HTTP_TIMEOUT, **kwargs)
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
        raise requests.HTTPError(f"All {self.retry_count} attempts failed for {method} {url}") from last_exc

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
        self.session.headers.update({"Content-Type": "application/json", "Accept": "application/json"})
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
                status = exc.response.status_code if hasattr(exc, "response") and exc.response is not None else "?"
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
        raise requests.HTTPError(f"All {self.retry_count} attempts failed for {method} {url}") from last_exc

    def create_namespace(self, namespace: str, owner: str, description: str) -> None:
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
        marquez_fields = [{"name": f["name"], "type": f["type"].upper(), "description": ""} for f in fields]
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


def _indent(line: str) -> int:
    """Return the leading-space indentation width of a line."""
    return len(line) - len(line.lstrip(" "))


def _unquote(value: str) -> str:
    """Strip one layer of surrounding single/double quotes from a scalar."""
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def _split_value_comment(raw: str) -> tuple[str, str]:
    """Split a raw YAML scalar into ``(value_with_quotes, trailing_comment)``."""
    stripped = raw.lstrip()
    if stripped[:1] in ("'", '"'):
        quote = stripped[0]
        closing = stripped.find(quote, 1)
        if closing == -1:
            return stripped, ""
        return stripped[: closing + 1], stripped[closing + 1 :]
    marker = stripped.find(" #")
    if marker == -1:
        return stripped.rstrip(), ""
    return stripped[:marker].rstrip(), stripped[marker:]


def _quote_scalar(value: str, original: str) -> str:
    """Quote ``value`` the same way ``original`` was quoted (plain if it was plain)."""
    if original[:1] in ("'", '"'):
        quote = original[0]
        return f"{quote}{value}{quote}"
    if ": " in value or " #" in value or value[:1] in "-?:,[]{}#&*!|>'\"%@`":
        return f'"{value}"'
    return value


def _find_assets_key(lines: list[str]) -> int | None:
    """Return the index of the ``assets:`` key declared directly under ``spec:``."""
    spec_idx: int | None = None
    for idx, raw in enumerate(lines):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if spec_idx is None:
            if _indent(raw) == 0 and stripped.rstrip() == "spec:":
                spec_idx = idx
            continue
        if _indent(raw) == 0:
            return None
        if stripped == "assets:":
            return idx
    return None


def _iter_asset_spans(lines: list[str], assets_idx: int, assets_indent: int) -> list[tuple[int, int, int]]:
    """Return ``(start, end, dash_indent)`` line spans of each ``spec.assets`` item."""
    spans: list[tuple[int, int, int]] = []
    item_start: int | None = None
    item_indent = 0
    for idx in range(assets_idx + 1, len(lines)):
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = _indent(raw)
        if indent < assets_indent:
            break
        is_item = stripped == "-" or stripped.startswith("- ")
        if is_item:
            if item_start is None or indent <= item_indent:
                if item_start is not None:
                    spans.append((item_start, idx, item_indent))
                item_start = idx
                item_indent = indent
            continue
        if indent <= assets_indent:
            break
    if item_start is not None:
        spans.append((item_start, len(lines), item_indent))
    return spans


def _item_key_values(lines: list[str], span: tuple[int, int, int]) -> list[tuple[int, str, str]]:
    """Return ``(line_index, key, raw_value)`` for the flat keys of one asset item."""
    start, end, dash_indent = span
    key_indent = dash_indent + 2
    found: list[tuple[int, str, str]] = []
    for idx in range(start, end):
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if idx == start:
            if not (stripped == "-" or stripped.startswith("- ")):
                continue
            content = stripped[1:].lstrip()
        else:
            if _indent(raw) != key_indent:
                continue
            content = stripped
        if content.startswith("#"):
            continue
        key, sep, value = content.partition(":")
        if sep:
            found.append((idx, key.strip(), value))
    return found


def _replace_asset_path(text: str, asset_name: str, actual_path: str) -> str | None:
    """Rewrite only the ``path`` value of one ``spec.assets`` entry.

    The edit is line-targeted so every other byte of the file - comments,
    quoting style, key order, line endings - is preserved (ruamel.yaml is not
    a dependency, and a PyYAML round-trip would drop comments and re-quote
    scalars such as ``created_at: "2024-02-10"``).

    Args:
        text: Full file content.
        asset_name: Name of the asset entry to update.
        actual_path: New path value.

    Returns:
        The new file content, or ``None`` when the entry or its ``path`` key
        cannot be located in the text.
    """
    lines = text.splitlines(keepends=True)
    assets_idx = _find_assets_key(lines)
    if assets_idx is None:
        return None
    assets_indent = _indent(lines[assets_idx])
    for span in _iter_asset_spans(lines, assets_idx, assets_indent):
        keys = _item_key_values(lines, span)
        name_value = next((v for _, key, v in keys if key == "name"), None)
        if name_value is None or _unquote(name_value) != asset_name:
            continue
        path_entry = next(((idx, v) for idx, key, v in keys if key == "path"), None)
        if path_entry is None:
            return None
        line_idx, raw_value = path_entry
        match = re.match(r"^(\s*(?:-\s+)?path\s*:\s*)(.*?)(\r?\n)?$", lines[line_idx])
        if match is None:
            return None
        scalar, comment = _split_value_comment(raw_value)
        new_scalar = _quote_scalar(actual_path, scalar)
        newline = match.group(3) or ""
        lines[line_idx] = f"{match.group(1)}{new_scalar}{comment}{newline}"
        return "".join(lines)
    return None


class ManifestUpdater:
    """Scans and updates data product manifest YAML files.

    Looks for ``product.yaml`` files under ``manifests_dir`` and patches the
    ``path`` of an EXISTING ``spec.assets`` entry in place with a line-targeted
    edit, preserving comments, quoting and structure. Unknown assets are
    skipped with a warning - entries are never appended, and an asset without
    the schema-required ``type`` is never written.

    Args:
        manifests_dir: Root directory to scan for ``product.yaml`` files.
    """

    def __init__(self, manifests_dir: str | Path) -> None:
        self.manifests_dir = Path(manifests_dir)
        if not self.manifests_dir.exists():
            raise FileNotFoundError(f"Manifests directory not found: {self.manifests_dir}")
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

    def update_table_path(
        self,
        domain: str,
        product: str,
        asset_name: str,
        actual_path: str,
    ) -> bool:
        """Update the ``path`` of a named asset inside a product manifest.

        Manifests keep their assets under ``spec.assets`` (see
        ``domains/sales/data-products/orders/product.yaml``). The asset is
        located by name and its ``path`` line is rewritten in place, leaving
        every other byte of the file untouched.

        Fail-closed rules (see governance/schemas/product-schema.json, which
        requires ``name``, ``type`` and ``path`` on every asset):
          * asset not found -> warn and skip (entries are NEVER appended);
          * asset without ``type`` -> warn and skip (never write an invalid asset).

        Args:
            domain: Domain name used to locate the manifest.
            product: Product name used to locate the manifest.
            asset_name: Name of the asset entry inside the YAML ``spec.assets`` list.
            actual_path: New physical path (e.g. S3 URI) to write.

        Returns:
            ``True`` when the manifest was updated (or already correct),
            ``False`` when the update was skipped or could not be applied.
        """
        key = (domain, product)
        if key not in self._manifest_map:
            logger.warning(
                "No product.yaml found for domain=%s product=%s; skipping path update.",
                domain,
                product,
            )
            return False
        yaml_path = self._manifest_map[key]
        with yaml_path.open("r", encoding="utf-8", newline="") as fh:
            text = fh.read()
        data = yaml.safe_load(text) or {}
        spec = data.get("spec") if isinstance(data, dict) else None
        assets = spec.get("assets") if isinstance(spec, dict) else None
        match: dict[str, Any] | None = None
        if isinstance(assets, list):
            match = next(
                (asset for asset in assets if isinstance(asset, dict) and asset.get("name") == asset_name),
                None,
            )
        if match is None:
            logger.warning(
                "Asset '%s' not found in spec.assets of %s; skipping path update (manifest assets are never appended).",
                asset_name,
                yaml_path,
            )
            return False
        if not match.get("type"):
            logger.warning(
                "Asset '%s' in %s has no 'type' (required by product-schema.json); refusing to write it.",
                asset_name,
                yaml_path,
            )
            return False
        if match.get("path") == actual_path:
            logger.debug("Asset '%s' already at %s; nothing to do.", asset_name, actual_path)
            return True
        new_text = _replace_asset_path(text, asset_name, actual_path)
        if new_text is None:
            logger.warning(
                "Could not locate the 'path' key of asset '%s' in %s text; leaving the manifest untouched.",
                asset_name,
                yaml_path,
            )
            return False
        with yaml_path.open("w", encoding="utf-8", newline="") as fh:
            fh.write(new_text)
        logger.info("Updated asset '%s' path in %s → %s", asset_name, yaml_path, actual_path)
        return True

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
        logger.warning("MANIFESTS_DIR not set; skipping manifest updates.")
        return None
    try:
        return ManifestUpdater(manifests_dir=manifests_dir)
    except FileNotFoundError as exc:
        logger.warning("ManifestUpdater init failed: %s; skipping.", exc)
        return None


def _register_polaris_namespaces(client: PolarisClient) -> int:
    """Register all known namespaces in Polaris.

    Args:
        client: Initialised PolarisClient.

    Returns:
        Number of namespaces that failed to register.
    """
    logger.info("--- Registering Polaris namespaces ---")
    failures = 0
    for ns in NAMESPACES:
        try:
            client.register_namespace(ns)
        except requests.RequestException as exc:
            failures += 1
            logger.error("Failed to register namespace '%s': %s", ns, exc)
    return failures


def _register_polaris_tables(client: PolarisClient) -> int:
    """Register all gold and stream Iceberg tables in Polaris.

    Args:
        client: Initialised PolarisClient.

    Returns:
        Number of tables that failed to register.
    """
    failures = 0
    for label, tables in (("gold", GOLD_TABLES), ("stream", STREAM_TABLES)):
        logger.info("--- Registering %s Iceberg tables in Polaris ---", label)
        for tbl in tables:
            try:
                client.register_table(
                    namespace=tbl["namespace"],
                    table_name=tbl["name"],
                    location=tbl["location"],
                    schema=tbl["schema"],
                )
            except requests.RequestException as exc:
                failures += 1
                logger.error(
                    "Failed to register table %s.%s: %s",
                    tbl["namespace"],
                    tbl["name"],
                    exc,
                )
    return failures


def _register_marquez_namespaces(client: MarquezClient) -> int:
    """Register every namespace used by datasets or jobs in Marquez.

    Args:
        client: Initialised MarquezClient.

    Returns:
        Number of namespaces that failed to register.
    """
    logger.info("--- Registering Marquez namespaces ---")
    failures = 0
    for ns in marquez_namespaces():
        owner, desc = MARQUEZ_NAMESPACE_META.get(ns, ("data-platform", f"Namespace '{ns}'"))
        try:
            client.create_namespace(namespace=ns, owner=owner, description=desc)
        except requests.RequestException as exc:
            failures += 1
            logger.error("Failed to create Marquez namespace '%s': %s", ns, exc)
    return failures


def _register_marquez_datasets(client: MarquezClient) -> int:
    """Register all datasets in Marquez.

    Args:
        client: Initialised MarquezClient.

    Returns:
        Number of datasets that failed to register.
    """
    logger.info("--- Registering Marquez datasets ---")
    failures = 0
    for ds in lineage_datasets():
        try:
            client.create_dataset(
                namespace=ds["namespace"],
                name=ds["name"],
                fields=ds["schema"],
                source_name=ds["source"],
                description=ds["description"],
                tags=ds["tags"],
            )
        except requests.RequestException as exc:
            failures += 1
            logger.error(
                "Failed to register Marquez dataset %s/%s: %s",
                ds["namespace"],
                ds["name"],
                exc,
            )
    return failures


def _register_marquez_jobs(client: MarquezClient) -> int:
    """Register all pipeline job definitions in Marquez.

    Job inputs/outputs are the same ``(namespace, name)`` pairs used to create
    the datasets. Any reference to a dataset id that was never registered is
    rejected before the call, so lineage can never be left dangling.

    Args:
        client: Initialised MarquezClient.

    Returns:
        Number of jobs that failed to register (including skipped jobs with
        unknown dataset references).
    """
    logger.info("--- Registering Marquez jobs ---")
    failures = 0
    known = set(DATASET_IDS)
    for job in PIPELINE_JOBS:
        missing = sorted({ref for ref in job["inputs"] + job["outputs"] if ref not in known})
        if missing:
            failures += 1
            logger.error(
                "Marquez job %s/%s references dataset ids never registered: %s; skipping job registration.",
                job["namespace"],
                job["name"],
                ", ".join(missing),
            )
            continue
        try:
            client.create_job(
                namespace=job["namespace"],
                name=job["name"],
                inputs=[dataset_identity(ref) for ref in job["inputs"]],
                outputs=[dataset_identity(ref) for ref in job["outputs"]],
                description=job["description"],
                location=job["location"],
            )
        except requests.RequestException as exc:
            failures += 1
            logger.error(
                "Failed to register Marquez job %s/%s: %s",
                job["namespace"],
                job["name"],
                exc,
            )
    return failures


def _update_manifests(updater: ManifestUpdater) -> None:
    """Patch the manifests of registered tables with their actual table paths.

    The asset targets come from the table registry (``manifest`` field) and are
    real ``spec.assets[].name`` values (``gold_orders``, ``gold_customers``).
    Tables with no manifest asset representing them are skipped with a warning.

    Args:
        updater: Initialised ManifestUpdater.
    """
    logger.info("--- Updating product.yaml manifests ---")
    for tbl in ALL_TABLES:
        target = tbl.get("manifest")
        if not isinstance(target, dict):
            logger.warning(
                "No manifest asset represents %s (%s); skipping path update.",
                tbl["logical"],
                tbl["location"],
            )
            continue
        updater.update_table_path(
            target["domain"],
            target["product"],
            target["asset"],
            tbl["location"],
        )


def _print_summary(polaris_client: PolarisClient) -> int:
    """Print a summary report of registered namespaces and tables.

    Args:
        polaris_client: PolarisClient used for the registration run.

    Returns:
        Number of namespaces that failed to list (counted as step failures).
    """
    print("\n" + "=" * 70)
    print("  CATALOG REGISTRATION SUMMARY")
    print("=" * 70)
    print(f"  Timestamp : {datetime.now(UTC).isoformat()}")
    print(f"  Polaris   : {polaris_client.base_url}")
    print()

    failures = 0
    for ns in NAMESPACES:
        try:
            tables = polaris_client.list_tables(ns)
            print(f"  Namespace '{ns}': {len(tables)} table(s)")
            for tbl in sorted(tables):
                print(f"    - {tbl}")
        except requests.RequestException as exc:
            failures += 1
            print(f"  Namespace '{ns}': ERROR listing tables — {exc}")

    print()
    print(f"  Gold tables registered  : {len(GOLD_TABLES)}")
    print(f"  Stream tables registered: {len(STREAM_TABLES)}")
    print(f"  Lineage datasets        : {len(DATASET_IDS)}")
    print(f"  Pipeline jobs registered: {len(PIPELINE_JOBS)}")
    print("=" * 70 + "\n")
    return failures


def run_registration(
    polaris_client: PolarisClient,
    marquez_client: MarquezClient,
    manifest_updater: ManifestUpdater | None = None,
) -> dict[str, int]:
    """Run every registration step and return the per-step failure counts.

    Steps never raise on HTTP failures; each one counts its failures so the
    caller can decide the process exit code (fail-closed).

    Args:
        polaris_client: Polaris client.
        marquez_client: Marquez client.
        manifest_updater: Optional manifest updater (skipped when ``None``).

    Returns:
        Mapping of step name -> number of failed calls.
    """
    failures: dict[str, int] = {
        "polaris_namespaces": _register_polaris_namespaces(polaris_client),
        "polaris_tables": _register_polaris_tables(polaris_client),
        "marquez_namespaces": _register_marquez_namespaces(marquez_client),
        "marquez_datasets": _register_marquez_datasets(marquez_client),
        "marquez_jobs": _register_marquez_jobs(marquez_client),
    }
    if manifest_updater is not None:
        _update_manifests(manifest_updater)
    else:
        logger.info("Skipping manifest updates (MANIFESTS_DIR not configured).")
    failures["polaris_summary"] = _print_summary(polaris_client)
    return failures


def main() -> None:
    """Orchestrate full catalog registration and manifest update.

    Steps:
    1. Read config from environment variables (missing vars exit 1).
    2. Register all namespaces and tables in Polaris.
    3. Register namespaces, datasets, and jobs in Marquez.
    4. Update product.yaml manifests with actual table paths.
    5. Print summary report.

    Exit code:
        0 only when every Polaris/Marquez call succeeded; 1 otherwise.
    """
    logger.info("Starting catalog_registration.py")

    polaris_client = _build_polaris_client()
    marquez_client = _build_marquez_client()
    manifest_updater = _build_manifest_updater()

    failures = run_registration(polaris_client, marquez_client, manifest_updater)

    total_failures = sum(failures.values())
    if total_failures > 0:
        breakdown = ", ".join(f"{step}={count}" for step, count in failures.items() if count)
        logger.error(
            "catalog_registration.py FAILED: %d call(s) failed (%s).",
            total_failures,
            breakdown,
        )
        sys.exit(1)
    logger.info("catalog_registration.py completed successfully.")


if __name__ == "__main__":
    main()
