"""Builds the PyIceberg REST catalog used by the Iceberg lakehouse adapter.

Reads connection details from the environment so the chart is the single
source of truth. Imported lazily to keep PyIceberg optional.
"""

from __future__ import annotations

import os
from typing import Any

#: Scope required by Polaris (its own default "catalog" is rejected).
DEFAULT_OAUTH2_SCOPE = "PRINCIPAL_ROLE:ALL"


def _require(name: str, *aliases: str) -> str:
    """Returns the first non-empty environment variable among name + aliases.

    ``ICEBERG_CATALOG_URL`` is accepted as an alias of ``ICEBERG_CATALOG_URI``.
    """
    for candidate in (name, *aliases):
        value = os.getenv(candidate, "").strip()
        if value:
            return value
    raise ValueError(f"{name} is required for the Iceberg lakehouse backend.")


def iceberg_catalog_properties() -> dict[str, Any]:
    """The property set the PyIceberg REST catalog is built with."""
    catalog_uri = _require("ICEBERG_CATALOG_URI", "ICEBERG_CATALOG_URL")
    catalog_name = os.getenv("ICEBERG_CATALOG_NAME", "").strip() or os.getenv("ICEBERG_WAREHOUSE", "").strip()
    if not catalog_name:
        raise ValueError("ICEBERG_CATALOG_NAME (or ICEBERG_WAREHOUSE) is required for the Iceberg backend.")
    credential = _require("POLARIS_OAUTH2_CREDENTIAL")
    scope = os.getenv("POLARIS_OAUTH2_SCOPE", "").strip() or DEFAULT_OAUTH2_SCOPE
    endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
    access_key = os.getenv("MINIO_ACCESS_KEY", "").strip()
    secret_key = os.getenv("MINIO_SECRET_KEY", "").strip()

    properties: dict[str, Any] = {
        "uri": catalog_uri,
        # For a REST catalog this is the CATALOG NAME, not an S3 path: Polaris
        # resolves the client with GET /api/catalog/v1/config?warehouse=<name>.
        "warehouse": catalog_name,
        "credential": credential,
        "scope": scope,
        "oauth2-server-uri": f"{catalog_uri.rstrip('/')}/v1/oauth/tokens",
    }
    if endpoint:
        properties["s3.endpoint"] = endpoint
        properties["s3.path-style-access"] = "true"
    if access_key:
        properties["s3.access-key-id"] = access_key
    if secret_key:
        properties["s3.secret-access-key"] = secret_key
    region = os.getenv("MINIO_REGION", "").strip() or os.getenv("AWS_REGION", "").strip()
    if region:
        properties["s3.region"] = region
    return properties


def _iceberg_catalog() -> Any:
    """Builds the PyIceberg REST catalog.

    The catalog name and ``warehouse`` are required by Polaris. The
    access-delegation header is stripped (static MinIO credentials are used).
    """
    from pyiceberg.catalog.rest import RestCatalog

    properties = iceberg_catalog_properties()
    # Catalog name identifies the catalog locally; ``warehouse`` resolves it.
    name = str(properties.get("warehouse", "polaris"))
    catalog = RestCatalog(name, **properties)
    session = getattr(catalog, "_session", None)
    if session is not None:
        session.headers.pop("X-Iceberg-Access-Delegation", None)
    return catalog
