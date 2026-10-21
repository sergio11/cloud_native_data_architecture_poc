"""
Builds the PyIceberg catalog the Iceberg lakehouse adapter talks to.

It lives in its own module so that importing the composition root does NOT
require PyIceberg: the local profile and the unit tests run on the SQLite
adapter, and PyIceberg is only needed when ``LAKEHOUSE_BACKEND=iceberg``.

The connection is the same one the Flink job and Trino use, and it is read
from the environment so the chart is the single source of truth:

    ICEBERG_CATALOG_URI   the Polaris REST catalog URI
    ICEBERG_CATALOG_NAME  the catalog name inside Polaris
    ICEBERG_WAREHOUSE     alias of the above, for symmetry with the other engines
    POLARIS_OAUTH2_CREDENTIAL   "client_id:client_secret"
    POLARIS_OAUTH2_SCOPE         the OAuth2 scope Polaris expects
    MINIO_ENDPOINT / MINIO_ACCESS_KEY / MINIO_SECRET_KEY
"""

from __future__ import annotations

import os
from typing import Any

#: The scope Polaris requires. Polaris rejects anything else with
#: "invalid_scope: The scope is invalid", and the Iceberg client's own default
#: ("catalog") is one of the things it rejects.
DEFAULT_OAUTH2_SCOPE = "PRINCIPAL_ROLE:ALL"


def _require(name: str, *aliases: str) -> str:
    """Returns the first non-empty environment variable among name + aliases.

    The alias exists because the two spellings were mixed in the charts:
    `ICEBERG_CATALOG_URL` (s1-ingestion, s3-batch) and `ICEBERG_CATALOG_URI`
    (the Flink jobs, the diagnostic probes and this module). The mismatch was
    silent until a real run: the medallion died with "ICEBERG_CATALOG_URI is
    required" while the chart was setting the other name. The canonical name
    is the URI one; the URL spelling is accepted so no deployment has to be
    changed to keep working.
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

    Imported lazily so the module (and the composition root) can be imported
    without PyIceberg installed.

    PyIceberg's RestCatalog takes the catalogue NAME as its first argument and
    the properties as keyword arguments, so the properties are unpacked rather
    than passed as a dict. The NAME identifies the catalog locally while the
    "warehouse" PROPERTY is what the REST client sends as ?warehouse= to resolve
    it; both are needed, and dropping the property makes Polaris answer
    "Please specify a warehouse".

    The access-delegation header is removed on purpose. PyIceberg sets
    ``X-Iceberg-Access-Delegation: vended-credentials`` on every request, and
    Polaris reads it as "this client wants to write to the table directly", which
    it enforces with a privilege the catalog_admin role does not carry:

        ForbiddenException: Principal 'root' ... is not authorized for op
        CREATE_TABLE_DIRECT_WITH_WRITE_DELEGATION

    The very same request without the header is accepted. The delegation is not
    wanted here: this client authenticates against MinIO with static
    credentials instead of asking the catalog to vend them.
    """
    from pyiceberg.catalog.rest import RestCatalog

    properties = iceberg_catalog_properties()
    # The catalogue NAME identifies the catalog locally, while the "warehouse"
    # PROPERTY is what the REST client sends as ?warehouse= to resolve it.
    # Both are needed: dropping the property makes Polaris answer
    # "Please specify a warehouse".
    name = str(properties.get("warehouse", "polaris"))
    catalog = RestCatalog(name, **properties)
    session = getattr(catalog, "_session", None)
    if session is not None:
        session.headers.pop("X-Iceberg-Access-Delegation", None)
    return catalog
