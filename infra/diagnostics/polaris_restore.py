import json
import os
import sys

import requests

BASE = os.environ["POLARIS_URL"].rstrip("/")
CLIENT_ID = os.environ.get("POLARIS_CLIENT_ID", "poc-admin")
CLIENT_SECRET = os.environ.get("POLARIS_CLIENT_SECRET", "poc-admin-secret")
CATALOG = os.environ.get("CATALOG_NAME", "polaris")
WAREHOUSE = os.environ.get("WAREHOUSE_LOCATION", "s3://warehouse/")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio.platform.svc.cluster.local:9000")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
NAMESPACES = [n for n in os.environ.get("NAMESPACES", "sales,marketing,finance").split(",") if n]

# Tables to re-register, as "namespace.table=metadata-location" entries. Each
# location is the current Iceberg metadata JSON in MinIO (highest
# "last-updated-ms"); re-registering restores tables without touching data.
TABLE_SPECS = os.environ.get("TABLES", "")
TABLES = []
for _spec in TABLE_SPECS.split(","):
    _spec = _spec.strip()
    if not _spec:
        continue
    _identifier, _, _location = _spec.partition("=")
    _namespace, _, _table = _identifier.strip().partition(".")
    if not _namespace or not _table or not _location:
        print(f"WARNING: ignoring malformed table spec '{_spec}'")
        continue
    TABLES.append((_namespace, _table, _location))

session = requests.Session()
headers = {}


def fail(message):
    print(f"ERROR: {message}")
    sys.exit(1)


def show(response, limit=400):
    body = response.text or ""
    if len(body) > limit:
        body = body[:limit] + "..."
    return f"HTTP {response.status_code} {body}"


def authenticate():
    response = session.post(
        f"{BASE}/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "PRINCIPAL_ROLE:ALL",
        },
        timeout=30,
    )
    if response.status_code != 200:
        fail(f"token request failed: {show(response)}")
    token = response.json().get("access_token")
    if not token:
        fail("token response did not contain access_token")
    headers["Authorization"] = f"Bearer {token}"
    print(f"Token obtained for principal '{CLIENT_ID}'.")


def catalog_definition():
    response = session.get(
        f"{BASE}/api/management/v1/catalogs/{CATALOG}", headers=headers, timeout=30
    )
    if response.status_code == 200:
        return response.json()
    if response.status_code == 404:
        return None
    fail(f"unexpected answer reading catalog '{CATALOG}': {show(response)}")


def list_tables(namespace):
    response = session.get(
        f"{BASE}/api/catalog/v1/{CATALOG}/namespaces/{namespace}/tables",
        headers=headers,
        timeout=30,
    )
    if response.status_code == 200:
        # Polaris returns {"identifiers": [{"namespace": ["sales"], "name": ...}]}.
        return [entry.get("name") for entry in response.json().get("identifiers", [])]
    if response.status_code == 404:
        return []
    fail(f"unexpected answer listing tables of '{namespace}': {show(response)}")


def drop_catalog():
    response = session.delete(
        f"{BASE}/api/management/v1/catalogs/{CATALOG}",
        headers=headers,
        params={"cascade": "true", "purge": "true"},
        timeout=60,
    )
    if response.status_code in (200, 204, 404):
        print(f"Catalog dropped (HTTP {response.status_code}).")
        return
    fail(f"dropping catalog failed: {show(response)}")


def create_catalog():
    payload = {
        "name": CATALOG,
        "type": "INTERNAL",
        "properties": {"default-base-location": WAREHOUSE},
        "storageConfigInfo": {
            "storageType": "S3",
            "allowedLocations": [WAREHOUSE],
            "endpoint": S3_ENDPOINT,
            "region": AWS_REGION,
            "pathStyleAccess": True,
            "s3.endpoint": S3_ENDPOINT,
            "s3.region": AWS_REGION,
            "s3.path-style-access": "true",
        },
    }
    response = session.post(
        f"{BASE}/api/management/v1/catalogs",
        headers=headers,
        json=payload,
        timeout=60,
    )
    if response.status_code in (200, 201, 409):
        print(f"Catalog '{CATALOG}' ready (HTTP {response.status_code}).")
        return
    fail(f"catalog creation failed: {show(response)}")


def grant_catalog_role():
    payload = {
        "catalogRole": {"name": "catalog_admin"},
        "principal": {"type": "SERVICE_ROLE", "name": CLIENT_ID},
    }
    response = session.post(
        f"{BASE}/api/management/v1/catalogs/{CATALOG}/catalog-roles",
        headers=headers,
        json=payload,
        timeout=30,
    )
    if response.status_code in (200, 201, 204, 409):
        print(f"catalog_admin granted to '{CLIENT_ID}' (HTTP {response.status_code}).")
        return
    fail(f"granting catalog_admin failed: {show(response)}")


def create_namespaces():
    for namespace in NAMESPACES:
        response = session.post(
            f"{BASE}/api/catalog/v1/{CATALOG}/namespaces",
            headers=headers,
            json={"namespace": [namespace]},
            timeout=30,
        )
        if response.status_code in (200, 201, 409):
            print(f"Namespace '{namespace}' ready (HTTP {response.status_code}).")
            continue
        fail(f"namespace '{namespace}' creation failed: {show(response)}")


def load_table(namespace, table):
    response = session.get(
        f"{BASE}/api/catalog/v1/{CATALOG}/namespaces/{namespace}/tables/{table}",
        headers=headers,
        timeout=30,
    )
    if response.status_code == 200:
        return response.json()
    if response.status_code == 404:
        return None
    fail(f"unexpected answer loading table '{namespace}.{table}': {show(response)}")


def drop_table(namespace, table):
    response = session.delete(
        f"{BASE}/api/catalog/v1/{CATALOG}/namespaces/{namespace}/tables/{table}",
        headers=headers,
        params={"purgeRequested": "false"},
        timeout=60,
    )
    if response.status_code in (200, 204, 404):
        print(f"Table '{namespace}.{table}' dropped (HTTP {response.status_code}).")
        return
    fail(f"dropping table '{namespace}.{table}' failed: {show(response)}")


def register_table(namespace, table, metadata_location):
    payload = {"name": table, "metadata-location": metadata_location}
    response = session.post(
        f"{BASE}/api/catalog/v1/{CATALOG}/namespaces/{namespace}/register",
        headers=headers,
        json=payload,
        timeout=60,
    )
    if response.status_code in (200, 201):
        print(f"Table '{namespace}.{table}' registered at {metadata_location}.")
        return
    if response.status_code != 409:
        fail(f"registering '{namespace}.{table}' failed: {show(response)}")
    current = load_table(namespace, table)
    snapshots = (current or {}).get("metadata", {}).get("snapshots", [])
    if snapshots:
        print(
            f"Table '{namespace}.{table}' already registered with {len(snapshots)} snapshot(s); kept as-is."
        )
        return
    print(f"Table '{namespace}.{table}' is registered but empty; re-registering.")
    drop_table(namespace, table)
    response = session.post(
        f"{BASE}/api/catalog/v1/{CATALOG}/namespaces/{namespace}/register",
        headers=headers,
        json=payload,
        timeout=60,
    )
    if response.status_code not in (200, 201):
        fail(f"re-registering '{namespace}.{table}' failed: {show(response)}")
    print(f"Table '{namespace}.{table}' registered at {metadata_location}.")


def verify_warehouse():
    response = session.get(
        f"{BASE}/api/catalog/v1/config",
        headers=headers,
        params={"warehouse": CATALOG},
        timeout=30,
    )
    if response.status_code != 200:
        fail(f"warehouse '{CATALOG}' does not resolve: {show(response)}")
    print(f"Warehouse '{CATALOG}' resolves for Iceberg REST clients.")


def main():
    print("=== Polaris catalog restore ===")
    authenticate()

    definition = catalog_definition()
    if definition is None:
        print(f"Catalog '{CATALOG}' does not exist; creating it.")
        create_catalog()
    else:
        properties = definition.get("properties", {}) or {}
        base_location = properties.get("default-base-location")
        tables = list_tables("sales")
        print(f"Existing catalog '{CATALOG}' base location: {base_location}")
        if not tables and base_location != WAREHOUSE:
            # An empty catalog with a different base location would refuse every table.
            print("Existing catalog is empty but its base location differs; recreating it.")
            drop_catalog()
            create_catalog()
        elif tables:
            # A catalog that already holds tables is never dropped.
            print(f"Catalog already holds tables {tables}; they are re-registered below.")
        else:
            print(f"Catalog '{CATALOG}' exists, is empty and matches the expected base location.")

    grant_catalog_role()
    create_namespaces()

    for namespace, table, metadata_location in TABLES:
        register_table(namespace, table, metadata_location)

    verify_warehouse()

    for namespace in NAMESPACES:
        print(f"Tables in '{namespace}': {list_tables(namespace)}")

    print("RESTORE: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
