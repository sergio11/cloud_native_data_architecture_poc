"""
Tests for the environment contract of the Iceberg lakehouse adapter.

The adapter reads its connection from the environment so the Helm chart is the
single source of truth, and that contract is invisible until a pod runs: a
mismatched variable name is not a test failure, it is a medallion that dies in
the cluster with "ICEBERG_CATALOG_URI is required for the Iceberg lakehouse
backend" while the chart was setting ICEBERG_CATALOG_URL.

So the spellings are pinned here, including the alias, so the next mismatch is
a red test instead of a broken pod.
"""

from __future__ import annotations

import pytest

from src.adapters.storage.iceberg_catalog import (
    DEFAULT_OAUTH2_SCOPE,
    iceberg_catalog_properties,
)

CATALOG_URI = "http://polaris.platform.svc.cluster.local:8181/api/catalog"


@pytest.fixture
def iceberg_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The environment every engine of the platform runs with."""
    monkeypatch.setenv("ICEBERG_CATALOG_URI", CATALOG_URI)
    monkeypatch.setenv("ICEBERG_CATALOG_NAME", "polaris")
    monkeypatch.setenv("POLARIS_OAUTH2_CREDENTIAL", "poc-admin:poc-admin-secret")
    monkeypatch.setenv("POLARIS_OAUTH2_SCOPE", "PRINCIPAL_ROLE:ALL")
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio.platform.svc.cluster.local:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_REGION", "us-east-1")


def test_canonical_uri_is_used(iceberg_env: None) -> None:
    properties = iceberg_catalog_properties()
    assert properties["uri"] == CATALOG_URI
    # The OAuth2 token endpoint is derived from the URI, without the double
    # slash: Polaris answers 404 on /api/catalog//v1/oauth/tokens.
    assert properties["oauth2-server-uri"] == f"{CATALOG_URI}/v1/oauth/tokens"


def test_url_spelling_is_accepted_as_an_alias(monkeypatch: pytest.MonkeyPatch, iceberg_env: None) -> None:
    """The charts used to set ICEBERG_CATALOG_URL while the code read the URI."""
    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    monkeypatch.setenv("ICEBERG_CATALOG_URL", CATALOG_URI)
    properties = iceberg_catalog_properties()
    assert properties["uri"] == CATALOG_URI


def test_canonical_name_wins_over_the_alias(monkeypatch: pytest.MonkeyPatch, iceberg_env: None) -> None:
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "other-catalog")
    assert iceberg_catalog_properties()["warehouse"] == "polaris"


def test_warehouse_is_an_alias_of_the_catalog_name(monkeypatch: pytest.MonkeyPatch, iceberg_env: None) -> None:
    monkeypatch.delenv("ICEBERG_CATALOG_NAME", raising=False)
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "polaris")
    assert iceberg_catalog_properties()["warehouse"] == "polaris"


def test_missing_uri_is_reported_with_the_name_it_looked_for(
    monkeypatch: pytest.MonkeyPatch, iceberg_env: None
) -> None:
    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    monkeypatch.delenv("ICEBERG_CATALOG_URL", raising=False)
    with pytest.raises(ValueError, match="ICEBERG_CATALOG_URI is required"):
        iceberg_catalog_properties()


def test_missing_catalog_name_is_reported(monkeypatch: pytest.MonkeyPatch, iceberg_env: None) -> None:
    monkeypatch.delenv("ICEBERG_CATALOG_NAME", raising=False)
    monkeypatch.delenv("ICEBERG_WAREHOUSE", raising=False)
    with pytest.raises(ValueError, match="ICEBERG_CATALOG_NAME"):
        iceberg_catalog_properties()


def test_scope_defaults_to_what_polaris_demands(monkeypatch: pytest.MonkeyPatch, iceberg_env: None) -> None:
    monkeypatch.delenv("POLARIS_OAUTH2_SCOPE", raising=False)
    assert iceberg_catalog_properties()["scope"] == DEFAULT_OAUTH2_SCOPE


def test_minio_settings_reach_the_s3_client(iceberg_env: None) -> None:
    properties = iceberg_catalog_properties()
    assert properties["s3.endpoint"] == "http://minio.platform.svc.cluster.local:9000"
    assert properties["s3.path-style-access"] == "true"
    assert properties["s3.access-key-id"] == "minioadmin"
    assert properties["s3.secret-access-key"] == "minioadmin"
    assert properties["s3.region"] == "us-east-1"
