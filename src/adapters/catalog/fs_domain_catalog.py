"""Modern Data Architecture POC - Filesystem Domain Catalog Adapter.

Implements DomainCatalogPort by scanning ``domains/**`` for product, contract
and schema manifests. Malformed files are skipped and logged.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Callable
from typing import IO, Any

import yaml

from src.ports.outbound import DomainCatalogPort

logger = logging.getLogger("adapter.catalog.fs_domain_catalog")


class FsDomainCatalogAdapter(DomainCatalogPort):
    """Reads data-product manifests from the repository filesystem."""

    def __init__(self, repo_root: str = ".") -> None:
        self._repo_root = repo_root

    def list_products(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (path, parsed product.yaml) pairs, sorted by path."""
        return self._scan("product.yaml", yaml.safe_load)

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (path, parsed contract.yaml) pairs, sorted by path."""
        return self._scan("contract.yaml", yaml.safe_load)

    def list_schemas(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (path, parsed schema.json) pairs, sorted by path."""
        return self._scan("schema.json", json.load)

    def load_governance_schema(self, schema_filename: str) -> dict[str, Any] | None:
        """Loads ``<repo_root>/governance/schemas/<schema_filename>`` as a dict.

        Returns None when missing or invalid; validation then runs structural checks.
        """
        schema_path = os.path.join(self._repo_root, "governance", "schemas", schema_filename)
        try:
            with open(schema_path, encoding="utf-8") as handle:
                data = json.load(handle)
        except FileNotFoundError:
            logger.warning(
                "Governance schema not found: %s; skipping JSON-schema validation",
                schema_path,
            )
            return None
        except Exception as exc:
            logger.warning(
                "Governance schema %s is invalid: %s; skipping JSON-schema validation",
                schema_path,
                exc,
            )
            return None
        if not isinstance(data, dict):
            logger.warning(
                "Governance schema %s is invalid: root document is not a mapping (got %s); "
                "skipping JSON-schema validation",
                schema_path,
                type(data).__name__,
            )
            return None
        return data

    def _scan(
        self,
        filename: str,
        loader: Callable[[IO[Any]], Any],
    ) -> list[tuple[str, dict[str, Any]]]:
        domains_dir = os.path.join(self._repo_root, "domains")
        if not os.path.isdir(domains_dir):
            logger.error("Domains directory not found: %s", domains_dir)
            return []

        found: list[tuple[str, dict[str, Any]]] = []
        for dirpath, dirnames, filenames in os.walk(domains_dir):
            dirnames.sort()
            if filename not in filenames:
                continue
            path = os.path.join(dirpath, filename)
            try:
                with open(path, encoding="utf-8") as handle:
                    data = loader(handle)
            except Exception as exc:  # malformed files must not break discovery
                logger.error("Skipping unreadable or malformed file %s: %s", path, exc)
                continue
            if not isinstance(data, dict):
                logger.error(
                    "Skipping %s: root document is not a mapping (got %s)",
                    path,
                    type(data).__name__,
                )
                continue
            found.append((path, data))

        found.sort(key=lambda item: item[0])
        return found
