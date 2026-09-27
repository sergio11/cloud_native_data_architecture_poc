"""
Modern Data Architecture POC - Filesystem Domain Catalog Adapter.
Implements DomainCatalogPort by recursively scanning ``domains/**`` for
``product.yaml``, ``contract.yaml`` and ``schema.json`` manifests (RF-13/RF-15).

Discovery is deterministic (results sorted by path) and never crashes on
malformed input: a file that fails to parse (YAML/JSON error) or whose root
document is not a mapping is skipped with ``logger.error`` including its path.
The port contract exposes 2-tuples ``(path, parsed_content)`` only, so parse
failures surface to consumers indirectly - the validation use case detects
skipped files through sibling/count cross-checks (missing product/contract/
schema for a manifest that does exist on disk), while the adapter log always
carries the exact parse error. Only stdlib + PyYAML + logging; no network.
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
