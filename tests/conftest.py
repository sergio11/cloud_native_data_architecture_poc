"""Shared hermetic test fixture (root conftest).

Keeps every suite offline: Marquez POST fails instantly and the Trino /
PostgreSQL backends are disabled by stripping ``TRINO_HOST`` / ``PGPASSWORD``.
"""

from __future__ import annotations

from typing import Any

import pytest
import requests

from src.adapters.lineage import marquez_lineage


@pytest.fixture(autouse=True)
def _hermetic_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keeps every test offline and CWD-independent (strictest variant)."""

    def _unreachable_post(*args: Any, **kwargs: Any) -> Any:
        raise requests.exceptions.ConnectionError("network disabled: tests must be hermetic")

    monkeypatch.setattr(marquez_lineage.requests, "post", _unreachable_post)
    monkeypatch.delenv("TRINO_HOST", raising=False)
    monkeypatch.delenv("PGPASSWORD", raising=False)
