"""
Modern Data Architecture POC - Shared hermetic test fixture (root conftest).

Strictest variant unified here so every suite (unit + integration) runs
offline: Marquez HTTP POST fails instantly instead of burning the adapter's
socket timeout on every lineage emit, and the Trino / PostgreSQL backends can
never trigger (``TRINO_HOST`` / ``PGPASSWORD`` are stripped for the test).

Local ``_hermetic_environment`` duplicates were removed from the individual
modules; they all resolve to this single autouse fixture.
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
