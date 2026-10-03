"""
Modern Data Architecture POC - Unit Tests: OLTP source backend selection.

Regression tests for the PostgreSQL / synthetic-source contract: a reachable
PostgreSQL table is authoritative even when it holds zero rows (empty real
table yields ``[]``, never fabricated records); the synthetic generator
applies ONLY when PostgreSQL is unconfigured or the connection/query fails.
"""

from __future__ import annotations

import sys
import types
from typing import Any

from src.adapters.source.oltp_source import OltpSourceAdapter


class _FakeCursor:
    def __init__(self, rows_or_error: list[dict[str, Any]] | Exception) -> None:
        self._rows_or_error = rows_or_error
        self.query = ""

    def __enter__(self) -> _FakeCursor:
        return self

    def __exit__(self, *args: Any) -> bool:
        return False

    def execute(self, query: str) -> None:
        self.query = query

    def fetchall(self) -> list[dict[str, Any]]:
        if isinstance(self._rows_or_error, Exception):
            raise self._rows_or_error
        return [dict(r) for r in self._rows_or_error]


class _FakeConnection:
    instances: list[_FakeConnection] = []

    def __init__(self, rows_or_error: list[dict[str, Any]] | Exception) -> None:
        self._rows_or_error = rows_or_error
        self.cursor_obj = _FakeCursor(rows_or_error)
        self.closed = False
        _FakeConnection.instances.append(self)

    def cursor(self, cursor_factory: Any = None) -> _FakeCursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


def _install_fake_psycopg2(monkeypatch, rows_or_error: list[dict[str, Any]] | Exception) -> None:
    """Registers a stub ``psycopg2``/``psycopg2.extras`` pair in sys.modules."""
    _FakeConnection.instances.clear()
    fake_extras = types.ModuleType("psycopg2.extras")
    fake_extras.RealDictCursor = object
    fake_pg = types.ModuleType("psycopg2")
    fake_pg.extras = fake_extras  # type: ignore[attr-defined]
    fake_pg.connect = lambda **kwargs: _FakeConnection(rows_or_error)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg2", fake_pg)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake_extras)


def test_empty_real_table_returns_empty_list_not_synthetic(monkeypatch) -> None:
    """Regression: a reachable but empty table yields [], never fabricated rows."""
    monkeypatch.setenv("PGPASSWORD", "secret")
    _install_fake_psycopg2(monkeypatch, [])

    result = OltpSourceAdapter().extract_table("orders", limit=10)

    assert result.records == []
    assert _FakeConnection.instances and _FakeConnection.instances[0].closed


def test_populated_real_table_returns_live_rows(monkeypatch) -> None:
    monkeypatch.setenv("PGPASSWORD", "secret")
    live = [{"id": 1, "status": "real"}]
    _install_fake_psycopg2(monkeypatch, live)

    result = OltpSourceAdapter().extract_table("orders", limit=5)

    assert result.records == live
    assert "LIMIT 5" in _FakeConnection.instances[0].cursor_obj.query


def test_connection_error_falls_back_to_synthetic(monkeypatch) -> None:
    monkeypatch.setenv("PGPASSWORD", "secret")
    _install_fake_psycopg2(monkeypatch, ConnectionError("boom"))

    result = OltpSourceAdapter(seed=42).extract_table("orders", limit=5)

    assert len(result.records) == 5  # synthetic fallback still serves the demo profile


def test_unconfigured_postgres_uses_synthetic_source(monkeypatch) -> None:
    monkeypatch.delenv("PGPASSWORD", raising=False)

    result = OltpSourceAdapter(seed=42).extract_table("orders", limit=7)

    assert len(result.records) == 7
