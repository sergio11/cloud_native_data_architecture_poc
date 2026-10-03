"""Unit tests: extraction provenance (W1) - honest ``ExtractionResult.origin``.

Covers the two ends of the provenance contract:
  * a successful PostgreSQL query (including one returning zero rows) reports
    ``origin="postgresql"``, and only connection/query failures (or an
    unconfigured source) fall back to the generator with
    ``origin="synthetic"`` - programming errors propagate instead;
  * ``IngestBronzeUseCase`` stamps the origin verbatim on the ``_source``
    audit column AND on the OpenLineage input dataset, so both always agree.
"""

from __future__ import annotations

import sys
import types
from typing import Any, Literal

import pytest

from src.adapters.source.oltp_source import OltpSourceAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.domain.entities import ExtractionResult
from src.use_cases.ingest_bronze import IngestBronzeUseCase, provenance_label
from tests.helpers import StubLineage

ORDER_ROW = {
    "id": 1,
    "customer_id": 100,
    "total_amount": 42.5,
    "status": "confirmed",
    "order_date": "2026-09-25",
    "created_at": "2026-09-25T10:00:00+00:00",
}


# ---------------------------------------------------------------------------
# OltpSourceAdapter: origin reporting
# ---------------------------------------------------------------------------


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
    def __init__(self, rows_or_error: list[dict[str, Any]] | Exception) -> None:
        self.cursor_obj = _FakeCursor(rows_or_error)

    def cursor(self, cursor_factory: Any = None) -> _FakeCursor:
        return self.cursor_obj

    def close(self) -> None:
        pass


def _install_fake_psycopg2(
    monkeypatch,
    rows_or_error: list[dict[str, Any]] | Exception,
    *,
    connect_error: Exception | None = None,
) -> None:
    """Registers a stub ``psycopg2``/``psycopg2.extras`` pair in sys.modules."""
    fake_extras = types.ModuleType("psycopg2.extras")
    fake_extras.RealDictCursor = object
    fake_pg = types.ModuleType("psycopg2")
    fake_pg.extras = fake_extras  # type: ignore[attr-defined]

    if connect_error is not None:

        def _connect(**kwargs: Any) -> Any:
            raise connect_error
    else:

        def _connect(**kwargs: Any) -> Any:
            return _FakeConnection(rows_or_error)

    fake_pg.connect = _connect  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "psycopg2", fake_pg)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake_extras)


def test_successful_query_reports_postgresql_origin(monkeypatch) -> None:
    monkeypatch.setenv("PGPASSWORD", "secret")
    _install_fake_psycopg2(monkeypatch, [dict(ORDER_ROW)])

    result = OltpSourceAdapter().extract_table("orders", limit=10)

    assert isinstance(result, ExtractionResult)
    assert result.origin == "postgresql"
    assert result.records == [dict(ORDER_ROW)]


def test_empty_successful_query_reports_postgresql_origin(monkeypatch) -> None:
    """A real but empty table is authoritative: postgresql origin, zero rows."""
    monkeypatch.setenv("PGPASSWORD", "secret")
    _install_fake_psycopg2(monkeypatch, [])

    result = OltpSourceAdapter().extract_table("orders", limit=10)

    assert result.origin == "postgresql"
    assert result.records == []


def test_connection_failure_falls_back_with_synthetic_origin(monkeypatch) -> None:
    monkeypatch.setenv("PGPASSWORD", "secret")
    _install_fake_psycopg2(monkeypatch, [], connect_error=OSError("connection refused"))

    result = OltpSourceAdapter(seed=42).extract_table("orders", limit=5)

    assert result.origin == "synthetic"
    assert len(result.records) == 5


def test_unconfigured_source_falls_back_with_synthetic_origin(monkeypatch) -> None:
    monkeypatch.delenv("PGPASSWORD", raising=False)

    result = OltpSourceAdapter(seed=42).extract_table("orders", limit=5)

    assert result.origin == "synthetic"
    assert len(result.records) == 5


def test_programming_error_propagates_instead_of_synthetic_fallback(monkeypatch) -> None:
    """Narrow except: a TypeError during row mapping must NOT be swallowed."""
    monkeypatch.setenv("PGPASSWORD", "secret")
    _install_fake_psycopg2(monkeypatch, TypeError("boom"))

    with pytest.raises(TypeError):
        OltpSourceAdapter().extract_table("orders", limit=5)


# ---------------------------------------------------------------------------
# IngestBronzeUseCase: provenance stamping (_source + lineage)
# ---------------------------------------------------------------------------


class _StubSource:
    """OltpSourcePort stub returning a canned ExtractionResult."""

    def __init__(self, records: list[dict[str, Any]], origin: Literal["postgresql", "synthetic"]) -> None:
        self._result = ExtractionResult(records=[dict(r) for r in records], origin=origin)

    def extract_table(self, table_name: str, limit: int | None = None) -> ExtractionResult:
        return self._result


def _ingest(tmp_path, records: list[dict[str, Any]], origin: Literal["postgresql", "synthetic"]):
    storage = LakehouseStorageAdapter(db_path=str(tmp_path / "lakehouse.db"))
    lineage = StubLineage()
    use_case = IngestBronzeUseCase(
        source=_StubSource(records, origin),
        storage=storage,
        lineage=lineage,
    )
    result = use_case.execute("orders")
    return storage, lineage, result


def test_synthetic_rows_stamp_synthetic_provenance(tmp_path) -> None:
    storage, lineage, result = _ingest(tmp_path, [ORDER_ROW], "synthetic")

    assert result["status"] == "SUCCESS"
    rows = storage.read_table("bronze.sales.orders")
    assert rows[0]["_source"] == "synthetic.local.orders"
    assert lineage.events[0]["inputs"] == ["synthetic.local.orders"]
    assert lineage.events[0]["outputs"] == ["bronze.sales.orders"]


def test_postgresql_rows_stamp_postgresql_provenance(tmp_path) -> None:
    storage, lineage, result = _ingest(tmp_path, [ORDER_ROW], "postgresql")

    assert result["status"] == "SUCCESS"
    rows = storage.read_table("bronze.sales.orders")
    assert rows[0]["_source"] == "postgresql.public.orders"
    assert lineage.events[0]["inputs"] == ["postgresql.public.orders"]


def test_empty_synthetic_source_reports_empty_with_honest_lineage(tmp_path) -> None:
    storage, lineage, result = _ingest(tmp_path, [], "synthetic")

    assert result["status"] == "EMPTY"
    assert storage.read_table("bronze.sales.orders") == []
    assert lineage.events[0]["inputs"] == ["synthetic.local.orders"]


def test_provenance_label_maps_origin_verbatim() -> None:
    synthetic = ExtractionResult(records=[], origin="synthetic")
    live = ExtractionResult(records=[], origin="postgresql")
    assert provenance_label(synthetic, "customers") == "synthetic.local.customers"
    assert provenance_label(live, "customers") == "postgresql.public.customers"
