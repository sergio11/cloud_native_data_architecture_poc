"""
Modern Data Architecture POC - Integration Test Fixtures (hermetic world).

Builds a fully isolated hexagonal world under ``tmp_path``: lakehouse SQLite
database, persistent event log and lineage audit JSONL all live inside the
test's temporary directory, so nothing is ever written to the repository's
``data/`` folder. Adapters and use cases are constructed DIRECTLY (mirroring
``src/container.py`` wiring) - ``src.container`` is never imported here simply
to keep this fixture fully self-contained and hermetic.

Network is disabled at the seams:
  * ``MarquezLineageAdapter`` points at an unroutable address AND its HTTP
    POST is monkeypatched to raise instantly, so the audit-trail path is still
    exercised on every emit without paying the 2s connection timeout.
  * ``TRINO_HOST``/``PGPASSWORD`` are stripped from the environment so the
    serving and source adapters can never attempt a connection.

The OLTP source is an inline deterministic stub (fixed rows, no random) to
keep every acceptance run reproducible.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.adapters.catalog.fs_domain_catalog import FsDomainCatalogAdapter
from src.adapters.lineage.marquez_lineage import MarquezLineageAdapter
from src.adapters.quality.expectation_engine import ExpectationEngineAdapter
from src.adapters.serving.sql_serving import SqlServingAdapter
from src.adapters.storage.lakehouse_storage import LakehouseStorageAdapter
from src.adapters.streaming.event_stream import EventStreamAdapter
from src.domain.entities import ExtractionResult
from src.use_cases.aggregate_gold import AggregateGoldUseCase
from src.use_cases.cleanse_silver import CleanseSilverUseCase
from src.use_cases.ingest_bronze import IngestBronzeUseCase
from src.use_cases.process_stream import ProcessStreamUseCase
from src.use_cases.query_serving import QueryServingUseCase
from src.use_cases.run_quality_gate import RunQualityGateUseCase
from src.use_cases.validate_contract import ValidateContractUseCase

REPO_ROOT = Path(__file__).resolve().parents[2]
QUALITY_DIR = REPO_ROOT / "quality" / "expectations"

ORDERS_SUITE = str(QUALITY_DIR / "silver_orders_suite.json")
CUSTOMERS_SUITE = str(QUALITY_DIR / "silver_customers_suite.json")
GOLD_DAILY_SUITE = str(QUALITY_DIR / "gold_daily_metrics_suite.json")
GOLD_CUSTOMERS_SUITE = str(QUALITY_DIR / "gold_customers_suite.json")


def _demo_orders() -> list[dict[str, Any]]:
    """12 deterministic orders (>= the suites' min row count of 10).

    Two business dates with every ``status`` value covered on both days, so
    Gold ``(order_date, status)`` keys are predictable for RF-11 assertions.
    """
    statuses = ["confirmed", "pending", "shipped", "cancelled"]
    rows: list[dict[str, Any]] = []
    for index in range(1, 13):
        date = "2026-09-25" if index <= 6 else "2026-09-26"
        rows.append(
            {
                "id": index,
                "customer_id": 100 + ((index - 1) % 4) + 1,
                "total_amount": round(50.0 + 25.5 * index, 2),
                "status": statuses[(index - 1) % 4],
                "order_date": date,
                "created_at": f"{date}T10:00:00+00:00",
            }
        )
    return rows


def _demo_customers() -> list[dict[str, Any]]:
    """12 deterministic customers with raw PII for the GDPR Gold masking path."""
    first_names = ["Alice", "Bob", "Carol", "David", "Emma", "Frank"]
    last_names = ["Smith", "Jones", "White", "Brown", "Watson", "Miller"]
    segments = ["premium", "standard", "basic"]
    rows: list[dict[str, Any]] = []
    for position, customer_id in enumerate(range(101, 113)):
        first = first_names[position % len(first_names)]
        last = last_names[position % len(last_names)]
        rows.append(
            {
                "id": customer_id,
                "name": f"{first} {last}",
                "email": f"{first.lower()}.{last.lower()}{customer_id}@example.com",
                "segment": segments[position % len(segments)],
                "registration_date": "2026-01-15",
                "created_at": "2026-01-15T09:00:00+00:00",
            }
        )
    return rows


DEMO_ORDERS = _demo_orders()
DEMO_CUSTOMERS = _demo_customers()


class DeterministicOltpSource:
    """OltpSourcePort stub serving fixed snapshots (no random, no network).

    Repeated extractions return byte-identical rows, which is what makes the
    Bronze content-hash idempotency assertions (RNF-05 / RF-01) meaningful.
    """

    def __init__(self, tables: dict[str, list[dict[str, Any]]]) -> None:
        self._tables = tables
        self.extract_calls: list[str] = []

    def extract_table(self, table_name: str, limit: int | None = None) -> ExtractionResult:
        self.extract_calls.append(table_name)
        rows = self._tables.get(table_name, [])
        if limit is not None:
            rows = rows[: int(limit)]
        # Fixed fixture rows, not a live database: the honest origin is
        # "synthetic" so Bronze lineage/audit provenance never claims PostgreSQL.
        return ExtractionResult(records=[dict(row) for row in rows], origin="synthetic")


@pytest.fixture
def world(tmp_path: Path, _hermetic_environment: None) -> SimpleNamespace:
    """Isolated hexagonal world wired exactly like ``src.container.Container``
    but bound to tmp paths, a deterministic source and a stubbed HTTP layer."""
    lakehouse_db = tmp_path / "lakehouse.db"
    events_db = tmp_path / "events.db"
    audit_file = tmp_path / "lineage.jsonl"

    storage = LakehouseStorageAdapter(db_path=str(lakehouse_db))
    broker = EventStreamAdapter(db_path=str(events_db))
    lineage = MarquezLineageAdapter(
        marquez_url="http://127.0.0.1:1",
        audit_file_path=str(audit_file),
    )
    quality_engine = ExpectationEngineAdapter()
    serving_sql = SqlServingAdapter(db_path=str(lakehouse_db))
    domain_catalog = FsDomainCatalogAdapter(repo_root=str(REPO_ROOT))
    source = DeterministicOltpSource({"orders": DEMO_ORDERS, "customers": DEMO_CUSTOMERS})

    def _lineage_events() -> list[dict[str, Any]]:
        """Parses the local OpenLineage audit trail written so far."""
        if not audit_file.exists():
            return []
        events: list[dict[str, Any]] = []
        for line in audit_file.read_text(encoding="utf-8").splitlines():
            if line.strip():
                events.append(json.loads(line))
        return events

    return SimpleNamespace(
        tmp_path=tmp_path,
        db_path=str(lakehouse_db),
        audit_file=audit_file,
        storage=storage,
        broker=broker,
        lineage=lineage,
        quality_engine=quality_engine,
        serving_sql=serving_sql,
        source=source,
        orders_suite=ORDERS_SUITE,
        customers_suite=CUSTOMERS_SUITE,
        gold_daily_metrics_suite=GOLD_DAILY_SUITE,
        gold_customers_suite=GOLD_CUSTOMERS_SUITE,
        demo_orders=DEMO_ORDERS,
        demo_customers=DEMO_CUSTOMERS,
        lineage_events=_lineage_events,
        ingest_bronze=IngestBronzeUseCase(source=source, storage=storage, lineage=lineage),
        cleanse_silver=CleanseSilverUseCase(
            storage_reader=storage,
            storage_writer=storage,
            quality_engine=quality_engine,
            lineage=lineage,
            catalog=domain_catalog,
        ),
        aggregate_gold=AggregateGoldUseCase(
            storage_reader=storage,
            storage_writer=storage,
            lineage=lineage,
            quality_engine=quality_engine,
            catalog=domain_catalog,
        ),
        process_stream=ProcessStreamUseCase(broker=broker, storage=storage, lineage=lineage),
        query_serving=QueryServingUseCase(query_executor=serving_sql),
        run_quality_gate=RunQualityGateUseCase(storage=storage, quality_engine=quality_engine, catalog=domain_catalog),
        validate_contract=ValidateContractUseCase(catalog=domain_catalog),
    )
