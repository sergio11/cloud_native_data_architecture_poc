"""
Modern Data Architecture POC - Outbound (Driven) Ports.
Defines interfaces that the application core requires from external infrastructure
(Databases, Lakehouse storage, event broker, lineage publisher, Quality Engine,
SQL serving engine, Data Product catalog).
Follows Liskov Substitution Principle (LSP) and Dependency Inversion Principle (DIP).
"""

from __future__ import annotations

from typing import Any, Protocol

from src.domain.entities import ExtractionResult, QualityGateEvaluation
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD


class OltpSourcePort(Protocol):
    """Port for extracting operational relational tables."""

    def extract_table(self, table_name: str, limit: int | None = None) -> ExtractionResult:
        """Reads snapshot records from source OLTP database.

        The returned ``origin`` must describe where the records actually came
        from: a successful query (including one returning zero rows) reports
        ``"postgresql"``, while rows fabricated by a fallback generator report
        ``"synthetic"``. Callers stamp provenance/lineage from ``origin`` and
        must never assume a live database.
        """
        ...


class LakehouseTableWriter(Protocol):
    """Narrow port: writing lakehouse tables (Bronze landing, Silver/Gold promotion)."""

    def write_table(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        mode: str = "append",
    ) -> int:
        """Writes records to the specified lakehouse table, returning count of rows written.

        Contract (RF-01): writes are append-only with content-hash idempotency.
        Re-executing identical content must not duplicate rows; ``mode="overwrite"``
        marks prior rows superseded rather than physically deleting them, so the
        prior snapshot stays readable via time travel.
        """
        ...


class LakehouseTableReader(Protocol):
    """Narrow port: reading lakehouse tables, current state or time travel (RF-07)."""

    def read_table(
        self,
        table_name: str,
        limit: int | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        """Reads records from the specified lakehouse table.

        ``version`` reads the table state AT that snapshot version (time travel,
        RF-07); ``version=None`` reads the current state. ``limit=None`` reads all rows.

        Ordering guarantee: rows come back in INSERTION ORDER (oldest write
        first, i.e. ascending rowid/monotonic sequence), independent of the
        ``version`` filter, and ``limit`` is applied AFTER that ordering. Callers
        whose logic depends on "later row wins" (e.g. Silver latest-snapshot
        deduplication) may therefore rely on this order without re-sorting.
        """
        ...


class LakehouseTableInspector(Protocol):
    """Narrow port: lakehouse metadata for observability and tests (RF-07)."""

    def table_exists(self, table_name: str) -> bool:
        """Checks if the table exists."""
        ...

    def get_table_schema(self, table_name: str) -> list[str]:
        """Returns column names for the table."""
        ...

    def list_versions(self, table_name: str) -> list[dict[str, Any]]:
        """Lists every snapshot version available for time travel (RF-07).

        Returns one dict per snapshot with keys ``snapshot_version`` (int),
        ``row_count`` (int) and ``created_at`` (str), ordered oldest to newest.
        """
        ...


class LakehouseStoragePort(LakehouseTableReader, LakehouseTableWriter, LakehouseTableInspector, Protocol):
    """Composite port: the full lakehouse capability (Bronze, Silver, Gold, Speed).

    Isolates all persistence concerns (RF-01 idempotent landing,
    RF-07 snapshot time travel) behind a storage-agnostic interface so the
    application core never depends on a concrete lakehouse engine. Clients
    depend on the narrow ports above; only the composite is implemented.
    """


class EventBrokerPort(Protocol):
    """Port for producing and consuming real-time events (Kappa log, RF-10).

    Isolates the append-only event log so the application core can consume,
    replay and paginate streams without depending on a concrete broker.
    """

    def publish_event(self, topic: str, key: str, payload: dict[str, Any]) -> bool:
        """Publishes an event message to a topic."""
        ...

    def consume_events(
        self,
        topic: str,
        max_messages: int | None = None,
        replay: bool = False,
        consumer_group: str = "default",
    ) -> list[dict[str, Any]]:
        """Consumes event messages from a topic in ascending offset order.

        When ``replay=False``: returns only events NOT yet consumed by
        ``consumer_group`` for that topic, in ascending offset order, and
        atomically advances that group's committed cursor past the returned
        batch. ``max_messages=None`` means no limit.

        When ``replay=True``: returns ALL events of the topic from the beginning
        in ascending order (full Kappa log replay, RF-10), does NOT advance any
        consumer group cursor, and ``max_messages`` (if given) caps the batch for
        pagination.

        The adapter must NEVER fabricate events when the log is empty.
        """
        ...


class LineagePublisherPort(Protocol):
    """Port for emitting OpenLineage run events to the configured metadata backend."""

    def emit_job_event(
        self,
        job_name: str,
        inputs: list[str],
        outputs: list[str],
        status: str = "COMPLETE",
    ) -> bool:
        """Publishes a standardized OpenLineage run event."""
        ...


class QualityEnginePort(Protocol):
    """Port for evaluating Great Expectations validation suites against data.

    Encapsulates the quality engine so the application core enforces fail-closed
    promotion rules (RF-06) without importing any validation library directly.
    """

    def evaluate_suite(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        threshold: float = DEFAULT_QUALITY_THRESHOLD,
        table_name: str = "",
    ) -> QualityGateEvaluation:
        """Executes quality assertions on records against the suite specification.

        ``table_name`` is reported in the returned evaluation so the verdict names
        the real table under test.

        Fail-closed semantics (RF-06): an unsupported expectation type, zero
        assertions, or an empty record set must NOT silently pass - they must be
        reported as a failing (or explicitly erroring) evaluation. A missing suite
        file raises ``src.domain.exceptions.SuiteNotFoundError`` instead of
        yielding an empty or passing result.
        """
        ...


class QueryExecutorPort(Protocol):
    """Port for executing ANSI SQL against the serving/lakehouse surface.

    Keeps SQL execution out of the application core while still exposing a
    single query abstraction (RF-11).
    """

    def execute_query(self, sql: str) -> list[dict[str, Any]]:
        """Executes an ANSI SQL query and returns rows as dicts.

        Behaviour contract: a query that cannot be executed (unknown backend,
        invalid SQL, transport failure) surfaces as a raised exception - the
        adapter must never return an empty result set as a stand-in for a
        failure, so callers can distinguish "no matching rows" from "query
        did not run".
        """
        ...


class ContractCatalogPort(Protocol):
    """Narrow port: reading data contracts (quality-threshold governance)."""

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (source_file_path, parsed contract.yaml) pairs, sorted by path."""
        ...


class DomainCatalogPort(ContractCatalogPort, Protocol):
    """Composite port for discovering and reading data-product manifests in the repository.

    Lets the application core validate data-product manifests without filesystem
    knowledge (RF-13/RF-15), isolating glob/YAML/JSON I/O in an adapter.
    Governance validation needs the full manifest surface; quality gating only
    needs ``ContractCatalogPort``.
    """

    def list_products(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (source_file_path, parsed product.yaml) pairs, sorted by path."""
        ...

    def list_schemas(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (source_file_path, parsed schema.json) pairs, sorted by path."""
        ...

    def load_governance_schema(self, schema_filename: str) -> dict[str, Any] | None:
        """Loads a governance JSON-schema document by filename.

        Reads ``<repo_root>/governance/schemas/<schema_filename>``. Returns None
        when the file is missing or invalid; the validation use case then runs
        structural checks only.
        """
        ...
