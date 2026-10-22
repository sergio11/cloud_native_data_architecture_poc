"""Modern Data Architecture POC - Outbound (driven) ports.

Interfaces the application core requires from infrastructure.
"""

from __future__ import annotations

from typing import Any, Protocol

from src.domain.entities import ExtractionResult, QualityGateEvaluation
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD


class OltpSourcePort(Protocol):
    """Port for extracting operational relational tables."""

    def extract_table(self, table_name: str, limit: int | None = None) -> ExtractionResult:
        """Reads snapshot records from the source OLTP database.

        ``origin`` reports ``"postgresql"`` or ``"synthetic"`` truthfully.
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
        """Writes records to a lakehouse table, returning the row count.

        Appends are content-hash idempotent; overwrite marks prior rows
        superseded instead of deleting them.
        """
        ...


class LakehouseTableReader(Protocol):
    """Narrow port: reading lakehouse tables, current state or time travel."""

    def read_table(
        self,
        table_name: str,
        limit: int | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        """Reads records from a lakehouse table.

        ``version`` selects a snapshot; ``limit`` applies after insertion order.
        """
        ...


class LakehouseTableInspector(Protocol):
    """Narrow port: lakehouse metadata for observability and tests."""

    def table_exists(self, table_name: str) -> bool:
        """Checks if the table exists."""
        ...

    def get_table_schema(self, table_name: str) -> list[str]:
        """Returns column names for the table."""
        ...

    def list_versions(self, table_name: str) -> list[dict[str, Any]]:
        """Lists snapshot versions available for time travel, oldest first."""
        ...


class LakehouseStoragePort(LakehouseTableReader, LakehouseTableWriter, LakehouseTableInspector, Protocol):
    """Composite port: the full lakehouse capability (Bronze, Silver, Gold, Speed)."""


class EventBrokerPort(Protocol):
    """Port for producing and consuming real-time events (Kappa log)."""

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

        ``replay=False`` returns only unconsumed events and advances the
        cursor; ``replay=True`` returns the whole log without advancing it.
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
    """Port for evaluating validation suites against data (fail-closed)."""

    def evaluate_suite(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        threshold: float = DEFAULT_QUALITY_THRESHOLD,
        table_name: str = "",
    ) -> QualityGateEvaluation:
        """Executes quality assertions on records against the suite.

        Unsupported expectations, zero assertions or an empty record set fail
        closed; a missing suite raises ``SuiteNotFoundError``.
        """
        ...


class QueryExecutorPort(Protocol):
    """Port for executing ANSI SQL against the serving/lakehouse surface."""

    def execute_query(self, sql: str) -> list[dict[str, Any]]:
        """Executes an ANSI SQL query and returns rows as dicts.

        Failures raise; an empty result is never a stand-in for an error.
        """
        ...


class ContractCatalogPort(Protocol):
    """Narrow port: reading data contracts (quality-threshold governance)."""

    def list_contracts(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (source_file_path, parsed contract.yaml) pairs, sorted by path."""
        ...


class DomainCatalogPort(ContractCatalogPort, Protocol):
    """Composite port for discovering data-product manifests in the repository."""

    def list_products(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (source_file_path, parsed product.yaml) pairs, sorted by path."""
        ...

    def list_schemas(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns (source_file_path, parsed schema.json) pairs, sorted by path."""
        ...

    def load_governance_schema(self, schema_filename: str) -> dict[str, Any] | None:
        """Loads a governance JSON-schema document by filename.

        Returns None when missing or invalid.
        """
        ...
