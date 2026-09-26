"""
Modern Data Architecture POC - Inbound (Driving) Ports.
Defines interfaces that application entrypoints (CLI, Dagster, APIs) call to execute use cases.
Follows Interface Segregation Principle (ISP) and Dependency Inversion Principle (DIP).
"""

from __future__ import annotations

from typing import Any, Protocol

from src.domain.entities import QualityGateEvaluation


class IngestBronzePort(Protocol):
    """Driving port to trigger Bronze tier extraction and ingestion."""

    def execute(self, entity: str) -> dict[str, Any]:
        """Ingests raw records from OLTP to Bronze tier."""
        ...


class CleanseSilverPort(Protocol):
    """Driving port to trigger Silver tier cleansing, typing, and quality gate."""

    def execute(self, entity: str) -> dict[str, Any]:
        """Cleanses Bronze records and promotes valid records to Silver."""
        ...


class AggregateGoldPort(Protocol):
    """Driving port to trigger Gold tier business modeling and PII masking.

    Gold is a full recomputation by design (Lambda batch layer): every execution
    rebuilds the target aggregates from Silver, so no incremental replay flag exists.
    """

    def execute(self, target: str = "all") -> dict[str, Any]:
        """Materializes aggregated Gold datasets by recomputing them from Silver."""
        ...


class ProcessStreamPort(Protocol):
    """Driving port for streaming speed layer processing (Kappa architecture)."""

    def execute(
        self,
        topic: str = "sales.orders.events",
        target_table: str = "stream.sales.orders_windowed",
        replay: bool = False,
    ) -> dict[str, Any]:
        """Consumes stream events and updates speed buffer tables.

        ``replay=True`` rewinds to the beginning of the topic log (RF-10 full
        replay); otherwise only not-yet-consumed events are processed.
        """
        ...


class RunQualityGatePort(Protocol):
    """Driving port to evaluate quality expectation suites (fail-closed, RF-06)."""

    def execute(
        self,
        table_name: str,
        suite_path: str,
        threshold: float = 0.95,
        records: list[dict[str, Any]] | None = None,
    ) -> QualityGateEvaluation:
        """Evaluates assertions against table and returns quality verdict.

        When ``records`` is provided they are evaluated as-is (file-input
        scenarios); otherwise the use case loads the table from storage.
        """
        ...


class ValidateContractPort(Protocol):
    """Driving port to validate DataProduct manifests and DataContracts."""

    def execute(self, repo_root: str) -> dict[str, Any]:
        """Validates contract structure and evolution rules across domains."""
        ...


class QueryServingPort(Protocol):
    """Driving port for the Serving layer executing unified queries (RF-11)."""

    def execute_unified_revenue_query(
        self,
        order_date_gte: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Executes a single unified revenue query across batch Gold and stream Speed tiers.

        RF-11: one query, no double counting - for the same (order_date, status)
        key the exact batch value wins; stream rows only fill keys absent from
        batch. Rows are filtered by ``order_date_gte`` when given and capped at
        ``limit``.
        """
        ...
