"""Modern Data Architecture POC - Inbound (driving) ports.

Interfaces called by entrypoints to execute use cases.
"""

from __future__ import annotations

from typing import Any, Literal, Protocol, Required, TypedDict

from src.domain.entities import QualityGateEvaluation
from src.domain.naming import stream_table
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD


class IngestBronzeResult(TypedDict):
    """Result contract of the Bronze ingestion use case (both outcomes)."""

    entity: str
    target_table: str
    batch_id: str
    rows_ingested: int
    idempotent_hit: bool
    status: Literal["SUCCESS", "EMPTY"]


class CleanseSilverResult(TypedDict):
    """Result contract of the Silver cleansing use case (both outcomes)."""

    entity: str
    target_table: str
    rows_promoted: int
    quality_pass_rate: float
    status: Literal["SUCCESS", "EMPTY"]


class AggregateGoldResult(TypedDict, total=False):
    """Result contract of the Gold aggregation use case.

    ``status`` is always present; each published product adds its row-count
    key (``daily_metrics_rows`` / ``customers_masked_rows``).
    """

    status: Required[Literal["SUCCESS"]]
    daily_metrics_rows: int
    customers_masked_rows: int


class ProcessStreamResult(TypedDict):
    """Result contract of the streaming speed-layer use case (both outcomes)."""

    topic: str
    target_table: str
    events_consumed: int
    windows_generated: int
    windows_written: int
    mode: Literal["replay_full", "append"]
    status: Literal["SUCCESS", "NO_EVENTS"]


class ValidateContractResult(TypedDict):
    """Result contract of the governance validation use case."""

    products_evaluated: int
    contracts_evaluated: int
    passed: bool
    errors: list[dict[str, Any]]
    details: list[dict[str, Any]]


class UnifiedRevenueRow(TypedDict):
    """One row of the unified batch + stream revenue query."""

    order_date: str | None
    status: str | None
    total_orders: int
    total_revenue: float
    avg_order_value: float
    source_tier: str | None


class IngestBronzePort(Protocol):
    """Driving port to trigger Bronze tier extraction and ingestion."""

    def execute(self, entity: str) -> IngestBronzeResult:
        """Ingests raw records from OLTP to Bronze tier."""
        ...


class CleanseSilverPort(Protocol):
    """Driving port to trigger Silver tier cleansing, typing, and quality gate."""

    def execute(
        self,
        entity: str,
        suite_path: str,
        quality_threshold: float | None = None,
    ) -> CleanseSilverResult:
        """Cleanses Bronze records and promotes valid records to Silver.

        ``suite_path`` is required. ``quality_threshold`` applies only when the
        entity contract declares no thresholds.
        """
        ...


class AggregateGoldPort(Protocol):
    """Driving port to trigger Gold modeling and PII masking.

    Gold is fully recomputed from Silver on every run (Lambda batch layer).
    """

    def execute(
        self,
        target: str = "all",
        *,
        daily_metrics_suite: str,
        customers_suite: str,
        quality_threshold: float | None = None,
    ) -> AggregateGoldResult:
        """Materializes Gold datasets by recomputing them from Silver.

        Suite paths are required; ``quality_threshold`` is only a fallback.
        """
        ...


class ProcessStreamPort(Protocol):
    """Driving port for streaming speed layer processing (Kappa architecture)."""

    def execute(
        self,
        topic: str = "sales.orders.events",
        target_table: str = stream_table(),
        replay: bool = False,
    ) -> ProcessStreamResult:
        """Consumes stream events and updates speed buffer tables.

        ``replay=True`` rewinds to the earliest offset; otherwise only new
        events are processed.
        """
        ...


class RunQualityGatePort(Protocol):
    """Driving port to evaluate quality expectation suites (fail-closed)."""

    def execute(
        self,
        table_name: str,
        suite_path: str,
        threshold: float = DEFAULT_QUALITY_THRESHOLD,
        records: list[dict[str, Any]] | None = None,
    ) -> QualityGateEvaluation:
        """Evaluates assertions against a table and returns the verdict.

        ``records`` override storage when provided. Contract thresholds take
        precedence; a failing verdict raises ``QualityGateBreachError``.
        """
        ...


class ValidateContractPort(Protocol):
    """Driving port to validate DataProduct manifests and DataContracts."""

    def execute(self) -> ValidateContractResult:
        """Validates contract structure and evolution rules across domains."""
        ...


class QueryServingPort(Protocol):
    """Driving port for the Serving layer executing unified queries."""

    def execute(
        self,
        order_date_gte: str | None = None,
        limit: int = 20,
    ) -> list[UnifiedRevenueRow]:
        """Executes one unified revenue query across Gold batch and stream Speed.

        Batch wins on ``(order_date, status)``; stream only fills absent keys.
        """
        ...
