"""
Modern Data Architecture POC - Inbound (Driving) Ports.
Defines interfaces that application entrypoints (CLI, Dagster, APIs) call to execute use cases.
Follows Interface Segregation Principle (ISP) and Dependency Inversion Principle (DIP).
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
    """One row of the RF-11 unified batch + stream revenue query."""

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

        ``suite_path`` is REQUIRED: the driver resolves the quality
        expectations suite path per entity and passes it explicitly (the core
        holds no filesystem conventions). ``quality_threshold`` is a fallback
        used only when the entity contract declares no quality thresholds;
        otherwise the contract thresholds govern per dimension.
        """
        ...


class AggregateGoldPort(Protocol):
    """Driving port to trigger Gold tier business modeling and PII masking.

    Gold is a full recomputation by design (Lambda batch layer): every execution
    rebuilds the target aggregates from Silver, so no incremental replay flag exists.
    """

    def execute(
        self,
        target: str = "all",
        *,
        daily_metrics_suite: str,
        customers_suite: str,
        quality_threshold: float | None = None,
    ) -> AggregateGoldResult:
        """Materializes aggregated Gold datasets by recomputing them from Silver.

        Both expectation suite paths are REQUIRED keyword arguments resolved by
        the driver (the core holds no filesystem conventions).
        ``quality_threshold`` is a fallback used only when the governing
        contract declares no quality thresholds.
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
        threshold: float = DEFAULT_QUALITY_THRESHOLD,
        records: list[dict[str, Any]] | None = None,
    ) -> QualityGateEvaluation:
        """Evaluates assertions against table and returns quality verdict.

        When ``records`` is provided they are evaluated as-is (file-input
        scenarios); otherwise the use case loads the table from storage.

        Threshold precedence mirrors the Silver/Gold promotion gate: the
        thresholds declared by the contract governing the table's entity take
        precedence per dimension, and ``threshold`` applies only when no such
        contract declares any (fallback).

        Fail-closed (RF-06): a failing verdict raises
        ``QualityGateBreachError`` carrying the evaluation instead of
        returning it, so programmatic callers cannot ignore a breach; only
        a passing evaluation is returned.
        """
        ...


class ValidateContractPort(Protocol):
    """Driving port to validate DataProduct manifests and DataContracts."""

    def execute(self) -> ValidateContractResult:
        """Validates contract structure and evolution rules across domains.

        The catalog owns the repository root, so no path argument is needed.
        """
        ...


class QueryServingPort(Protocol):
    """Driving port for the Serving layer executing unified queries (RF-11)."""

    def execute(
        self,
        order_date_gte: str | None = None,
        limit: int = 20,
    ) -> list[UnifiedRevenueRow]:
        """Executes a single unified revenue query across batch Gold and stream Speed tiers.

        RF-11: one query, no double counting - for the same (order_date, status)
        key the exact batch value wins; stream rows only fill keys absent from
        batch. Rows are filtered by ``order_date_gte`` when given and capped at
        ``limit``.
        """
        ...
