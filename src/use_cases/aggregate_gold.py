"""
Modern Data Architecture POC - Gold Layer Business Modeling & PII Masking Use Case.
Executes business metric aggregations and enforces GDPR pseudonymization policies.

Quality gates (RF-06) mirror the Silver pattern (CleanseSilverUseCase): each Gold
target is evaluated via the shared domain gate BEFORE publication. On breach a FAIL
lineage event is emitted and QualityGateBreachError is raised, blocking publication
of that target (fail-closed per target - the other target proceeds independently).
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.aggregations import BusinessAggregator
from src.domain.exceptions import QualityGateBreachError
from src.domain.naming import GOLD_PRODUCTS, gold_table, silver_table
from src.domain.privacy import DEFAULT_PII_HASH_SALT, PrivacyPolicy
from src.ports.inbound import AggregateGoldPort, AggregateGoldResult
from src.ports.outbound import (
    ContractCatalogPort,
    LakehouseTableReader,
    LakehouseTableWriter,
    LineagePublisherPort,
    QualityEnginePort,
)
from src.use_cases.gate_support import enforce_quality_gate

logger = logging.getLogger("use_case.aggregate_gold")

_GOLD_DAILY_METRICS_TABLE = gold_table("daily_metrics")
_GOLD_CUSTOMERS_TABLE = gold_table("customers")
_SILVER_ORDERS_TABLE = silver_table("orders")
_SILVER_CUSTOMERS_TABLE = silver_table("customers")


class AggregateGoldUseCase(AggregateGoldPort):
    """Orchestrates Silver-to-Gold dimensional modeling and GDPR PII masking.

    Gold is a full recomputation from Silver on every run (Lambda batch layer),
    which IS the replay/RF-08 behaviour - there is no incremental replay flag.
    """

    def __init__(
        self,
        storage_reader: LakehouseTableReader,
        storage_writer: LakehouseTableWriter,
        lineage: LineagePublisherPort,
        quality_engine: QualityEnginePort,
        catalog: ContractCatalogPort,
        privacy_salt: str = DEFAULT_PII_HASH_SALT,
    ) -> None:
        self._reader = storage_reader
        self._writer = storage_writer
        self._lineage = lineage
        self._quality = quality_engine
        self._catalog = catalog
        self._privacy_salt = privacy_salt

    def execute(
        self,
        target: str = "all",
        *,
        daily_metrics_suite: str,
        customers_suite: str,
        quality_threshold: float | None = None,
    ) -> AggregateGoldResult:
        logger.info("Executing Gold Layer aggregation (target='%s')", target)
        publishers: dict[str, tuple[Any, str]] = {
            "daily_metrics": (self._publish_daily_metrics, daily_metrics_suite),
            "customers": (self._publish_customers, customers_suite),
        }
        valid_targets: tuple[str, ...] = ("all",)
        for product in GOLD_PRODUCTS:
            valid_targets += (product.name, gold_table(product.name))
        if target not in valid_targets:
            raise ValueError(
                f"Unknown Gold target {target!r}. Expected one of {sorted(valid_targets)}; "
                "refusing to report SUCCESS without materializing anything."
            )
        results: AggregateGoldResult = {"status": "SUCCESS"}
        breach: QualityGateBreachError | None = None

        # Data-driven fan-out over GOLD_PRODUCTS (open for extension: a new
        # product adds a descriptor plus its publisher, the loop is untouched).
        # Fail-closed per target: a breach blocks only its target - the other
        # proceeds independently.
        for product in GOLD_PRODUCTS:
            if target not in ("all", product.name, gold_table(product.name)):
                continue
            publish, suite_path = publishers[product.name]
            try:
                results[product.result_key] = publish(quality_threshold, suite_path)  # type: ignore[literal-required]
            except QualityGateBreachError as exc:
                if breach is None:
                    breach = exc
                else:
                    # Both targets breached concurrently: the first recorded
                    # breach is the one re-raised (fail-closed either way), but
                    # discarding this one without a trace would hide a real breach.
                    logger.warning(
                        "Second concurrent QualityGateBreachError while one is already "
                        "recorded; keeping the first breach (both block publication): %s",
                        exc,
                    )

        if breach is not None:
            raise breach

        return results

    def _publish_daily_metrics(self, quality_threshold: float | None, suite_path: str) -> int:
        orders_silver = self._reader.read_table(_SILVER_ORDERS_TABLE)
        logger.info("Aggregating %d silver orders for daily metrics rollup...", len(orders_silver))
        metrics = BusinessAggregator.aggregate_daily_sales(orders_silver)
        metrics_dicts = [m.to_dict() for m in metrics]

        target_table = _GOLD_DAILY_METRICS_TABLE
        enforce_quality_gate(
            quality_engine=self._quality,
            lineage=self._lineage,
            catalog=self._catalog,
            records=metrics_dicts,
            suite_path=suite_path,
            table_name=target_table,
            job_name="gold_daily_sales_rollup",
            inputs=[_SILVER_ORDERS_TABLE],
            entity="orders",
            quality_threshold=quality_threshold,
        )

        written = self._writer.write_table(target_table, metrics_dicts, mode="overwrite")
        logger.info("Published %d daily aggregation rows to '%s'", written, target_table)

        self._lineage.emit_job_event(
            job_name="gold_daily_sales_rollup",
            inputs=[_SILVER_ORDERS_TABLE],
            outputs=[target_table],
            status="COMPLETE",
        )
        return written

    def _publish_customers(self, quality_threshold: float | None, suite_path: str) -> int:
        customers_silver = self._reader.read_table(_SILVER_CUSTOMERS_TABLE)
        logger.info("Applying GDPR Art.25 pseudonymization on %d customers...", len(customers_silver))

        masked_customers: list[dict[str, Any]] = [
            PrivacyPolicy.apply_customer_masking(c, salt=self._privacy_salt) for c in customers_silver
        ]

        target_cust_table = _GOLD_CUSTOMERS_TABLE
        enforce_quality_gate(
            quality_engine=self._quality,
            lineage=self._lineage,
            catalog=self._catalog,
            records=masked_customers,
            suite_path=suite_path,
            table_name=target_cust_table,
            job_name="gold_customer_pseudonymization",
            inputs=[_SILVER_CUSTOMERS_TABLE],
            entity="customers",
            quality_threshold=quality_threshold,
        )

        written_cust = self._writer.write_table(target_cust_table, masked_customers, mode="overwrite")
        logger.info("Published %d pseudonymized records to '%s'", written_cust, target_cust_table)

        self._lineage.emit_job_event(
            job_name="gold_customer_pseudonymization",
            inputs=[_SILVER_CUSTOMERS_TABLE],
            outputs=[target_cust_table],
            status="COMPLETE",
        )
        return written_cust
