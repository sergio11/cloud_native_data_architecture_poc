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
from src.domain.naming import gold_table, silver_table
from src.domain.privacy import PrivacyPolicy
from src.domain.quality_gate import DEFAULT_QUALITY_THRESHOLD, evaluate_and_enforce, find_contract_thresholds
from src.ports.inbound import AggregateGoldPort
from src.ports.outbound import DomainCatalogPort, LakehouseStoragePort, LineagePublisherPort, QualityEnginePort

logger = logging.getLogger("use_case.aggregate_gold")

_GOLD_DAILY_METRICS_TABLE = gold_table("daily_metrics")
_GOLD_CUSTOMERS_TABLE = gold_table("customers")
_SILVER_ORDERS_TABLE = silver_table("orders")
_SILVER_CUSTOMERS_TABLE = silver_table("customers")

#: Gold product -> governing contract entity (contracts live under data-products/<entity>/).
GOLD_PRODUCT_CONTRACT_ENTITY: dict[str, str] = {"daily_metrics": "orders", "customers": "customers"}


class AggregateGoldUseCase(AggregateGoldPort):
    """Orchestrates Silver-to-Gold dimensional modeling and GDPR PII masking.

    Gold is a full recomputation from Silver on every run (Lambda batch layer),
    which IS the replay/RF-08 behaviour - there is no incremental replay flag.
    """

    def __init__(
        self,
        storage: LakehouseStoragePort,
        lineage: LineagePublisherPort,
        quality_engine: QualityEnginePort,
        catalog: DomainCatalogPort | None = None,
    ) -> None:
        self._storage = storage
        self._lineage = lineage
        self._quality = quality_engine
        self._catalog = catalog

    def execute(
        self,
        target: str = "all",
        *,
        daily_metrics_suite: str,
        customers_suite: str,
        quality_threshold: float | None = None,
    ) -> dict[str, Any]:
        logger.info("Executing Gold Layer aggregation (target='%s')", target)
        results: dict[str, Any] = {}
        breach: QualityGateBreachError | None = None

        # 1. Aggregate Daily Sales Metrics (fail-closed: a breach blocks only
        #    this target - the customers target below proceeds independently).
        if target in ("daily_metrics", _GOLD_DAILY_METRICS_TABLE, "all"):
            try:
                results["daily_metrics_rows"] = self._publish_daily_metrics(
                    quality_threshold, daily_metrics_suite
                )
            except QualityGateBreachError as exc:
                breach = breach or exc

        # 2. Pseudonymize Customer Dimensions (GDPR PII Masking, gated likewise).
        if target in ("customers", _GOLD_CUSTOMERS_TABLE, "all"):
            try:
                results["customers_masked_rows"] = self._publish_customers(
                    quality_threshold, customers_suite
                )
            except QualityGateBreachError as exc:
                breach = breach or exc

        if breach is not None:
            raise breach

        results["status"] = "SUCCESS"
        return results

    def _resolve_gate(
        self,
        product: str,
        quality_threshold: float | None,
    ) -> tuple[float, dict[str, float] | None]:
        """Resolves the gate policy for a Gold product from its governing contract.

        Contract thresholds (data-products/<entity>/) govern per dimension and
        ignore the global threshold; the explicit threshold (or its 0.95
        default) is only a fallback when the contract is silent.
        """
        entity = GOLD_PRODUCT_CONTRACT_ENTITY[product]
        contract_thresholds = find_contract_thresholds(self._catalog, entity)
        if contract_thresholds is not None and quality_threshold is not None:
            logger.info(
                "contract thresholds take precedence over explicit quality_threshold=%.2f for '%s'",
                quality_threshold, gold_table(product),
            )
        effective = quality_threshold if quality_threshold is not None else DEFAULT_QUALITY_THRESHOLD
        return effective, contract_thresholds

    def _publish_daily_metrics(self, quality_threshold: float | None, suite_path: str) -> int:
        orders_silver = self._storage.read_table(_SILVER_ORDERS_TABLE)
        logger.info("Aggregating %d silver orders for daily metrics rollup...", len(orders_silver))
        metrics = BusinessAggregator.aggregate_daily_sales(orders_silver)
        metrics_dicts = [m.to_dict() for m in metrics]

        target_table = _GOLD_DAILY_METRICS_TABLE
        effective_threshold, contract_thresholds = self._resolve_gate("daily_metrics", quality_threshold)
        evaluate_and_enforce(
            self._quality,
            self._lineage,
            records=metrics_dicts,
            suite_path=suite_path,
            table_name=target_table,
            job_name="gold_daily_sales_rollup",
            inputs=[_SILVER_ORDERS_TABLE],
            threshold=effective_threshold,
            contract_thresholds=contract_thresholds,
        )

        written = self._storage.write_table(target_table, metrics_dicts, mode="overwrite")
        logger.info("Published %d daily aggregation rows to '%s'", written, target_table)

        self._lineage.emit_job_event(
            job_name="gold_daily_sales_rollup",
            inputs=[_SILVER_ORDERS_TABLE],
            outputs=[target_table],
            status="COMPLETE",
        )
        return written

    def _publish_customers(self, quality_threshold: float | None, suite_path: str) -> int:
        customers_silver = self._storage.read_table(_SILVER_CUSTOMERS_TABLE)
        logger.info("Applying GDPR Art.25 pseudonymization on %d customers...", len(customers_silver))

        masked_customers: list[dict[str, Any]] = [
            PrivacyPolicy.apply_customer_masking(c) for c in customers_silver
        ]

        target_cust_table = _GOLD_CUSTOMERS_TABLE
        effective_threshold, contract_thresholds = self._resolve_gate("customers", quality_threshold)
        evaluate_and_enforce(
            self._quality,
            self._lineage,
            records=masked_customers,
            suite_path=suite_path,
            table_name=target_cust_table,
            job_name="gold_customer_pseudonymization",
            inputs=[_SILVER_CUSTOMERS_TABLE],
            threshold=effective_threshold,
            contract_thresholds=contract_thresholds,
        )

        written_cust = self._storage.write_table(target_cust_table, masked_customers, mode="overwrite")
        logger.info("Published %d pseudonymized records to '%s'", written_cust, target_cust_table)

        self._lineage.emit_job_event(
            job_name="gold_customer_pseudonymization",
            inputs=[_SILVER_CUSTOMERS_TABLE],
            outputs=[target_cust_table],
            status="COMPLETE",
        )
        return written_cust
