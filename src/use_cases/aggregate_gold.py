"""
Modern Data Architecture POC - Gold Layer Business Modeling & PII Masking Use Case.
Executes business metric aggregations and enforces GDPR pseudonymization policies.
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.aggregations import BusinessAggregator
from src.domain.privacy import PrivacyPolicy
from src.ports.outbound import LakehouseStoragePort, LineagePublisherPort

logger = logging.getLogger("use_case.aggregate_gold")


class AggregateGoldUseCase:
    """Orchestrates Silver-to-Gold dimensional modeling and GDPR PII masking.

    Gold is a full recomputation from Silver on every run (Lambda batch layer),
    which IS the replay/RF-08 behaviour - there is no incremental replay flag.
    """

    def __init__(
        self,
        storage: LakehouseStoragePort,
        lineage: LineagePublisherPort,
    ) -> None:
        self._storage = storage
        self._lineage = lineage

    def execute(self, target: str = "all") -> dict[str, Any]:
        logger.info("Executing Gold Layer aggregation (target='%s')", target)
        results: dict[str, Any] = {}

        # 1. Aggregate Daily Sales Metrics
        if target in ("daily_metrics", "gold.sales.daily_metrics", "all"):
            orders_silver = self._storage.read_table("silver.sales.orders")
            logger.info("Aggregating %d silver orders for daily metrics rollup...", len(orders_silver))
            metrics = BusinessAggregator.aggregate_daily_sales(orders_silver)
            metrics_dicts = [m.to_dict() for m in metrics]

            target_table = "gold.sales.daily_metrics"
            written = self._storage.write_table(target_table, metrics_dicts, mode="overwrite")
            logger.info("Published %d daily aggregation rows to '%s'", written, target_table)

            self._lineage.emit_job_event(
                job_name="gold_daily_sales_rollup",
                inputs=["silver.sales.orders"],
                outputs=[target_table],
                status="COMPLETE",
            )
            results["daily_metrics_rows"] = written

        # 2. Pseudonymize Customer Dimensions (GDPR PII Masking)
        if target in ("customers", "gold.sales.customers", "all"):
            customers_silver = self._storage.read_table("silver.sales.customers")
            logger.info("Applying GDPR Art.25 pseudonymization on %d customers...", len(customers_silver))

            masked_customers: list[dict[str, Any]] = [
                PrivacyPolicy.apply_customer_masking(c) for c in customers_silver
            ]

            target_cust_table = "gold.sales.customers"
            written_cust = self._storage.write_table(target_cust_table, masked_customers, mode="overwrite")
            logger.info("Published %d pseudonymized records to '%s'", written_cust, target_cust_table)

            self._lineage.emit_job_event(
                job_name="gold_customer_pseudonymization",
                inputs=["silver.sales.customers"],
                outputs=[target_cust_table],
                status="COMPLETE",
            )
            results["customers_masked_rows"] = written_cust

        results["status"] = "SUCCESS"
        return results
