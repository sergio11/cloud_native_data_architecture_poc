"""
Modern Data Architecture POC - Gold Layer Business Modeling & PII Masking Use Case.
Executes business metric aggregations and enforces GDPR pseudonymization policies.

Quality gates (RF-06) mirror the Silver pattern (CleanseSilverUseCase): each Gold
target is evaluated via the QualityEnginePort BEFORE publication. On breach a FAIL
lineage event is emitted and QualityGateBreachError is raised, blocking publication
of that target (fail-closed per target - the other target proceeds independently).
"""

from __future__ import annotations

import logging
from typing import Any

from src.domain.aggregations import BusinessAggregator
from src.domain.entities import QualityGateEvaluation
from src.domain.exceptions import QualityGateBreachError
from src.domain.privacy import PrivacyPolicy
from src.ports.inbound import AggregateGoldPort
from src.ports.outbound import LakehouseStoragePort, LineagePublisherPort, QualityEnginePort

logger = logging.getLogger("use_case.aggregate_gold")

GOLD_DAILY_METRICS_SUITE = "quality/expectations/gold_daily_metrics_suite.json"
GOLD_CUSTOMERS_SUITE = "quality/expectations/gold_customers_suite.json"


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
    ) -> None:
        self._storage = storage
        self._lineage = lineage
        self._quality = quality_engine

    def execute(
        self,
        target: str = "all",
        quality_threshold: float = 0.95,
        daily_metrics_suite: str = GOLD_DAILY_METRICS_SUITE,
        customers_suite: str = GOLD_CUSTOMERS_SUITE,
    ) -> dict[str, Any]:
        logger.info("Executing Gold Layer aggregation (target='%s')", target)
        results: dict[str, Any] = {}
        breach: QualityGateBreachError | None = None

        # 1. Aggregate Daily Sales Metrics (fail-closed: a breach blocks only
        #    this target - the customers target below proceeds independently).
        if target in ("daily_metrics", "gold.sales.daily_metrics", "all"):
            try:
                results["daily_metrics_rows"] = self._publish_daily_metrics(
                    quality_threshold, daily_metrics_suite
                )
            except QualityGateBreachError as exc:
                breach = breach or exc

        # 2. Pseudonymize Customer Dimensions (GDPR PII Masking, gated likewise).
        if target in ("customers", "gold.sales.customers", "all"):
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

    def _enforce_gate(
        self,
        records: list[dict[str, Any]],
        suite_path: str,
        table_name: str,
        job_name: str,
        inputs: list[str],
        quality_threshold: float,
    ) -> QualityGateEvaluation:
        """Evaluates the quality suite and fail-closes on breach (Silver pattern).

        A missing suite raises SuiteNotFoundError from the engine and propagates
        (never a silent pass). A failing evaluation emits a FAIL lineage event
        and raises QualityGateBreachError BEFORE any write for this target.
        """
        logger.info(
            "Evaluating Quality Gate suite '%s' (threshold=%.2f)...",
            suite_path, quality_threshold
        )
        evaluation: QualityGateEvaluation = self._quality.evaluate_suite(
            records=records,
            suite_path=suite_path,
            threshold=quality_threshold,
            table_name=table_name,
        )
        if not evaluation.passed:
            logger.error(
                "Quality gate FAILED for '%s'! Pass rate: %.2f < %.2f. Blocking promotion.",
                table_name, evaluation.pass_rate, quality_threshold
            )
            # RF-06 owner notification trail: record the FAILED run before raising.
            self._lineage.emit_job_event(
                job_name=job_name,
                inputs=inputs,
                outputs=[table_name],
                status="FAIL",
            )
            raise QualityGateBreachError(
                f"Quality gate breached for {table_name} "
                f"(Pass rate: {evaluation.pass_rate:.2f} < threshold {quality_threshold:.2f})",
                pass_rate=evaluation.pass_rate,
                threshold=quality_threshold,
            )
        logger.info("Quality gate PASSED for '%s' (Pass rate: %.2f)", table_name, evaluation.pass_rate)
        return evaluation

    def _publish_daily_metrics(self, quality_threshold: float, suite_path: str) -> int:
        orders_silver = self._storage.read_table("silver.sales.orders")
        logger.info("Aggregating %d silver orders for daily metrics rollup...", len(orders_silver))
        metrics = BusinessAggregator.aggregate_daily_sales(orders_silver)
        metrics_dicts = [m.to_dict() for m in metrics]

        target_table = "gold.sales.daily_metrics"
        self._enforce_gate(
            records=metrics_dicts,
            suite_path=suite_path,
            table_name=target_table,
            job_name="gold_daily_sales_rollup",
            inputs=["silver.sales.orders"],
            quality_threshold=quality_threshold,
        )

        written = self._storage.write_table(target_table, metrics_dicts, mode="overwrite")
        logger.info("Published %d daily aggregation rows to '%s'", written, target_table)

        self._lineage.emit_job_event(
            job_name="gold_daily_sales_rollup",
            inputs=["silver.sales.orders"],
            outputs=[target_table],
            status="COMPLETE",
        )
        return written

    def _publish_customers(self, quality_threshold: float, suite_path: str) -> int:
        customers_silver = self._storage.read_table("silver.sales.customers")
        logger.info("Applying GDPR Art.25 pseudonymization on %d customers...", len(customers_silver))

        masked_customers: list[dict[str, Any]] = [
            PrivacyPolicy.apply_customer_masking(c) for c in customers_silver
        ]

        target_cust_table = "gold.sales.customers"
        self._enforce_gate(
            records=masked_customers,
            suite_path=suite_path,
            table_name=target_cust_table,
            job_name="gold_customer_pseudonymization",
            inputs=["silver.sales.customers"],
            quality_threshold=quality_threshold,
        )

        written_cust = self._storage.write_table(target_cust_table, masked_customers, mode="overwrite")
        logger.info("Published %d pseudonymized records to '%s'", written_cust, target_cust_table)

        self._lineage.emit_job_event(
            job_name="gold_customer_pseudonymization",
            inputs=["silver.sales.customers"],
            outputs=[target_cust_table],
            status="COMPLETE",
        )
        return written_cust
