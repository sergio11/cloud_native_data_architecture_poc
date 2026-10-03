"""
Dagster custom resources for the Modern Data Architecture POC.
Provides connectors to Apache Spark, Apache Flink, Marquez/OpenLineage, and Great Expectations.
"""

from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Any

import requests
from dagster import ConfigurableResource

from pipelines.orchestration.local_dispatch import run_script

if TYPE_CHECKING:
    from src.domain.entities import QualityGateEvaluation

logger = logging.getLogger("dagster.resources")


class SparkResource(ConfigurableResource):  # type: ignore[type-arg]  # dagster Generic base
    """Resource to dispatch pipeline scripts for Spark jobs.

    The K8s profile would submit to the cluster; the LOCAL profile (RF-22)
    dispatches the equivalent local pipeline script in a subprocess via
    :mod:`pipelines.orchestration.local_dispatch`.
    """

    master_url: str = "k8s://https://kubernetes.default.svc"
    spark_image: str = "docker.io/apache/spark:3.5.0-scala2.12-java17-python3"

    def submit_job(self, script_path: str, args: list[str] | None = None) -> bool:
        """Run the pipeline script locally; non-zero exit raises so the asset fails."""
        run_script(script_path, args)
        return True


class FlinkResource(ConfigurableResource):  # type: ignore[type-arg]  # dagster Generic base
    """Resource to interact with Apache Flink (K8s Operator in the K8s profile)."""

    operator_endpoint: str = "http://flink-operator.platform.svc:8080"

    def trigger_savepoint_and_replay(self, deployment_name: str, earliest_offset: bool = True) -> bool:
        """Replay the orders stream locally (mirrors `rake stream:replay`).

        Runs ``pipelines/stream/flink_orders_stream.py --topic sales.orders.events``
        with ``--start-offset earliest`` when ``earliest_offset`` is set; a
        non-zero exit raises so Dagster marks the asset failed.
        """
        logger.info(
            "LOCAL profile: replaying stream deployment '%s' (earliest_offset=%s) via local dispatch",
            deployment_name,
            earliest_offset,
        )
        args = ["--topic", "sales.orders.events"]
        if earliest_offset:
            args.extend(["--start-offset", "earliest"])
        run_script("pipelines/stream/flink_orders_stream.py", args)
        return True


class MarquezResource(ConfigurableResource):  # type: ignore[type-arg]  # dagster Generic base
    """Resource to query OpenLineage/Marquez metadata graph and alerts."""

    marquez_url: str = "http://marquez.platform.svc:5000"

    def get_dataset_lineage(self, namespace: str, dataset_name: str) -> dict[str, Any]:
        url = f"{self.marquez_url}/api/v1/namespaces/{namespace}/datasets/{dataset_name}/lineage"
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                payload = resp.json()
                if isinstance(payload, dict):
                    return payload
            logger.warning(
                "Marquez lineage API unavailable for %s.%s (HTTP %s); returning mocked marker",
                namespace,
                dataset_name,
                resp.status_code,
            )
        except Exception as e:
            logger.warning("Could not reach Marquez lineage API: %s", e)
        return {"namespace": namespace, "dataset": dataset_name, "status": "mocked", "mocked": True}


class GreatExpectationsResource(ConfigurableResource):  # type: ignore[type-arg]  # dagster Generic base
    """Resource to execute data quality suites via the hexagonal core quality gate."""

    suites_dir: str = "quality/expectations"

    @staticmethod
    def _execute_quality_gate(table_name: str, suite_path: str, threshold: float) -> QualityGateEvaluation:
        # Lazy import so Dagster only pays for the container when a gate actually runs.
        from src.container import container

        return container.run_quality_gate.execute(
            table_name=table_name,
            suite_path=suite_path,
            threshold=threshold,
        )

    def run_suite(self, suite_name: str, table_ref: str, threshold: float = 0.95) -> bool:
        """Run an expectation suite against a table and return the gate verdict.

        Args:
            suite_name: Suite file name (resolved against ``suites_dir``) or a
                path containing a directory component (used as-is).
            table_ref: Fully qualified table name (e.g. ``silver.sales.orders``).
            threshold: Minimum pass rate required for approval.

        Returns:
            ``True`` only when the core quality gate approved the data.
            ``False`` when the gate failed or could not be executed (the
            failure is logged so Dagster assets can surface it).
        """
        from src.domain.exceptions import SuiteNotFoundError

        suite_path = suite_name if os.path.dirname(suite_name) else os.path.join(self.suites_dir, suite_name)
        logger.info("Executing quality gate suite '%s' against '%s'", suite_path, table_ref)
        try:
            evaluation = self._execute_quality_gate(
                table_name=table_ref,
                suite_path=suite_path,
                threshold=threshold,
            )
            verdict = bool(evaluation.passed)
        except SuiteNotFoundError as exc:
            logger.error("Quality suite missing for table '%s': %s", table_ref, exc)
            return False
        except Exception as exc:
            logger.error(
                "Quality gate execution failed for suite '%s' on table '%s': %s",
                suite_path,
                table_ref,
                exc,
            )
            return False
        logger.info(
            "Quality gate verdict for '%s' via '%s': %s",
            table_ref,
            suite_path,
            "PASS" if verdict else "FAIL",
        )
        return verdict
