"""
Dagster custom resources for the Modern Data Architecture POC.
Provides connectors to Apache Spark, Apache Flink, Marquez/OpenLineage, and Great Expectations.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import requests
from dagster import ConfigurableResource

from pipelines.orchestration.flink_replay import (
    CONSUMER_GROUP_VARS,
    START_OFFSET_VAR,
    build_replay_env,
    replay_group_id,
)
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
    """Resource to drive the Apache Flink speed layer on the cluster (RF-10).

    The speed layer is a REAL Flink job submitted by the Flink Kubernetes
    Operator (``FlinkDeployment`` in the s4-stream chart). It has no local
    counterpart, so this resource talks to the cluster with ``kubectl``
    read-modify-write against the deployment instead of shelling out to a
    Python script.
    """

    operator_endpoint: str = "http://flink-operator.platform.svc:8080"
    compute_namespace: str = "sales"
    kubectl_binary: str = "kubectl"

    def _kubectl(self, args: list[str], stdin: str | None = None) -> str:
        """Runs a kubectl command, raising with its output when it fails."""
        result = subprocess.run(
            [self.kubectl_binary, *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            input=stdin,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"kubectl {' '.join(args)} exited with {result.returncode}: {result.stderr.strip()}")
        return result.stdout

    def current_deployment(self, deployment_name: str) -> dict[str, Any]:
        """Returns the live FlinkDeployment object as a dict."""
        raw = self._kubectl(["get", "flinkdeployment", deployment_name, "-n", self.compute_namespace, "-o", "json"])
        parsed: Any = None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"kubectl returned invalid JSON for {deployment_name}: {exc}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError(f"kubectl returned a non-object for {deployment_name}.")
        return dict(parsed)

    def trigger_savepoint_and_replay(
        self,
        deployment_name: str = "flink-orders-stream",
        earliest_offset: bool = True,
    ) -> bool:
        """Triggers a Kappa replay of a speed-layer deployment (RF-10).

        Rotates the Kafka consumer group and rewinds the read offset so the job
        re-reads the whole topic log from offset 0 and re-emits every window;
        the Iceberg sink upserts on ``(window_start, key)``, so the replay is
        idempotent and the table ends up with the recomputed values.

        The consumer group MUST be rotated as well as the offset: a committed
        group ignores ``earliest`` and would replay nothing while still
        reporting success (see :mod:`pipelines.orchestration.flink_replay`).

        Returns:
            True once the patched deployment has been accepted.
        """
        stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        deployment = self.current_deployment(deployment_name)
        spec = deployment.get("spec") or {}
        env = list(spec.get("env") or [])

        group_var = next((var for var in CONSUMER_GROUP_VARS if any(e.get("name") == var for e in env)), None)
        if group_var is None:
            raise RuntimeError(
                f"{deployment_name} declares no known consumer group variable "
                f"({', '.join(CONSUMER_GROUP_VARS)}); refusing to trigger a replay that may be a no-op."
            )

        group_id = replay_group_id(deployment_name, stamp)
        patched_env = build_replay_env(
            env,
            consumer_group_var=group_var,
            group_id=group_id,
        )
        if not earliest_offset:
            patched_env = [
                dict(entry, value="latest") if entry.get("name") == START_OFFSET_VAR else entry for entry in patched_env
            ]

        deployment["spec"]["env"] = patched_env
        logger.info(
            "Triggering Kappa replay on %s: %s=%s, %s=%s",
            deployment_name,
            group_var,
            group_id,
            START_OFFSET_VAR,
            "earliest" if earliest_offset else "latest",
        )
        self._kubectl(["replace", "-f", "-"], stdin=json.dumps(deployment))
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
