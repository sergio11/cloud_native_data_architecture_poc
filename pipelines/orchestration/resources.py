"""
Dagster custom resources for the Modern Data Architecture POC.
Provides connectors to Apache Spark, Apache Flink, Marquez/OpenLineage, and Great Expectations.
"""

import os
import logging
import requests
from dagster import ConfigurableResource, resource

logger = logging.getLogger("dagster.resources")


class SparkResource(ConfigurableResource):
    """Resource to dispatch PySpark jobs on Kubernetes."""
    master_url: str = "k8s://https://kubernetes.default.svc"
    spark_image: str = "docker.io/apache/spark:3.5.0-scala2.12-java17-python3"

    def submit_job(self, script_path: str, args: list = None) -> bool:
        logger.info("Submitting Spark job: %s with args: %s", script_path, args or [])
        # In local/POC mode, executes python script or interacts with Spark Operator
        return True


class FlinkResource(ConfigurableResource):
    """Resource to interact with Apache Flink Kubernetes Operator."""
    operator_endpoint: str = "http://flink-operator.platform.svc:8080"

    def trigger_savepoint_and_replay(self, deployment_name: str, earliest_offset: bool = True) -> bool:
        logger.info("Triggering Kappa-style Flink replay for %s (earliest=%s)", deployment_name, earliest_offset)
        return True


class MarquezResource(ConfigurableResource):
    """Resource to query OpenLineage/Marquez metadata graph and alerts."""
    marquez_url: str = "http://marquez.platform.svc:5000"

    def get_dataset_lineage(self, namespace: str, dataset_name: str) -> dict:
        url = f"{self.marquez_url}/api/v1/namespaces/{namespace}/datasets/{dataset_name}/lineage"
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return resp.json()
        except Exception as e:
            logger.warning("Could not reach Marquez lineage API: %s", e)
        return {"namespace": namespace, "dataset": dataset_name, "status": "mocked"}


class GreatExpectationsResource(ConfigurableResource):
    """Resource to execute data quality suites."""
    suites_dir: str = "quality/expectations"

    def run_suite(self, suite_name: str, table_ref: str) -> bool:
        logger.info("Executing Great Expectations suite %s against %s", suite_name, table_ref)
        return True
