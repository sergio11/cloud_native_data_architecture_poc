# ==============================================================================
# Spark medallion batch image (subsystem S3).
#
# Base image MUST stay in sync with infra/helm/s3-batch/values.yaml
# (sparkDefaults.image) and with the ScheduledSparkApplication
# sparkVersion (3.5.1). Puts the repo code inside the image so
#   mainApplicationFile: local:///opt/spark/work-dir/pipelines/medallion/*.py
# resolves (the official apache/spark image ships no repo code).
#
# WHY NOT apache/spark:3.5.1-scala2.12-java17-python3-ubuntu
#   That image is Ubuntu 20.04 and ships PYTHON 3.8. The medallion core
#   needs 3.11: `from enum import StrEnum` (src/domain/contracts.py) and
#   `from datetime import UTC` (src/use_cases/ingest_bronze.py) are 3.11
#   imports that fail at IMPORT time on 3.8, so the driver died with an
#   ImportError before running a single line of the pipeline.
#   This image is python:3.11-slim (the same base as poc/ingest:0.1.0) plus
#   the Spark distribution from the PyPI wheel, which is the SAME 3.5.1
#   release and ~3x smaller to pull than the 2.5 GB official image.
#
# The Spark Operator invokes /opt/spark/bin/spark-submit in the driver pod,
# so the distribution has to live at /opt/spark (that is where the official
# images put it). The wheel ships bin/, sbin/, jars/ and conf/; they are
# symlinked there and conf/ is copied so SPARK_CONF_DIR resolves.
#
# Build from the REPO ROOT:
#   podman build -f infra/docker/spark.Dockerfile -t poc/spark-medallion:0.1.0 .
# ==============================================================================
# bookworm, not the default trixie: trixie only ships openjdk-21/25 and
# Spark 3.5 does not support Java 21 (Java 17 is the version it is tested
# against; Java 21 support landed in Spark 4.0).
FROM python:3.11-slim-bookworm

# The Spark driver runs in a JVM even for a Python application, so the image
# needs a JRE. Java 17 is the version Spark 3.5 is tested against.
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:$PATH \
    PYSPARK_PYTHON=/usr/local/bin/python3

WORKDIR /opt/spark/work-dir

# Spark distribution from the PyPI wheel, placed at /opt/spark.
RUN pip install --no-cache-dir "pyspark==3.5.1" \
 && PYSPARK_HOME="$(python3 -c 'import os, pyspark; print(os.path.dirname(pyspark.__file__))')" \
 && mkdir -p "$SPARK_HOME/conf" \
 && for d in bin sbin jars; do ln -sfn "$PYSPARK_HOME/$d" "$SPARK_HOME/$d"; done \
 && if [ -d "$PYSPARK_HOME/conf" ]; then cp -r "$PYSPARK_HOME/conf/." "$SPARK_HOME/conf/"; fi \
 && /opt/spark/bin/spark-submit --version 2>&1 | tail -1

# Repo code: pipelines/* is the Spark entrypoint, src/* is imported via the
# sys.path bootstrap at the top of each pipeline script (repo root on path).
# domains/* (data contracts for quality thresholds), quality/* (GE-format
# expectation suites required by the silver/gold gates) and governance/*
# (the JSON schemas the contract validator loads) must ride along: without
# them the gates fail with SuiteNotFoundError / fall back to the
# global threshold instead of enforcing contract thresholds.
COPY pipelines/ /opt/spark/work-dir/pipelines/
COPY src/ /opt/spark/work-dir/src/
COPY domains/ /opt/spark/work-dir/domains/
COPY quality/ /opt/spark/work-dir/quality/
COPY governance/ /opt/spark/work-dir/governance/

# Minimal runtime deps of the medallion drivers (yaml/requests/click +
# psycopg2-binary so bronze can read PostgreSQL instead of synthetic fallback).
# pyiceberg is the Iceberg client the medallion's storage adapter uses when
# LAKEHOUSE_BACKEND=iceberg, so the batch layer writes to the real lakehouse
# instead of the local SQLite file.
RUN pip install --no-cache-dir pyyaml requests click psycopg2-binary \
        "pyiceberg[pyarrow]==0.12.0"

# procps: Spark's load-spark-env.sh calls `ps` to look at its own process
# while it resolves the JVM, and the slim base has no `ps` at all (the
# spark-submit startup log would say "ps: command not found" every time).
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

# The Spark Operator starts the driver and the executor with an EMPTY command
# and "driver"/"executor" as the first argument: it expects the image to
# dispatch on it, the way the official apache/spark image does through
# /opt/entrypoint.sh. Without this the pod dies with
# `exec: "driver": executable file not found in $PATH`.
COPY infra/docker/spark-entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

ENTRYPOINT ["/opt/spark/entrypoint.sh"]

# NOTE: the `python`/`python3` names already point at the 3.11 interpreter in
# the python:3.11 base image (they live in /usr/local/bin, not /usr/bin), so
# there is nothing to link here. spark-submit resolves the interpreter as
# `python` by default and PYSPARK_PYTHON above pins it explicitly.
