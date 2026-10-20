# ==============================================================================
# Spark medallion batch image (subsystem S3).
# Base image MUST stay in sync with infra/helm/s3-batch/values.yaml
# (sparkDefaults.image) and with the ScheduledSparkApplication
# sparkVersion (3.5.1). Puts the repo code inside the image so
#   mainApplicationFile: local:///opt/spark/work-dir/pipelines/medallion/*.py
# resolves (the official apache/spark image ships no repo code).
#
# Build from the REPO ROOT:
#   docker build -f infra/docker/spark.Dockerfile -t poc/spark-medallion:0.1.0 .
# ==============================================================================
FROM apache/spark:3.5.1-scala2.12-java17-python3-ubuntu

USER root
WORKDIR /opt/spark/work-dir

# Repo code: pipelines/* is the Spark entrypoint, src/* is imported via the
# sys.path bootstrap at the top of each pipeline script (repo root on path).
# domains/* (data contracts for quality thresholds) and quality/* (GE-format
# expectation suites required by the silver/gold gates) must ride along:
# without them the gates fail with SuiteNotFoundError / fall back to the
# global threshold instead of enforcing contract thresholds.
COPY pipelines/ /opt/spark/work-dir/pipelines/
COPY src/ /opt/spark/work-dir/src/
COPY domains/ /opt/spark/work-dir/domains/
COPY quality/ /opt/spark/work-dir/quality/

# Minimal runtime deps of the medallion drivers (yaml/requests/click +
# psycopg2-binary so bronze can read PostgreSQL instead of synthetic fallback).
# pyiceberg is the Iceberg client the medallion's storage adapter uses when
# LAKEHOUSE_BACKEND=iceberg, so the batch layer writes to the real lakehouse
# instead of the local SQLite file.
# `--break-system-packages` only exists on newer pips, so try plain first.
RUN python3 -m pip install --no-cache-dir pyyaml requests click psycopg2-binary \
        "pyiceberg[pyarrow]==0.12.0" \
    || python3 -m pip install --no-cache-dir --break-system-packages \
       pyyaml requests click psycopg2-binary "pyiceberg[pyarrow]==0.12.0"

# spark-submit resolves the interpreter as `python` by default.
RUN ln -sf /usr/bin/python3 /usr/local/bin/python