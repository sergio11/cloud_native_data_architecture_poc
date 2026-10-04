# ==============================================================================
# Spark medallion batch image (subsystem S3).
# Base image MUST stay in sync with infra/helm/s3-batch/values.yaml
# (sparkDefaults.image). Puts the repo code inside the image so
#   mainApplicationFile: local:///opt/spark/work-dir/pipelines/medallion/*.py
# resolves (upstream apache/spark-py:v3.5.1 ships no repo code).
#
# Build from the REPO ROOT:
#   docker build -f infra/docker/spark.Dockerfile -t poc/spark-medallion:0.1.0 .
# ==============================================================================
FROM apache/spark-py:v3.5.1

WORKDIR /opt/spark/work-dir

# Repo code: pipelines/* is the Spark entrypoint, src/* is imported via the
# sys.path bootstrap at the top of each pipeline script (repo root on path).
COPY pipelines/ /opt/spark/work-dir/pipelines/
COPY src/ /opt/spark/work-dir/src/

# Minimal runtime deps of the medallion drivers (yaml/requests/click).
RUN pip install --no-cache-dir pyyaml requests click
