# ==============================================================================
# Flink streaming image (subsystem S4).
# Base image MUST stay in sync with infra/helm/s4-stream/values.yaml
# (flinkJobs.*.image). Puts the repo code inside the image so the
# FlinkDeployment job args resolve:
#   args: ["-py", "/opt/flink/pipelines/stream/flink_orders_stream.py"]
# Layout note: the stream drivers bootstrap sys.path with "../.." relative to
# their own file, i.e. they expect the repo root at /opt/flink, hence
#   pipelines/ -> /opt/flink/pipelines/.
#
# Only pipelines/ is baked in on purpose: the speed layer is a standalone Flink
# job and does not import the hexagonal core in src/. If a future job needs src/,
# add it back explicitly rather than re-introducing the coupling by accident.
#
# Build from the REPO ROOT:
#   docker build -f infra/docker/flink.Dockerfile -t poc/flink-stream:0.1.0 .
# ==============================================================================
FROM flink:1.18-scala_2.12

COPY pipelines/ /opt/flink/pipelines/

# The base image ships no Python interpreter, but the PyFlink PythonDriver
# executes the `python` binary. The POC requires Python >= 3.11 (`datetime.UTC`),
# while Ubuntu 22.04 defaults to 3.10, so install 3.11 from deadsnakes into an
# isolated venv and expose it as `python` (what Spark/PyFlink resolves first).
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg software-properties-common \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends python3.11 python3.11-venv \
    && rm -rf /var/lib/apt/lists/*

# NOTE: the venv must be on PATH, not symlinked: `python -> /opt/py311/bin/python`
# loses the venv context and therefore site-packages, and every import fails.
RUN python3.11 -m venv /opt/py311 \
    && /opt/py311/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/py311/bin/pip install --no-cache-dir pyyaml requests click \
    && /opt/py311/bin/python -c "import yaml, requests, click; print('venv deps ok')"

# The jobs use the DataStream API (KafkaSource) and the Table API only to write
# into Iceberg, so the DataStream connector artifacts are the ones required. The
# SQL uber jar flink-sql-connector-kafka is deliberately not used: it bundles a
# second, relocated Kafka client that would conflict on the classpath.
# ---------------------------------------------------------------------------
ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG FLINK_VERSION=1.18.1
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ARG KAFKA_CLIENTS_VERSION=3.4.0
ARG ICEBERG_VERSION=1.6.1

RUN set -eux; \
    curl -fSL -o /opt/flink/lib/flink-connector-base-${FLINK_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/flink/flink-connector-base/${FLINK_VERSION}/flink-connector-base-${FLINK_VERSION}.jar"; \
    curl -fSL -o /opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o /opt/flink/lib/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fSL -o /opt/flink/lib/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar"

# RocksDB state backend: the charts set state.backend=rocksdb and checkpoints go
# to s3a://. flink-dist may ship it in lib/ or only in opt/ depending on the
# build, so normalise it into lib/ and verify: a missing state backend only
# fails at the first checkpoint, hours later.
RUN set -eux; \
    if ! ls /opt/flink/lib/flink-statebackend-rocksdb-*.jar >/dev/null 2>&1; then \
        ls /opt/flink/opt/flink-statebackend-rocksdb-*.jar >/dev/null; \
        cp /opt/flink/opt/flink-statebackend-rocksdb-*.jar /opt/flink/lib/; \
    fi; \
    ls /opt/flink/lib/flink-statebackend-rocksdb-*.jar

# S3 filesystem plugin for MinIO. No `|| true` here: a missing jar turns every
# checkpoint into a job failure, and swallowing the error is how this image
# looked healthy while broken.
RUN set -eux; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/; \
    ls /opt/flink/plugins/s3-fs-hadoop/*.jar

ENV PATH="/opt/py311/bin:${PATH}"
ENV PYTHONPATH="/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:${PYTHONPATH}"