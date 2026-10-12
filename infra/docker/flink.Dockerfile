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
#   podman build -f infra/docker/flink.Dockerfile -t poc/flink-stream:0.1.0 .
# ==============================================================================
FROM flink:1.18-scala_2.12

# ---------------------------------------------------------------------------
# Python runtime
#
# Python 3.10 (the Ubuntu 22.04 system interpreter), NOT 3.11. This is not a
# preference: PyFlink 1.18 declares `pemja==0.3.0` as a runtime dependency and
# pemja publishes no cp311 wheel, so on 3.11 it tries to build from Rust source
# and fails. PyFlink's own supported range for Flink 1.18 tops out at 3.10.
# Consequence: code shipped in this image must avoid 3.11-only stdlib such as
# `datetime.UTC`; use `timezone.utc` instead.
# ---------------------------------------------------------------------------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl python3.10 python3.10-venv \
    && rm -rf /var/lib/apt/lists/*

# PyFlink's runtime dependencies, taken from flink-python/setup.py at
# release-1.18. The flink:1.18 image ships pyflink.zip and py4j but NOT their
# pip requirements, so without these the job dies inside
# get_execution_environment() with a chain of "No module named ..." errors.
# Pins that matter:
#   * apache-beam <2.49  - PyFlink 1.18's declared range; 2.7x imports but is
#                          unsupported and fails later in the runner.
#   * cloudpickle <3     - required by apache-beam 2.48.
#   * avro-python3       - provides the `avro` module the coder imports.
#   * pemja              - declared by PyFlink; needs a cp310 wheel.
# apache-flink-libraries and py4j are omitted: the first duplicates jars already
# in the Flink distribution, the second ships in /opt/flink/opt/python.
RUN python3.10 -m venv /opt/py310 \
    && /opt/py310/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/py310/bin/pip install --no-cache-dir \
        "apache-beam==2.48.0" \
        "avro-python3==1.10.2" \
        "cloudpickle==2.2.1" \
        "fastavro==1.9.7" \
        "pemja==0.3.0" \
        "grpcio==1.62.3" \
        "httplib2==0.22.0" \
        "pandas==2.3.3" \
        "python-dateutil==2.9.0.post0" \
        "pytz==2026.5" \
        "requests==2.32.5" \
        "pyyaml==6.0.2" \
        "click==8.1.7" \
    && /opt/py310/bin/python -c "import yaml, requests, click, grpc, apache_beam, pyarrow, numpy, pandas, cloudpickle, avro, pemja, fastavro" \
    && /opt/py310/bin/pip freeze > /opt/py310/requirements.lock.txt \
    && echo 'venv deps ok'

# ---------------------------------------------------------------------------
# Connector jars
#
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

# RocksDB state backend: the charts set state.backend=rocksdb and checkpoints go
# to s3a://. The flink:1.18 image ships neither lib/ nor opt/ copy, so it is
# fetched explicitly. Verified at build time, because a missing state backend
# only fails at the first checkpoint, hours into a run.
RUN set -eux; \
    curl -fSL -o /opt/flink/lib/flink-statebackend-rocksdb-${FLINK_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/flink/flink-statebackend-rocksdb/${FLINK_VERSION}/flink-statebackend-rocksdb-${FLINK_VERSION}.jar"; \
    ls /opt/flink/lib/flink-statebackend-rocksdb-*.jar

RUN set -eux; \
    curl -fSL -o /opt/flink/lib/flink-connector-base-${FLINK_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/flink/flink-connector-base/${FLINK_VERSION}/flink-connector-base-${FLINK_VERSION}.jar"; \
    curl -fSL -o /opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar"; \
    curl -fSL -o /opt/flink/lib/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    curl -fSL -o /opt/flink/lib/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/iceberg/iceberg-flink-runtime-1.18/${ICEBERG_VERSION}/iceberg-flink-runtime-1.18-${ICEBERG_VERSION}.jar"

# S3 filesystem plugin for MinIO. No `|| true` here: a missing jar turns every
# checkpoint into a job failure, and swallowing the error is how this image
# looked healthy while broken.
RUN set -eux; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/; \
    ls /opt/flink/plugins/s3-fs-hadoop/*.jar

ENV PATH="/opt/py310/bin:${PATH}"
ENV PYTHONPATH="/opt/flink/opt/python/pyflink.zip:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# ---------------------------------------------------------------------------
# Application code, LAST on purpose.
#
# The job drivers change constantly while a pipeline is being debugged on the
# cluster, and the layers above (venv with ~15 wheels, four connector jars) cost
# minutes. With the COPY first, editing one .py invalidated all of them and every
# fix cost a full rebuild; here it is a single ~40 kB layer. Nothing below reads
# /opt/flink/pipelines, so the ordering is safe.
# ---------------------------------------------------------------------------
COPY pipelines/ /opt/flink/pipelines/