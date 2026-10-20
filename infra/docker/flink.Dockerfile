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
ARG HADOOP_SHADED_VERSION=2.8.3-10.0
# hadoop-aws MUST be 2.8.x. flink-shaded-hadoop-2-uber shades Hadoop 2.8.3, and
# hadoop-aws 3.x needs org.apache.hadoop.fs.StreamCapabilities (Hadoop 3.2+),
# which is not in the shaded jar:
#   NoClassDefFoundError: org/apache/hadoop/fs/StreamCapabilities
# 2.8.3 is also the version that still ships org.apache.hadoop.fs.s3.S3FileSystem
# (removed in Hadoop 3.0), which is what registers the scheme `s3` that Polaris
# hands out as the warehouse. Verified in the cluster:
#   s3:// + hadoop-aws 3.3.6 -> NoClassDefFoundError
#   s3:// + hadoop-aws 2.8.3 -> the scheme resolves
ARG HADOOP_AWS_VERSION=2.8.3
# AWS SDK bundle stays on 1.12.x: it is compiled against a much older SDK and
# hadoop-aws 2.8.3 only uses the S3 client surface from it.
ARG AWS_SDK_VERSION=1.12.367
# Unshaded Guava, and it is NOT optional.
#
# state.checkpoints.dir is s3a://, which resolves to
# org.apache.hadoop.fs.s3a.S3AFileSystem from hadoop-aws 2.8.3. That class uses
# com.google.common.util.concurrent.ListeningExecutorService, and
# flink-shaded-hadoop-2-uber RELOCATES its Guava to org.apache.flink.shaded.guava31,
# so the original name is absent and the class cannot even be loaded:
#   NoClassDefFoundError: com/google/common/util/concurrent/ListeningExecutorService
# There is no configuration that fixes a missing dependency; the class has to be
# on the classpath under its real name.
#
# 28.2-jre rather than the 11.0.2 that hadoop-common 2.8 pins: Guava 11 reaches
# for sun.misc.Cleaner in FinalizableReferenceQueue, which does not exist on
# Java 11 (the JVM this image runs), so it would fail the same way. 28.2-jre is
# the last line before the package split, is Java 11 safe, and still provides
# the S3A surface. Verified with `rake stream:probe`, which exercises s3a:// and
# fails loudly if this regresses.
ARG GUAVA_VERSION=28.2-jre

# Hadoop runtime classes. Registering the Iceberg REST catalog reaches
# org.apache.hadoop.conf.Configuration (the catalog builds a Hadoop FileSystem for
# the warehouse and the s3.* options), and the flink:1.18 image ships no Hadoop
# jar, so the job died at startup with
#   java.lang.ClassNotFoundException: org.apache.hadoop.conf.Configuration.
# The shaded uber jar is what Flink's own flink-hadoop module puts on the
# classpath, so it is the variant Iceberg is tested against; the unshaded
# Hadoop client jars would drag in their own AWS SDK and conflict with it.
RUN set -eux; \
    curl -fSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_SHADED_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_SHADED_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_SHADED_VERSION}.jar"; \
    ls -lh /opt/flink/lib/flink-shaded-hadoop-2-uber-*.jar

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

# ---------------------------------------------------------------------------
# Iceberg AWS bundle (S3FileIO)
#
# iceberg-flink-runtime does NOT contain org.apache.iceberg.aws.s3.S3FileIO -
# verified with `unzip -l ... | grep iceberg/aws/s3/S3FileIO` -> 0 entries. Nor
# does it bundle the AWS SDK. So Iceberg falls back to HadoopFileIO, which asks
# Hadoop for a filesystem for the warehouse scheme and gets:
#
#   org.apache.iceberg.exceptions.RuntimeIOException: Failed to get file system
#   for path: s3://warehouse/sales/orders_windowed/data/00001-...parquet
#   Caused by: java.io.IOException: No FileSystem for scheme: s3
#
# iceberg-aws-bundle is the single self-contained jar with the AWS SDK shaded
# in, and S3FileIO is then reachable. The table selects it explicitly via
# 'io-impl' (the same choice s3-batch makes for Spark in
# spark-defaults-configmap.yaml), so no Hadoop S3 stack is needed.
#
# Credentials are NOT configured here: they arrive through the
# s3.access-key / s3.secret-key / s3.endpoint keys already declared in
# flinkConfiguration.
RUN set -eux; \
    curl -fSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    ls -lh /opt/flink/lib/iceberg-aws-bundle-*.jar

# ---------------------------------------------------------------------------
# Hadoop AWS, so the scheme `s3` actually resolves
#
# With a Polaris INTERNAL catalog, io-impl is decided server-side and CANNOT be
# set on the table:
#   BadRequestException: Cannot set property 'io-impl' to
#   'org.apache.iceberg.aws.s3.S3FileIO' for this catalog.
# Polaris also returns NO s3.* properties from GET /v1/config (verified against
# 1.8.0: only defaults/overrides/endpoints), so the client keeps Iceberg's
# default HadoopFileIO, which then asks Hadoop for the warehouse scheme:
#   RuntimeIOException: Failed to get file system for path:
#   s3://warehouse/sales/orders_windowed/data/00001-...parquet
#   Caused by: java.io.IOException: No FileSystem for scheme: s3
#
# hadoop-aws is the jar that registers org.apache.hadoop.fs.s3.S3FileSystem,
# the implementation of the scheme `s3` that Polaris hands out as the warehouse;
# flink-shaded-hadoop ships hadoop-common only.
#
# Endpoints and credentials reach Hadoop through the fs.s3.* keys in
# flinkConfiguration (see flink-deployments.yaml), which is what MinIO needs:
# Hadoop 2.8's S3FileSystem reads fs.s3.awsAccessKeyId / fs.s3.awsSecretAccessKey
# and ignores the s3.* names that S3FileIO would have used.
#
# Version MUST match the Hadoop that flink-shaded-hadoop-2-uber shades, which is
# 2.8.3 - see the ARG comment above.
RUN set -eux; \
    curl -fSL -o /opt/flink/lib/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fSL -o /opt/flink/lib/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
        "${MAVEN_REPO}/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"; \
    ls -lh /opt/flink/lib/hadoop-aws-*.jar /opt/flink/lib/aws-java-sdk-bundle-*.jar

RUN set -eux; \
    curl -fSL -o /opt/flink/lib/guava-${GUAVA_VERSION}.jar \
        "${MAVEN_REPO}/com/google/guava/guava/${GUAVA_VERSION}/guava-${GUAVA_VERSION}.jar"; \
    ls -lh /opt/flink/lib/guava-*.jar

# S3 filesystem plugin for MinIO. No `|| true` here: a missing jar turns every
# checkpoint into a job failure, and swallowing the error is how this image
# looked healthy while broken.
RUN set -eux; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/; \
    ls /opt/flink/plugins/s3-fs-hadoop/*.jar

# ---------------------------------------------------------------------------
# PyFlink must be EXTRACTED, not left as pyflink.zip
#
# The flink:1.18 image ships PyFlink only as /opt/flink/opt/python/pyflink.zip
# and puts it on PYTHONPATH. That is enough to import pyflink in the client, but
# NOT enough to run a job under the Kubernetes Operator: every UDF/operator
# stage is executed by shelling out to
#   /opt/flink/opt/python/pyflink.zip/pyflink/bin/pyflink-udf-runner.sh
# and a path inside a zip is not a directory, so the task dies on open():
#
#   java.io.IOException: Cannot run program ".../pyflink.zip/pyflink/bin/
#   pyflink-udf-runner.sh": error=20, Not a directory
#   ... at BeamPythonFunctionRunner.createStageBundleFactory
#
# The job reaches RUNNING and then every task restarts on this, which looks like
# a flaky cluster rather than a packaging bug.
#
# python3.10 (already installed above) does the unzip, so no extra package and
# no second copy of PyFlink beyond the zip itself.
RUN python3.10 -c "import zipfile; zipfile.ZipFile('/opt/flink/opt/python/pyflink.zip').extractall('/opt/flink/opt/python/')" \
    && chmod +x /opt/flink/opt/python/pyflink/bin/*.sh \
    && ls -l /opt/flink/opt/python/pyflink/bin/pyflink-udf-runner.sh

ENV PATH="/opt/py310/bin:${PATH}"
# /opt/flink/opt/python - the PARENT of the extracted package, not
# /opt/flink/opt/python/pyflink.
#
# pyflink.zip contains pyflink/ at its root, so extracting it into
# /opt/flink/opt/python yields /opt/flink/opt/python/pyflink/{bin,common,
# datastream,...}. Putting .../pyflink on PYTHONPATH makes those subdirectories
# top-level modules, so `import pyflink` fails with ModuleNotFoundError - which
# is what ProcessPythonEnvironmentManager reports when it shells out to
# `python -c "import pyflink"`. The parent directory is what makes pyflink a
# package again. pyflink.zip is deliberately NOT in the path any more.
ENV PYTHONPATH="/opt/flink/opt/python:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
# Where pyflink/bin lives, which is what the UDF runner script resolves against.
ENV PYFLINK_HOME="/opt/flink/opt/python/pyflink"

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