# Flink streaming image (S4). Keep the base tag in sync with
# flinkJobs.*.image in infra/helm/s4-stream/values.yaml. Only pipelines/ is
# baked in; build with `podman build -f infra/docker/flink.Dockerfile .`.
FROM flink:1.18-scala_2.12

# Python 3.10, not 3.11: PyFlink 1.18 depends on pemja, which has no cp311
# wheel. Shipped code must avoid 3.11-only stdlib.

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates curl python3.10 python3.10-venv \
    && rm -rf /var/lib/apt/lists/*

# PyFlink runtime dependencies: the image ships pyflink.zip and py4j but not
# their pip requirements. apache-beam <2.49 and cloudpickle <3 are PyFlink
# 1.18's declared range; apache-flink-libraries and py4j are already present.
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

# DataStream connectors: the SQL uber jar bundles a relocated Kafka client that
# conflicts on the classpath.

ARG MAVEN_REPO=https://repo1.maven.org/maven2
ARG FLINK_VERSION=1.18.1
ARG KAFKA_CONNECTOR_VERSION=3.1.0-1.18
ARG KAFKA_CLIENTS_VERSION=3.4.0
ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_SHADED_VERSION=2.8.3-10.0
# Must match the Hadoop 2.8.3 shaded by flink-shaded-hadoop-2-uber: hadoop-aws
# 3.x needs StreamCapabilities, which the shaded jar does not provide.
ARG HADOOP_AWS_VERSION=2.8.3
# hadoop-aws 2.8.3 only uses the older S3 client surface of this bundle.
ARG AWS_SDK_VERSION=1.12.367
# Unshaded Guava: flink-shaded-hadoop relocates its copy, and S3AFileSystem needs
# com.google.common.util.concurrent on the real name; 28.2-jre is Java 11 safe.
ARG GUAVA_VERSION=28.2-jre

# Hadoop runtime classes: the Iceberg REST catalog builds a Hadoop FileSystem and
# the Flink image ships no Hadoop jar.
RUN set -eux; \
    curl -fSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-${HADOOP_SHADED_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_SHADED_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_SHADED_VERSION}.jar"; \
    ls -lh /opt/flink/lib/flink-shaded-hadoop-2-uber-*.jar

# RocksDB state backend, declared by the charts but not shipped in the image.
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

# S3FileIO and the AWS SDK are not part of iceberg-flink-runtime. Credentials
# come from the s3.* keys in flinkConfiguration.
RUN set -eux; \
    curl -fSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        "${MAVEN_REPO}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    ls -lh /opt/flink/lib/iceberg-aws-bundle-*.jar

# Registers the `s3` scheme Polaris hands out; an INTERNAL catalog decides
# io-impl server-side, so the job falls back to HadoopFileIO.
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

# S3 filesystem plugin for MinIO. A missing jar fails every checkpoint, so the
# copy must not be guarded.
RUN set -eux; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/; \
    ls /opt/flink/plugins/s3-fs-hadoop/*.jar

# PyFlink must be extracted: the operator shells out to pyflink-udf-runner.sh,
# and a path inside pyflink.zip is not a directory.
RUN python3.10 -c "import zipfile; zipfile.ZipFile('/opt/flink/opt/python/pyflink.zip').extractall('/opt/flink/opt/python/')" \
    && chmod +x /opt/flink/opt/python/pyflink/bin/*.sh \
    && ls -l /opt/flink/opt/python/pyflink/bin/pyflink-udf-runner.sh

ENV PATH="/opt/py310/bin:${PATH}"
# Parent of the extracted package: pyflink.zip already contains pyflink/.
ENV PYTHONPATH="/opt/flink/opt/python:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
# Directory the UDF runner script resolves against.
ENV PYFLINK_HOME="/opt/flink/opt/python/pyflink"

# Application code last, so editing a driver does not invalidate the layers
# above. Nothing above reads /opt/flink/pipelines.
COPY pipelines/ /opt/flink/pipelines/