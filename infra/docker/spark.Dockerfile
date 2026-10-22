# Spark medallion batch image (S3). Keep the Spark version in sync with
# sparkDefaults.image and sparkVersion in infra/helm/s3-batch/values.yaml.
# Needs Python 3.11 (StrEnum, datetime.UTC), so Spark comes from the PyPI wheel.
#
# bookworm, not trixie: trixie only ships openjdk-21/25 and Spark 3.5 needs 17.
FROM python:3.11-slim-bookworm

# The driver runs in a JVM even for a Python application.
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

# Repo code and assets the quality gates load at runtime: domains/ for contract
# thresholds, quality/ for expectation suites, governance/ for contract schemas.
COPY pipelines/ /opt/spark/work-dir/pipelines/
COPY src/ /opt/spark/work-dir/src/
COPY domains/ /opt/spark/work-dir/domains/
COPY quality/ /opt/spark/work-dir/quality/
COPY governance/ /opt/spark/work-dir/governance/

# Runtime deps of the medallion drivers; pyiceberg is used when
# LAKEHOUSE_BACKEND=iceberg.
RUN pip install --no-cache-dir pyyaml requests click psycopg2-binary \
        "pyiceberg[pyarrow]==0.12.0"

# procps: load-spark-env.sh calls `ps` and the slim base has none.
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*

# The operator starts the driver and executors with "driver"/"executor" as the
# first argument and expects the image to dispatch on it.
COPY infra/docker/spark-entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

ENTRYPOINT ["/opt/spark/entrypoint.sh"]

# python/python3 already point at 3.11 in the base image; PYSPARK_PYTHON pins it.
