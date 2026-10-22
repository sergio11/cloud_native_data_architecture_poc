# Ingestion runner image (S1 CronJob). WORKDIR is the repo root, matching
# workingDir: /app in the CronJob.
# Build with `podman build -f infra/docker/ingest.Dockerfile -t poc/ingest:0.1.0 .`.
FROM python:3.11-slim

WORKDIR /app

# Pipeline code, core, and the assets loaded at runtime.
COPY pipelines/ /app/pipelines/
COPY src/ /app/src/
COPY domains/ /app/domains/
COPY governance/ /app/governance/
COPY quality/ /app/quality/
# Event producer: the broker name only resolves inside the cluster.
COPY data-generator/ /app/data-generator/

# confluent-kafka is the producer client; keep it in sync with
# data-generator/requirements.txt.
RUN pip install --no-cache-dir pyyaml requests click rich psycopg2-binary \
    "pyiceberg[pyarrow]==0.12.0" \
    "confluent-kafka==2.3.0"

CMD ["python", "pipelines/medallion/bronze_ingestion.py", "--entity", "all"]
