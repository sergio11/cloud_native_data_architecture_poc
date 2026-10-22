# ==============================================================================
# Ingestion runner image (subsystem S1 CronJob medallion-bronze-ingestion-cron).
# Self-contained python image with the repo code + contracts baked in, so the
# CronJob can run the REAL pipeline instead of echo stubs:
#   python pipelines/medallion/bronze_ingestion.py --entity all
# (WORKDIR /app is the repo root; the CronJob sets workingDir: /app to match.)
#
# Build from the REPO ROOT:
#   docker build -f infra/docker/ingest.Dockerfile -t poc/ingest:0.1.0 .
# ==============================================================================
FROM python:3.11-slim

WORKDIR /app

# Pipeline code + hexagonal core + contracts referenced at runtime
# (domains/ product manifests, governance/ policies, quality/ suites).
COPY pipelines/ /app/pipelines/
COPY src/ /app/src/
COPY domains/ /app/domains/
COPY governance/ /app/governance/
COPY quality/ /app/quality/
# The event producer, so the speed layer can be fed from INSIDE the cluster.
#
# It cannot run from the workstation: the broker advertises
# kafka.platform.svc.cluster.local:9092, a name that does not resolve
# outside the cluster, and the workstation's Python does not even have
# confluent-kafka installed. Running it here means the events travel the
# same path a real producer's would.
COPY data-generator/ /app/data-generator/

# confluent-kafka is the client the event producer uses. It is pinned to the
# same version as data-generator/requirements.txt.
RUN pip install --no-cache-dir pyyaml requests click rich psycopg2-binary \
    "pyiceberg[pyarrow]==0.12.0" \
    "confluent-kafka==2.3.0"

CMD ["python", "pipelines/medallion/bronze_ingestion.py", "--entity", "all"]
