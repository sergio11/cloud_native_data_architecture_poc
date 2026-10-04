# ==============================================================================
# Flink streaming image (subsystem S4).
# Base image MUST stay in sync with infra/helm/s4-stream/values.yaml
# (flinkJobs.*.image). Puts the repo code inside the image so the
# FlinkDeployment job args resolve:
#   args: ["-py", "/opt/flink/pipelines/stream/flink_orders_stream.py"]
# Layout note: the stream drivers bootstrap sys.path with "../.." relative to
# their own file, i.e. they expect the repo root at /opt/flink, hence
#   pipelines/ -> /opt/flink/pipelines/  and  src/ -> /opt/flink/src/.
#
# Build from the REPO ROOT:
#   docker build -f infra/docker/flink.Dockerfile -t poc/flink-stream:0.1.0 .
# ==============================================================================
FROM flink:1.18-scala_2.12

COPY pipelines/ /opt/flink/pipelines/
COPY src/ /opt/flink/src/

# Minimal runtime deps of the streaming drivers (yaml/requests/click).
RUN pip install --no-cache-dir pyyaml requests click
