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

# The flink:1.18 base image ships no Python interpreter, but the PyFlink
# PythonDriver executes the `python` binary: install it (no-op if present)
# and expose it under the expected name.
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

# Minimal runtime deps of the streaming drivers (yaml/requests/click).
# Debian bookworm's pip honours PEP 668 here, hence --break-system-packages.
RUN python3 -m pip install --no-cache-dir pyyaml requests click \
    || python3 -m pip install --no-cache-dir --break-system-packages pyyaml requests click
