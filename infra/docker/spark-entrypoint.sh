#!/usr/bin/env bash
# ==============================================================================
# Entrypoint of the Spark medallion image.
#
# The Spark Operator does NOT start spark-submit itself: it starts the pod
# with an EMPTY command and puts "driver" (or "executor") as the first
# argument, expecting the IMAGE to dispatch on it. The official apache/spark
# image does that through /opt/entrypoint.sh, and the operator's contract
# assumes it; a custom image without it dies with
#
#   exec: "driver": executable file not found in $PATH
#
# This is the official entrypoint (apache/spark 3.5.1,
# resource-managers/kubernetes/docker/src/main/dockerfiles/spark/entrypoint.sh)
# reduced to what this image needs: it runs as root (so there is no passwd
# entry to create) and it does not use tini.
#
# The two branches are NOT the same:
#
#   driver   spark-submit with --deploy-mode client. The operator has ALREADY
#            created this pod, so the driver has to run INSIDE it; without
#            the flag spark-submit tries to create the driver pod again and
#            dies with `pods "spark-...-driver" already exists` (409).
#   executor the Spark executor backend started directly with the
#            SPARK_EXECUTOR_* variables the operator injects.
#
# The operator's own arguments follow the keyword and are passed through
# untouched:
#   driver:   --properties-file /opt/spark/conf/spark.properties --class ... <file>
#   executor: --properties-file ... (the executor backend takes its own args)
# ==============================================================================
set -eo pipefail

if [ -z "${JAVA_HOME:-}" ]; then
  JAVA_HOME=$(java -XshowSettings:properties -version 2>&1 > /dev/null | grep 'java.home' | awk '{print $3}')
fi

SPARK_CLASSPATH="${SPARK_CLASSPATH:-}:${SPARK_HOME}/jars/*"
if [ -n "${SPARK_CONF_DIR:-}" ]; then
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_CONF_DIR"
else
  SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_HOME/conf"
fi
# SPARK-43540: the current working directory belongs to the executor classpath.
SPARK_CLASSPATH="$SPARK_CLASSPATH:$PWD"

case "${1:-}" in
  driver)
    shift 1
    exec "$SPARK_HOME/bin/spark-submit" \
      --conf "spark.driver.bindAddress=${SPARK_DRIVER_BIND_ADDRESS:-}" \
      --conf "spark.executorEnv.SPARK_DRIVER_POD_IP=${SPARK_DRIVER_BIND_ADDRESS:-}" \
      --deploy-mode client \
      "$@"
    ;;
  executor)
    shift 1
    exec "${JAVA_HOME}/bin/java" \
      -Xms"${SPARK_EXECUTOR_MEMORY:-512m}" -Xmx"${SPARK_EXECUTOR_MEMORY:-512m}" \
      -cp "$SPARK_CLASSPATH" \
      org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend \
      --driver-url "${SPARK_DRIVER_URL:-}" \
      --executor-id "${SPARK_EXECUTOR_ID:-}" \
      --cores "${SPARK_EXECUTOR_CORES:-1}" \
      --app-id "${SPARK_APPLICATION_ID:-}" \
      --hostname "${SPARK_EXECUTOR_POD_IP:-}" \
      --resourceProfileId "${SPARK_RESOURCE_PROFILE_ID:-}" \
      --podName "${SPARK_EXECUTOR_POD_NAME:-}"
    ;;
  *)
    # Anything else is a plain command: pass-through, so the image is still
    # usable as a normal python container (`docker run ... python -c ...`).
    exec "$@"
    ;;
esac
