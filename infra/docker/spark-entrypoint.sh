#!/usr/bin/env bash
# Conventional Spark image entrypoint: the operator starts the pod with
# "driver"/"executor" as first argument and expects the image to dispatch.
# The driver needs --deploy-mode client because the operator already created the pod.
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
    # Any other first argument is a plain command: run it as-is.
    exec "$@"
    ;;
esac
