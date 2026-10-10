#!/usr/bin/env bash
# ==============================================================================
# scripts/demo.sh - RF-24 end-to-end demo, ON THE CLUSTER.
#
# There is no local execution profile. The demo boots the kind cluster with the
# `demo` startup profile (all subsystems), publishes events into Kafka, and
# reads the result back through Trino. Every step below is a real deployment;
# nothing is simulated in Python.
# ==============================================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "==> Verifying code (the only local step)"
rake governance:validate
rake lint
rake typecheck
rake test

echo "==> Deploying the demo profile (all 8 subsystems) to the local kind cluster"
bash scripts/profile-up.sh demo

echo "==> Waiting for the Flink speed-layer jobs to be RUNNING"
kubectl wait --for=condition=ready pod -l app=flink-orders-stream -n sales --timeout=300s || true
kubectl get flinkdeployments -A

echo "==> Publishing events into Kafka for 60 s"
rake stream:produce

echo "==> Waiting one window plus watermark before reading back"
sleep 15

echo "==> Querying the speed layer through Trino (batch + stream, RF-11)"
kubectl run trino-cli --rm -i --restart=Never --image=trinodb/trino:444 \
  --command -- trino --server http://trino.platform.svc.cluster.local:8080 \
  --execute "SELECT status, count(*) AS windows, sum(window_orders) AS orders, sum(window_revenue) AS revenue FROM stream.sales.orders_windowed GROUP BY status ORDER BY status"

echo "==> Querying the unified serving view (RF-11)"
kubectl run trino-unified --rm -i --restart=Never --image=trinodb/trino:444 \
  --command -- trino --server http://trino.platform.svc.cluster.local:8080 \
  --execute "SELECT order_date, status, total_orders, total_revenue, source_tier FROM iceberg.serving.sales_unified_revenue ORDER BY order_date DESC LIMIT 20"

echo
echo "Demo finished. Stream logs: rake stream:logs"