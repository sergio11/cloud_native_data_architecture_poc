#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - Profile Deactivation Script
# Scales down running compute pods to free memory
# ==============================================================================
set -euo pipefail

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

failures=0
scaled=0

if ! command -v kubectl >/dev/null 2>&1; then
    echo -e "${RED}Error: kubectl is not installed or not on PATH.${NC}"
    exit 1
fi

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}   DEACTIVATING COMPUTE WORKLOADS & RECLAIMING RAM                ${NC}"
echo -e "${BLUE}==================================================================${NC}"

# One probe up front: distinguishes "cluster unreachable" (hard failure)
# from "namespace does not exist" (nothing to do).
if ! namespaces=$(kubectl get namespaces -o name 2>/dev/null); then
    echo -e "${RED}Error: cannot reach the Kubernetes cluster ('kubectl get namespaces' failed).${NC}"
    exit 1
fi

# Scale down deployments in platform and domain namespaces
for ns in platform sales marketing finance ops; do
    echo -e "Scaling down deployments in namespace: ${YELLOW}$ns${NC}"
    if ! printf '%s\n' "$namespaces" | grep -q "^namespace/${ns}$"; then
        echo -e "  Namespace '${ns}' does not exist - nothing to scale."
        continue
    fi
    if ! deployments=$(kubectl get deployment -o name -n "$ns" 2>/dev/null); then
        echo -e "  ${RED}FAILED${NC}: could not list deployments in namespace '${ns}'"
        failures=$((failures + 1))
        continue
    fi
    if [ -z "$deployments" ]; then
        echo -e "  No deployments found in namespace '${ns}'."
        continue
    fi
    if kubectl scale deployment --all --replicas=0 -n "$ns"; then
        echo -e "  ${GREEN}OK${NC}: scaled down deployments in '${ns}'"
        scaled=$((scaled + 1))
    else
        echo -e "  ${RED}FAILED${NC}: could not scale deployments in namespace '${ns}'"
        failures=$((failures + 1))
    fi
done

echo ""
if [ "$failures" -gt 0 ]; then
    echo -e "${RED}Deactivation finished with ${failures} failed step(s) (${scaled} namespace(s) scaled down).${NC}"
    exit 1
fi
echo -e "${GREEN}Workloads scaled down (${scaled} namespace(s)). Core persistent state retained in PVCs.${NC}"
