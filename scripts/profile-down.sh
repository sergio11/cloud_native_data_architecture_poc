#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - Profile Deactivation Script
# Scales down running compute pods to free memory
# ==============================================================================
set -euo pipefail

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}   DEACTIVATING COMPUTE WORKLOADS & RECLAIMING RAM                ${NC}"
echo -e "${BLUE}==================================================================${NC}"

# Scale down deployments in platform and domain namespaces
for ns in platform sales marketing finance ops; do
    echo -e "Scaling down deployments in namespace: ${YELLOW}$ns${NC}"
    kubectl scale deployment --all --replicas=0 -n "$ns" 2>/dev/null || true
done

echo -e "\n${GREEN}Workloads scaled down. Core persistent state retained in PVCs.${NC}"
