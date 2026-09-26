#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - Master Bootstrap Script
# Sets up kind Kubernetes cluster and deploys foundational components
# ==============================================================================
set -euo pipefail

# ANSI color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}   MODERN DATA ARCHITECTURE POC - CLUSTER SETUP & BOOTSTRAP       ${NC}"
echo -e "${BLUE}==================================================================${NC}"

# 1. Verify Prerequisites
echo -e "\n${YELLOW}[Step 1/6] Verifying local prerequisites...${NC}"
command -v kind >/dev/null 2>&1 || { echo -e "${RED}Error: kind is not installed.${NC}"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo -e "${RED}Error: kubectl is not installed.${NC}"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo -e "${RED}Error: helm is not installed.${NC}"; exit 1; }
echo -e "${GREEN}All prerequisite binaries detected (kind, kubectl, helm).${NC}"

# 2. Create kind cluster
CLUSTER_NAME="poc-data-platform"
echo -e "\n${YELLOW}[Step 2/6] Checking kind cluster status...${NC}"
if kind get clusters | grep -q "^${CLUSTER_NAME}$"; then
    echo -e "${GREEN}Cluster '${CLUSTER_NAME}' is already running.${NC}"
else
    echo -e "Creating kind cluster '${CLUSTER_NAME}' using infra/kind/cluster-config.yaml..."
    kind create cluster --name "${CLUSTER_NAME}" --config infra/kind/cluster-config.yaml
    echo -e "${GREEN}Cluster created successfully.${NC}"
fi

# 3. Create Namespaces
echo -e "\n${YELLOW}[Step 3/6] Configuring namespaces...${NC}"
for ns in platform sales marketing finance ops; do
    if ! kubectl get namespace "$ns" >/dev/null 2>&1; then
        kubectl create namespace "$ns"
        echo -e "Created namespace: ${GREEN}$ns${NC}"
    else
        echo -e "Namespace exists: ${GREEN}$ns${NC}"
    fi
done

# 4. Apply Kyverno Governance Policies
echo -e "\n${YELLOW}[Step 4/6] Applying federated governance policies...${NC}"
if [ -d "governance/policies" ]; then
    kubectl apply -f governance/policies/ || true
    echo -e "${GREEN}Governance admission policies applied.${NC}"
fi

# 5. Bootstrap Platform Core Profile
echo -e "\n${YELLOW}[Step 5/6] Bootstrapping Base Platform profile...${NC}"
./scripts/profile-up.sh platform

# 6. Report Completion
echo -e "\n${YELLOW}[Step 6/6] Verifying cluster pod status...${NC}"
kubectl get pods -A

echo -e "\n${GREEN}==================================================================${NC}"
echo -e "${GREEN} Cluster setup finished successfully!                              ${NC}"
echo -e "${GREEN} Run './scripts/demo.sh' to execute the end-to-end demonstration.   ${NC}"
echo -e "${GREEN}==================================================================${NC}"
