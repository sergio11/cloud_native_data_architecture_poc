#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - Profile Activation Script
# Activates one of: platform | stream | batch | demo
# Tuned for 8 GB RAM local constraint (RC-02)
# ==============================================================================
set -euo pipefail

PROFILE="${1:-demo}"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}   ACTIVATING OPERATIONAL PROFILE: ${YELLOW}${PROFILE^^}${NC}"
echo -e "${BLUE}==================================================================${NC}"

VALUES_FILE="infra/profiles/${PROFILE}.yaml"
if [ ! -f "$VALUES_FILE" ]; then
    echo -e "${RED}Error: Profile configuration $VALUES_FILE not found!${NC}"
    echo -e "Available profiles: platform, stream, batch, demo"
    exit 1
fi

case "$PROFILE" in
    platform)
        echo -e "${YELLOW}Deploying Platform Base: MinIO, PostgreSQL, Kafka, Polaris, Marquez...${NC}"
        helm upgrade --install s1-ingestion infra/helm/s1-ingestion -n platform -f "$VALUES_FILE"
        helm upgrade --install s2-lakehouse infra/helm/s2-lakehouse -n platform -f "$VALUES_FILE"
        helm upgrade --install s7-fabric infra/helm/s7-fabric -n platform -f "$VALUES_FILE"
        ;;
    stream)
        echo -e "${YELLOW}Deploying Stream/Kappa Profile: Kafka, Flink Operator, Serving Views...${NC}"
        helm upgrade --install s1-ingestion infra/helm/s1-ingestion -n platform -f "$VALUES_FILE"
        helm upgrade --install s2-lakehouse infra/helm/s2-lakehouse -n platform -f "$VALUES_FILE"
        helm upgrade --install s4-stream infra/helm/s4-stream -n platform -f "$VALUES_FILE"
        helm upgrade --install s5-serving infra/helm/s5-serving -n platform -f "$VALUES_FILE"
        ;;
    batch)
        echo -e "${YELLOW}Deploying Batch/Lambda Profile: Spark on K8s, Medallion, Dagster, Trino...${NC}"
        helm upgrade --install s1-ingestion infra/helm/s1-ingestion -n platform -f "$VALUES_FILE"
        helm upgrade --install s2-lakehouse infra/helm/s2-lakehouse -n platform -f "$VALUES_FILE"
        helm upgrade --install s3-batch infra/helm/s3-batch -n platform -f "$VALUES_FILE"
        helm upgrade --install s5-serving infra/helm/s5-serving -n platform -f "$VALUES_FILE"
        ;;
    demo)
        echo -e "${YELLOW}Deploying DEMO Profile (RF-24): All subsystems active in lightweight mode...${NC}"
        for chart in s1-ingestion s2-lakehouse s3-batch s4-stream s5-serving s6-mesh s7-fabric s8-ops; do
            if [ -d "infra/helm/$chart" ]; then
                echo -e "Deploying Helm chart: ${GREEN}$chart${NC}"
                helm upgrade --install "$chart" "infra/helm/$chart" -n platform -f "$VALUES_FILE" || true
            fi
        done
        ;;
    *)
        echo -e "${RED}Unknown profile: $PROFILE. Choose from: platform, stream, batch, demo${NC}"
        exit 1
        ;;
esac

echo -e "\n${GREEN}Profile '${PROFILE}' applied successfully!${NC}"
