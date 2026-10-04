#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - Profile Activation Script
# Activates one of: platform | stream | batch | demo
# Tuned for 8 GB RAM local constraint (RC-02)
#
# CONTRACT (K-B): infra/profiles/<profile>.yaml holds ONE section per chart
# under `subsystems:` whose keys are verbatim Helm values for that chart
# (verified 1:1 against infra/helm/<chart>/values.yaml). This script extracts
# each chart's section with python3/PyYAML into a temp file and passes it as
# `helm upgrade --install <release> <chartdir> -n platform -f <tmp>`.
# A chart with no section installs with chart defaults. No template reads
# `.Values.subsystems` anymore.
# ==============================================================================
set -euo pipefail

PROFILE="${1:-demo}"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Per-step accounting: never report success when a critical step failed.
failures=0
deployed=0

TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

# Extracts subsystems.<chart> from the profile YAML into $TMPDIR/<chart>.values.yaml.
# Prints the tmp path on stdout. Return codes: 0 = section written,
# 3 = chart has no section (caller installs with defaults), else hard error.
extract_chart_values() {
    local chart="$1"
    python3 -c "
import sys, yaml
chart, profile_path, out_path = sys.argv[1], sys.argv[2], sys.argv[3]
try:
    with open(profile_path) as f:
        doc = yaml.safe_load(f) or {}
except FileNotFoundError:
    print(f'profile file not found: {profile_path}', file=sys.stderr)
    sys.exit(2)
except yaml.YAMLError as e:
    print(f'invalid YAML in {profile_path}: {e}', file=sys.stderr)
    sys.exit(2)
sec = (doc.get('subsystems') or {}).get(chart)
if sec is None:
    sys.exit(3)
with open(out_path, 'w') as out:
    yaml.safe_dump(sec, out, default_flow_style=False)
" "$chart" "$VALUES_FILE" "$TMPDIR/${chart}.values.yaml"
}

deploy_chart() {
    local chart="$1"
    if [ ! -d "infra/helm/$chart" ]; then
        echo -e "  ${RED}MISSING${NC}: infra/helm/$chart not found"
        failures=$((failures + 1))
        return 0
    fi
    echo -e "Deploying Helm chart: ${GREEN}${chart}${NC}"
    extra_args=()
    extract_rc=0
    extract_chart_values "$chart" >"$TMPDIR/${chart}.path" || extract_rc=$?
    if [ "$extract_rc" -eq 0 ]; then
        echo -e "  profile section -> -f $TMPDIR/${chart}.values.yaml"
        extra_args=(-f "$TMPDIR/${chart}.values.yaml")
    elif [ "$extract_rc" -eq 3 ]; then
        echo -e "  ${YELLOW}no '${chart}' section in profile; installing with chart defaults${NC}"
    else
        echo -e "  ${RED}FAILED${NC}: could not extract '${chart}' section from $VALUES_FILE"
        failures=$((failures + 1))
        return 0
    fi
    if helm upgrade --install "$chart" "infra/helm/$chart" -n platform "${extra_args[@]}"; then
        echo -e "  ${GREEN}OK${NC}: ${chart}"
        deployed=$((deployed + 1))
    else
        echo -e "  ${RED}FAILED${NC}: ${chart}"
        failures=$((failures + 1))
    fi
    return 0
}

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}   ACTIVATING OPERATIONAL PROFILE: ${YELLOW}$(printf '%s' "$PROFILE" | tr '[:lower:]' '[:upper:]')${NC}"
echo -e "${BLUE}==================================================================${NC}"

VALUES_FILE="infra/profiles/${PROFILE}.yaml"
if [ ! -f "$VALUES_FILE" ]; then
    echo -e "${RED}Error: Profile configuration $VALUES_FILE not found!${NC}"
    echo -e "Available profiles: platform, stream, batch, demo"
    exit 1
fi

if ! command -v helm >/dev/null 2>&1; then
    echo -e "${RED}Error: helm is not installed or not on PATH.${NC}"
    exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
    echo -e "${RED}Error: python3 is not installed or not on PATH (needed for section extraction).${NC}"
    exit 1
fi

case "$PROFILE" in
    platform)
        echo -e "${YELLOW}Deploying Platform Base: MinIO, PostgreSQL, Kafka, Polaris, Marquez...${NC}"
        deploy_chart s1-ingestion
        deploy_chart s2-lakehouse
        deploy_chart s7-fabric
        ;;
    stream)
        echo -e "${YELLOW}Deploying Stream/Kappa Profile: Kafka, Flink Operator, Serving Views...${NC}"
        deploy_chart s1-ingestion
        deploy_chart s2-lakehouse
        deploy_chart s4-stream
        deploy_chart s5-serving
        ;;
    batch)
        echo -e "${YELLOW}Deploying Batch/Lambda Profile: Spark on K8s, Medallion, Dagster, Trino...${NC}"
        deploy_chart s1-ingestion
        deploy_chart s2-lakehouse
        deploy_chart s3-batch
        deploy_chart s5-serving
        ;;
    demo)
        echo -e "${YELLOW}Deploying DEMO Profile (RF-24): All subsystems active in lightweight mode...${NC}"
        for chart in s1-ingestion s2-lakehouse s3-batch s4-stream s5-serving s6-mesh s7-fabric s8-ops; do
            deploy_chart "$chart"
        done
        ;;
    *)
        echo -e "${RED}Unknown profile: $PROFILE. Choose from: platform, stream, batch, demo${NC}"
        exit 1
        ;;
esac

echo ""
if [ "$failures" -gt 0 ]; then
    echo -e "${RED}Profile '${PROFILE}' FAILED: ${deployed} chart(s) deployed, ${failures} step(s) failed.${NC}"
    exit 1
fi
echo -e "${GREEN}Profile '${PROFILE}' applied successfully! (${deployed} chart(s) deployed)${NC}"
