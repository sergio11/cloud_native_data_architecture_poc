#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - Profile Deactivation Script
# Usage: profile-down.sh [profile]   (default: demo)
# Coherent inverse of profile-up.sh (K-B): uninstalls exactly the Helm releases
# that profile-up.sh installs for the given profile (release name == chart
# directory name, namespace platform). Namespaces/PVCs are left intact so
# persistent state survives a down/up cycle.
# ==============================================================================
set -euo pipefail

PROFILE="${1:-demo}"

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

failures=0
removed=0

uninstall_chart() {
    local chart="$1"
    echo -e "Removing Helm release: ${YELLOW}${chart}${NC}"
    if helm uninstall "$chart" -n platform; then
        echo -e "  ${GREEN}OK${NC}: ${chart} uninstalled"
        removed=$((removed + 1))
    else
        echo -e "  ${YELLOW}SKIP/FAILED${NC}: ${chart} (maybe not installed)"
        failures=$((failures + 1))
    fi
    return 0
}

if ! command -v helm >/dev/null 2>&1; then
    echo -e "${RED}Error: helm is not installed or not on PATH.${NC}"
    exit 1
fi

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}   DEACTIVATING OPERATIONAL PROFILE: ${YELLOW}$(printf '%s' "$PROFILE" | tr '[:lower:]' '[:upper:]')${NC}"
echo -e "${BLUE}==================================================================${NC}"

case "$PROFILE" in
    platform)
        for chart in s1-ingestion s2-lakehouse s7-fabric; do
            uninstall_chart "$chart"
        done
        ;;
    stream)
        for chart in s1-ingestion s2-lakehouse s4-stream s5-serving; do
            uninstall_chart "$chart"
        done
        ;;
    batch)
        for chart in s1-ingestion s2-lakehouse s3-batch s5-serving; do
            uninstall_chart "$chart"
        done
        ;;
    demo)
        for chart in s1-ingestion s2-lakehouse s3-batch s4-stream s5-serving s6-mesh s7-fabric s8-ops; do
            uninstall_chart "$chart"
        done
        ;;
    *)
        echo -e "${RED}Unknown profile: $PROFILE. Choose from: platform, stream, batch, demo${NC}"
        exit 1
        ;;
esac

echo ""
if [ "$failures" -gt 0 ]; then
    echo -e "${RED}Profile '${PROFILE}' deactivation finished with ${failures} failed/skipped step(s) (${removed} release(s) removed).${NC}"
    exit 1
fi
echo -e "${GREEN}Profile '${PROFILE}' removed successfully! (${removed} release(s) uninstalled)${NC}"
