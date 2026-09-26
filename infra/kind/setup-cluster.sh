#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - Kind Cluster Setup Script
# File: infra/kind/setup-cluster.sh
# Description: Validates system prerequisites, initializes host persistence
#              directories, provisions a 3-node Kind cluster, installs storage
#              provisioners, creates platform namespaces, and configures Helm repos.
# ==============================================================================

set -euo pipefail

# ------------------------------------------------------------------------------
# Color Definitions and Terminal Output Helpers
# ------------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info() {
    printf "${BLUE}[INFO]${NC} %s\n" "$*"
}

log_success() {
    printf "${GREEN}[SUCCESS]${NC} %s\n" "$*"
}

log_warn() {
    printf "${YELLOW}[WARN]${NC} %s\n" "$*"
}

log_error() {
    printf "${RED}[ERROR]${NC} %s\n" "$*" >&2
}

log_step() {
    printf "\n${CYAN}===> %s${NC}\n" "$*"
}

# ------------------------------------------------------------------------------
# Script Directory and Path Configuration
# ------------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/cluster-config.yaml"
CLUSTER_NAME="poc-data-platform"

# ------------------------------------------------------------------------------
# Display Help Banner
# ------------------------------------------------------------------------------
display_help() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Provisions a multi-node Kubernetes in Docker (kind) cluster for the Modern Data Architecture POC.

Options:
  -c, --cluster-name NAME   Override default cluster name (default: poc-data-platform)
  -f, --force-recreate      Delete and recreate cluster if it already exists
  -s, --skip-prereqs        Skip container runtime and CLI prerequisite verification
  -h, --help                Show this help message and exit

Environment Variables:
  CONTAINER_RUNTIME         Force container runtime ('docker' or 'podman')
  KIND_EXPERIMENTAL_PROVIDER Set to 'podman' when using rootless podman

Examples:
  ./infra/kind/setup-cluster.sh
  ./infra/kind/setup-cluster.sh --force-recreate
EOF
}

# ------------------------------------------------------------------------------
# Parse Command Line Arguments
# ------------------------------------------------------------------------------
FORCE_RECREATE=false
SKIP_PREREQS=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--cluster-name)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        -f|--force-recreate)
            FORCE_RECREATE=true
            shift
            ;;
        -s|--skip-prereqs)
            SKIP_PREREQS=true
            shift
            ;;
        -h|--help)
            display_help
            exit 0
            ;;
        *)
            log_error "Unknown argument: $1"
            display_help
            exit 1
            ;;
    esac
done

# ------------------------------------------------------------------------------
# Step 1: Verify Prerequisites
# ------------------------------------------------------------------------------
verify_prerequisites() {
    log_step "Verifying system prerequisites and required CLI tools..."
    local missing_tools=()

    # Detect container runtime (podman or docker)
    local container_runtime=""
    if [[ -n "${CONTAINER_RUNTIME:-}" ]]; then
        container_runtime="${CONTAINER_RUNTIME}"
    elif command -v docker &>/dev/null; then
        container_runtime="docker"
    elif command -v podman &>/dev/null; then
        container_runtime="podman"
    fi

    if [[ -z "${container_runtime}" ]]; then
        missing_tools+=("docker or podman (container runtime)")
    else
        log_info "Detected container runtime: ${container_runtime}"
        if [[ "${container_runtime}" == "podman" ]]; then
            export KIND_EXPERIMENTAL_PROVIDER=podman
            log_info "Set KIND_EXPERIMENTAL_PROVIDER=podman for kind compatibility"
        fi
        # Test if container runtime daemon is running
        if ! ${container_runtime} info &>/dev/null; then
            log_error "Container runtime '${container_runtime}' daemon is not responsive. Please start the engine."
            exit 1
        fi
    fi

    # Check kind
    if ! command -v kind &>/dev/null; then
        missing_tools+=("kind (Kubernetes in Docker CLI - https://kind.sigs.k8s.io/)")
    else
        log_info "Found kind: $(kind version | head -n1)"
    fi

    # Check kubectl
    if ! command -v kubectl &>/dev/null; then
        missing_tools+=("kubectl (Kubernetes CLI - https://kubernetes.io/docs/tasks/tools/)")
    else
        log_info "Found kubectl: $(kubectl version --client=true -o yaml 2>/dev/null | grep gitVersion | head -n1 | tr -d ' ' || echo 'available')"
    fi

    # Check helm
    if ! command -v helm &>/dev/null; then
        missing_tools+=("helm (Package manager for Kubernetes - https://helm.sh/)")
    else
        log_info "Found helm: $(helm version --short 2>/dev/null || echo 'available')"
    fi

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        log_error "Missing required tools:"
        for tool in "${missing_tools[@]}"; do
            printf "  - %s\n" "${tool}"
        done
        exit 1
    fi

    log_success "All prerequisite CLI tools are available."
}

# ------------------------------------------------------------------------------
# Step 2: Initialize Host Storage Directories
# ------------------------------------------------------------------------------
initialize_host_storage() {
    log_step "Initializing host persistence directories..."
    local host_data_dirs=(
        "${REPO_ROOT}/data/control-plane"
        "${REPO_ROOT}/data/worker-storage"
        "${REPO_ROOT}/data/worker-compute"
    )

    for dir in "${host_data_dirs[@]}"; do
        if [[ ! -d "${dir}" ]]; then
            log_info "Creating directory: ${dir}"
            mkdir -p "${dir}"
        else
            log_info "Directory exists: ${dir}"
        fi
    done
    log_success "Host persistence directories initialized."
}

# ------------------------------------------------------------------------------
# Step 3: Create Kind Cluster
# ------------------------------------------------------------------------------
provision_cluster() {
    log_step "Checking Kind cluster status..."

    local existing_clusters
    existing_clusters="$(kind get clusters 2>/dev/null || true)"

    if echo "${existing_clusters}" | grep -q "^${CLUSTER_NAME}$"; then
        if [[ "${FORCE_RECREATE}" == "true" ]]; then
            log_warn "Cluster '${CLUSTER_NAME}' already exists. Force-recreate requested. Deleting..."
            kind delete cluster --name "${CLUSTER_NAME}"
        else
            log_info "Cluster '${CLUSTER_NAME}' already exists. Skipping creation."
            kubectl config use-context "kind-${CLUSTER_NAME}"
            return 0
        fi
    fi

    log_info "Creating Kind cluster '${CLUSTER_NAME}' using configuration: ${CONFIG_FILE}"
    kind create cluster --name "${CLUSTER_NAME}" --config "${CONFIG_FILE}" --wait 180s

    log_info "Setting active kubectl context to 'kind-${CLUSTER_NAME}'..."
    kubectl config use-context "kind-${CLUSTER_NAME}"

    log_success "Kind cluster '${CLUSTER_NAME}' successfully provisioned."
}

# ------------------------------------------------------------------------------
# Step 4: Verify and Label Worker Nodes
# ------------------------------------------------------------------------------
configure_node_roles() {
    log_step "Verifying and labeling cluster nodes..."

    # Wait for all nodes to be in Ready state
    log_info "Waiting for all cluster nodes to reach Ready state..."
    kubectl wait --for=condition=Ready nodes --all --timeout=120s

    # Ensure worker nodes have explicit role labels
    local storage_node
    storage_node="$(kubectl get nodes -l node-role.kubernetes.io/storage=true -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [[ -n "${storage_node}" ]]; then
        kubectl label node "${storage_node}" node-role.kubernetes.io/worker=true --overwrite
        log_info "Storage worker node identified: ${storage_node}"
    fi

    local compute_node
    compute_node="$(kubectl get nodes -l node-role.kubernetes.io/compute=true -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
    if [[ -n "${compute_node}" ]]; then
        kubectl label node "${compute_node}" node-role.kubernetes.io/worker=true --overwrite
        log_info "Compute worker node identified: ${compute_node}"
    fi

    log_info "Current cluster node topology:"
    kubectl get nodes -o wide --show-labels
    log_success "Node roles configured."
}

# ------------------------------------------------------------------------------
# Step 5: Install Storage Provisioner and Configure StorageClasses
# ------------------------------------------------------------------------------
configure_storage() {
    log_step "Configuring Kubernetes StorageClass and persistent volume provisioner..."

    # Kind comes with standard Rancher local-path-provisioner pre-installed.
    # We verify it and ensure the default storage class is properly marked.
    local default_sc
    default_sc="$(kubectl get sc -o jsonpath='{.items[?(@.metadata.annotations.storageclass\.kubernetes\.io/is-default-class=="true")].metadata.name}' 2>/dev/null || true)"

    if [[ -z "${default_sc}" ]]; then
        log_info "Marking 'standard' storageclass as default..."
        kubectl annotate storageclass standard storageclass.kubernetes.io/is-default-class="true" --overwrite || true
    else
        log_info "Default storageclass already configured: ${default_sc}"
    fi

    # Create local-storage StorageClass with WaitForFirstConsumer binding
    cat <<'EOF' | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-path-fast
provisioner: rancher.io/local-path
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
EOF

    log_success "StorageClasses successfully verified."
}

# ------------------------------------------------------------------------------
# Step 6: Create Namespaces for Data Platform Architecture
# ------------------------------------------------------------------------------
create_namespaces() {
    log_step "Creating core and domain namespaces..."

    local namespaces=(
        "platform:Infrastructure, catalogs, message broker, object storage"
        "sales:Sales domain data products and pipelines"
        "marketing:Marketing domain data products and funnels"
        "finance:Finance domain data products and ledger"
        "ops:Observability, orchestration, and business intelligence"
    )

    for item in "${namespaces[@]}"; do
        local ns="${item%%:*}"
        local desc="${item##*:}"

        if ! kubectl get namespace "${ns}" &>/dev/null; then
            log_info "Creating namespace '${ns}' (${desc})..."
            kubectl create namespace "${ns}"
            kubectl label namespace "${ns}" \
                data-mesh.io/domain="${ns}" \
                data-platform.io/managed="true" \
                data-platform.io/description="${desc}" \
                --overwrite
        else
            log_info "Namespace '${ns}' already exists. Updating labels..."
            kubectl label namespace "${ns}" \
                data-mesh.io/domain="${ns}" \
                data-platform.io/managed="true" \
                --overwrite
        fi
    done

    log_success "Platform and domain namespaces ready."
}

# ------------------------------------------------------------------------------
# Step 7: Register Required Helm Repositories
# ------------------------------------------------------------------------------
setup_helm_repositories() {
    log_step "Registering official Helm chart repositories..."

    declare -A helm_repos=(
        ["strimzi"]="https://strimzi.io/charts/"
        ["minio"]="https://charts.min.io/"
        ["trino"]="https://trinodb.github.io/charts"
        ["dagster"]="https://dagster-io.github.io/helm"
        ["prometheus-community"]="https://prometheus-community.github.io/helm-charts"
        ["kyverno"]="https://kyverno.github.io/kyverno/"
        ["spark-operator"]="https://kubeflow.github.io/spark-operator"
    )

    for repo_name in "${!helm_repos[@]}"; do
        local repo_url="${helm_repos[${repo_name}]}"
        log_info "Adding Helm repo '${repo_name}' (${repo_url})..."
        helm repo add "${repo_name}" "${repo_url}" --force-update 2>/dev/null || \
            log_warn "Could not reach remote repo '${repo_name}'. Local cached charts will be used."
    done

    log_info "Updating Helm repository indexes..."
    helm repo update 2>/dev/null || log_warn "Helm repo update encountered warnings (offline mode)."

    log_success "Helm repositories configured."
}

# ------------------------------------------------------------------------------
# Main Execution Entrypoint
# ------------------------------------------------------------------------------
main() {
    printf "\n"
    printf "========================================================================\n"
    printf " Modern Data Architecture POC - Kubernetes Kind Cluster Setup\n"
    printf "========================================================================\n"
    printf "\n"

    if [[ "${SKIP_PREREQS}" != "true" ]]; then
        verify_prerequisites
    fi

    initialize_host_storage
    provision_cluster
    configure_node_roles
    configure_storage
    create_namespaces
    setup_helm_repositories

    printf "\n"
    printf "========================================================================\n"
    log_success "Kind cluster '${CLUSTER_NAME}' is ready!"
    printf "========================================================================\n"
    printf "  - Kubernetes API:   https://127.0.0.1:6443\n"
    printf "  - Storage Node:     worker (node-role.kubernetes.io/storage)\n"
    printf "  - Compute Node:     worker2 (node-role.kubernetes.io/compute)\n"
    printf "  - Platform Web UIs:\n"
    printf "      * Grafana:      http://localhost:3000\n"
    printf "      * Dagster:      http://localhost:3001\n"
    printf "      * Marquez:      http://localhost:3002\n"
    printf "      * Metabase:     http://localhost:3003\n"
    printf "      * Trino:        http://localhost:8080\n"
    printf "      * Kafka UI:     http://localhost:8090\n"
    printf "      * MinIO API:    http://localhost:9000\n"
    printf "      * MinIO UI:     http://localhost:9001\n"
    printf "      * Polaris REST: http://localhost:8181\n"
    printf "========================================================================\n\n"
}

main "$@"
