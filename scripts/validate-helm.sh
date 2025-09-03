#!/usr/bin/env bash
# Helm chart validation script
# This script validates Helm charts for syntax, schema compliance, and best practices
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if helm is available
check_helm() {
    if ! command -v helm >/dev/null 2>&1; then
        log_error "helm not found in PATH"
        return 1
    fi
    log_info "Found helm: $(helm version --short)"
}

# Check if kubeconform is available
check_kubeconform() {
    if ! command -v kubeconform >/dev/null 2>&1; then
        log_warning "kubeconform not found - skipping Kubernetes schema validation"
        return 1
    fi
    log_info "Found kubeconform: $(kubeconform -version 2>&1 | head -1)"
}

# Check if helm-docs is available
check_helm_docs() {
    if ! command -v helm-docs >/dev/null 2>&1; then
        log_warning "helm-docs not found - skipping documentation validation"
        return 1
    fi
    log_info "Found helm-docs"
}

# Validate a single Helm chart
validate_chart() {
    local chart_dir="$1"
    local chart_name=$(basename "$chart_dir")

    log_info "Validating chart: $chart_name"

    # Check if Chart.yaml exists
    if [[ ! -f "$chart_dir/Chart.yaml" ]]; then
        log_error "Chart.yaml not found in $chart_dir"
        return 1
    fi

    # Run helm lint
    log_info "Running helm lint..."
    if ! helm lint "$chart_dir"; then
        log_error "helm lint failed for $chart_name"
        return 1
    fi

    # Run helm template and validate with kubeconform if available
    if check_kubeconform >/dev/null 2>&1; then
        log_info "Running kubeconform validation..."
        if ! helm template "validate-$chart_name" "$chart_dir" | kubeconform -strict -summary; then
            log_error "kubeconform validation failed for $chart_name"
            return 1
        fi
    fi

    log_success "Chart $chart_name validation passed"
}

# Update documentation with helm-docs if available
update_docs() {
    if check_helm_docs >/dev/null 2>&1; then
        log_info "Updating documentation with helm-docs..."
        if ! (cd "$REPO_ROOT" && helm-docs); then
            log_error "helm-docs failed"
            return 1
        fi
        log_success "Documentation updated"
    fi
}

# Main validation function
validate_helm_charts() {
    local helm_dir="$REPO_ROOT/helm"

    if [[ ! -d "$helm_dir" ]]; then
        log_error "Helm directory not found: $helm_dir"
        return 1
    fi

    log_info "Starting Helm chart validation..."

    # Check prerequisites
    check_helm || return 1

    # Find all chart directories
    local chart_dirs=()
    while IFS= read -r -d '' chart_file; do
        chart_dirs+=("$(dirname "$chart_file")")
    done < <(find "$helm_dir" -name "Chart.yaml" -type f -print0)

    if [[ ${#chart_dirs[@]} -eq 0 ]]; then
        log_warning "No Helm charts found in $helm_dir"
        return 0
    fi

    log_info "Found ${#chart_dirs[@]} Helm chart(s)"

    # Validate each chart
    local failed_charts=()
    for chart_dir in "${chart_dirs[@]}"; do
        if ! validate_chart "$chart_dir"; then
            failed_charts+=("$(basename "$chart_dir")")
        fi
    done

    # Update documentation
    update_docs

    # Report results
    if [[ ${#failed_charts[@]} -eq 0 ]]; then
        log_success "All Helm charts passed validation!"
        return 0
    else
        log_error "Failed charts: ${failed_charts[*]}"
        return 1
    fi
}

# Run validation if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    validate_helm_charts
fi
