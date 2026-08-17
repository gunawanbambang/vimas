#!/usr/bin/env bash
# ==============================================================================
# End-to-End Test & Validation Automation Suite
# Project: elevate-505410 | Region: asia-southeast2
# ==============================================================================
set -euo pipefail

# Configurations (override via env if needed)
export PROJECT_ID="${PROJECT_ID:-elevate-505410}"
export REGION="${REGION:-asia-southeast2}"
export COMPOSER_ENV_NAME="${COMPOSER_ENV_NAME:-composer-demo}"
export IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/data-pipelines/flatfile-processor:v1.0.0"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=============================================================================="
echo "  Cloud Composer Gen 2 Flat File Pod Offloading - Test Suite"
echo "  Project:  ${PROJECT_ID}"
echo "  Region:   ${REGION}"
echo "  Composer: ${COMPOSER_ENV_NAME}"
echo "=============================================================================="

usage() {
    echo "Usage: $0 [all|infra|build|data|dags|test-negative|test-positive|clean]"
    echo "  infra         : Apply Terraform infrastructure (Buckets, GSA, IAM, Secret)"
    echo "  build         : Build and push container image via Cloud Build"
    echo "  k8s-ksa       : Apply Kubernetes Service Account (flatfile-processor-ksa)"
    echo "  data          : Generate & upload test data (15MB and 12GB+ archives)"
    echo "  dags          : Deploy Airflow DAGs to Composer environment"
    echo "  test-negative : Trigger TC-00 (reproduce in-worker 10GB eviction failure)"
    echo "  test-positive : Trigger TC-01 / TC-02 (verify Pod 50GB successful run)"
    echo "  all           : Run full end-to-end setup and validation"
    exit 1
}

step_infra() {
    echo -e "\n[Step 1] Applying Terraform Infrastructure..."
    cd "${ROOT_DIR}/terraform"
    terraform init
    terraform apply -auto-approve -var="project_id=${PROJECT_ID}" -var="region=${REGION}"
    cd "${ROOT_DIR}"
}

step_build() {
    echo -e "\n[Step 2] Building container image via Google Cloud Build..."
    cd "${ROOT_DIR}/docker"
    gcloud builds submit --project="${PROJECT_ID}" --tag="${IMAGE_TAG}" .
    cd "${ROOT_DIR}"
}

step_ksa() {
    echo -e "\n[Step 3] Configuring Kubernetes Service Account & Workload Identity..."
    echo "Retrieving GKE cluster credentials for Composer environment '${COMPOSER_ENV_NAME}'..."
    gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --format="value(config.gkeCluster)" > /tmp/gke_cluster.txt || true
    
    if [[ -s /tmp/gke_cluster.txt ]]; then
        CLUSTER_URI=$(cat /tmp/gke_cluster.txt)
        CLUSTER_NAME=$(basename "${CLUSTER_URI}")
        CLUSTER_ZONE=$(echo "${CLUSTER_URI}" | awk -F'/locations/' '{print $2}' | awk -F'/clusters/' '{print $1}')
        
        echo "Connecting to GKE Autopilot Cluster: ${CLUSTER_NAME} in ${CLUSTER_ZONE}..."
        gcloud container clusters get-credentials "${CLUSTER_NAME}" \
            --region="${CLUSTER_ZONE}" \
            --project="${PROJECT_ID}"
        
        echo "Applying KSA manifest..."
        kubectl apply -f "${ROOT_DIR}/k8s/ksa.yaml"
        echo "KSA applied successfully."
    else
        echo "WARNING: Could not auto-detect GKE cluster from Composer environment. Ensure kubectl is configured manually."
    fi
}

step_data() {
    echo -e "\n[Step 4] Generating synthetic test archives..."
    python3 "${ROOT_DIR}/scripts/generate_synthetic_test_data.py" --mode=small --output-dir="${ROOT_DIR}/test_data"
    
    echo "Uploading test archives to Landing GCS bucket: gs://${PROJECT_ID}-flatfile-landing/test/..."
    gcloud storage cp "${ROOT_DIR}/test_data"/* "gs://${PROJECT_ID}-flatfile-landing/test/"
    echo "Test data uploaded."
}

step_dags() {
    echo -e "\n[Step 5] Deploying DAGs to Cloud Composer environment..."
    gcloud composer environments storage dags import \
        --environment="${COMPOSER_ENV_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --source="${ROOT_DIR}/dags/flatfile_direct_load_ingestion.py"

    gcloud composer environments storage dags import \
        --environment="${COMPOSER_ENV_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --source="${ROOT_DIR}/dags/dag_test_worker_storage_failure.py"

    gcloud composer environments storage dags import \
        --environment="${COMPOSER_ENV_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        --source="${ROOT_DIR}/dags/dag_test_pod_storage_success.py"

    echo "DAGs deployed successfully."
}

step_test_negative() {
    echo -e "\n[TC-00] Triggering Negative Test (Worker 10GB storage failure)..."
    gcloud composer environments run "${COMPOSER_ENV_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        dags trigger -- test_worker_ephemeral_storage_failure

    echo "Monitoring Cloud Logging for worker eviction error..."
    echo "Run the following command to observe the 10239Mi eviction in Cloud Logging:"
    echo '  gcloud logging read '\''resource.type="k8s_pod" AND jsonPayload.reason="Evicted"'\'' --project="'"${PROJECT_ID}"'" --limit=10'
}

step_test_positive() {
    echo -e "\n[TC-01 / TC-02] Triggering Positive Test (Pod 50GB storage success)..."
    gcloud composer environments run "${COMPOSER_ENV_NAME}" \
        --location="${REGION}" \
        --project="${PROJECT_ID}" \
        dags trigger -- test_pod_ephemeral_storage_success

    echo "Monitoring Pod logs in composer-user-workloads namespace..."
    echo "Check GCS output directory:"
    echo "  gcloud storage ls gs://${PROJECT_ID}-flatfile-staging/test_output/"
}

CMD="${1:-all}"
case "${CMD}" in
    infra)         step_infra ;;
    build)         step_build ;;
    k8s-ksa)       step_ksa ;;
    data)          step_data ;;
    dags)          step_dags ;;
    test-negative) step_test_negative ;;
    test-positive) step_test_positive ;;
    all)
        step_infra
        step_build
        step_ksa
        step_data
        step_dags
        echo -e "\n=============================================================================="
        echo "  Setup Complete! Ready to execute tests:"
        echo "    1. Negative Test : $0 test-negative"
        echo "    2. Positive Test : $0 test-positive"
        echo "=============================================================================="
        ;;
    *) usage ;;
esac
