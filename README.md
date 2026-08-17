# Cloud Composer Gen 2: Flat File Decompression & Decryption Offloading Runbook

This guide provides step-by-step instructions to deploy, configure, and validate the offloaded flat file decompression and decryption architecture using **`KubernetesPodOperator`** in **Cloud Composer Gen 2** (`asia-southeast2` / Jakarta).

---

## 1. Architecture Overview

### The Problem
When Cloud Composer Gen 2 Celery workers decompress or decrypt large files (>10 GB uncompressed) locally, they exceed the worker container's hard limit and fail with:
```
Pod ephemeral local storage usage exceeds the total limit of containers 10239Mi
```

### The Solution
We offload Step 7 of the ingestion pipeline to an ephemeral Kubernetes Pod running within the `composer-user-workloads` namespace on Composer's underlying GKE Autopilot cluster:
- **Dedicated Ephemeral Storage:** `50 GiB` (auto-provisioned per pod, scales on-demand).
- **Compute Isolation:** `2 vCPU`, `4 GiB RAM` allocated independently of Composer workers.
- **Zero Idle Cost:** Pod terminates and cleans up immediately upon completion (`is_delete_operator_pod=True`).
- **Security:** Secret Manager keys fetched at runtime via Google Cloud Workload Identity.

```
Composer Worker (DAG Scheduler)
      │
      ▼
KubernetesPodOperator (in-cluster)
      │
      ▼
Ephemeral GKE Pod [composer-user-workloads] (50GB Ephemeral Disk)
      ├── 1. Fetch GPG Key from Secret Manager
      ├── 2. Download archive from Landing GCS
      ├── 3. Decrypt & Decompress into /tmp
      ├── 4. Upload raw CSV to Staging GCS
      └── 5. Cleanup /tmp & Terminate
      │
      ▼
Downstream BigQuery Direct Load
```

---

## 2. Directory Structure

```
vimas/
├── README.md                                  # This step-by-step execution guide
├── planvimas.md                               # Detailed architecture blueprint & decision log
├── docker/
│   ├── Dockerfile                             # Minimal Alpine + gcloud + GPG + ncompress image
│   └── entrypoint.sh                          # Multi-format decryption & decompression script
├── k8s/
│   └── ksa.yaml                               # Kubernetes Service Account manifest (Workload Identity)
├── dags/
│   ├── flatfile_direct_load_ingestion.py      # Production DAG with KubernetesPodOperator
│   ├── dag_test_worker_storage_failure.py     # TC-00 Negative Test DAG (reproduces 10GB eviction)
│   └── dag_test_pod_storage_success.py        # TC-01/TC-02 Positive Test DAG (50GB Pod Storage)
├── terraform/
│   ├── main.tf                                # Terraform provider configuration
│   ├── variables.tf                           # Variables (project_id, region, buckets, etc.)
│   ├── storage.tf                             # GCS buckets & Artifact Registry repository
│   ├── iam.tf                                 # GSA, IAM roles, Secret Manager, & Workload Identity
│   └── outputs.tf                             # Output resource identifiers
└── scripts/
    ├── generate_synthetic_test_data.py        # Generates small (15MB) and large (12GB+/32GB) test files
    └── run_test_suite.sh                      # Automated test runner and orchestration script
```

---

## 3. Prerequisites & Environment Setup

### Required Tools
Ensure you have the following installed and authenticated:
* `gcloud` (Google Cloud SDK)
* `kubectl`
* `terraform` (>= 1.5.0)
* `python3` (with `google-cloud-storage`)

### Set Environment Variables
```bash
export PROJECT_ID="elevate-505410"
export REGION="asia-southeast2"
export COMPOSER_ENV_NAME="your-composer-env-name"   # Replace with your Composer environment name
export IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/data-pipelines/flatfile-processor:v1.0.0"
```

---

## 4. Step-by-Step Deployment Guide

### Option A: Automated One-Click Setup
You can run the entire setup sequence with a single command:
```bash
./scripts/run_test_suite.sh all
```

---

### Option B: Manual Step-by-Step Execution

#### Step 1: Provision Infrastructure with Terraform
Creates GCS landing/staging buckets, Artifact Registry Docker repository, Google Service Account (`flatfile-pod-processor-sa`), Secret Manager placeholder, and IAM Workload Identity bindings.

```bash
cd terraform/
terraform init
terraform apply -var="project_id=${PROJECT_ID}" -var="region=${REGION}"
cd ..
```

#### Step 2: Build & Push the Pod Container Image
Submits the container build to Google Cloud Build and pushes the image to Artifact Registry:

```bash
cd docker/
gcloud builds submit --project="${PROJECT_ID}" --tag="${IMAGE_TAG}" .
cd ..
```

#### Step 3: Apply the Kubernetes Service Account (KSA)
Retrieve the credentials for the Composer GKE cluster and apply the KSA manifest:

```bash
# 1. Get GKE Autopilot cluster credentials from Composer
CLUSTER_URI=$(gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --format="value(config.gkeCluster)")

CLUSTER_NAME=$(basename "${CLUSTER_URI}")
CLUSTER_ZONE=$(echo "${CLUSTER_URI}" | awk -F'/locations/' '{print $2}' | awk -F'/clusters/' '{print $1}')

gcloud container clusters get-credentials "${CLUSTER_NAME}" \
    --region="${CLUSTER_ZONE}" \
    --project="${PROJECT_ID}"

# 2. Apply KSA in the composer-user-workloads namespace
kubectl apply -f k8s/ksa.yaml

# 3. Verify KSA annotation
kubectl get sa flatfile-processor-ksa -n composer-user-workloads -o yaml
```

#### Step 4: Configure GPG Decryption Secret (Optional / For Encrypted Files)
If testing encrypted files, store your GPG private key in Secret Manager:

```bash
# Store private key into Secret Manager
gcloud secrets versions add sftp-gpg-private-key \
    --data-file=/path/to/private.key \
    --project="${PROJECT_ID}"
```

#### Step 5: Generate Synthetic Test Datasets
Generate a small test file (15 MB compressed) and upload to the Landing GCS bucket:

```bash
# Generate small test dataset (TC-01)
python3 scripts/generate_synthetic_test_data.py --mode=small --output-dir=./test_data

# Upload to landing bucket test folder
gcloud storage cp ./test_data/* gs://${PROJECT_ID}-flatfile-landing/test/
```

To generate the large 12 GB+ archive (expands to 32 GB CSV) for TC-00/TC-02:
```bash
python3 scripts/generate_synthetic_test_data.py --mode=large --output-dir=./test_data
gcloud storage cp ./test_data/large_12gb_archive.zip gs://${PROJECT_ID}-flatfile-landing/test/
```

#### Step 6: Deploy Airflow DAGs to Cloud Composer
Upload the production and test DAGs to Cloud Composer's DAG bucket:

```bash
# 1. Production DAG
gcloud composer environments storage dags import \
    --environment="${COMPOSER_ENV_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --source="dags/flatfile_direct_load_ingestion.py"

# 2. Negative Test DAG (Failure Reproduction)
gcloud composer environments storage dags import \
    --environment="${COMPOSER_ENV_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --source="dags/dag_test_worker_storage_failure.py"

# 3. Positive Test DAG (Pod Validation)
gcloud composer environments storage dags import \
    --environment="${COMPOSER_ENV_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --source="dags/dag_test_pod_storage_success.py"
```

---

## 5. Testing & Verification Runbook

### Test Case 00: Negative Test (Reproduce Worker 10GB Eviction)
**Objective:** Prove that processing large archives directly on the standard Celery worker fails due to local storage limits (`10239Mi`).

1. **Trigger the negative test DAG:**
   ```bash
   gcloud composer environments run "${COMPOSER_ENV_NAME}" \
       --location="${REGION}" \
       --project="${PROJECT_ID}" \
       dags trigger -- test_worker_ephemeral_storage_failure
   ```

2. **Verify the eviction error in Cloud Logging:**
   ```bash
   gcloud logging read 'resource.type="k8s_pod" AND (jsonPayload.reason="Evicted" OR textPayload=~"Pod ephemeral local storage usage exceeds the total limit of containers 10239Mi")' \
       --project="${PROJECT_ID}" \
       --limit=10 \
       --format="table(timestamp, resource.labels.pod_name, jsonPayload.message)"
   ```
   **Expected Result:** The Celery worker pod is evicted and restarted; the Airflow task is marked `failed`.

---

### Test Case 01 & 02: Positive Test (Validate Pod 50GB Execution)
**Objective:** Confirm that `KubernetesPodOperator` dynamically allocates 50 GB ephemeral storage, extracts the payload, uploads the uncompressed CSV to GCS, and terminates cleanly.

1. **Trigger the positive test DAG:**
   ```bash
   gcloud composer environments run "${COMPOSER_ENV_NAME}" \
       --location="${REGION}" \
       --project="${PROJECT_ID}" \
       dags trigger -- test_pod_ephemeral_storage_success
   ```

2. **Monitor the active Pod in the cluster:**
   ```bash
   kubectl get pods -n composer-user-workloads -w
   ```

3. **Stream live Pod execution logs:**
   ```bash
   kubectl logs -n composer-user-workloads -l app=test-pod-unzip-large-archive -c base -f
   ```

4. **Verify output in Staging GCS bucket:**
   ```bash
   gcloud storage ls -l gs://${PROJECT_ID}-flatfile-staging/test_output/
   ```
   **Expected Result:** Task completes with `exit 0`, the full uncompressed CSV is present in the staging bucket, and the ephemeral pod is automatically deleted.

---

### Test Case 03: Production Pipeline Trigger
Trigger the full end-to-end production DAG:
```bash
gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    dags trigger -- flatfile_direct_load_ingestion
```

---

## 6. Monitoring & Troubleshooting

### Check Pod Status and Events
```bash
# List all pods in the user workloads namespace
kubectl get pods -n composer-user-workloads -o wide

# Describe a specific pod to inspect resource allocations or events
kubectl describe pod <pod-name> -n composer-user-workloads
```

### Inspect GCS Buckets
```bash
# View landing files
gcloud storage ls -l gs://${PROJECT_ID}-flatfile-landing/raw/

# View staging files
gcloud storage ls -l gs://${PROJECT_ID}-flatfile-staging/unzipped/
```

### Common Gotchas & Fixes
| Symptom | Cause | Solution |
| :--- | :--- | :--- |
| `Permission denied` accessing GCS from Pod | Workload Identity not bound | Check KSA annotation and `roles/iam.workloadIdentityUser` binding in `terraform/iam.tf`. |
| `Secret not found` | `SECRET_GPG_KEY_NAME` does not exist | Create the secret in Secret Manager or verify `PROJECT_ID` environment variable. |
| Pod pending / insufficient quota | Autopilot quota limit in region | Check Compute Engine vCPU and IP address quota in `asia-southeast2`. |
| Resource requests adjusted | Autopilot requires equal requests/limits | Ensure `requests` and `limits` match in `KubernetesPodOperator` (`cpu: "2"`, `memory: "4Gi"`, `ephemeral-storage: "50Gi"`). |

---

## 7. Teardown / Cleanup

To destroy all provisioned test resources when validation is complete:

```bash
# 1. Delete DAGs from Composer
gcloud composer environments storage dags delete \
    --environment="${COMPOSER_ENV_NAME}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    dag_test_worker_storage_failure.py dag_test_pod_storage_success.py

# 2. Destroy Terraform resources
cd terraform/
terraform destroy -var="project_id=${PROJECT_ID}" -var="region=${REGION}"
cd ..
```
