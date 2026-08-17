# Architecture & Implementation Plan: Offloading Flat File Decompression & Decryption via `GKEStartPodOperator` in Cloud Composer Gen 2

## Executive Summary
This document provides a comprehensive blueprint to resolve Cloud Composer Gen 2 worker ephemeral storage bottlenecks (`Pod ephemeral local storage usage exceeds 10239Mi`) when ingesting and processing compressed (`.z`) and encrypted (`.gpg`) flat files. 

By replacing in-worker decompression/decryption (Step 7 in the Direct Load pipeline) with **`GKEStartPodOperator`**, compute and storage are dynamically offloaded to an isolated, ephemeral Kubernetes Pod with dedicated ephemeral disk space (e.g., 20–50 GiB) and custom vCPU/RAM. This eliminates the need to statically over-provision Composer workers (scaling from 2 to 4 workers) during 2:00–3:00 AM batch windows.

**Environment Parameters:**
* **GCP Project ID:** `elevate-505410`
* **GCP Region:** `asia-southeast2` (Jakarta)

---

## 1. Solution Architecture & Target State

### 1.1 Architecture Diagram
```
+---------------------------------------------------------------------------------------------------+
| Cloud Composer Gen 2 Environment (Managed Control Plane)                                          |
| Project: elevate-505410 | Region: asia-southeast2                                                  |
|                                                                                                   |
|   +-------------------------------------------------------------------------------------------+   |
|   | Airflow DAG: Flat File Ingestion Direct Load                                              |   |
|   |                                                                                           |   |
|   |  [Step 1-6] Metadata, Date Calc, SFTP Pull -> Cloud Storage (Landing Bucket)              |   |
|   |         |                                                                                 |   |
|   |         v                                                                                 |   |
|   |  [Step 7: GKEStartPodOperator]                                                            |   |
|   |         | (Bypasses Celery Worker concurrency & 10GB ephemeral disk limit)                |   |
|   |         | Launches on-demand container in GKE Autopilot cluster                           |   |
|   +---------|---------------------------------------------------------------------------------+   |
+-------------|-------------------------------------------------------------------------------------+
              |
              v
+---------------------------------------------------------------------------------------------------+
| Dedicated Ephemeral GKE Pod (Isolated Compute & Storage)                                          |
|                                                                                                   |
|   - Image: gcr.io/elevate-505410/flatfile-processor:v1.0.0 (or google/cloud-sdk:slim)             |
|   - Resources: 2 vCPU, 4 GiB RAM, 50 GiB Ephemeral Disk (Auto-scaled by Autopilot)                |
|                                                                                                   |
|   [Execution Steps]                                                                               |
|   1. Pull .z / .gpg files from Landing GCS Bucket via gcloud / gsutil                             |
|   2. Decrypt .gpg using Cloud KMS / Secret Manager private key into /tmp (up to 50GB storage)     |
|   3. Decompress .z files into raw flat CSV / TSV / delimited data                                 |
|   4. Upload decompressed payload to Raw Stage GCS Bucket                                          |
|   5. Exit 0 & Pod Automatically Deleted (Zero idle compute/storage costs)                         |
+---------------------------------------------------------------------------------------------------+
              |
              v
+---------------------------------------------------------------------------------------------------+
| Downstream Pipeline Execution (Native GCP Services)                                               |
|                                                                                                   |
|   [Step 8-14] Load from Stage GCS -> BigQuery Stage 1 & Stage 2 (Direct Load / Truncate-Insert)   |
|   [Step 15-20] BQ Job Executor -> Final ODS Table & Archive Storage                               |
|   [Step 21-22] Sync BigLake External Connection / Data Catalog / Dataplex Governance              |
+---------------------------------------------------------------------------------------------------+
```

### 1.2 Updated Decision Tree
```
                                Data Source
                                     │
                           Is Data size ≥ 2 GB?
                          ┌──────────┴──────────┐
                         YES                    NO
                          │                     │
                [Option A: Composer       Is Data size > 20 MB
                 + Data Fusion]             & < 2 GB?
                                       ┌────────┴────────┐
                                      YES                NO
                                       │                 │
                             [Option B: Composer     [Option C: Composer
                              + Pre-Warm Data         (GKE Pod for Step 7)
                                Fusion]               + BigQuery]
```

---

## 2. Step-by-Step Implementation Guide

### Step 1: Workload Identity & IAM Setup
Ensure the GKE Kubernetes Service Account (KSA) is bound to a dedicated Google Service Account (GSA) with least-privilege access:
* `roles/storage.objectAdmin` on landing and raw staging buckets (`gs://elevate-505410-flatfile-landing` and `gs://elevate-505410-flatfile-staging`).
* `roles/secretmanager.secretAccessor` (if GPG keys are fetched from Secret Manager).
* `roles/cloudkms.cryptoKeyDecrypter` (if encryption uses Cloud KMS).

### Step 2: Container Image Preparation
For production, build and push a minimal hardened container to Artifact Registry or Container Registry in `asia-southeast2`:

**Dockerfile:**
```dockerfile
FROM alpine:3.19
RUN apk add --no-cache bash curl gnupg gzip tar ncurses google-cloud-cli
WORKDIR /workspace
COPY entrypoint.sh /workspace/entrypoint.sh
RUN chmod +x /workspace/entrypoint.sh
ENTRYPOINT ["/workspace/entrypoint.sh"]
```

**Build & Push to GCP Project:**
```bash
gcloud builds submit --tag asia-southeast2-docker.pkg.dev/elevate-505410/data-pipelines/flatfile-processor:v1.0.0 .
```

### Step 3: Configure Airflow DAG Task
Replace the existing `PythonOperator` / `BashOperator` in Step 7 with `GKEStartPodOperator`:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import GKEStartPodOperator
from kubernetes.client import models as k8s

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# Define isolated compute & storage resource requests/limits
pod_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "2",
        "memory": "4Gi",
        "ephemeral-storage": "25Gi"
    },
    limits={
        "cpu": "4",
        "memory": "8Gi",
        "ephemeral-storage": "60Gi"
    }
)

with DAG(
    dag_id="flatfile_direct_load_ingestion",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    # Step 7: Offloaded Extraction & Decryption
    unzip_and_decrypt_task = GKEStartPodOperator(
        task_id="step7_unzip_decrypt_flatfile",
        name="k8s-pod-unzip-decrypt",
        namespace="composer-user-workloads",
        in_cluster=True,
        image="asia-southeast2-docker.pkg.dev/elevate-505410/data-pipelines/flatfile-processor:v1.0.0",
        container_resources=pod_resources,
        env_vars={
            "GCS_LANDING_URI": "gs://elevate-505410-flatfile-landing/raw/{{ ds_nodash }}/",
            "GCS_STAGE_URI": "gs://elevate-505410-flatfile-staging/unzipped/{{ ds_nodash }}/",
            "SECRET_GPG_KEY_NAME": "sftp-gpg-private-key",
            "PROJECT_ID": "elevate-505410",
        },
        cmds=["/workspace/entrypoint.sh"],
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=300,
    )
```

---

## 3. Terraform Infrastructure as Code (GCP)

Complete Terraform module targeting `elevate-505410` in region `asia-southeast2`.

```hcl
# --- variables.tf ---
variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "elevate-505410"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "asia-southeast2"
}

variable "composer_cluster_name" {
  description = "Name of the Composer GKE Autopilot Cluster"
  type        = string
  default     = "composer-gke-cluster"
}

# --- storage.tf ---
resource "google_storage_bucket" "landing_bucket" {
  name                        = "${var.project_id}-flatfile-landing"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = false
}

resource "google_storage_bucket" "staging_bucket" {
  name                        = "${var.project_id}-flatfile-staging"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = false
}

# --- iam_workload_identity.tf ---
resource "google_service_account" "pod_sa" {
  account_id   = "flatfile-pod-processor-sa"
  display_name = "Service Account for GKE Pod Flatfile Decompression"
  project      = var.project_id
}

resource "google_storage_bucket_iam_member" "landing_access" {
  bucket = google_storage_bucket.landing_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pod_sa.email}"
}

resource "google_storage_bucket_iam_member" "staging_access" {
  bucket = google_storage_bucket.staging_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pod_sa.email}"
}

resource "google_project_iam_member" "secret_access" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.pod_sa.email}"
}

# Workload Identity binding with Composer GKE namespace
resource "google_service_account_iam_member" "workload_identity_user" {
  service_account_id = google_service_account.pod_sa.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "serviceAccount:${var.project_id}.svc.id.goog[composer-user-workloads/default]"
}
```

---

## 4. Test & Evaluation Cases

| Test Case ID | Scenario | Input Data Specs | Expected Outcome | Verification Metric |
| :--- | :--- | :--- | :--- | :--- |
| **TC-00** | **Baseline Negative Test (Worker Failure)** | Standard Airflow Worker unzips 12 GB+ extracted flatfile | **Pod Eviction / Failure:** Task fails with `Pod ephemeral local storage usage exceeds the total limit of containers 10239Mi` | Worker pod terminated; Airflow task marked `failed`/`up_for_retry` |
| **TC-01** | Baseline Small Extraction | 15 MB `.z` archive | Extracts cleanly within 30s | Task exits 0; GCS raw directory populated |
| **TC-02** | Large Archive Exceeding Worker Limit | 8 GB `.z` expanding to 32 GB uncompressed | Pod scales to 50 GB ephemeral storage without failure | Zero `10239Mi` storage errors |
| **TC-03** | Concurrent Batch Run (2–3 AM Spike) | 10 DAG runs triggered in parallel | Autopilot launches 10 pods concurrently; Airflow worker CPU/RAM stays flat | DAG concurrency unaffected; no queue starvation |
| **TC-04** | Invalid / Corrupted Decryption Key | Wrong GPG key passed via env | Pod captures stderr, fails gracefully with non-zero exit code | Airflow task retries as configured |
| **TC-05** | Pod Cleanup Verification | Finished run | Pod resource is purged immediately | `kubectl get pods -n composer-user-workloads` shows no zombie pods |

---

## 5. Negative Test Case: Reproducing Worker Failure (TC-00)

To demonstrably prove the failure mode on the standard Composer Celery worker before applying the fix, deploy this failure reproduction test DAG.

### 5.1 Test DAG Code (`dag_test_worker_storage_failure.py`)
```python
import os
import zipfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

def trigger_worker_storage_overflow():
    bucket_name = "elevate-505410-flatfile-landing"
    blob_name = "test/large_12gb_archive.zip"
    local_zip_path = "/tmp/overflow_test.zip"
    extract_dir = "/tmp/extracted_overflow/"
    
    os.makedirs(extract_dir, exist_ok=True)
    
    print(f"Downloading {blob_name} from {bucket_name} to {local_zip_path}...")
    client = storage.Client(project="elevate-505410")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_zip_path)
    
    print(f"Extracting files into {extract_dir} (Worker disk limit: 10GB)...")
    with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)
        
    print("Extraction completed successfully (unexpected if file >10GB).")

with DAG(
    dag_id="test_worker_ephemeral_storage_failure",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    test_failure_task = PythonOperator(
        task_id="extract_large_zip_on_worker",
        python_callable=trigger_worker_storage_overflow,
    )
```

### 5.2 Failure Execution & Cloud Logging Verification
1. **Trigger the test:**
   ```bash
   gcloud composer environments run your-composer-env \
       --location asia-southeast2 \
       --project elevate-505410 \
       dags trigger -- test_worker_ephemeral_storage_failure
   ```
2. **Filter Cloud Logging for the eviction error:**
   ```sql
   resource.type="k8s_pod"
   resource.labels.project_id="elevate-505410"
   resource.labels.location="asia-southeast2"
   jsonPayload.reason="Evicted"
   OR textPayload=~"Pod ephemeral local storage usage exceeds the total limit of containers 10239Mi"
   ```

---

## 6. Mock-Up Lab Build (Local / Sandbox Validation)

Commands to test and validate in `asia-southeast2` under project `elevate-505410`:

```bash
# 1. Set environment variables
export PROJECT_ID="elevate-505410"
export REGION="asia-southeast2"
export COMPOSER_ENV_NAME="your-composer-env"

# 2. Create Synthetic Big Flat File (15GB raw)
python3 -c "
with open('large_sample.csv', 'w') as f:
    f.write('id,account_no,tx_date,amount,description\n')
    chunk = '123456,ACC-99887766,2026-08-17,1500000.00,TELCO_TOPUP_BILLING_TRANSACTION_PAYMENT\n' * 100000
    for _ in range(2500):
        f.write(chunk)
"
zip large_12gb_archive.zip large_sample.csv

# 3. Upload to landing bucket for both TC-00 and TC-02
gcloud storage cp large_12gb_archive.zip gs://${PROJECT_ID}-flatfile-landing/test/

# 4. Import Production DAG to Cloud Composer Gen 2
gcloud composer environments storage dags import \
    --environment ${COMPOSER_ENV_NAME} \
    --location ${REGION} \
    --project ${PROJECT_ID} \
    --source flatfile_direct_load_ingestion.py

# 5. Trigger DAG run
gcloud composer environments run ${COMPOSER_ENV_NAME} \
    --location ${REGION} \
    --project ${PROJECT_ID} \
    dags trigger -- flatfile_direct_load_ingestion

# 6. Monitor Pod logs in Cloud Logging
gcloud logging read 'resource.type="k8s_pod" AND resource.labels.project_id="elevate-505410" AND resource.labels.location="asia-southeast2"' --limit 50 --project ${PROJECT_ID}
```

---

## 7. What Else You Need to Consider (Gotchas & Best Practices)

1. **Autopilot Resource Quotas in `asia-southeast2`:** Ensure project `elevate-505410` has adequate vCPU and In-Use IP Address quotas in `asia-southeast2` to accommodate parallel pod executions.
2. **Storage Clean Up on Container Crash:** Include `trap 'rm -rf /tmp/*' EXIT` inside the pod entrypoint to clear disk in case of mid-execution failures.
3. **BigQuery Direct Ingestion:** For subsequent optimizations, note that BigQuery native `LOAD DATA` can ingest `.gz` files directly from GCS without any unzipping step.
4. **Composer Namespace Isolation:** Ensure the pod executes within the `composer-user-workloads` or dedicated user namespace to avoid namespace conflicts with Airflow system pods.
