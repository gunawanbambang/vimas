"""
Production Airflow DAG: Flat File Direct Load Ingestion Pipeline
Demonstrates offloading Step 7 (archive decompression & decryption) to an isolated GKE Autopilot Pod
via KubernetesPodOperator in Cloud Composer Gen 2.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

PROJECT_ID = "elevate-505410"
LOCATION = "asia-southeast2"
LANDING_BUCKET = f"{PROJECT_ID}-flatfile-landing"
STAGING_BUCKET = f"{PROJECT_ID}-flatfile-staging"
CONTAINER_IMAGE = f"{LOCATION}-docker.pkg.dev/{PROJECT_ID}/data-pipelines/flatfile-processor:v1.1.0"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Autopilot-compliant resource specification: matching requests and limits
pod_resources = k8s.V1ResourceRequirements(
    requests={
        "cpu": "2",
        "memory": "4Gi",
    },
    limits={
        "cpu": "2",
        "memory": "4Gi",
    },
)

scratch_volume = k8s.V1Volume(
    name="scratch-storage",
    empty_dir=k8s.V1EmptyDirVolumeSource(size_limit="50Gi"),
)
scratch_volume_mount = k8s.V1VolumeMount(
    name="scratch-storage",
    mount_path="/tmp",
)

with DAG(
    dag_id="flatfile_direct_load_ingestion",
    default_args=default_args,
    description="Batch flat file ingestion pipeline with Pod-offloaded decompression/decryption",
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["flatfile", "gke-pod", "production"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    # Step 1-6 placeholder: Ingestion / SFTP Pull -> Landing Bucket
    sftp_pull_to_landing = EmptyOperator(
        task_id="step1_6_sftp_pull_to_landing_bucket"
    )

    # Step 7: Offloaded Archive Decompression & Decryption on Dedicated Pod
    unzip_and_decrypt_pod = KubernetesPodOperator(
        task_id="step7_unzip_decrypt_flatfile_pod",
        name="pod-flatfile-unzip-decrypt",
        namespace="composer-user-workloads",
        service_account_name="flatfile-processor-ksa",
        in_cluster=True,
        image=CONTAINER_IMAGE,
        image_pull_policy="Always",
        container_resources=pod_resources,
        volumes=[scratch_volume],
        volume_mounts=[scratch_volume_mount],
        env_vars={
            "PROJECT_ID": PROJECT_ID,
            "GCS_LANDING_URI": f"gs://{LANDING_BUCKET}/raw/{{{{ ds_nodash }}}}/",
            "GCS_STAGE_URI": f"gs://{STAGING_BUCKET}/unzipped/{{{{ ds_nodash }}}}/",
        },
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=300,
    )

    # Step 8-14: Load from Staging GCS to BigQuery
    load_staging_to_bigquery = EmptyOperator(
        task_id="step8_14_load_gcs_to_bigquery_stage"
    )

    # Step 15-20: BigQuery ODS Merge & Transformation
    transform_and_merge_ods = EmptyOperator(
        task_id="step15_20_bq_ods_transformation"
    )

    # Step 21-22: Catalog Governance & Completion
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Pipeline Dependencies
    start_pipeline >> sftp_pull_to_landing >> unzip_and_decrypt_pod >> load_staging_to_bigquery >> transform_and_merge_ods >> end_pipeline
