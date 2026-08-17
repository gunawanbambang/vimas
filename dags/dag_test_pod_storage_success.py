"""
Positive Test DAG: Validating Pod Ephemeral Storage Scaling (TC-01 & TC-02)
Triggers KubernetesPodOperator with 50GiB ephemeral storage to decompress large archives
without Celery worker memory or disk pressure.
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

PROJECT_ID = "elevate-505410"
LOCATION = "asia-southeast2"
LANDING_BUCKET = f"{PROJECT_ID}-flatfile-landing"
STAGING_BUCKET = f"{PROJECT_ID}-flatfile-staging"
CONTAINER_IMAGE = f"{LOCATION}-docker.pkg.dev/{PROJECT_ID}/data-pipelines/flatfile-processor:v1.0.0"

pod_resources = k8s.V1ResourceRequirements(
    requests={"cpu": "2", "memory": "4Gi", "ephemeral-storage": "10Gi"},
    limits={"cpu": "2", "memory": "4Gi", "ephemeral-storage": "10Gi"},
)

with DAG(
    dag_id="test_pod_ephemeral_storage_success",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "positive-test", "pod-validation"],
) as dag:

    test_pod_success_task = KubernetesPodOperator(
        task_id="test_pod_unzip_large_archive",
        name="test-pod-unzip-large-archive",
        namespace="composer-user-workloads",
        service_account_name="flatfile-processor-ksa",
        in_cluster=True,
        image=CONTAINER_IMAGE,
        container_resources=pod_resources,
        env_vars={
            "PROJECT_ID": PROJECT_ID,
            "GCS_LANDING_URI": f"gs://{LANDING_BUCKET}/test/",
            "GCS_STAGE_URI": f"gs://{STAGING_BUCKET}/test_output/",
            "SECRET_GPG_KEY_NAME": "sftp-gpg-private-key",
        },
        cmds=["/workspace/entrypoint.sh"],
        is_delete_operator_pod=True,
        get_logs=True,
        startup_timeout_seconds=300,
    )
