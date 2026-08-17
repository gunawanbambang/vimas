"""
Negative Test DAG: Reproducing Worker Ephemeral Storage Eviction (TC-00)
Attempts to download and decompress/write a 12GB+ payload directly within the standard
Cloud Composer Celery worker /tmp directory to demonstrate the 10239Mi eviction failure.
"""

import os
import shutil
import zipfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

PROJECT_ID = "elevate-505410"
LANDING_BUCKET = f"{PROJECT_ID}-flatfile-landing"

def trigger_worker_storage_overflow():
    blob_name = "test/large_12gb_archive.zip"
    local_zip_path = "/tmp/overflow_test.zip"
    extract_dir = "/tmp/extracted_overflow"
    
    os.makedirs(extract_dir, exist_ok=True)
    
    # Check if large archive exists in GCS
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(LANDING_BUCKET)
    blob = bucket.blob(blob_name)
    
    if blob.exists():
        print(f"Downloading {blob_name} from {LANDING_BUCKET} to {local_zip_path}...")
        blob.download_to_filename(local_zip_path)
        print(f"Extracting files into {extract_dir} (Celery Worker ephemeral storage limit: 10GB)...")
        with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
    else:
        print("large_12gb_archive.zip not found in GCS; generating 12GB+ payload directly in worker /tmp...")
        chunk = b"0" * (10 * 1024 * 1024) # 10MB chunk
        total_written = 0
        file_index = 1
        
        # Write files in /tmp until exceeding 11GB to trigger 10239Mi Pod eviction
        while total_written < (12 * 1024 * 1024 * 1024):
            file_path = os.path.join(extract_dir, f"overflow_chunk_{file_index}.dat")
            with open(file_path, "wb") as f:
                for _ in range(100): # 1GB file
                    f.write(chunk)
                    total_written += len(chunk)
            file_index += 1
            print(f"Worker disk usage in /tmp: {total_written / (1024 * 1024 * 1024):.2f} GB (Limit: 10GB)...")
            
    print("WARNING: Task finished without container eviction. Worker ephemeral storage might be larger than 10GB.")

with DAG(
    dag_id="test_worker_ephemeral_storage_failure",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "negative-test", "ephemeral-storage-failure"],
) as dag:

    test_failure_task = PythonOperator(
        task_id="extract_large_zip_directly_on_celery_worker",
        python_callable=trigger_worker_storage_overflow,
    )
