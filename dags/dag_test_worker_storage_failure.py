"""
Negative Test DAG: Reproducing Worker Ephemeral Storage Eviction (TC-00)
Attempts to download and decompress a 15GB+ flat file directly within the standard
Cloud Composer Celery worker local /tmp directory to demonstrate the 10239Mi eviction failure.
"""

import os
import gzip
import zipfile
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage

PROJECT_ID = "elevate-505410"
LANDING_BUCKET = f"{PROJECT_ID}-flatfile-landing"

def trigger_worker_storage_overflow():
    local_dir = "/tmp/worker_overflow_test"
    os.makedirs(local_dir, exist_ok=True)
    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(LANDING_BUCKET)
    
    # Target the 15GB archive in Landing GCS
    target_blobs = [
        "raw/20260817/large_15gb_transactions.csv.gz",
        "raw/20260816/large_15gb_transactions.csv.gz",
        "test/large_12gb_archive.zip"
    ]
    found_blob = None
    for b_name in target_blobs:
        b = bucket.blob(b_name)
        if b.exists():
            found_blob = b
            break
            
    if found_blob:
        local_archive = os.path.join(local_dir, os.path.basename(found_blob.name))
        print(f"Downloading {found_blob.name} ({found_blob.size / (1024*1024):.2f} MB) to {local_archive} on Celery Worker...")
        found_blob.download_to_filename(local_archive)
        
        out_csv = os.path.join(local_dir, "uncompressed_worker_large.csv")
        print(f"Attempting local in-worker decompression to {out_csv} (Worker local disk limit: 10GB)...")
        
        if local_archive.endswith(".gz"):
            with gzip.open(local_archive, "rb") as f_in, open(out_csv, "wb") as f_out:
                bytes_written = 0
                while chunk := f_in.read(16 * 1024 * 1024):
                    f_out.write(chunk)
                    bytes_written += len(chunk)
                    if bytes_written % (1024 * 1024 * 1024) < (16 * 1024 * 1024):
                        print(f"Worker /tmp disk usage: {bytes_written / (1024**3):.2f} GB (Approaching 10GB limit)...", flush=True)
        elif local_archive.endswith(".zip"):
            with zipfile.ZipFile(local_archive, "r") as zf:
                zf.extractall(local_dir)
    else:
        print("Large archive not found in GCS; simulating 15GB local decompression directly in worker /tmp...")
        chunk = b"0" * (10 * 1024 * 1024)
        total_written = 0
        file_index = 1
        while total_written < (15 * 1024 * 1024 * 1024):
            f_path = os.path.join(local_dir, f"chunk_{file_index}.dat")
            with open(f_path, "wb") as f:
                for _ in range(100):
                    f.write(chunk)
                    total_written += len(chunk)
            file_index += 1
            print(f"Worker disk usage in /tmp: {total_written / (1024**3):.2f} GB (Limit: 10GB)...", flush=True)

    print("WARNING: Task finished without container eviction.")

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
