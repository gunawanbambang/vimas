#!/usr/bin/env python3
"""
Streaming Large Archive Generator
Generates a compressed archive that expands to exact target uncompressed size (e.g. 15.0 GB).
Uses streaming compression to minimize local disk footprint during generation.
"""

import os
import sys
import gzip
import zipfile
import time

HEADER = "transaction_id,account_number,transaction_date,amount,currency,channel,status,description\n"
SAMPLE_ROW = "{tx_id},ACC-9988{acc_suffix},2026-08-17,1500000.00,IDR,MOBILE_BANKING,SUCCESS,MONTHLY_TELCO_TOPUP_BILLING_TRANSACTION_PAYMENT\n"

def generate_streaming_gzip(output_gz: str, target_uncompressed_gb: float = 15.0):
    print(f"Generating streaming GZIP archive '{output_gz}' (~{target_uncompressed_gb} GB uncompressed)...")
    start_time = time.time()
    target_bytes = int(target_uncompressed_gb * 1024 * 1024 * 1024)
    written_uncompressed = 0
    tx_counter = 10000000

    chunk_rows = 20000
    chunk_lines = []
    for i in range(chunk_rows):
        chunk_lines.append(SAMPLE_ROW.format(tx_id=tx_counter + i, acc_suffix=i % 10000))
    chunk_block = "".join(chunk_lines).encode("utf-8")
    chunk_len = len(chunk_block)

    with open(output_gz, "wb") as f_out:
        with gzip.GzipFile(filename="large_transaction_data.csv", mode="wb", fileobj=f_out, compresslevel=6) as gz_out:
            gz_out.write(HEADER.encode("utf-8"))
            written_uncompressed += len(HEADER.encode("utf-8"))

            while written_uncompressed < target_bytes:
                gz_out.write(chunk_block)
                written_uncompressed += chunk_len
                tx_counter += chunk_rows
                if (written_uncompressed // (1024*1024*1024)) > ((written_uncompressed - chunk_len) // (1024*1024*1024)):
                    current_gb = written_uncompressed / (1024*1024*1024)
                    print(f"  ... stream progress: {current_gb:.1f} / {target_uncompressed_gb:.1f} GB uncompressed")

    elapsed = time.time() - start_time
    compressed_mb = os.path.getsize(output_gz) / (1024 * 1024)
    uncompressed_gb = written_uncompressed / (1024 * 1024 * 1024)
    print(f"DONE in {elapsed:.1f}s!")
    print(f"  Compressed archive size : {compressed_mb:.2f} MB")
    print(f"  Uncompressed data size  : {uncompressed_gb:.2f} GB ({written_uncompressed:,} bytes)")

if __name__ == "__main__":
    out_dir = sys.argv[1] if len(sys.argv) > 1 else "./test_data"
    os.makedirs(out_dir, exist_ok=True)
    gz_target = os.path.join(out_dir, "large_15gb_transactions.csv.gz")
    generate_streaming_gzip(gz_target, target_uncompressed_gb=15.0)
