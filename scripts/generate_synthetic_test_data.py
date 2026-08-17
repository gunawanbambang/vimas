#!/usr/bin/env python3
"""
Synthetic Flat File Data Generator
Generates sample banking/telecom transaction CSV archives for testing:
1. Small test files (15 MB .zip, .csv.gz, and .csv.z) for baseline validation (TC-01)
2. Large test file (12 GB+ archive) for negative (TC-00) & positive (TC-02) scale testing
"""

import os
import sys
import gzip
import zipfile
import argparse

HEADER = "transaction_id,account_number,transaction_date,amount,currency,channel,status,description\n"
SAMPLE_ROW = "{tx_id},ACC-9988{acc_suffix},2026-08-17,1500000.00,IDR,MOBILE_BANKING,SUCCESS,MONTHLY_TELCO_TOPUP_BILLING_TRANSACTION_PAYMENT\n"

def generate_csv(output_path: str, target_size_mb: int):
    print(f"Generating synthetic CSV '{output_path}' (~{target_size_mb} MB)...")
    target_bytes = target_size_mb * 1024 * 1024
    written = 0
    tx_counter = 10000000

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(HEADER)
        written += len(HEADER)
        
        # Write in 10,000 row chunks for speed
        chunk_rows = 10000
        chunk_lines = []
        for i in range(chunk_rows):
            chunk_lines.append(SAMPLE_ROW.format(tx_id=tx_counter + i, acc_suffix=i % 10000))
        chunk_block = "".join(chunk_lines)
        chunk_len = len(chunk_block)
        
        while written < target_bytes:
            f.write(chunk_block)
            written += chunk_len
            tx_counter += chunk_rows

    actual_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Generated raw CSV: {output_path} ({actual_mb:.2f} MB)")

def compress_to_zip(source_file: str, zip_path: str):
    print(f"Compressing {source_file} into ZIP -> {zip_path}...")
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(source_file, arcname=os.path.basename(source_file))
    actual_mb = os.path.getsize(zip_path) / (1024 * 1024)
    print(f"Created ZIP archive: {zip_path} ({actual_mb:.2f} MB)")

def compress_to_gzip(source_file: str, gz_path: str):
    print(f"Compressing {source_file} into GZIP -> {gz_path}...")
    with open(source_file, "rb") as f_in, gzip.open(gz_path, "wb") as f_out:
        while chunk := f_in.read(64 * 1024):
            f_out.write(chunk)
    actual_mb = os.path.getsize(gz_path) / (1024 * 1024)
    print(f"Created GZIP archive: {gz_path} ({actual_mb:.2f} MB)")

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction test files.")
    parser.add_argument("--mode", choices=["small", "large", "both"], default="small",
                        help="Generate small (15MB), large (12GB+), or both datasets.")
    parser.add_argument("--output-dir", default="./test_data", help="Output directory for generated files.")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    if args.mode in ["small", "both"]:
        small_csv = os.path.join(args.output_dir, "small_sample.csv")
        small_zip = os.path.join(args.output_dir, "small_15mb_archive.zip")
        small_gz = os.path.join(args.output_dir, "small_sample.csv.gz")
        small_z = os.path.join(args.output_dir, "small_sample.csv.z")
        
        generate_csv(small_csv, target_size_mb=40)
        compress_to_zip(small_csv, small_zip)
        compress_to_gzip(small_csv, small_gz)
        compress_to_gzip(small_csv, small_z) # .z compatibility test file
        
        os.remove(small_csv)
        print(f"\n[TC-01] Small test datasets generated in {args.output_dir}:")
        print(f"  - {small_zip}")
        print(f"  - {small_gz}")
        print(f"  - {small_z}")

    if args.mode in ["large", "both"]:
        large_csv = os.path.join(args.output_dir, "large_sample.csv")
        large_zip = os.path.join(args.output_dir, "large_12gb_archive.zip")
        print("\nNote: Large dataset generation requires ~35GB free disk space temporarily.")
        generate_csv(large_csv, target_size_mb=32000) # ~32GB uncompressed
        compress_to_zip(large_csv, large_zip)
        os.remove(large_csv)
        print(f"[TC-00 / TC-02] Large test file ready: {large_zip}")

if __name__ == "__main__":
    main()
