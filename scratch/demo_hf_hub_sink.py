#!/usr/bin/env python3
"""
Demo/Test: HF Hub Sink - "Hub is your disk" Pattern

This script demonstrates Polars' native HF Hub sink feature:
- Streaming read from one HF dataset
- Filter/transform with minimal memory
- Streaming write to another HF dataset

The "Hub is your disk" pattern enables processing large datasets with minimal RAM
by leveraging lazy evaluation and streaming.

Requirements:
- Polars with hf_bucket_sink feature (from feature/hf-bucket-sink branch)
- HF_TOKEN environment variable or pass token directly

Install (Colab):
    pip uninstall polars polars-runtime-32 -y
    pip install --no-deps --force-reinstall polars-*.whl polars_runtime_32-*.whl
    # Then restart runtime

Usage:
    HF_TOKEN=hf_xxx python scratch/demo_hf_hub_sink.py
"""

import os
import sys
import time
import uuid

# Enable verbose output for rate limit visibility
os.environ["POLARS_VERBOSE"] = "1"

import polars as pl

# =============================================================================
# Configuration
# =============================================================================

# Token from environment (required)
HF_TOKEN = os.environ.get("HF_TOKEN")
if not HF_TOKEN:
    print("ERROR: HF_TOKEN environment variable not set", flush=True)
    print("Usage: HF_TOKEN=hf_xxx python scratch/demo_hf_hub_sink.py", flush=True)
    sys.exit(1)

# Source dataset (nvidia/OpenMathReasoning - math reasoning dataset)
SOURCE_DATASET = "nvidia/OpenMathReasoning"
SOURCE_PATH = f"hf://datasets/{SOURCE_DATASET}/data/cot-*.parquet"

# Target repo (write access required)
TARGET_REPO = "davanstrien/test-polars-streaming"

# Processing parameters
NUM_ROWS = 50_000        # 50K rows - interesting but fast


def main():
    test_id = uuid.uuid4().hex[:8]
    target_path = f"hf://datasets/{TARGET_REPO}/data/demo-{test_id}.parquet"

    print("=" * 70, flush=True)
    print("HF Hub Sink Demo/Test: 'Hub is your disk' Pattern", flush=True)
    print("=" * 70, flush=True)
    print(flush=True)

    # =========================================================================
    # [1/5] Setup
    # =========================================================================
    print("[1/5] Setup", flush=True)
    print(f"  Polars version: {pl.__version__}", flush=True)
    print(f"  Source: {SOURCE_PATH}", flush=True)
    print(f"  Target: {target_path}", flush=True)
    print(f"  Rows: {NUM_ROWS:,}", flush=True)
    print(flush=True)

    # =========================================================================
    # [2/5] Streaming pipeline: scan -> filter -> sink
    # =========================================================================
    print("[2/5] Streaming Pipeline: scan -> head -> sink_parquet", flush=True)
    print("  This is the 'Hub is your disk' pattern:", flush=True)
    print("  - Lazy scan from HF Hub (no data loaded yet)", flush=True)
    print("  - Take first N rows", flush=True)
    print("  - Stream directly to HF Hub", flush=True)
    print(flush=True)

    start = time.time()
    try:
        # Lazy scan - no data loaded yet
        lf = pl.scan_parquet(SOURCE_PATH, storage_options={"token": HF_TOKEN})

        # Take first N rows (streaming)
        filtered = lf.head(NUM_ROWS)

        # Stream to HF Hub - this is the key feature!
        filtered.sink_parquet(target_path, storage_options={"token": HF_TOKEN})

        elapsed = time.time() - start
        print(f"  Pipeline complete! Time: {elapsed:.1f}s", flush=True)
    except Exception as e:
        elapsed = time.time() - start
        print(f"  PIPELINE FAILED after {elapsed:.1f}s: {e}", flush=True)
        sys.exit(1)
    print(flush=True)

    # =========================================================================
    # [3/5] Read back uploaded file
    # =========================================================================
    print("[3/5] Reading back from HF Hub...", flush=True)
    start = time.time()
    try:
        result = pl.read_parquet(target_path, storage_options={"token": HF_TOKEN})
        elapsed = time.time() - start
        print(f"  Read {result.shape[0]:,} rows, {result.shape[1]} columns", flush=True)
        print(f"  Time: {elapsed:.1f}s", flush=True)
    except Exception as e:
        elapsed = time.time() - start
        print(f"  READ FAILED after {elapsed:.1f}s: {e}", flush=True)
        sys.exit(1)
    print(flush=True)

    # =========================================================================
    # [4/5] Verify data integrity
    # =========================================================================
    print("[4/5] Verifying data integrity...", flush=True)
    errors = []

    # Check row count
    if result.shape[0] == 0:
        errors.append("No rows in result")
    elif result.shape[0] > NUM_ROWS:
        errors.append(f"Too many rows: {result.shape[0]} > {MAX_ROWS}")

    # Check we have some columns
    if len(result.columns) == 0:
        errors.append("No columns in result")

    # Sample data check
    if result.shape[0] > 0:
        print(f"  Columns: {result.columns}", flush=True)
        print(f"  Row count: {result.shape[0]:,}", flush=True)
        # Show first text column if available
        text_cols = [c for c in result.columns if "text" in c.lower()]
        if text_cols:
            sample_text = result[text_cols[0]][0]
            if sample_text:
                print(f"  Sample {text_cols[0]} (first 100 chars): {str(sample_text)[:100]}...", flush=True)

    if errors:
        print("  VERIFICATION FAILED:", flush=True)
        for err in errors:
            print(f"    - {err}", flush=True)
        sys.exit(1)
    else:
        print("  All checks passed!", flush=True)
    print(flush=True)

    # =========================================================================
    # [5/5] Summary
    # =========================================================================
    print("=" * 70, flush=True)
    print("RESULT: PASS", flush=True)
    print("=" * 70, flush=True)
    print(flush=True)
    print("Summary:", flush=True)
    print(f"  - Read from: {SOURCE_DATASET}", flush=True)
    print(f"  - Took: {NUM_ROWS:,} rows", flush=True)
    print(f"  - Wrote: {result.shape[0]:,} rows to {TARGET_REPO}", flush=True)
    print(f"  - File: {target_path}", flush=True)
    print(flush=True)
    print("The 'Hub is your disk' pattern works!", flush=True)
    print("Stream from one HF dataset to another with minimal memory.", flush=True)


if __name__ == "__main__":
    main()
