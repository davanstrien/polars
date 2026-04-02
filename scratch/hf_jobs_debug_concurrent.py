# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars",
#     "polars-runtime-32",
#     "huggingface_hub",
#     "psutil"
# ]
# [tool.uv.sources]
# polars = { url = "https://github.com/davanstrien/polars/releases/download/hf-sink-v0.1.0-alpha/polars-1.37.1-py3-none-any.whl" }
# polars-runtime-32 = { url = "https://github.com/davanstrien/polars/releases/download/hf-sink-v0.1.0-alpha/polars_runtime_32-1.37.1-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" }
# ///
"""
ISSUE-005 Debug Script: Test impact of POLARS_MAX_CONCURRENT_SCANS on memory usage.

This script tests the hypothesis that OOM during multi-file HF scan is caused by
too many concurrent file readers. Run with different concurrent_limit values.

Usage on HF Jobs:
    # Test with limit=4 (conservative)
    hf jobs uv run scratch/hf_jobs_debug_concurrent.py -s HF_TOKEN --flavor cpu-upgrade --timeout 30m -- 4

    # Test with limit=1 (most conservative)
    hf jobs uv run scratch/hf_jobs_debug_concurrent.py -s HF_TOKEN --flavor cpu-upgrade --timeout 30m -- 1

    # Test with limit=16 (may OOM on 34GB machine)
    hf jobs uv run scratch/hf_jobs_debug_concurrent.py -s HF_TOKEN --flavor cpu-upgrade --timeout 30m -- 16
"""
import os
import sys
import time
import threading
import psutil

# Parse command line argument for concurrent limit
concurrent_limit = int(sys.argv[1]) if len(sys.argv) > 1 else 4
prefetch_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 2

# Set environment variables BEFORE importing polars
os.environ["POLARS_MAX_CONCURRENT_SCANS"] = str(concurrent_limit)
os.environ["POLARS_ROW_GROUP_PREFETCH_SIZE"] = str(prefetch_limit)
os.environ["POLARS_TRACK_METRICS"] = "1"

# Now import polars
import polars as pl

print("=" * 60)
print("ISSUE-005 Debug: Concurrent Scan Memory Test")
print("=" * 60)
print(f"Polars version: {pl.__version__}")
print(f"POLARS_MAX_CONCURRENT_SCANS: {concurrent_limit}")
print(f"POLARS_ROW_GROUP_PREFETCH_SIZE: {prefetch_limit}")
print()

# Memory monitoring
peak_memory_gb = 0
running = True

def monitor_memory():
    global peak_memory_gb
    while running:
        try:
            current_gb = psutil.Process().memory_info().rss / 1024**3
            peak_memory_gb = max(peak_memory_gb, current_gb)
            sys.stdout.write(f"\rMemory: {current_gb:.2f} GB (peak: {peak_memory_gb:.2f} GB)   ")
            sys.stdout.flush()
        except:
            pass
        time.sleep(2)

monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
monitor_thread.start()

print(f"Initial memory: {psutil.Process().memory_info().rss / 1024**3:.2f} GB")
print()

# Test: scan_parquet with filter and head()
# Using nvidia/OpenMathReasoning dataset
# Note: Use explicit file list or simpler glob pattern

# First, let's verify HF token is available
hf_token = os.environ.get("HF_TOKEN")
print(f"HF_TOKEN available: {hf_token is not None and len(hf_token) > 0}")

# Use a simpler glob pattern - just one subdirectory level
SOURCE_PATH = "hf://datasets/nvidia/OpenMathReasoning/data/train/*.parquet"
# Alternative: explicit single file for minimal test
# SOURCE_PATH = "hf://datasets/nvidia/OpenMathReasoning/data/train/train-00000-of-00266.parquet"

print("Starting scan_parquet test...")
print(f"Source: {SOURCE_PATH}")
print(f"Filter: problem_source == 'MATH_training_set'")
print()

start_time = time.time()
try:
    lf = pl.scan_parquet(
        SOURCE_PATH,
        storage_options={"token": hf_token} if hf_token else None
    )

    # Apply filter and take head to avoid full materialization
    result = lf.filter(
        pl.col("problem_source") == "MATH_training_set"
    ).head(1000).collect()

    elapsed = time.time() - start_time

    print()
    print("=" * 60)
    print("SUCCESS!")
    print("=" * 60)
    print(f"Rows returned: {len(result)}")
    print(f"Columns: {result.columns}")
    print(f"Elapsed time: {elapsed:.1f}s")
    print(f"Peak memory: {peak_memory_gb:.2f} GB")
    print()
    print("Result preview:")
    print(result.head(5))

except Exception as e:
    elapsed = time.time() - start_time
    print()
    print("=" * 60)
    print("FAILED!")
    print("=" * 60)
    print(f"Error: {e}")
    print(f"Elapsed time before failure: {elapsed:.1f}s")
    print(f"Peak memory: {peak_memory_gb:.2f} GB")
    raise

finally:
    running = False
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"POLARS_MAX_CONCURRENT_SCANS: {concurrent_limit}")
    print(f"POLARS_ROW_GROUP_PREFETCH_SIZE: {prefetch_limit}")
    print(f"Peak memory: {peak_memory_gb:.2f} GB")
    print(f"Total time: {time.time() - start_time:.1f}s")
