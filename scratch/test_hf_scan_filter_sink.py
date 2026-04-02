#!/usr/bin/env python3
"""
Realistic streaming test: scan HF dataset → filter → sink to HF bucket.

This is the pattern that failed at scale with the old git-LFS sink approach.
"""

import os, sys, time, uuid
import polars as pl

HF_TOKEN = os.environ.get("HF_TOKEN")
if not HF_TOKEN:
    print("ERROR: HF_TOKEN not set"); sys.exit(1)

# --- Source: a real HF dataset (parquet) ---
# Using a smallish public dataset to start; swap for larger ones to stress test
# Use glob — the dataset has a config/split directory structure
SOURCE = "hf://datasets/wikimedia/wikipedia/20231101.en/*.parquet"

NAMESPACE = "davanstrien"
BUCKET_NAME = "test-polars-bucket"
test_id = uuid.uuid4().hex[:8]
TARGET = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/filtered-wiki-{test_id}.parquet"

print(f"polars version: {pl.__version__}")
print(f"Source: {SOURCE}")
print(f"Target: {TARGET}")
print()

# Lazy scan → filter → sink
print("Scanning + filtering + sinking...")
start = time.time()

lf = pl.scan_parquet(SOURCE, storage_options={"token": HF_TOKEN})
print(f"Schema: {lf.collect_schema()}")

# Apply a filter — keep rows where text length > 5000 chars
filtered = lf.filter(pl.col("text").str.len_chars() > 5000)

filtered.sink_parquet(TARGET, storage_options={"token": HF_TOKEN})

elapsed = time.time() - start
print(f"Done in {elapsed:.1f}s")
print(f"Uploaded to {TARGET}")
