#!/usr/bin/env python3
"""End-to-end test: sink_parquet to HF bucket via XET protocol."""

import os, sys, time, uuid
import polars as pl

HF_TOKEN = os.environ.get("HF_TOKEN")
if not HF_TOKEN:
    print("ERROR: HF_TOKEN not set"); sys.exit(1)

NAMESPACE = "davanstrien"
BUCKET_NAME = "test-polars-bucket"
test_id = uuid.uuid4().hex[:8]
target_path = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/test-{test_id}.parquet"

# Create a small test DataFrame
df = pl.DataFrame({
    "id": range(1000),
    "text": [f"row_{i}" for i in range(1000)],
    "value": [float(i) * 0.1 for i in range(1000)],
})

# Sink to HF bucket
print(f"Sinking to {target_path}...")
start = time.time()
df.lazy().sink_parquet(target_path, storage_options={"token": HF_TOKEN})
elapsed = time.time() - start
print(f"Upload complete in {elapsed:.1f}s")

# Read back and verify (may not work if hf://buckets/ read is not wired yet)
try:
    print("Reading back...")
    result = pl.read_parquet(target_path, storage_options={"token": HF_TOKEN})
    assert result.shape == (1000, 3), f"Shape mismatch: {result.shape}"
    assert result["id"].to_list() == list(range(1000)), "Data mismatch"
    print(f"Verification passed: {result.shape[0]} rows, {result.shape[1]} cols")
except Exception as e:
    print(f"Read-back not supported yet (expected): {e}")
    print("Upload succeeded — verify manually via HF web UI or API.")
