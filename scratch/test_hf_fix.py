"""Test script to validate HF Hub recursive tree API fix."""

import time
import polars as pl

print("=" * 60)
print("Testing HF Hub Glob Expansion Fix (Issue #25389)")
print("=" * 60)

# Test 1: The problematic glob pattern that was causing 429 errors
print("\n[Test 1] Glob pattern that previously caused rate limiting...")
print("Pattern: hf://datasets/HuggingFaceFW/finepdfs-edu/data/*_Latn/train/*.parquet")

start = time.time()
try:
    lf = pl.scan_parquet(
        "hf://datasets/HuggingFaceFW/finepdfs-edu/data/*_Latn/train/*.parquet"
    )
    schema = lf.collect_schema()
    elapsed = time.time() - start
    print(f"SUCCESS: Found {len(schema.names())} columns in {elapsed:.2f}s")
    print(f"Columns: {schema.names()[:5]}...")  # First 5 columns
    # we also want to do some opperations
    # group by dump and get mean token_count per dump
    print("\nComputing mean token_count per dump (streaming)...")
    start = time.time()
    result = (
        lf.group_by("dump")
        .agg(pl.col("token_count").mean().alias("mean_token_count"))
        .collect(engine="streaming")
    )
    query_time = time.time() - start
    print(f"Streaming aggregation completed in {query_time:.2f}s")
    print(f"\nMean token counts per dump (first 5 rows):")
    print(result.head(5))

except Exception as e:
    print(f"FAILED: {e}")

# Test 2: Smaller test - single file (should still work)
print("\n[Test 2] Single file access (no glob)...")
start = time.time()
try:
    df = pl.read_parquet(
        "hf://datasets/poloclub/diffusiondb/metadata.parquet", n_rows=5
    )
    elapsed = time.time() - start
    print(f"SUCCESS: Read {len(df)} rows in {elapsed:.2f}s")
    print(df)
except Exception as e:
    print(f"FAILED: {e}")

# Test 3: Another glob pattern
print("\n[Test 3] Another glob pattern...")
print("Pattern: hf://datasets/roneneldan/TinyStories/data/*.parquet")
start = time.time()
try:
    lf = pl.scan_parquet("hf://datasets/roneneldan/TinyStories/data/*.parquet")
    schema = lf.collect_schema()
    elapsed = time.time() - start
    print(f"SUCCESS: Found {len(schema.names())} columns in {elapsed:.2f}s")
except Exception as e:
    print(f"FAILED: {e}")

print("\n" + "=" * 60)
print("If all tests passed without 429 errors, the fix is working!")
print("=" * 60)
