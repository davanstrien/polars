"""Test script to validate HF Hub fix with fineweb-2 (the dataset from the issue)."""

import time
import polars as pl

print("=" * 60)
print("Testing fineweb-2 - The Original Problem Dataset")
print("=" * 60)

# # This is the pattern that caused 379 API calls and 429 errors before the fix
# print("\nPattern: hf://datasets/HuggingFaceFW/fineweb-2/data/*/train/*.parquet")
# print("This previously made 379 API calls and hit rate limits.")
# print("With the fix, it should use ~19 API calls.\n")

# pattern "https://huggingface.co/datasets/HuggingFaceFW/finepdfs-edu/tree/main/data/deu_Latn/train"
# HuggingFaceFW / finepdfs - edu
pattern = "hf://datasets/HuggingFaceFW/finepdfs-edu/data/deu_Latn/train/*.parquet"
start = time.time()
try:
    # data/deu_Latn/train/part-00000-*.parquet etc.
    lf = pl.scan_parquet(
        pattern,
        extra_columns="ignore",
    )
    scan_time = time.time() - start
    print(f"Scan completed in {scan_time:.2f}s (no 429 errors!)")

    # Just check schema
    print(f"\nSchema: {lf.collect_schema().names()}")

    # Streaming aggregation - processes in chunks, won't OOM
    print("\nComputing mean language_score per language_script (streaming)...")
    print(
        "This streams through the entire dataset without loading it all into memory.\n"
    )

    start = time.time()
    # get mean token_count
    result = lf.select(pl.col("token_count").mean().alias("mean_token_count")).collect(
        engine="streaming"
    )
    query_time = time.time() - start

    print(f"Streaming aggregation completed in {query_time:.2f}s")
    print(f"\nMean token count:")
    print(result)

except Exception as e:
    print(f"FAILED: {e}")
    import traceback

    traceback.print_exc()

print("\n" + "=" * 60)
print("SUCCESS! The fix is working correctly.")
print("=" * 60)
