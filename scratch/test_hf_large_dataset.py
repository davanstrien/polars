#!/usr/bin/env python3
"""
Large dataset streaming test for HF bucket sink.

Tests with increasing dataset sizes to validate that streaming memory
behavior stays constant (doesn't grow linearly with dataset size).

Usage (local):
    HF_TOKEN="hf_..." python scratch/test_hf_large_dataset.py

Usage (Colab):
    1. Upload both .whl files (polars + polars_runtime_32)
    2. Install with: pip install --no-deps --force-reinstall polars-*.whl polars_runtime_32-*.whl
    3. Restart runtime, then set HF_TOKEN in the environment or Colab secrets
    4. Copy-paste this script into a cell and run
"""

import os
import sys
import time
import uuid
import resource

# --- Config ---
HF_TOKEN = os.environ.get("HF_TOKEN")
if not HF_TOKEN:
    # Try Colab secrets
    try:
        from google.colab import userdata
        HF_TOKEN = userdata.get("HF_TOKEN")
    except Exception:
        pass

if not HF_TOKEN:
    print("ERROR: HF_TOKEN not set (env var or Colab secret)")
    sys.exit(1)

NAMESPACE = "davanstrien"
BUCKET_NAME = "test-polars-bucket"
SIZES = [1_000, 10_000, 100_000, 1_000_000]


def get_peak_rss_mb():
    """Get peak RSS in MB (works on Linux and macOS)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        return usage.ru_maxrss / (1024 * 1024)  # bytes -> MB on macOS
    return usage.ru_maxrss / 1024  # KB -> MB on Linux


def make_dataframe(n_rows):
    """Create a test DataFrame with text-heavy columns."""
    import polars as pl
    return pl.DataFrame({
        "id": range(n_rows),
        "text": [f"row_{i}_" + "x" * 100 for i in range(n_rows)],
        "category": [f"cat_{i % 50}" for i in range(n_rows)],
        "value_a": [float(i) * 0.1 for i in range(n_rows)],
        "value_b": [float(i) * 0.01 for i in range(n_rows)],
    })


def run_test(n_rows):
    """Sink a DataFrame of n_rows to HF bucket, return (elapsed_s, peak_rss_mb)."""
    import polars as pl

    test_id = uuid.uuid4().hex[:8]
    target = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/bench-{n_rows}-{test_id}.parquet"

    df = make_dataframe(n_rows)
    rss_before = get_peak_rss_mb()

    start = time.time()
    df.lazy().sink_parquet(target, storage_options={"token": HF_TOKEN})
    elapsed = time.time() - start

    rss_after = get_peak_rss_mb()
    return elapsed, rss_before, rss_after


def main():
    import polars as pl
    print(f"polars version: {pl.__version__}")
    print(f"{'Rows':>12} | {'Time (s)':>10} | {'RSS before (MB)':>16} | {'RSS after (MB)':>16}")
    print("-" * 65)

    for n in SIZES:
        print(f"{'':>12} | Running {n:,} rows...", end="\r")
        try:
            elapsed, rss_before, rss_after = run_test(n)
            print(f"{n:>12,} | {elapsed:>10.2f} | {rss_before:>16.1f} | {rss_after:>16.1f}")
        except Exception as e:
            print(f"{n:>12,} | FAILED: {e}")

    print("\nDone. If RSS stays roughly constant across sizes, streaming is working.")


if __name__ == "__main__":
    main()
