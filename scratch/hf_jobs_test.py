# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars @ https://github.com/davanstrien/polars/releases/download/hf-sink-v0.1.0-alpha/polars-1.37.1-py3-none-any.whl",
#     "polars-runtime-32 @ https://github.com/davanstrien/polars/releases/download/hf-sink-v0.1.0-alpha/polars_runtime_32-1.37.1-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
# ]
# ///

"""
HF Jobs Scale Test: Hub-to-Hub Streaming

Validates Polars hf_sink feature on HF Jobs infrastructure.

Mode 1 - head() (may buffer):
    hf jobs uv run scratch/hf_jobs_test.py \
        -s HF_TOKEN --flavor cpu-upgrade --timeout 30m \
        -- --output-repo davanstrien/test-polars-streaming --num-rows 1000

Mode 2 - filter() (true streaming):
    hf jobs uv run scratch/hf_jobs_test.py \
        -s HF_TOKEN --flavor cpu-upgrade --timeout 30m \
        -- --output-repo davanstrien/test-polars-streaming --filter-kaggle

    hf jobs uv run scratch/hf_jobs_test.py \
        -s HF_TOKEN --flavor cpu-upgrade --timeout 2h \
        -- --output-repo davanstrien/test-polars-streaming --filter-source amc_aime

Mode 3 - inspect data:
    hf jobs uv run scratch/hf_jobs_test.py \
        -s HF_TOKEN --flavor cpu-upgrade --timeout 10m \
        -- --output-repo davanstrien/test-polars-streaming --inspect
"""

import argparse
import os
import sys
import time
import uuid
import resource

import polars as pl

# Source: nvidia/OpenMathReasoning (no List columns, safe for streaming)
SOURCE_DATASET = "nvidia/OpenMathReasoning"
# Single file for testing (~200MB) - use SOURCE_GLOB for multiple files
SOURCE_SINGLE = f"hf://datasets/{SOURCE_DATASET}/data/cot-00000-of-00266.parquet"
SOURCE_GLOB = f"hf://datasets/{SOURCE_DATASET}/data/cot-*.parquet"
SOURCE = SOURCE_SINGLE  # Default to single file for memory-safe testing


def get_memory_mb():
    """Get current memory usage in MB (Linux)."""
    try:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except Exception:
        return 0


def inspect_data(hf_token):
    """Inspect the source data to find good filter values."""
    print("=" * 60)
    print("Inspecting source data...")
    print("=" * 60)

    lf = pl.scan_parquet(SOURCE, storage_options={"token": hf_token})

    # Get schema
    print("\nSchema:")
    print(lf.collect_schema())

    # Sample first row
    print("\nFirst row (truncated):")
    first = lf.head(1).collect()
    for col in first.columns:
        val = first[col][0]
        if isinstance(val, str) and len(val) > 80:
            print(f"  {col}: {val[:80]}...")
        else:
            print(f"  {col}: {val}")

    # Get unique problem_source values
    print("\nUnique problem_source values:")
    sources = lf.select("problem_source").unique().collect()
    print(sources)

    # Count by problem_source (sample 100K rows for speed)
    print("\nCounts by problem_source (sampled 100K rows):")
    counts = lf.head(100000).group_by("problem_source").len().collect()
    print(counts)

    # Count used_in_kaggle
    print("\nused_in_kaggle distribution (sampled 100K rows):")
    kaggle = lf.head(100000).group_by("used_in_kaggle").len().collect()
    print(kaggle)


def main():
    parser = argparse.ArgumentParser(description="HF Jobs Scale Test")
    parser.add_argument(
        "--output-repo",
        required=True,
        help="Target HF dataset repo (e.g., user/repo)",
    )
    # Mode selection
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--num-rows",
        type=int,
        help="Use head(N) - may buffer in memory",
    )
    group.add_argument(
        "--filter-kaggle",
        action="store_true",
        help="Filter: used_in_kaggle == True (true streaming)",
    )
    group.add_argument(
        "--filter-source",
        type=str,
        help="Filter: problem_source == VALUE (true streaming)",
    )
    group.add_argument(
        "--inspect",
        action="store_true",
        help="Inspect source data to find filter values",
    )
    args = parser.parse_args()

    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        print("ERROR: HF_TOKEN not set. Pass it as a secret: -s HF_TOKEN")
        sys.exit(1)

    # Inspect mode
    if args.inspect:
        inspect_data(hf_token)
        return

    # Default to 1000 rows if no mode specified
    if not args.num_rows and not args.filter_kaggle and not args.filter_source:
        args.num_rows = 1000

    test_id = uuid.uuid4().hex[:8]
    target = f"hf://datasets/{args.output_repo}/data/hf-jobs-test-{test_id}.parquet"

    # Build filter description
    if args.num_rows:
        filter_desc = f"head({args.num_rows:,})"
    elif args.filter_kaggle:
        filter_desc = "filter(used_in_kaggle == True)"
    elif args.filter_source:
        filter_desc = f"filter(problem_source == '{args.filter_source}')"

    print("=" * 60)
    print("HF Jobs Scale Test: Hub-to-Hub Streaming")
    print("=" * 60)
    print(f"  Polars version: {pl.__version__}")
    print(f"  Source: {SOURCE_DATASET}")
    print(f"  Target: {args.output_repo}")
    print(f"  Filter: {filter_desc}")
    print()

    # [1] Streaming pipeline
    print("[1/3] Running streaming pipeline...")
    start = time.time()
    mem_before = get_memory_mb()

    lf = pl.scan_parquet(SOURCE, storage_options={"token": hf_token})

    # Apply filter or head
    if args.num_rows:
        lf = lf.head(args.num_rows)
    elif args.filter_kaggle:
        lf = lf.filter(pl.col("used_in_kaggle") == True)
    elif args.filter_source:
        lf = lf.filter(pl.col("problem_source") == args.filter_source)

    lf.sink_parquet(target, storage_options={"token": hf_token}, engine="streaming")

    elapsed = time.time() - start
    mem_after = get_memory_mb()

    print(f"      Time: {elapsed:.1f}s")
    print(f"      Memory: {mem_before:.0f}MB -> {mem_after:.0f}MB")
    print()

    # [2] Verify upload
    print("[2/3] Verifying upload...")
    verify_start = time.time()
    result = pl.read_parquet(target, storage_options={"token": hf_token})
    verify_time = time.time() - verify_start
    rows_written = result.shape[0]
    print(f"      Rows: {rows_written:,}")
    print(f"      Columns: {result.shape[1]}")
    print(f"      Read time: {verify_time:.1f}s")
    print()

    # [3] Summary
    print("[3/3] Summary")
    print("=" * 60)
    if rows_written > 0:
        print("RESULT: PASS")
        print(f"  Output: {target}")
        throughput = rows_written / elapsed if elapsed > 0 else 0
        print(f"  Throughput: {throughput:,.0f} rows/sec")
    else:
        print("RESULT: FAIL - No rows written")
        sys.exit(1)


if __name__ == "__main__":
    main()
