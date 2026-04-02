"""E2E test: scan → filter → transform → sink, mimicking a dataset reformatting pipeline.

Usage:
    HF_TOKEN=hf_... python scratch/test_streaming_e2e.py

Reads CoderForge-Preview from HF datasets, applies filters and transforms
(like a real data curation pipeline), then sinks the result to an HF bucket.
Everything runs lazily — scan_parquet → polars expressions → sink_parquet.
"""

import os
import time

import polars as pl

HF_TOKEN = os.environ["HF_TOKEN"]
NAMESPACE = os.environ.get("HF_BUCKET_NAMESPACE", "davanstrien")
BUCKET = os.environ.get("HF_BUCKET_NAME", "polars-ci-test")
STORAGE_OPTS = {"token": HF_TOKEN}

SOURCE = "hf://datasets/togethercomputer/CoderForge-Preview/trajectories/SWE_Rebench-*.parquet"
TARGET_FILE = "e2e_coderforge_curated.parquet"
TARGET = f"hf://buckets/{NAMESPACE}/{BUCKET}/{TARGET_FILE}"

print(f"Scanning {SOURCE} ...")
t0 = time.time()

lf = pl.scan_parquet(SOURCE, storage_options=STORAGE_OPTS)
schema = lf.collect_schema()
print(f"  Schema: {schema}")

# ---------------------------------------------------------------------------
# ETL pipeline: filter + transform (all lazy, streamed)
# ---------------------------------------------------------------------------

lf = (
    lf
    # Filter: only keep rows with positive reward
    .filter(pl.col("reward") > 0)
    # Filter: drop rows with null/empty messages
    .filter(pl.col("messages").str.len_chars() > 0)
    # Transform: add computed columns
    .with_columns(
        # Message length in characters
        pl.col("messages").str.len_chars().alias("message_len"),
        # Categorize reward into buckets
        pl.when(pl.col("reward") >= 1.0)
        .then(pl.lit("high"))
        .when(pl.col("reward") >= 0.5)
        .then(pl.lit("medium"))
        .otherwise(pl.lit("low"))
        .alias("reward_tier"),
        # Extract finish reason as categorical-style column
        pl.col("finish_reason").str.to_lowercase().alias("finish_reason_clean"),
    )
    # Select: reorder and pick columns (drop raw image column to save space)
    .select(
        "trajectory_id",
        "finish_reason_clean",
        "reward",
        "reward_tier",
        "message_len",
        "messages",
        "tools",
        "license",
    )
    # Sort by reward descending
    .sort("reward", descending=True)
    # Bound the output
    .head(10_000)
)

print(f"\nPipeline: filter(reward>0) → add columns → select → sort → head(10k)")
print(f"Sinking to {TARGET} ...")
t1 = time.time()

lf.sink_parquet(TARGET, storage_options=STORAGE_OPTS)

elapsed = time.time() - t1
print(f"  Upload complete in {elapsed:.1f}s")

# ---------------------------------------------------------------------------
# Read back and verify
# ---------------------------------------------------------------------------

print("\nReading back for verification...")
from huggingface_hub import download_bucket_files
import tempfile

with tempfile.TemporaryDirectory() as tmpdir:
    local_path = os.path.join(tmpdir, TARGET_FILE)
    download_bucket_files(
        f"{NAMESPACE}/{BUCKET}",
        files=[(TARGET_FILE, local_path)],
        token=HF_TOKEN,
    )
    result = pl.read_parquet(local_path)

print(f"  Shape: {result.shape}")
print(f"  Columns: {result.columns}")
print(f"  Memory: {result.estimated_size('mb'):.1f} MB")

# Verify transforms were applied
assert "reward_tier" in result.columns, "Missing reward_tier column"
assert "message_len" in result.columns, "Missing message_len column"
assert "finish_reason_clean" in result.columns, "Missing finish_reason_clean column"
assert (result["reward"] > 0).all(), "Filter not applied: found reward <= 0"
assert result["reward"].is_sorted(descending=True), "Sort not applied"
print(f"  Reward range: {result['reward'].min():.2f} — {result['reward'].max():.2f}")
print(f"  Reward tiers: {result['reward_tier'].value_counts().sort('reward_tier').to_dict()}")
print(f"  Avg message length: {result['message_len'].mean():.0f} chars")

print(f"\nTotal time: {time.time() - t0:.1f}s")
print("All checks passed!")
