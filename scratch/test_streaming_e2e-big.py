"""E2E test: full dataset scan → filter → transform → sink.

Usage:
    HF_TOKEN=hf_... python scratch/test_streaming_e2e-big.py

    # With memray memory profiling:
    HF_TOKEN=hf_... python -m memray run -o scratch/memray-big.bin scratch/test_streaming_e2e-big.py
    python -m memray stats scratch/memray-big.bin
    python -m memray flamegraph scratch/memray-big.bin -o scratch/memray-big.html

Processes the FULL SWE_Rebench split (no .head() limit) to validate that
memory stays bounded during streaming upload.
"""

import os
import time

import polars as pl

HF_TOKEN = os.environ["HF_TOKEN"]
NAMESPACE = os.environ.get("HF_BUCKET_NAMESPACE", "davanstrien")
BUCKET = os.environ.get("HF_BUCKET_NAME", "polars-ci-test")
STORAGE_OPTS = {"token": HF_TOKEN}

SOURCE = "hf://datasets/togethercomputer/CoderForge-Preview/trajectories/SWE_Rebench-*.parquet"
TARGET_FILE = "e2e_coderforge_curated-big.parquet"
TARGET = f"hf://buckets/{NAMESPACE}/{BUCKET}/{TARGET_FILE}"

print(f"Scanning {SOURCE} ...")
t0 = time.time()

lf = pl.scan_parquet(SOURCE, storage_options=STORAGE_OPTS)
schema = lf.collect_schema()
print(f"  Schema: {schema}")

# ---------------------------------------------------------------------------
# ETL pipeline: filter + transform (all lazy, streamed — NO .head() limit)
# ---------------------------------------------------------------------------

lf = (
    lf
    .filter(pl.col("reward") > 0)
    .filter(pl.col("messages").str.len_chars() > 0)
    .with_columns(
        pl.col("messages").str.len_chars().alias("message_len"),
        pl.when(pl.col("reward") >= 1.0)
        .then(pl.lit("high"))
        .when(pl.col("reward") >= 0.5)
        .then(pl.lit("medium"))
        .otherwise(pl.lit("low"))
        .alias("reward_tier"),
        pl.col("finish_reason").str.to_lowercase().alias("finish_reason_clean"),
    )
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
)

print(f"\nPipeline: filter(reward>0) → add columns → select (FULL dataset, no sort)")
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

assert "reward_tier" in result.columns, "Missing reward_tier column"
assert "message_len" in result.columns, "Missing message_len column"
assert "finish_reason_clean" in result.columns, "Missing finish_reason_clean column"
assert (result["reward"] > 0).all(), "Filter not applied: found reward <= 0"
print(f"  Reward range: {result['reward'].min():.2f} — {result['reward'].max():.2f}")
print(
    f"  Reward tiers: {result['reward_tier'].value_counts().sort('reward_tier').to_dict()}"
)
print(f"  Avg message length: {result['message_len'].mean():.0f} chars")

print(f"\nTotal time: {time.time() - t0:.1f}s")
print("All checks passed!")
