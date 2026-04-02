# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "huggingface-hub>=1.6.0",
#     "polars==1.38.1",
#     "polars-runtime-32==1.38.1",
# ]
#
# [tool.uv.sources]
# polars = {url = "https://huggingface.co/datasets/davanstrien/polars-hf-bucket-sink-wheels/resolve/main/polars-1.38.1-py3-none-any.whl"}
# polars-runtime-32 = {url = "https://huggingface.co/datasets/davanstrien/polars-hf-bucket-sink-wheels/resolve/main/polars_runtime_32-1.38.1-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"}
# ///
"""
Filter FinePDFs → HF Bucket

Scans 3.65TB of FinePDFs (1733 languages), filters to high-quality
educational content, and streams directly to an HF Bucket.

Run locally:
    uv run scratch/finepdfs_to_bucket.py

Run on HF Jobs:
    hf jobs uv run scratch/finepdfs_to_bucket.py --flavor cpu-upgrade --timeout 6h
"""

import os
import sys
import time

import polars as pl
from huggingface_hub import HfApi
from huggingface_hub.utils._auth import get_token


def main():
    test_mode = "--test" in sys.argv
    token = os.environ.get("HF_TOKEN") or get_token()
    so = {"token": token}

    bucket_name = "finepdfs-edu-gold"
    bucket_id = f"davanstrien/{bucket_name}"
    filename = "test-sample.parquet" if test_mode else "finepdfs-edu-gold.parquet"
    output_path = f"hf://buckets/{bucket_id}/{filename}"

    api = HfApi()
    api.create_bucket(bucket_name, private=False, exist_ok=True)

    mode_label = "TEST (head 100)" if test_mode else "FULL"
    print("=" * 60)
    print(f"FinePDFs → HF Bucket: Educational Gold Filter [{mode_label}]")
    print("=" * 60)
    print(f"Source:  hf://datasets/HuggingFaceFW/finepdfs/data/*/train/*.parquet")
    print(f"Output:  {output_path}")
    print(f"Filters: edu>3.0, no dupes, not truncated, tokens>500")
    print()

    t0 = time.time()

    lf = (
        pl.scan_parquet(
            "hf://datasets/HuggingFaceFW/finepdfs/data/*/train/*.parquet",
            storage_options=so,
            missing_columns="insert",
        )
        .filter(pl.col("fw_edu_scores").list.mean() > 3.0)
        .filter(pl.col("duplicate_count") == 0)
        .filter(~pl.col("is_truncated"))
        .filter(pl.col("token_count") > 500)
        .select("id", "url", "text", "token_count", "language")
    )

    if test_mode:
        lf = lf.head(100)

    lf.sink_parquet(output_path, storage_options=so)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s ({elapsed/3600:.1f}h)")

    print("\nBucket contents:")
    for f in api.list_bucket_tree(bucket_id):
        print(f"  {f.path}: {f.size / (1024*1024):.1f} MB")


if __name__ == "__main__":
    main()
