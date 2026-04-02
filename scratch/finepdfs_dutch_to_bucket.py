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
Filter Dutch FinePDFs → HF Bucket

Scans ~74GB of Dutch PDFs from FinePDFs (25 shards), filters to
high-quality educational content, and streams directly to an HF Bucket.

Run on HF Jobs:
    hf jobs uv run scratch/finepdfs_dutch_to_bucket.py --flavor cpu-upgrade --timeout 2h --secrets HF_TOKEN
"""

import os
import sys
import time
import threading

import polars as pl
from huggingface_hub import HfApi
from huggingface_hub.utils._auth import get_token


def heartbeat(t0, stop_event):
    """Print a heartbeat every 60s so we know the job is alive."""
    while not stop_event.is_set():
        stop_event.wait(60)
        if not stop_event.is_set():
            elapsed = time.time() - t0
            print(f"  [heartbeat] {elapsed:.0f}s elapsed, pipeline still running...", flush=True)


def main():
    test_mode = "--test" in sys.argv
    token = os.environ.get("HF_TOKEN") or get_token()
    so = {"token": token}

    bucket_name = "finepdfs-edu-gold"
    bucket_id = f"davanstrien/{bucket_name}"
    filename = "nld-test-sample.parquet" if test_mode else "nld-edu-gold.parquet"
    output_path = f"hf://buckets/{bucket_id}/{filename}"

    api = HfApi()
    api.create_bucket(bucket_name, private=False, exist_ok=True)

    lang = "nld_Latn"
    source = f"hf://datasets/HuggingFaceFW/finepdfs/data/{lang}/train/*.parquet"

    mode_label = "TEST (head 100)" if test_mode else "FULL"
    print("=" * 60, flush=True)
    print(f"FinePDFs Dutch → HF Bucket [{mode_label}]", flush=True)
    print("=" * 60, flush=True)
    print(f"Source:  {source}", flush=True)
    print(f"Output:  {output_path}", flush=True)
    print(f"Shards:  25 (~74 GB)", flush=True)
    print(f"Filters: edu>3.0, no dupes, not truncated, tokens>500", flush=True)
    print(flush=True)

    t0 = time.time()

    # Start heartbeat thread
    stop = threading.Event()
    hb = threading.Thread(target=heartbeat, args=(t0, stop), daemon=True)
    hb.start()

    print(f"[{time.time()-t0:.0f}s] Building lazy frame...", flush=True)

    lf = (
        pl.scan_parquet(source, storage_options=so)
        .filter(pl.col("fw_edu_scores").list.mean() > 3.0)
        .filter(pl.col("duplicate_count") == 0)
        .filter(~pl.col("is_truncated"))
        .filter(pl.col("token_count") > 500)
        .select("id", "url", "text", "token_count", "language")
    )

    if test_mode:
        lf = lf.head(100)

    print(f"[{time.time()-t0:.0f}s] Starting sink_parquet → {output_path}", flush=True)

    lf.sink_parquet(output_path, storage_options=so)

    stop.set()
    elapsed = time.time() - t0
    print(f"\n[DONE] {elapsed:.0f}s ({elapsed/60:.1f}m)", flush=True)

    print("\nBucket contents:", flush=True)
    for f in api.list_bucket_tree(bucket_id):
        print(f"  {f.path}: {f.size / (1024**3):.2f} GB", flush=True)


if __name__ == "__main__":
    main()
