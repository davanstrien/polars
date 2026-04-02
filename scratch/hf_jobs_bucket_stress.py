# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "huggingface_hub",
#     "psutil",
# ]
# ///
"""Stress test: scan FineWeb-Edu (full 10BT sample), filter, sink to HF Bucket.

Run via HF Jobs:
    hf jobs uv run scratch/hf_jobs_bucket_stress.py \
        --secret HF_TOKEN \
        --flavor cpu-basic \
        --timeout 7200

Monitors RSS memory every 30s to confirm O(row_group) streaming behavior.
"""

import os
import subprocess
import sys
import threading
import time

# Force unbuffered stdout so logs appear in real-time on HF Jobs
os.environ["PYTHONUNBUFFERED"] = "1"
sys.stdout.reconfigure(line_buffering=True)

# ── Install custom Polars wheels before importing polars ──
HF_WHEEL_REPO = "https://huggingface.co/datasets/davanstrien/polars-hf-bucket-sink-wheels/resolve/main"
WHEELS = [
    f"{HF_WHEEL_REPO}/polars-1.38.1-py3-none-any.whl",
    f"{HF_WHEEL_REPO}/polars_runtime_32-1.38.1-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl",
]

print("Installing custom Polars wheels...")
subprocess.check_call(
    ["uv", "pip", "install", "--no-deps", "--force-reinstall", "--python", sys.executable, *WHEELS],
    stdout=sys.stdout,
    stderr=sys.stderr,
)

import polars as pl  # noqa: E402
import psutil  # noqa: E402

print(f"Polars version: {pl.__version__}")

# ── Config ──
TOKEN = os.environ["HF_TOKEN"]
NAMESPACE = os.environ.get("HF_BUCKET_NAMESPACE", "davanstrien")
BUCKET = os.environ.get("HF_BUCKET_NAME", "polars-ci-test")
BUCKET_BASE = f"hf://buckets/{NAMESPACE}/{BUCKET}"
STORAGE_OPTIONS = {"token": TOKEN}

# Configurable via env vars for different subsets (10BT, 100BT, 350BT)
SOURCE = os.environ.get(
    "SOURCE",
    "hf://datasets/HuggingFaceFW/fineweb-edu/sample/10BT/*.parquet",
)
OUTPUT_NAME = os.environ.get("OUTPUT_NAME", "stress-fineweb-edu-10bt-filtered.parquet")
OUTPUT = f"{BUCKET_BASE}/{OUTPUT_NAME}"


# ── Memory monitor ──
def memory_monitor(interval: int = 30, stop_event: threading.Event | None = None):
    """Log RSS every `interval` seconds."""
    process = psutil.Process()
    peak_mb = 0.0
    while not (stop_event and stop_event.is_set()):
        rss_mb = process.memory_info().rss / (1024 * 1024)
        peak_mb = max(peak_mb, rss_mb)
        print(f"[mem] RSS={rss_mb:.0f} MB  peak={peak_mb:.0f} MB")
        stop_event.wait(interval) if stop_event else time.sleep(interval)
    print(f"[mem] final peak={peak_mb:.0f} MB")


# ── Run ──
print(f"Source: {SOURCE}")
print(f"Output: {OUTPUT}")
print("Starting scan -> filter -> sink ...")

stop = threading.Event()
monitor = threading.Thread(target=memory_monitor, args=(30, stop), daemon=True)
monitor.start()

t0 = time.time()

(
    pl.scan_parquet(SOURCE, storage_options=STORAGE_OPTIONS)
    .filter(pl.col("score").ge(4.0))
    .filter(pl.col("token_count").le(800))
    .sink_parquet(OUTPUT, storage_options=STORAGE_OPTIONS)
)

elapsed = time.time() - t0
stop.set()
monitor.join(timeout=5)

print(f"\nDone in {elapsed:.0f}s ({elapsed / 60:.1f} min)")
print(f"Output: {OUTPUT}")
