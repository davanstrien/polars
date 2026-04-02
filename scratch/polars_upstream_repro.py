# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars",
#     "huggingface_hub",
#     "psutil"
# ]
# ///
"""
Upstream Polars reproducer (no custom wheels).

Reproduces multi-file scan memory growth on local parquet files.
Uses HF snapshot_download to fetch a subset locally, then scans from disk.
"""
import os
import sys
import time
import threading
import argparse
import fnmatch
import psutil

print("Polars upstream reproducer: script start", flush=True)

try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass


def parse_args():
    parser = argparse.ArgumentParser(description="Polars upstream memory repro")
    parser.add_argument("--concurrent-scans", type=int, default=1,
                        help="POLARS_MAX_CONCURRENT_SCANS (default: 1)")
    parser.add_argument("--prefetch-size", type=int, default=1,
                        help="POLARS_ROW_GROUP_PREFETCH_SIZE (default: 1)")
    parser.add_argument("--source", type=str,
                        default="hf://datasets/nvidia/OpenMathReasoning/data/cot-0000*-of-00144.parquet",
                        help="HF dataset glob (default: 10 cot files)")
    parser.add_argument("--download-dir", type=str, default="/tmp/hf_cache",
                        help="Local directory for downloaded files")
    parser.add_argument("--local-sink", type=str, default="/tmp/out.parquet",
                        help="Local sink path")
    parser.add_argument("--monitor-interval", type=float, default=1.0,
                        help="Memory sampling interval in seconds")
    parser.add_argument("--post-sleep", type=float, default=60.0,
                        help="Seconds to wait after completion to observe memory release (default: 60)")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip HF glob preflight check")
    return parser.parse_args()


args = parse_args()

os.environ["POLARS_MAX_CONCURRENT_SCANS"] = str(args.concurrent_scans)
os.environ["POLARS_ROW_GROUP_PREFETCH_SIZE"] = str(args.prefetch_size)

import polars as pl

print("=" * 70, flush=True)
print("Upstream Polars Memory Repro", flush=True)
print("=" * 70, flush=True)
print(f"Polars version: {pl.__version__}", flush=True)
print(f"POLARS_MAX_CONCURRENT_SCANS: {args.concurrent_scans}", flush=True)
print(f"POLARS_ROW_GROUP_PREFETCH_SIZE: {args.prefetch_size}", flush=True)
print(f"Source: {args.source}", flush=True)
print(f"Download dir: {args.download_dir}", flush=True)
print(f"Local sink: {args.local_sink}", flush=True)
print("", flush=True)

# Memory monitoring
memory_history = []
peak_memory_gb = 0.0
running = True


def monitor_memory():
    global peak_memory_gb
    start = time.time()
    while running:
        try:
            current_gb = psutil.Process().memory_info().rss / 1024**3
            peak_memory_gb = max(peak_memory_gb, current_gb)
            elapsed = time.time() - start
            memory_history.append((elapsed, current_gb))
            print(f"[{elapsed:6.1f}s] Memory: {current_gb:.2f} GB (peak: {peak_memory_gb:.2f} GB)", flush=True)
        except Exception:
            pass
        time.sleep(args.monitor_interval)


monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
monitor_thread.start()

start_time = time.time()
try:
    prefix = "hf://datasets/"
    if not args.source.startswith(prefix):
        raise ValueError("--source must start with hf://datasets/ for this repro")

    path = args.source[len(prefix):]
    parts = path.split("/")
    if len(parts) < 3:
        raise ValueError("Invalid hf://datasets/<org>/<repo>/... source")

    repo_id = f"{parts[0]}/{parts[1]}"
    rel_glob = "/".join(parts[2:])

    if not args.skip_preflight:
        from huggingface_hub import HfApi
        api = HfApi()
        files = api.list_repo_files(
            repo_id=repo_id,
            repo_type="dataset",
            token=os.environ.get("HF_TOKEN"),
        )
        matched = [f for f in files if fnmatch.fnmatch(f, rel_glob)]
        print(f"Preflight: matched {len(matched)} file(s) for glob: {rel_glob}", flush=True)
        if len(matched) == 0:
            print("Preflight failed: no files matched. Exiting.", flush=True)
            sys.exit(1)

    from huggingface_hub import snapshot_download
    local_dir = snapshot_download(
        repo_id=repo_id,
        repo_type="dataset",
        allow_patterns=[rel_glob],
        local_dir=args.download_dir,
        local_dir_use_symlinks=False,
        token=os.environ.get("HF_TOKEN"),
    )

    scan_source = os.path.join(local_dir, rel_glob)
    print(f"Local source: {scan_source}", flush=True)

    lf = pl.scan_parquet(scan_source)
    filtered = lf.filter(pl.col("problem_source") == "MATH_training_set")
    filtered.sink_parquet(args.local_sink, engine="streaming")

    elapsed = time.time() - start_time
    print("", flush=True)
    print("=" * 70, flush=True)
    print("SUCCESS", flush=True)
    print("=" * 70, flush=True)
    print(f"Elapsed time: {elapsed:.1f}s", flush=True)
    print(f"Peak memory: {peak_memory_gb:.2f} GB", flush=True)

    if args.post_sleep and args.post_sleep > 0:
        print(f"Post-sleep: {args.post_sleep:.0f}s (observe memory release)", flush=True)
        time.sleep(args.post_sleep)

finally:
    running = False
    time.sleep(1)
    try:
        final_gb = psutil.Process().memory_info().rss / 1024**3
        peak_memory_gb = max(peak_memory_gb, final_gb)
        print(f"Final memory: {final_gb:.2f} GB", flush=True)
    except Exception:
        pass

    print("", flush=True)
    print("=" * 70, flush=True)
    print("Summary", flush=True)
    print("=" * 70, flush=True)
    print(f"Peak memory: {peak_memory_gb:.2f} GB", flush=True)
