# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars",
#     "polars-runtime-32",
#     "huggingface_hub",
#     "psutil"
# ]
# [tool.uv.sources]
# polars = { url = "https://github.com/davanstrien/polars/releases/download/hf-sink-v0.1.0-alpha/polars-1.37.1-py3-none-any.whl" }
# polars-runtime-32 = { url = "https://github.com/davanstrien/polars/releases/download/hf-sink-v0.1.0-alpha/polars_runtime_32-1.37.1-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" }
# ///
"""
ISSUE-005 Debug Script: Full sink_parquet test with memory monitoring.

This script runs the full filter + sink_parquet pipeline with conservative
memory settings and continuous memory monitoring.

Usage on HF Jobs:
    hf jobs uv run scratch/hf_jobs_memory_monitor.py \
        -s HF_TOKEN \
        --flavor cpu-upgrade \
        --timeout 60m \
        -- --concurrent-scans 4 --prefetch-size 2 --output-repo davanstrien/test-polars-streaming
"""
import os
import sys
import time
import threading
import argparse
import uuid
import psutil
import fnmatch

# Early banner to confirm script start in HF Jobs logs
print("ISSUE-005 Memory Monitor: script start", flush=True)

# Force line-buffered output in HF Jobs logs
try:
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
except Exception:
    pass

def parse_args():
    parser = argparse.ArgumentParser(description="ISSUE-005 Memory Monitor")
    parser.add_argument("--concurrent-scans", type=int, default=4,
                        help="POLARS_MAX_CONCURRENT_SCANS value (default: 4)")
    parser.add_argument("--prefetch-size", type=int, default=2,
                        help="POLARS_ROW_GROUP_PREFETCH_SIZE value (default: 2)")
    parser.add_argument("--output-repo", type=str, default="davanstrien/test-polars-streaming",
                        help="HF repo for output (default: davanstrien/test-polars-streaming)")
    parser.add_argument("--num-rows", type=int, default=None,
                        help="Limit number of rows (default: all filtered rows)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Don't actually write to HF, just measure memory")
    parser.add_argument("--local-sink", type=str, default=None,
                        help="Write to local path instead of hf:// (e.g. /tmp/out.parquet)")
    parser.add_argument("--download-local", action="store_true",
                        help="Download HF files to local disk and read from there")
    parser.add_argument("--download-dir", type=str, default="/tmp/hf_cache",
                        help="Local directory for downloaded files (default: /tmp/hf_cache)")
    parser.add_argument("--source", type=str,
                        default="hf://datasets/nvidia/OpenMathReasoning/data/*.parquet",
                        help="Input dataset glob (default: data/*.parquet)")
    parser.add_argument("--skip-preflight", action="store_true",
                        help="Skip HF glob preflight check")
    parser.add_argument("--monitor-interval", type=float, default=5.0,
                        help="Memory sampling interval in seconds (default: 5.0)")
    return parser.parse_args()

args = parse_args()

# Set environment variables BEFORE importing polars
os.environ["POLARS_MAX_CONCURRENT_SCANS"] = str(args.concurrent_scans)
os.environ["POLARS_ROW_GROUP_PREFETCH_SIZE"] = str(args.prefetch_size)
os.environ["POLARS_TRACK_METRICS"] = "1"
os.environ["POLARS_LOG_METRICS"] = "1"

# Now import polars
import polars as pl

print("=" * 70, flush=True)
print("ISSUE-005 Debug: Full Sink Parquet Memory Monitor", flush=True)
print("=" * 70, flush=True)
print(f"Polars version: {pl.__version__}", flush=True)
print(f"POLARS_MAX_CONCURRENT_SCANS: {args.concurrent_scans}", flush=True)
print(f"POLARS_ROW_GROUP_PREFETCH_SIZE: {args.prefetch_size}", flush=True)
print(f"Output repo: {args.output_repo}", flush=True)
print(f"Dry run: {args.dry_run}", flush=True)
print("", flush=True)

# Memory monitoring with history
memory_history = []
peak_memory_gb = 0
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
        except:
            pass
        time.sleep(args.monitor_interval)

monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
monitor_thread.start()

print(f"Initial memory: {psutil.Process().memory_info().rss / 1024**3:.2f} GB", flush=True)
print("", flush=True)

# Generate unique output filename
run_id = uuid.uuid4().hex[:8]
output_path = f"hf://datasets/{args.output_repo}/data/debug-memory-{run_id}.parquet"

print("Starting scan_parquet → filter → sink_parquet pipeline...", flush=True)
print(f"Source: {args.source}", flush=True)
print(f"Filter: problem_source == 'MATH_training_set'", flush=True)
print(f"Output: {output_path}", flush=True)
if args.local_sink:
    print(f"Local sink: {args.local_sink}", flush=True)
if args.download_local:
    print(f"Download local: {args.download_dir}", flush=True)
print("", flush=True)

start_time = time.time()
try:
    if not args.skip_preflight:
        try:
            prefix = "hf://datasets/"
            if args.source.startswith(prefix):
                path = args.source[len(prefix):]
                parts = path.split("/")
                if len(parts) >= 3:
                    repo_id = f"{parts[0]}/{parts[1]}"
                    rel_glob = "/".join(parts[2:])
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
                        print("Preflight failed: no files matched the glob. Exiting early.", flush=True)
                        sys.exit(1)
        except Exception as e:
            print(f"Preflight warning: failed to check HF glob ({type(e).__name__}: {e}). Continuing.", flush=True)

    scan_source = args.source
    storage_options = {"token": os.environ.get("HF_TOKEN")} if args.source.startswith("hf://") else None
    if args.download_local:
        prefix = "hf://datasets/"
        if not args.source.startswith(prefix):
            raise ValueError("--download-local requires hf://datasets/... source")
        path = args.source[len(prefix):]
        parts = path.split("/")
        if len(parts) < 3:
            raise ValueError("Invalid hf://datasets/<org>/<repo>/... source")
        repo_id = f"{parts[0]}/{parts[1]}"
        rel_glob = "/".join(parts[2:])
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
        storage_options = None
        print(f"Local source: {scan_source}", flush=True)

    lf = pl.scan_parquet(
        scan_source,
        storage_options=storage_options
    )

    # Apply filter
    filtered = lf.filter(pl.col("problem_source") == "MATH_training_set")

    # Optionally limit rows
    if args.num_rows:
        filtered = filtered.head(args.num_rows)

    if args.dry_run:
        # Just collect a sample to measure memory without writing
        print("DRY RUN: Collecting sample to measure memory...", flush=True)
        result = filtered.head(10000).collect()
        print(f"Sample collected: {len(result)} rows", flush=True)
    elif args.local_sink:
        print("Writing to local sink with engine='streaming'...", flush=True)
        filtered.sink_parquet(
            args.local_sink,
            engine="streaming"
        )
    else:
        # Full sink_parquet with streaming engine
        print("Running sink_parquet with engine='streaming'...", flush=True)
        filtered.sink_parquet(
            output_path,
            engine="streaming",
            storage_options={"token": os.environ.get("HF_TOKEN")}
        )

    elapsed = time.time() - start_time

    print("", flush=True)
    print("=" * 70, flush=True)
    print("SUCCESS!", flush=True)
    print("=" * 70, flush=True)
    print(f"Elapsed time: {elapsed:.1f}s", flush=True)
    print(f"Peak memory: {peak_memory_gb:.2f} GB", flush=True)
    if not args.dry_run:
        print(f"Output: {output_path}", flush=True)

except Exception as e:
    elapsed = time.time() - start_time
    print("", flush=True)
    print("=" * 70, flush=True)
    print("FAILED!", flush=True)
    print("=" * 70, flush=True)
    print(f"Error type: {type(e).__name__}", flush=True)
    print(f"Error: {e}", flush=True)
    print(f"Elapsed time before failure: {elapsed:.1f}s", flush=True)
    print(f"Peak memory: {peak_memory_gb:.2f} GB", flush=True)
    import traceback
    traceback.print_exc()

finally:
    running = False
    time.sleep(1)  # Let monitor thread finish
    try:
        final_gb = psutil.Process().memory_info().rss / 1024**3
        peak_memory_gb = max(peak_memory_gb, final_gb)
        print(f"Final memory: {final_gb:.2f} GB", flush=True)
    except Exception:
        pass

    print("", flush=True)
    print("=" * 70, flush=True)
    print("Memory History Summary", flush=True)
    print("=" * 70, flush=True)
    print("Configuration:", flush=True)
    print(f"  POLARS_MAX_CONCURRENT_SCANS: {args.concurrent_scans}", flush=True)
    print(f"  POLARS_ROW_GROUP_PREFETCH_SIZE: {args.prefetch_size}", flush=True)
    print("", flush=True)
    print("Results:", flush=True)
    print(f"  Peak memory: {peak_memory_gb:.2f} GB", flush=True)
    print(f"  Total time: {time.time() - start_time:.1f}s", flush=True)
    print("", flush=True)

    # Print memory growth pattern
    if len(memory_history) >= 5:
        print("Memory growth pattern (every 5th sample):", flush=True)
        for i, (t, mem) in enumerate(memory_history[::5]):
            bar = "#" * int(mem * 2)
            print(f"  {t:6.1f}s: {mem:5.2f} GB {bar}", flush=True)
