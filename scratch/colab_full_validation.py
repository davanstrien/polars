"""
Full validation suite for hf_bucket_sink feature on Colab.

Instructions:
1. Download both wheel artifacts from CI:
   gh run download <RUN_ID> -R davanstrien/polars -D wheels/
2. Upload .whl files to Colab
3. In a cell, run:
   !pip uninstall polars polars-runtime-32 -y
   !pip install --no-deps --force-reinstall polars-*.whl polars_runtime_32-*.whl
4. Restart runtime (Runtime -> Restart runtime)
5. Run this script

IMPORTANT: Always use --no-deps to prevent pip from pulling the upstream
polars-runtime-32 from PyPI (which lacks the hf_bucket_sink feature).

NOTE: Read-back verification uses huggingface_hub to download from buckets,
since polars does not yet support reading from hf://buckets/ URLs.
"""

import os
import sys
import time
import uuid
import tempfile
import traceback

# ---------------------------------------------------------------------------
# Token setup
# ---------------------------------------------------------------------------
HF_TOKEN = os.environ.get("HF_TOKEN")
if not HF_TOKEN:
    try:
        from google.colab import userdata
        HF_TOKEN = userdata.get("HF_TOKEN")
        os.environ["HF_TOKEN"] = HF_TOKEN
    except Exception:
        pass

if not HF_TOKEN:
    print("ERROR: HF_TOKEN not set (env var or Colab secret)")
    sys.exit(1)

import polars as pl

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NAMESPACE = "davanstrien"
BUCKET_NAME = "test-polars-bucket"
STORAGE_OPTS = {"token": HF_TOKEN}

results = []


def read_back_from_bucket(file_path):
    """Download a file from an HF bucket and read it locally.

    polars doesn't support reading hf://buckets/ URLs, so we use
    huggingface_hub to download first, then read locally.
    """
    from huggingface_hub import HfApi

    api = HfApi(token=HF_TOKEN)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, "downloaded.parquet")
        api.download_file_from_bucket(
            namespace=NAMESPACE,
            bucket_name=BUCKET_NAME,
            path_in_bucket=file_path,
            local_dir=tmpdir,
        )
        # The file is downloaded to tmpdir/file_path
        downloaded = os.path.join(tmpdir, file_path)
        if not os.path.exists(downloaded):
            # Might be flat in tmpdir
            downloaded = local_path
        return pl.read_parquet(downloaded)


def run_test(name, fn):
    """Run a test function, capture pass/fail and timing."""
    print(f"\n{'=' * 60}")
    print(f"TEST: {name}")
    print(f"{'=' * 60}")
    start = time.time()
    try:
        fn()
        elapsed = time.time() - start
        print(f"PASSED ({elapsed:.1f}s)")
        results.append((name, "PASS", elapsed))
    except Exception as e:
        elapsed = time.time() - start
        print(f"FAILED ({elapsed:.1f}s): {e}")
        traceback.print_exc()
        results.append((name, "FAIL", elapsed))


# ---------------------------------------------------------------------------
# Test 0: Version & feature check
# ---------------------------------------------------------------------------
def test_version_check():
    print(f"Polars version: {pl.__version__}")
    pl.show_versions()
    print(f"\nBasic import: OK")


# ---------------------------------------------------------------------------
# Test 1: Smoke test — 3 rows to bucket
# ---------------------------------------------------------------------------
def test_smoke_3_rows():
    test_id = uuid.uuid4().hex[:8]
    file_name = f"validate-smoke-{test_id}.parquet"
    target = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/{file_name}"
    print(f"Target: {target}")

    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df.lazy().sink_parquet(target, storage_options=STORAGE_OPTS)
    print("Write: OK (3 rows, 2 columns)")


# ---------------------------------------------------------------------------
# Test 2: Medium dataset — 10K synthetic rows
# ---------------------------------------------------------------------------
def test_medium_10k_rows():
    test_id = uuid.uuid4().hex[:8]
    file_name = f"validate-10k-{test_id}.parquet"
    target = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/{file_name}"
    n = 10_000

    df = pl.DataFrame({
        "id": range(n),
        "text": [f"row_{i}_" + "a" * 80 for i in range(n)],
        "category": [f"cat_{i % 100}" for i in range(n)],
        "value": [float(i) * 0.1 for i in range(n)],
    })
    print(f"Target: {target} ({n:,} rows, 4 columns)")

    df.lazy().sink_parquet(target, storage_options=STORAGE_OPTS)
    print(f"Write: OK ({n:,} rows)")


# ---------------------------------------------------------------------------
# Test 3: Scan HF dataset -> filter -> sink to bucket
# ---------------------------------------------------------------------------
def test_scan_filter_sink():
    test_id = uuid.uuid4().hex[:8]
    source = "hf://datasets/nvidia/OpenMathReasoning/data/cot-*.parquet"
    file_name = f"validate-scan-{test_id}.parquet"
    target = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/{file_name}"
    print(f"Source: {source}")
    print(f"Target: {target}")

    lf = pl.scan_parquet(source, storage_options=STORAGE_OPTS)
    print(f"Schema: {lf.collect_schema()}")

    # Take 5K rows with a filter
    filtered = lf.filter(
        pl.col("generated_solution").str.len_chars() > 500
    ).head(5_000)

    filtered.sink_parquet(target, storage_options=STORAGE_OPTS)
    print("Write: OK (up to 5,000 filtered rows)")


# ---------------------------------------------------------------------------
# Test 4: 100K rows — streaming memory check
# ---------------------------------------------------------------------------
def test_large_100k_rows():
    test_id = uuid.uuid4().hex[:8]
    file_name = f"validate-100k-{test_id}.parquet"
    target = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/{file_name}"
    n = 100_000

    df = pl.DataFrame({
        "id": range(n),
        "text": [f"row_{i}_" + "b" * 200 for i in range(n)],
        "value": [float(i) for i in range(n)],
    })
    print(f"Target: {target} ({n:,} rows)")

    df.lazy().sink_parquet(target, storage_options=STORAGE_OPTS)
    print(f"Write: OK ({n:,} rows)")


# ---------------------------------------------------------------------------
# Test 5: Write + read-back verification via huggingface_hub
# ---------------------------------------------------------------------------
def test_readback_verification():
    """Write a small DataFrame, download via huggingface_hub, verify contents."""
    test_id = uuid.uuid4().hex[:8]
    file_name = f"validate-readback-{test_id}.parquet"
    target = f"hf://buckets/{NAMESPACE}/{BUCKET_NAME}/{file_name}"

    df = pl.DataFrame({"x": [10, 20, 30], "y": ["a", "b", "c"]})
    df.lazy().sink_parquet(target, storage_options=STORAGE_OPTS)
    print("Write: OK")

    # Download via huggingface_hub and verify
    from huggingface_hub import HfApi
    api = HfApi(token=HF_TOKEN)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = api.download_file_from_bucket(
            namespace=NAMESPACE,
            bucket_name=BUCKET_NAME,
            path_in_bucket=file_name,
            local_dir=tmpdir,
        )
        result = pl.read_parquet(local_path)

    assert result.shape == (3, 2), f"Expected (3, 2), got {result.shape}"
    assert result["x"].to_list() == [10, 20, 30], f"Data mismatch: {result['x'].to_list()}"
    assert result["y"].to_list() == ["a", "b", "c"], f"Data mismatch: {result['y'].to_list()}"
    print(f"Read back: {result.shape[0]} rows, data integrity verified")


# ---------------------------------------------------------------------------
# Run all tests
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Polars HF Bucket Sink — Full Validation Suite")
    print(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print()

    run_test("Version & feature check", test_version_check)
    run_test("Smoke test (3 rows)", test_smoke_3_rows)
    run_test("Medium dataset (10K rows)", test_medium_10k_rows)
    run_test("Scan -> filter -> sink (HF dataset)", test_scan_filter_sink)
    run_test("Large dataset (100K rows)", test_large_100k_rows)
    run_test("Write + read-back verification", test_readback_verification)

    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print(f"{'=' * 60}")
    passed = sum(1 for _, status, _ in results if status == "PASS")
    total = len(results)
    for name, status, elapsed in results:
        marker = "OK" if status == "PASS" else "FAIL"
        print(f"  [{marker:>4}] {name} ({elapsed:.1f}s)")
    print(f"\n{passed}/{total} tests passed")

    if passed < total:
        sys.exit(1)
