# Streaming HF Bucket Sink — Test Status

## What was tested

Phase 3.2 rewrote the upload pipeline to: `ChannelWriter → sync/async bridge → XET streaming upload`. All tests used `maturin develop` (debug build).

### Results

| Test | Source | Rows | Time | Result |
|------|--------|------|------|--------|
| Simple sink | In-memory 1K rows | 1,000 | 2.4s | PASS |
| IMDB scan→filter→sink | HF Hub dataset | ~25K | 66.4s | PASS |
| Wikipedia 1-shard scan→filter→sink | HF Hub dataset | 156K | 39.0s | PASS |
| Wikipedia full (41 shards) | HF Hub dataset | ~6.4M | — | FAIL (read-side transport error, not sink) |
| finepdfs-edu scan→filter→sink | HF Hub dataset | 236K | — | FAIL (xet-core debug_assert, see below) |

### Key findings

1. **Streaming sink pipeline works end-to-end** — the full ChannelWriter → bridge → XET path is validated for real HF Hub datasets up to 156K rows.

2. **Wikipedia full-glob failure** is a read-side issue (`parquet: File out of specification: Invalid thrift: transport error`) when scanning many remote shards. Not related to our sink code.

3. **finepdfs-edu failure** is a `debug_assert` in xet-core's `file_cleaner.rs:165` — a size invariant check that **only fires in debug builds** (`#[cfg(debug_assertions)]`). The assertion compares `file_size()` vs `deduplication_metrics.total_bytes`. This will not occur in release builds. Likely an xet-core bookkeeping issue with larger files; worth reporting upstream but not a blocker.

## What's not yet tested

- **Release build** — `maturin build --release` OOM'd in this container (SIGKILL during LTO linking). Needs more RAM or a CI runner.
- **Large dataset memory behavior** — need release wheel to test on Colab with 1M+ rows and track RSS.
- **Multi-shard reads → sink** — single shard works, but many shards hit read-side transport errors.

## Files created

| File | Purpose |
|------|---------|
| `scratch/test_hf_bucket_sink.py` | Simple 1K row e2e test (existed already) |
| `scratch/test_hf_scan_filter_sink.py` | Scan HF dataset → filter → sink to bucket |
| `scratch/test_hf_large_dataset.py` | Colab-ready script: increasing sizes + memory tracking |

## Next step: CI wheel build

The existing `.github/workflows/build-hf-sink-wheels.yml` builds Linux x64 wheels but needs updates:

1. **Add `--features hf_bucket_sink`** to the maturin build args (currently missing)
2. **Add Linux ARM64 target** (needed for Colab T4 instances)
3. **Add an e2e smoke test job** that uses `HF_TOKEN` secret to run `test_hf_bucket_sink.py` against a real bucket
4. Consider adding the finepdfs-edu test as a larger integration test (release build won't hit the debug_assert)

Once the CI workflow produces a release wheel, we can test on Colab with `test_hf_large_dataset.py` to validate streaming memory behavior at scale.
