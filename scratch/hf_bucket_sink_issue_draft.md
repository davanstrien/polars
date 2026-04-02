# feat: Add streaming sink support for Hugging Face Storage Buckets

## Description

[Hugging Face Storage Buckets](https://huggingface.co/blog/storage-buckets) are S3-like object storage on the Hugging Face Hub, backed by XET, a content-addressed, deduplicated storage layer. They're designed for large-scale data workflows, including ML training data and dataset processing pipelines etc. Unlike dataset repos on Hugging Face, they don't use Git, so they avoid a lot of the overhead of Git.

I've been exploring adding a native streaming sink that enables `sink_parquet` to write directly to HF Storage Buckets via the XET protocol. I have built a proof-of-concept implementation on a fork to demonstrate feasibility.

## Why a native sink? (Why not fsspec or the existing cloud path?)

Polars uses the `object_store` Rust crate for cloud writes — not Python's fsspec. This means:

1. **`object_store` has no HF/XET backend** — there's no way to route `hf://buckets/` URLs through the existing cloud write path
2. **fsspec can't help with `sink_parquet`** — polars' streaming sink needs a Rust-level `Write` target, not a Python file object
3. **The only workaround is eager writes** — `df.write_parquet(hf_fs.open(..., "wb"))` works via fsspec but buffers the entire file in memory before uploading, defeating the purpose of streaming

The gap is specifically in the **streaming write path**: there's no way to incrementally write Parquet data to HF buckets as it's produced by the streaming engine.

## Proposed API

```python
import polars as pl

# Streaming sink — O(row_group_size) memory, handles arbitrarily large datasets
df.lazy().sink_parquet(
    "hf://buckets/username/my-bucket/data.parquet",
    storage_options={"token": "hf_xxx"}  # or HF_TOKEN env var / ~/.cache/huggingface/token
)

# Works with full lazy pipelines
(
    pl.scan_parquet("s3://source-bucket/raw/*.parquet")
    .filter(pl.col("language") == "en")
    .select("id", "text", "score")
    .sink_parquet("hf://buckets/myorg/processed-data/filtered.parquet")
)
```

## Implementation overview

The proof-of-concept is feature-gated behind `hf_bucket_sink`, i.e. opt-in, zero impact on default builds.

The PoC is for sure not ready to merge as is and I used AI to generate a lot of the code. I have been fairly careful in testing and think the PoC is realistic enough to suggest this feature is feasible.

**Architecture:**

```
sink_parquet("hf://buckets/...")
  → IR lowering detects hf://buckets/ prefix
  → Routes to HfBucketSinkNode (instead of generic FileSink)
  → StreamingBucketUploader:
      - Encodes parquet row groups incrementally
      - Streams bytes through bounded channels (backpressure)
      - Uploads via XET CAS protocol (content-addressed, deduplicated)
      - Registers file via HF batch API on completion
```

**Key properties:**

Some of these details could change. https://github.com/huggingface/xet-core/ is going through some refactors, so I would want to wait till those are settled before making a PR.

- **Memory**: O(row_group_size), not O(total_dataset) — bounded channel (capacity 16) provides backpressure between encoding and upload
- **Streaming**: true overlap between parquet encoding and network upload
- **Parquet-only**: validated at plan compilation time with a clear error for other formats (CSV, JSON, etc.)
- **Token management**: supports `HF_TOKEN` env var, `~/.cache/huggingface/token`, or explicit `storage_options`
- **Dependencies**: `hf-xet`, `xet-client` crates (feature-gated, only pulled in when `hf_bucket_sink` is enabled)

## Current status

**This is a proof-of-concept intended to demonstrate feasibility, not a production-ready PR.**

- **Branch**: [`feature/hf-bucket-sink`](https://github.com/davanstrien/polars/tree/feature/hf-bucket-sink) on davanstrien/polars
- **Tests**: 14 unit tests passing (URL parsing, token extraction, channel backpressure, streaming behavior)
- **Upstream sync**: merged with upstream main as of 2026-03-13
- Successfully tested with real HF buckets (datasets up to several GB) i.e. https://x.com/vanstriendaniel/status/2031989600340558249

## Scope

**This proposal covers:**
- Streaming `sink_parquet` to `hf://buckets/` URLs

**Out of scope (potential follow-ups):**
- `scan_parquet` from `hf://buckets/` URLs (read path — would need an ObjectStore impl backed by XET)
- Non-parquet sink formats (CSV, NDJSON could be nice to add too but Parquet might make sense to start).

## Questions for maintainers

Is this something you'd be open to adding? As mentioned, the current branch used AI heavily, so I would plan to get some feedback on a draft of the PR from some people on the HF side before asking for a review.
