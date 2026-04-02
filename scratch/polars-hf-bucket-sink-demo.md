# Polars → HF Buckets: Streaming Parquet Sink via XET

**TL;DR**: A feature-gated Polars sink that writes parquet directly to HuggingFace Buckets via the XET protocol. ~52 lines added to core Polars files. Memory stays at O(row_group_size), not O(dataset_size).

```python
import polars as pl

# The "Hub is your disk" pattern
(
    pl.scan_parquet("hf://datasets/wikimedia/wikipedia/20231101.en/*.parquet")
    .filter(pl.col("text").str.len_chars() > 5000)
    .sink_parquet("hf://buckets/my-org/my-bucket/wikipedia-long-articles.parquet")
)
```

Scan from the Hub. Transform. Sink to the Hub. Streaming end-to-end.

---

## Why Buckets?

HF Buckets are a new storage primitive — no git semantics, no LFS protocol, no SHA256 pre-hashing. The upload path is:

1. `XetWriter::write(bytes)` — stream parquet bytes as they're encoded
2. `XetWriter::close()` — get back a content hash
3. `bucket_batch()` — register the file via a single NDJSON API call

Compare this to the LFS-based approach which requires computing SHA256 upfront (buffering the entire dataset), multipart uploads, git commit creation, and LFS pointer management. Buckets cut all of that.

Buckets are in beta and will have a path to becoming full dataset repos on HF.

---

## Architecture

The sink hooks into `polars-stream`'s physical plan at three points:

```
lower_ir.rs:  detects hf://buckets/ URL → routes to HfBucketSink (not FileSink)
to_graph.rs:  creates HfBucketSinkNode (implements ComputeNode)
mod.rs:       HfBucketSink variant in PhysNodeKind enum
```

The node itself follows the exact same `ComputeNode` state-machine pattern as `IOSinkNode`:

```
HfBucketSinkNode state machine:
  Uninitialized → Initialized { phase_channel_tx, task_handle } → Finished

Background pipeline:
  Morsels → BatchedWriter<ChannelWriter> (parquet encode)
          → bounded sync channel (backpressure, 16 chunks)
          → async bridge → XetWriter (streaming upload to XET)
          → register_file() via bucket batch API
```

Memory stays constant because parquet row groups are encoded and streamed out immediately — nothing accumulates. The bounded channel provides backpressure so encoding doesn't outrun the upload.

---

## The Diff

### Core Polars changes (~52 lines, all `#[cfg(feature = "hf_bucket_sink")]` gated)

| File | Lines | What |
|------|-------|------|
| `polars-stream/src/physical_plan/lower_ir.rs` | ~20 | URL intercept: route `hf://buckets/` to HfBucketSink |
| `polars-stream/src/physical_plan/mod.rs` | ~12 | `HfBucketSink` variant + visit arm |
| `polars-stream/src/physical_plan/to_graph.rs` | ~8 | Graph wiring → `HfBucketSinkNode` |
| `polars-stream/src/physical_plan/fmt.rs` | ~2 | Display name |
| `polars-stream/src/nodes/io_sinks/mod.rs` | ~2 | Module declaration |
| `polars-io/src/cloud/mod.rs` | ~2 | Module declaration |
| `polars-io/src/path_utils/hugging_face.rs` | ~2 | `"buckets"` in known HF path prefixes |

Plus 6 `Cargo.toml` files threading the `hf_bucket_sink` feature flag:
`polars-io` → `polars-stream` → `polars-lazy` → `polars` → `polars-python` → `polars-runtime-32`

### New files (self-contained, no impact on existing code)

| File | Lines | What |
|------|-------|------|
| `polars-stream/.../hf_bucket_sink.rs` | ~250 | `HfBucketSinkNode` implementing `ComputeNode` |
| `polars-io/.../hf_bucket/mod.rs` | ~170 | Config, URL parsing, token extraction |
| `polars-io/.../hf_bucket/xet_upload.rs` | ~100 | XET token fetch, client, `BucketWriter` |
| `polars-io/.../hf_bucket/batch.rs` | ~70 | Bucket batch API (NDJSON operations) |
| `polars-io/.../hf_bucket/streaming_upload.rs` | ~160 | `StreamingBucketUploader`, `ChannelWriter`, sync/async bridge |

**Total new code**: ~750 lines across 5 files, all behind `#[cfg(feature = "hf_bucket_sink")]`.

---

## Validation Results

Tested on Google Colab (x86_64 + ARM64) across multiple scales:

| Test | Source | Rows | Output | Time | Memory |
|------|--------|------|--------|------|--------|
| Synthetic | In-memory | 1K | ~8 KB | 2.0s | Constant |
| Synthetic | In-memory | 10K | ~80 KB | 2.1s | Constant |
| Synthetic | In-memory | 100K | ~800 KB | 2.6s | Constant |
| Synthetic | In-memory | 1M | ~8 MB | 4.5s | Constant |
| Scan→filter→sink | `wikimedia/wikipedia` | 1K filtered | ~5 MB | 8.6s | Constant |
| Scan→filter→sink | `nvidia/OpenMathReasoning` | 50K filtered | 434 MB | ~30s | Constant |
| Full pipeline | `OpenMed/Medical-Reasoning-SFT-Mega` | Full | 2.7 GB | 167s | Constant |

RSS stays at ~156 MB from 1K to 100K rows — confirms the streaming memory model works.

---

## Current Limitations

This is a proof-of-concept:

- **Single-file output**: No sharding/partitioning yet (would be ~50 lines to add)
- **No token refresh**: XET tokens expire after ~1 hour; long uploads may fail (~30 lines to add)
- **Parquet only**: The sink writes parquet; other formats would need additional `BatchedWriter` implementations
- **Custom wheels required**: Needs `hf_bucket_sink` feature enabled at compile time — not in upstream Polars releases
- **No read support for buckets**: `pl.read_parquet("hf://buckets/...")` doesn't work yet (separate concern, tracked in [huggingface_hub#3807](https://github.com/huggingface/huggingface_hub/pull/3807))
- **Install friction**: Must use `pip install --no-deps --force-reinstall` to prevent pip from replacing the custom wheel with the upstream PyPI version (same version number)

---

## What Upstreaming Could Look Like

The design is deliberately minimal and non-invasive:

1. **Feature-gated**: Everything behind `#[cfg(feature = "hf_bucket_sink")]`. Zero impact on users who don't enable it.
2. **No new public API**: Uses the existing `sink_parquet()` API — just a new URL scheme (`hf://buckets/`).
3. **Same patterns**: `HfBucketSinkNode` implements `ComputeNode` with the same state machine as `IOSinkNode`.
4. **Single new dependency**: `subxet` (tree-shaken XET client, ~1 crate vs the 15+ transitive deps of the full `xet-core`).
5. **Clean separation**: All HF/XET logic lives in `polars-io/src/cloud/hf_bucket/` — the sink node in `polars-stream` is thin glue.

The remaining work for production-readiness (token refresh, sharding, tests) is incremental and self-contained.

---

## Try It

See the [demo notebook](./demo_hf_bucket_sink.ipynb) — runs in Colab, takes ~5 minutes to try.

**Branch**: [`feature/hf-bucket-sink`](https://github.com/davanstrien/polars/tree/feature/hf-bucket-sink)
