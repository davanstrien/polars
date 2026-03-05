# Polars HF Bucket Sink — Project Plan

## TL;DR

A streaming Polars sink that writes parquet directly to HuggingFace Buckets via the XET protocol. Replaces the ~5000-line LFS-based approach with a dramatically simpler bucket-based sink. Memory stays at O(row_group_size) — parquet bytes stream directly to XET storage with no temp files or full-dataset buffering.

**Status**: PoC works end-to-end. Validated up to 2.7 GB on Colab. Re-validated 2026-02-20 after upstream merge + subxet migration.

**Branch**: `feature/hf-bucket-sink` (all work here)

---

## Current Architecture

```
Python: df.sink_parquet("hf://buckets/namespace/bucket/file.parquet")
  → lower_ir.rs: detects hf://buckets/ URL → routes to HfBucketSink (not FileSink)
  → to_graph.rs: creates HfBucketSinkNode (implements ComputeNode)
  → HfBucketSinkNode state machine:
      Uninitialized → Initialized { phase_channel_tx, task_handle } → Finished
  → Background task:
      StreamingBucketUploader (polars-io)
        → BatchedWriter<ChannelWriter> encodes parquet row groups
        → ChannelWriter sends bytes over bounded sync channel
        → Async bridge task forwards to XetWriter (streaming upload)
        → On finish: register_file() calls bucket batch API
```

### Key properties
- Memory: O(row_group_size), not O(dataset_size)
- All HF/XET logic in `polars-io/src/cloud/hf_bucket/` (4 files, ~500 lines)
- Sink node in `polars-stream` is thin glue (~220 lines)
- Core polars touches: 7 files, ~52 lines, all `#[cfg(feature = "hf_bucket_sink")]` gated
- Implements `ComputeNode` directly (same pattern as upstream `IOSinkNode`)

---

## Files We Own

### HF/XET logic (polars-io) — unchanged by upstream
| File | Lines | What |
|------|-------|------|
| `crates/polars-io/src/cloud/hf_bucket/mod.rs` | ~170 | Config, URL parsing, token extraction, `register_file()` |
| `crates/polars-io/src/cloud/hf_bucket/xet_upload.rs` | ~100 | XET token fetch, client creation, `BucketWriter` |
| `crates/polars-io/src/cloud/hf_bucket/batch.rs` | ~70 | Bucket batch API (NDJSON `AddFile`/`DeleteFile`) |
| `crates/polars-io/src/cloud/hf_bucket/streaming_upload.rs` | ~160 | `StreamingBucketUploader`, `ChannelWriter`, sync/async bridge |

### Streaming engine integration (polars-stream)
| File | What |
|------|------|
| `nodes/io_sinks/hf_bucket_sink.rs` | `HfBucketSinkNode` implementing `ComputeNode` |
| `nodes/io_sinks/mod.rs` | `pub mod hf_bucket_sink;` declaration |
| `physical_plan/mod.rs` | `HfBucketSink` variant in `PhysNodeKind` + visit arm |
| `physical_plan/lower_ir.rs` | `hf://buckets/` URL intercept before `FileSink` |
| `physical_plan/to_graph.rs` | Graph wiring for `HfBucketSinkNode` |
| `physical_plan/fmt.rs` | Display arm for plan visualization |

### Feature flag chain (8 Cargo.toml files)
`polars-io` → `polars-stream` → `polars-lazy` → `polars` → `polars-python` → `polars-runtime-{32,64,compat}`

### Other
| File | What |
|------|------|
| `crates/polars-io/src/cloud/mod.rs` | `pub mod hf_bucket;` |
| `crates/polars-io/src/path_utils/hugging_face.rs` | `"buckets"` in BUCKETS const |
| `.github/workflows/build-hf-sink-wheels.yml` | CI wheel builds (x64 + ARM64) |

---

## Next Steps (priority order)

### 1. ~~Rebuild CI wheels + re-validate on Colab~~ — DONE
Re-validated on Colab (2026-02-20) after upstream merge + subxet migration. All writes pass. Streaming memory model confirmed (constant RSS up to 100K rows, 1M rows in 4.5s). Scan→filter→sink from HF dataset works.

**Key finding**: Install order matters — must use `pip install --no-deps --force-reinstall` to prevent pip from replacing the custom `polars-runtime-32` wheel with the upstream PyPI version (which lacks `hf_bucket_sink`). Without `--no-deps`, pip resolves the `polars-runtime-32 == 1.38.1` dependency from PyPI.

### 2. Share PoC publicly
Write a short demo notebook/blog snippet. Disclaimers: "PoC, single-file output, no token refresh, requires HF Buckets (beta API), install from CI wheel artifacts."

### 3. Token refresh (MEDIUM PRIORITY)
XET tokens expire ~1hr. Long uploads will fail. Implement `TokenRefresher` trait (re-fetch from `/api/buckets/{id}/xet-write-token`). Follow OpenDAL's pattern.
**Scope**: ~30 lines in `xet_upload.rs`

### 4. Multi-file / sharded output (MEDIUM PRIORITY)
Currently writes a single parquet file. Large datasets should shard (e.g. `part-00000.parquet`). Close current XetWriter at threshold, start new one, register all in one `bucket_batch()` call.
**Scope**: ~50 lines in `hf_bucket_sink.rs`

### 5. ~~Unit tests~~ — DONE (partial)
Added `#[cfg(test)] mod tests` to `hf_bucket/mod.rs` covering `parse_hf_bucket_url` (8 tests) and `extract_hf_token` (3 tests: env var, cached file, missing-token error). Mock HTTP tests for batch API deferred.

### 6. Error handling (LOW PRIORITY)
Wrap raw errors with context (bucket name, file path). Handle common failures: 404 (bucket missing), 401 (bad token), 429 (rate limit).
**Scope**: ~50 lines

### Backlog
- Investigate read-side `Invalid thrift: transport error` on large multi-shard HF dataset globs (not a sink issue but affects scan→sink pipeline)
- Read support for `hf://buckets/` — `pl.read_parquet("hf://buckets/...")` doesn't work (read path doesn't handle bucket URLs). Separate concern, see [huggingface_hub#3807](https://github.com/huggingface/huggingface_hub/pull/3807). Workaround: use `huggingface_hub` to download, then read locally.
- Publish wheels to HF repo or GitHub Release for easier install
- Consider bumping version to avoid `polars-runtime-32 == 1.38.1` collision with PyPI upstream (root cause of the Colab install issue)

---

## Validation Results

### 2026-02-20 — Post-merge + subxet migration (x86_64)

| Test | Source | Rows | Time | Result |
|------|--------|------|------|--------|
| Synthetic sink | In-memory | 1K | 2.0s | PASS |
| Synthetic sink | In-memory | 10K | 2.1s | PASS |
| Synthetic sink | In-memory | 100K | 2.6s | PASS |
| Synthetic sink | In-memory | 1M | 4.5s | PASS |
| Scan→filter→sink | `wikimedia/wikipedia` | 1K filtered | 8.6s | PASS |

Streaming memory model confirmed: RSS stays constant (~156 MB) from 1K to 100K rows.

### 2026-02-18 — Initial validation (ARM64)

| Test | Source | Output | Time | Result |
|------|--------|--------|------|--------|
| 1K rows | `nvidia/OpenMathReasoning` | 8.8 MB | ~10s | PASS |
| 50K rows filtered | `nvidia/OpenMathReasoning` | 434 MB | ~30s | PASS |
| Full dataset filter | `OpenMed/Medical-Reasoning-SFT-Mega` | 2.7 GB | 167s | PASS |

The full "Hub is your disk" pattern works: `scan_parquet("hf://datasets/...")` → filter → `sink_parquet("hf://buckets/...")` with constant memory.

### Install instructions (Colab)

```bash
# IMPORTANT: --no-deps prevents pip from replacing custom wheel with upstream PyPI version
pip uninstall polars polars-runtime-32 -y
pip install --no-deps --force-reinstall polars-*.whl polars_runtime_32-*.whl
# Then restart runtime
```

---

## Design Decisions

**Why buckets, not LFS?** Buckets have no git semantics, no LFS protocol, no SHA256 pre-hashing, no multipart upload handling. The entire upload is `XetWriter::write(bytes)` → `close()` → `bucket_batch()`. Buckets will have a path to become dataset repos.

**Why not a Polars plugin?** Polars plugins support expressions/functions but NOT custom sinks. Sink nodes are deeply integrated into the streaming engine's physical plan.

**Why a fork of Polars?** The sink node requires changes to the physical plan enum, IR lowering, and graph wiring — all internal to polars-stream. The diff is small (~52 lines in core files, all feature-gated) but can't be done externally.

**Memory model**: True streaming — parquet bytes flow through a bounded channel to XetWriter. Memory is O(row_group_size), not O(dataset_size).

**Error recovery**: If upload fails mid-stream, the bucket has whatever completed. User re-runs and we upload the rest. No checkpoint file needed.

---

## Key References

| What | Where |
|------|-------|
| Upstream Polars | https://github.com/pola-rs/polars |
| OpenDAL HF+XET (Rust reference) | https://github.com/apache/opendal/pull/7185 |
| xet-core fork (streaming API) | https://github.com/kszucs/xet-core/tree/download_bytes |
| subxet (tree-shaken xet-core) | https://github.com/kszucs/subxet |
| Bucket API (Python reference) | https://github.com/huggingface/huggingface_hub/pull/3673 |
| Old LFS sink branch (read-only) | `feature/hf-hub-sink` on this repo |
| Session history | `BUCKET_SINK_SESSION_LOG.md` in this repo |
| Phase 1 research docs | `PHASE1_SINK_INTERFACE.md`, `PHASE1_XET_REFERENCE.md` |

---

## Session Log

**Agents MUST append to this section after each session.** Keep entries concise — full history is in `BUCKET_SINK_SESSION_LOG.md`.

### Format
```
### YYYY-MM-DD — Brief description
**Status**: completed | in-progress | blocked
**What**: 1-3 bullet points
**Next**: What to do next
```

### 2026-02-19 — Merge upstream/main (257 commits)
**Status**: completed
**What**:
- Merged 257 upstream commits, resolved 5 conflicts (accept upstream + re-add our additions)
- Rewrote `hf_bucket_sink.rs` for new `ComputeNode` architecture (upstream removed `SinkNode` trait)
- Both `cargo check` passes: with and without `hf_bucket_sink` feature
**Next**: Rebuild CI wheels, re-validate on Colab, then share PoC publicly

### 2026-02-19 — Rebuild CI wheels post-merge
**Status**: in-progress
**What**:
- Pushed merge commit (`233ed6f`) to `feature/hf-bucket-sink`
- Triggered `build-hf-sink-wheels.yml` CI workflow (both x64 and ARM64)
- Created Colab validation script (`scratch/colab_post_merge_validation.py`)
**Next**: Download wheels once CI completes, run Colab validation (smoke test + 10K scan→filter→sink), update status here

### 2026-02-20 — Colab re-validation + install fix
**Status**: completed
**What**:
- Root-caused Colab failure: pip was replacing custom `polars-runtime-32` wheel with upstream PyPI version (same version `1.38.1`). Fix: `--no-deps` flag.
- Re-validated all writes on Colab (1K–1M synthetic rows + scan→filter→sink from Wikipedia). All pass.
- Streaming memory model confirmed: constant RSS from 1K to 100K rows.
- Cleaned up `lower_ir.rs`: removed debug `eprintln!`, added `#[cfg(not(feature = "hf_bucket_sink"))]` block with clear error for missing feature.
- Removed unused `use std::sync::Arc` from `hf_bucket_sink.rs`.
- Updated install instructions across all scripts to use `--no-deps --force-reinstall`.
- Known limitation: `pl.read_parquet("hf://buckets/...")` doesn't work (read path doesn't handle bucket URLs).
**Next**: Share PoC publicly, consider version bump to avoid PyPI collision

### 2026-02-19 — Migrate xet-core → subxet
**Status**: completed
**What**:
- Replaced 3 xet-core git deps (`xet-data`, `xet-utils`, `cas_types`) with single `subxet` crate in `polars-io/Cargo.toml`
- Updated `hf_bucket_sink` feature flag: `["cloud", "dep:subxet"]`
- Updated 4 type paths in `xet_upload.rs`: `xet_data::` → `subxet::data::`
- Replaced `panic!()` with `polars_bail!()` in `object_store_setup.rs` for unresolved hf:// paths
- Matches OpenDAL's migration pattern; cuts transitive deps from ~15 crates to 1
- Committed as `fcc5692`, pushed, CI wheel build triggered (run `22192141325`)
**Next**: Wait for CI wheels (~30min), download artifacts, run Colab validation (smoke test + scan→filter→sink)
**Cleanup**: Remove debug `eprintln!` statements in `lower_ir.rs` before sharing publicly

### 2026-02-20 — Review fixes (5 findings)
**Status**: partially completed
**What**:
Fresh-eyes review found 5 issues. All fixed, all behind `#[cfg(feature = "hf_bucket_sink")]`:

1. **Finding 1 (HIGH) — Non-parquet sinks silently produce parquet**: Added `FileWriteFormat::Parquet(_)` check in `lower_ir.rs` before routing to `HfBucketSink`. Non-parquet formats now bail with `ComputeError`. Simplified `hf_bucket_sink.rs` to use `unreachable!()` for non-parquet arm.
2. **Finding 2 (HIGH) — Upload task detached, errors lost**: Added `AbortOnDropHandle<T>` wrapper in `streaming_upload.rs`. If `StreamingBucketUploader` is dropped without calling `finish()`, the tokio upload task is aborted instead of orphaned.
3. **Finding 3 (MEDIUM) — Feature chain incomplete**: Added `hf_bucket_sink = ["polars-python/hf_bucket_sink"]` to `polars-runtime-64`, `polars-runtime-compat`, and `template/Cargo.template.toml`. Was already in `polars-runtime-32`.
4. **Finding 4 (MEDIUM) — Feature doesn't declare parquet dependency**: Changed `hf_bucket_sink = ["cloud", "dep:subxet"]` → `["cloud", "parquet", "dep:subxet"]` in `polars-io/Cargo.toml`. `streaming_upload.rs` has unguarded `use crate::parquet::write::*`.
5. **Finding 5 (MEDIUM) — No unit tests**: Added `#[cfg(test)] mod tests` in `hf_bucket/mod.rs` with 11 tests for `parse_hf_bucket_url` and `extract_hf_token`.

**Note**: `cargo check` blocked by nightly ICE (`rustc 1.94.0-nightly 31cd367b9`) in `futures-executor`/`tower` crates. Code verified via `cargo fmt` (syntax-clean) and manual review. Full compilation needs a newer nightly or stable channel.

**Post-commit audit gaps**:
- Finding 4: `polars-io/Cargo.toml` was fixed but `polars-stream/Cargo.toml:130` was missed — `hf_bucket_sink` there still lacked `parquet`, so `polars-plan/parquet` stays off and `FileWriteFormat::Parquet(_)` doesn't exist, causing compile failures in `lower_ir.rs` and `hf_bucket_sink.rs`.
- Finding 5: Tests cover `parse_hf_bucket_url` and `extract_hf_token` but nothing for `AbortOnDropHandle` or `ChannelWriter` in `streaming_upload.rs`.

### 2026-02-20 — Fix remaining review regressions (Finding 4 compile + Finding 5 tests)
**Status**: completed
**What**:
Follow-up to post-commit audit of `2c17c8e969`:

1. **Finding 4 fix**: Added `"parquet"` to `hf_bucket_sink` feature in `polars-stream/Cargo.toml` so the feature chain enables `polars-plan/parquet` and `FileWriteFormat::Parquet(_)` compiles.
2. **Finding 5 fix**: Added 5 unit tests in `streaming_upload.rs` for `AbortOnDropHandle` (abort-on-drop, join-returns-value) and `ChannelWriter` (sends bytes, empty write noop, broken pipe on closed channel).
3. **Import fix**: Added `polars_bail` and `polars_err` imports to `lower_ir.rs:9` — the `polars_bail!` macro at line 304 (added in Finding 1) was used without being imported, causing a compile error.
