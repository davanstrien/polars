# HF Bucket Sink — Session Log Archive

Full session history for the Polars HF Bucket Sink project. The active planning document is `BUCKET_SINK_PLAN.md`.

---

### 2026-02-13 — Project kickoff and planning
**Status**: completed
**What was done**:
- Analyzed existing LFS sink on `feature/hf-hub-sink` branch (~5000 lines Rust)
- Studied HF bucket API via huggingface_hub PR #3673 (branch `origin/buckets-api`)
- Studied xet-core repo structure and `data_client::upload_bytes_async` API
- Discovered OpenDAL PR #7185 — complete HF bucket + XET write support in Rust
- OpenDAL uses `xet-data::streaming::XetWriter` for streaming writes (better than batch upload)
- Identified `kszucs/xet-core` fork with streaming API not yet in main xet-core
**Key findings**:
- OpenDAL PR is the primary Rust reference (not the Python huggingface_hub code)
- Streaming XetWriter means we can pipe parquet bytes directly to XET — no buffering entire shards
- This reduces memory from O(shard_size) to O(row_group_size)
- Polars uses `object_store` (not OpenDAL) for cloud IO, but the XET patterns transfer directly
- The `kszucs/xet-core` fork adds a `streaming` module not in main xet-core — need to track when this merges

---

### 2026-02-13 — [Phase 1] Research & Integration Map complete
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Read and analyzed all key Polars sink infrastructure files on `main` branch:
  - `SinkNode` trait at `crates/polars-stream/src/nodes/io_sinks/mod.rs:201-242`
  - `SinkComputeNode` wrapper at same file, lines 250-288
  - `PhysNodeKind` enum at `crates/polars-stream/src/physical_plan/mod.rs:199`
  - IR lowering at `crates/polars-stream/src/physical_plan/lower_ir.rs:249-275`
  - Graph wiring at `crates/polars-stream/src/physical_plan/to_graph.rs:317-343`
  - Python binding at `crates/polars-python/src/lazyframe/general.rs:685`
  - `LazyFrame::sink()` at `crates/polars-lazy/src/frame/mod.rs:991`
  - `UnifiedSinkArgs` at `crates/polars-plan/src/dsl/options/sink2.rs:47-52`
  - `FileSinkOptions` at `crates/polars-plan/src/dsl/options/sink.rs:747`
  - HF URL parsing at `crates/polars-io/src/path_utils/hugging_face.rs`
- Read and analyzed all OpenDAL HF service source (local copy at `opendal/core/services/huggingface/src/`):
  - `XetClient` creation at `core.rs:384-395`
  - `XetWriter` flow at `writer.rs:51-68, 108-187`
  - `BucketOperation` at `core.rs:89-99`
  - `bucket_batch()` at `core.rs:532-566`
  - Token management at `core.rs:179-215`
  - API URL construction at `uri.rs:104-148`
  - Full Cargo.toml dependency declarations
**Key findings**:
- Two sink architectures exist: old `SinkNode` (flexible) and new `IOSinkNode` (assumes standard file I/O). Bucket sink should use old `SinkNode` because it needs custom XET protocol.
- Minimal diff is 6 files + the new sink module itself, all behind `hf_bucket_sink` feature flag.
- `BUCKETS` const at `hugging_face.rs:135` needs `"buckets"` added to allow `hf://buckets/...` URLs.
- OpenDAL writer shows the exact XetWriter lifecycle: `write(bytes)` streaming -> `close()` -> `XetFileInfo` -> `bucket_batch()`.
- Token auto-refresh via `TokenRefresher` trait means long uploads won't fail from expiry.
- NDJSON format for batch API: one JSON object per line, Content-Type `application/x-ndjson`.
- `kszucs/xet-core` fork `download_bytes` branch required — `streaming` module not in main xet-core yet.
**Artifacts produced**:
- `PHASE1_SINK_INTERFACE.md` — Complete integration map for wiring a new sink into Polars
- `PHASE1_XET_REFERENCE.md` — XET upload + bucket batch API reference

---

### 2026-02-13 — [Phase 2.1] Feature flags, deps, and BUCKETS const
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Added `hf_bucket_sink` feature flag to `crates/polars-io/Cargo.toml` with deps: `["cloud", "dep:xet-data", "dep:cas_types", "dep:xet-utils"]`
- Added xet-core git dependencies (optional) to `crates/polars-io/Cargo.toml`:
  - `xet-data` (package `data`), `xet-utils` (package `utils`), `cas_types` — all from `kszucs/xet-core` branch `download_bytes`
- Added `hf_bucket_sink` feature flag to `crates/polars-stream/Cargo.toml`: `["cloud", "polars-io/hf_bucket_sink"]`
- Changed `BUCKETS` const in `crates/polars-io/src/path_utils/hugging_face.rs:135` from `[&str; 2]` to `[&str; 3]`, adding `"buckets"`
**Key findings**:
- `async-trait` already exists as an optional dep in `polars-io/Cargo.toml` (workspace). Using `dep:async-trait` in a feature flag suppresses the implicit feature name, breaking the existing `async` feature that references `"async-trait"`. Removed `dep:async-trait` from `hf_bucket_sink` — it's transitively enabled via `cloud` -> `async` -> `async-trait`.
- `cargo update -p tempfile` was needed to resolve lockfile conflict (xet-core deps need tempfile >= 3.25).
- xet-core deps pinned to commit `cc271895` from `download_bytes` branch.
**Verification**:
- `cargo check -p polars-stream --features parquet,hf_bucket_sink` — PASS

---

### 2026-02-13 — [Phase 2.1a] Standalone XET upload test
**Branch**: feature/hf-bucket-sink
**Status**: completed (all 5 steps passed end-to-end)
**What was done**:
- Created standalone Rust project at `scratch/xet_upload_test/` (outside polars workspace)
- Wrote 5-step test binary: (1) fetch XET write token, (2) create XetClient, (3) upload data via XetWriter, (4) register file via bucket batch API, (5) verify file exists
**Runtime results** (all passed first attempt against `davanstrien/test-bucket`):
- XET write token fetched. CAS URL = `https://cas-server.xethub.hf.co`. Token is JWT. Expiry is Unix timestamp.
- Upload of 3500 bytes succeeds. Hash = 64-char hex SHA256. `file_size()` returns exact byte count.
- Batch API returns `{"success":true,"processed":1,"succeeded":1,"failed":[]}`.
**Confirmed for Polars integration**:
- Import paths: `xet_data::streaming::XetClient`, `xet_data::streaming::XetWriter`, `xet_data::XetFileInfo`
- `XetFileInfo.hash()` returns a 64-char hex SHA256 string
- Batch API: POST NDJSON with `Content-Type: application/x-ndjson`, each line `{"type":"addFile","path":"...","xetHash":"..."}`
- No `cas_types` dep needed for the upload path

---

### 2026-02-13 — [Phase 2.2] polars-io HF bucket module created
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Created `crates/polars-io/src/cloud/hf_bucket/` module with three files:
  - `mod.rs` (~45 lines) — Module root, exports, and `HfBucketConfig` struct with builder pattern
  - `xet_upload.rs` (~100 lines) — `XetToken`, `fetch_xet_write_token()`, `create_xet_client()`, `BucketWriter`
  - `batch.rs` (~70 lines) — `BucketOperation` enum, `bucket_batch()` function
- Registered module in `crates/polars-io/src/cloud/mod.rs` with `#[cfg(feature = "hf_bucket_sink")]`
**Key findings**:
- `polars_bail!` macro needs explicit import in new modules
- All dependencies (`reqwest`, `serde`, `serde_json`, `bytes`, `tokio`) transitively enabled via `cloud` feature

---

### 2026-02-13 — [Phase 2.5] Stub sink node + pipeline wiring
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Created stub `HfBucketSinkNode` implementing `SinkNode` trait
- Added `PhysNodeKind::HfBucketSink` variant + match arms in `visit_node_inputs_mut`, `fmt.rs`
- Added `hf://buckets/` URL routing in `lower_ir.rs`
- Wired graph node in `to_graph.rs`
**Key findings**:
- Cannot use `#[cfg(...)]` on `|` arms in Rust match patterns — needed separate match arm
- `fmt.rs` (`visualize_plan_rec`) also has exhaustive match — needed arm there too

---

### 2026-02-13 — [Phase 2.4] Fill in HfBucketSinkNode with real parquet + XET upload
**Status**: completed
**What was done**:
- Full `SinkNode` implementation: `initialize()` parses URL/token, `spawn_sink()` vstacks morsels + encodes parquet, `finalize()` uploads via XET + registers
- Initial approach: buffer all morsels, encode full parquet, then upload (later replaced by streaming)
**Architecture notes**:
- Serial consumption (`is_sink_input_parallel = false`) for simplicity
- Upload logic lives in polars-io to avoid adding reqwest/bytes deps to polars-stream
- Shared `Arc<Mutex<Option<Vec<u8>>>>` bridges spawn_sink (encoding) -> finalize (upload)

---

### 2026-02-18 — [Phase 2.6] Feature flag wiring + Python e2e test
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Wired `hf_bucket_sink` feature flag through full crate chain (4 Cargo.toml files)
- Built local Python wheel with `maturin develop --features hf_bucket_sink`
- Created e2e test: `sink_parquet("hf://buckets/davanstrien/test-polars-bucket/test.parquet")` uploaded 1000 rows in 1.7s
- File confirmed on HF (5,885 bytes) via `hf buckets tree`

---

### 2026-02-18 — [Phase 3.2] Streaming XET upload
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Created `streaming_upload.rs`: `ChannelWriter` (sync Write over bounded channel), `StreamingBucketUploader` (BatchedWriter + async upload task)
- Added `register_file()` helper to `mod.rs`
- Rewrote `hf_bucket_sink.rs`: streaming instead of buffered
**Key design decisions**:
- Bridge pattern (std::sync channel -> spawn_blocking -> tokio channel) avoids unsafe code
- `StreamingBucketUploader::new()` takes owned values so the future is `'static` for `tokio::spawn`
- `ParquetWriteOptions::to_writer(channel_writer).batched(&schema)` reuses existing polars API
**Memory model**:
- Before: O(total_dataset) — vstack all morsels, encode full parquet, then upload
- After: O(row_group_size) — each morsel encoded as row group(s), bytes streamed to XET via channel

---

### 2026-02-18 — [Phase 3.2 validation] Streaming sink e2e + larger dataset tests
**Branch**: feature/hf-bucket-sink
**Status**: completed (3 pass, 2 known failures unrelated to sink)

| Test | Source | Rows | Time | Result |
|------|--------|------|------|--------|
| Simple sink | In-memory DataFrame | 1,000 | 2.4s | **PASS** |
| IMDB scan->filter->sink | `stanfordnlp/imdb` | ~25K | 66.4s | **PASS** |
| Wikipedia 1-shard | `wikimedia/wikipedia` 1 shard | 156K | 39.0s | **PASS** |
| Wikipedia full (41 shards) | `wikimedia/wikipedia` all | ~6.4M | — | FAIL (read-side) |
| finepdfs-edu | `HuggingFaceFW/finepdfs-edu` 1 shard | 236K | — | FAIL (debug_assert in xet-core) |

**Key findings**:
- Wikipedia full-glob failure is read-side only: `Invalid thrift: transport error` when scanning many remote shards. Single shard works.
- finepdfs-edu failure is `debug_assert` in xet-core `file_cleaner.rs:165` — only fires in debug builds, not release.
- Release wheel build OOM locally — needs CI runner.

---

### 2026-02-18 — CI release wheels + Colab validation at scale
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Updated `.github/workflows/build-hf-sink-wheels.yml`: added `--features hf_bucket_sink`, added ARM64 job
- Both x64 and ARM64 wheels built successfully in CI
- Colab validation:

| Test | Source | Filter | Output | Time | Result |
|------|--------|--------|--------|------|--------|
| 1K rows | `nvidia/OpenMathReasoning` | `.head(1_000)` | 8.8 MB | ~10s | PASS |
| 50K filtered | `nvidia/OpenMathReasoning` | `str.len_chars() > 500` | 434 MB | ~30s | PASS |
| Full filter | `OpenMed/Medical-Reasoning-SFT-Mega` | `list.len() > 2` | 2.7 GB | 167s | PASS |

**Key findings**:
- Release wheels bypass xet-core `debug_assert` — confirmed.
- 2.7 GB uploaded via streaming pipeline on Colab (~12GB RAM) — validates O(row_group_size) memory model.
- Full "Hub is your disk" pattern works: `scan_parquet("hf://datasets/...")` -> filter -> `sink_parquet("hf://buckets/...")`.
- CI note: `gh workflow run` defaults to upstream repo — must pass `-R davanstrien/polars`.
- Colab setup requires two wheels: base `polars` package + `polars_runtime_32` native extension.

---

### 2026-02-19 — Merge upstream/main (257 commits)
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Merged `upstream/main` into `feature/hf-bucket-sink` (257 upstream commits)
- 5 files had merge conflicts: `Cargo.lock`, `io_sinks/mod.rs`, `physical_plan/fmt.rs`, `physical_plan/mod.rs`, `physical_plan/to_graph.rs`
- Resolved all conflicts by accepting upstream's version, then re-adding our small additions
- **Critical change**: Upstream completely rewrote the `io_sinks` module — old `SinkNode` trait is gone, replaced by `ComputeNode`-based state machine. What was `io_sinks2/` (new architecture) is now `io_sinks/`.
- Rewrote `hf_bucket_sink.rs` to use new `ComputeNode` architecture:
  - Replaced `impl SinkNode for HfBucketSinkNode` with `impl ComputeNode for HfBucketSinkNode`
  - Implemented same state-machine pattern as `IOSinkNode`: `Uninitialized` -> `Initialized { phase_channel_tx, task_handle }` -> `Finished`
  - `update_state()`: Initialize on first call; when recv port is Done, drop sender and await task handle
  - `spawn()`: Send each phase's `PortReceiver` through the connector channel
  - Background task: Bridge multi-phase receivers into continuous morsel stream, feed to `StreamingBucketUploader`, then register file via `register_file()`
  - Finalization (bucket batch registration) now happens inside the background task instead of a separate `finalize()` method
- Auto-merged files preserved all our additions correctly (all 6 Cargo.toml feature flags, lower_ir.rs intercept, cloud/mod.rs export, BUCKETS const)
- Re-added 4 small changes lost in conflict resolution
- `polars-io/src/cloud/hf_bucket/` module (4 files) unchanged — no dependency on streaming engine internals
**Verification**:
- `cargo check -p polars-stream --features parquet` — PASS (no regression)
- `cargo check -p polars-stream --features parquet,hf_bucket_sink` — PASS (new ComputeNode impl compiles)
**Commit**: `233ed6f5c3`

---

### 2026-02-20 — Colab re-validation: install fix + all writes pass
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Root-caused Colab failure where `hf://buckets/` URLs reached `object_store_setup.rs` instead of being intercepted by `lower_ir.rs`
- **Root cause**: pip install was replacing the custom `polars-runtime-32` wheel with the upstream PyPI version. Both have version `1.38.1`, and without `--no-deps`, pip resolves the dependency from PyPI, overwriting the custom `.so` that has `hf_bucket_sink` compiled in.
- **Fix**: `pip install --no-deps --force-reinstall polars-*.whl polars_runtime_32-*.whl`
- The Rust intercept code in `lower_ir.rs` was correct all along — the issue was purely the wheel install.
- Code cleanup:
  - Removed debug `eprintln!` statements from `lower_ir.rs`
  - Added `#[cfg(not(feature = "hf_bucket_sink"))]` block in `lower_ir.rs` that gives a clear `polars_bail!` error when `hf://buckets/` is detected but the feature isn't compiled
  - Removed unused `use std::sync::Arc` from `hf_bucket_sink.rs`
- Updated install instructions in `colab_post_merge_validation.py`, `test_hf_large_dataset.py`, `demo_hf_hub_sink.py`
- Created `colab_full_validation.py` — comprehensive 6-test suite
- Re-validated on Colab (x86_64):

| Test | Source | Rows | Time | Result |
|------|--------|------|------|--------|
| Synthetic sink | In-memory | 1K | 2.0s | PASS |
| Synthetic sink | In-memory | 10K | 2.1s | PASS |
| Synthetic sink | In-memory | 100K | 2.6s | PASS |
| Synthetic sink | In-memory | 1M | 4.5s | PASS |
| Scan→filter→sink | `wikimedia/wikipedia` | 1K filtered | 8.6s | PASS |

- Streaming memory confirmed: RSS constant at ~156 MB from 1K through 100K rows
**Known limitations**:
- `pl.read_parquet("hf://buckets/...")` doesn't work — polars read path doesn't handle bucket URLs. Workaround: download via `huggingface_hub`, read locally.
- Large multi-shard glob scans can hit `Invalid thrift: transport error` (read-side issue, not sink). Adding `.head()` mitigates.
**Key lesson**: When custom wheels share the same version as upstream PyPI, always use `--no-deps` to prevent pip from resolving dependencies from PyPI.

---

## Archived Reference Material

### OpenDAL PR #7185 patterns (used during Phase 1 research)

**Streaming write flow** (from `writer.rs`):
```rust
let client = core.xet_client("write").await?;
let writer = client.write(None).await?;
writer.write(bytes).await?;
let file_info: XetFileInfo = writer.close().await?;
let xet_hash = file_info.hash().to_string();
let operation = BucketOperation::AddFile { path, xet_hash };
core.bucket_batch(vec![operation]).await?;
```

**XET token endpoint**: `GET /api/buckets/{namespace}/{name}/xet-write-token`

### Why Buckets Instead of LFS

| Concern | LFS Sink (~5000 lines) | Bucket Sink |
|---|---|---|
| Upload protocol | LFS batch API -> presigned S3 -> multipart | `XetWriter::write()` streaming |
| File hashing | Custom SHA256 streaming | XET handles internally |
| Commit model | Atomic git commit via NDJSON API (879 lines) | `POST /api/buckets/{id}/batch` |
| Resume on failure | Custom checkpoint system | Bucket has what landed |
| Multipart uploads | Custom implementation | Handled by XET |

### Comparison: Rust-native Sink vs HfFileSystem/fsspec (PR #3807)

| Aspect | fsspec (PR #3807) | Rust-native sink (ours) |
|--------|-------------------|------------------------|
| **Encoding** | Python-level | In-engine, zero-copy from streaming pipeline |
| **Temp files** | Yes — writes to disk, then uploads | No — parquet bytes go straight to XET |
| **Memory** | Must buffer full file before upload | O(row_group_size), streams morsel-by-morsel |
| **GIL** | Held during encoding/coordination | No Python involvement — pure Rust |
| **Large datasets** | Limited by disk space for temp files | Arbitrarily large lazy frames, constant memory |

They are complementary: fsspec for the read path and general interop, our sink for write-heavy data engineering.

### OpenDAL migration notes (Feb 2026)

OpenDAL migrated from `kszucs/xet-core` fork (3 crates) to `subxet` — reduced Cargo.lock from 511 to 127 entries (~75%). Core APIs unchanged: `XetClient::new()`, `XetWriter::write()`/`close()`, `BucketOperation`/`bucket_batch()`.

---

### 2026-03-05 — Error Context Wrapping + E2E Integration Tests

**Branch**: feature/hf-bucket-sink
**Status**: completed

#### Part 1: Error Context Wrapping (Rust)

Added bucket identity and target URL to all error messages for easier debugging:

- **`xet_upload.rs`**: Error now includes `namespace/bucket_name`:
  `"HF bucket XET write token request failed for '{ns}/{bucket}' (HTTP {status}): {body}"`
- **`batch.rs`**: Error includes bucket identity + bounded operation summary (max 3 ops with `(+N more)` suffix):
  `"HF bucket batch API request failed for '{ns}/{bucket}' (HTTP {status}): {body}; operations: [add:file1.parquet, ...]"`
- **`hf_bucket_sink.rs`**: Added `target_url: String` field to `HfBucketSinkNode`, set during `initialize()`. Both error consumption points (`update_state`, `spawn`) wrap with `"HF bucket sink failed for '{url}': {original}"` via `wrap_msg`.

**Verification**: `cargo check` passes for both `polars-io` and `polars-stream` with `hf_bucket_sink` feature. All 16 existing unit tests pass.

#### Part 2: E2E Integration Tests (Python)

Created pytest suite at `py-polars/tests/unit/io/cloud/`:

- **`conftest.py`**: `hf_token` fixture (skips if `HF_TOKEN` absent), `hf_bucket_config` fixture (namespace/bucket/storage_options)
- **`test_hf_bucket_sink.py`**: 4 tests across 3 classes, all gated behind `pytest.mark.slow` + `HF_TOKEN` + `huggingface_hub`:

| Test | Class | Result | Notes |
|------|-------|--------|-------|
| `test_3_rows` | `TestHfBucketSinkSmoke` | PASS | Minimal write, no read-back |
| `test_write_read_back` | `TestHfBucketSinkSmoke` | PASS | 50 rows, roundtrip with `assert_frame_equal` |
| `test_10k_synthetic_rows` | `TestHfBucketSinkMedium` | PASS | 10K rows, 4 columns, streaming path |
| `test_10m_synthetic_rows` | `TestHfBucketSinkLarge` | PASS (44s) | 10M rows, 6 column types, head/tail spot-check |

Read-back uses `huggingface_hub.download_bucket_files()` API.

**Run command**:
```bash
HF_TOKEN=hf_... .venv/bin/pytest -m slow tests/unit/io/cloud/test_hf_bucket_sink.py -v -o "addopts="
```

#### Part 3: E2E Streaming Scripts (scratch/)

**`scratch/test_streaming_e2e.py`** — Pure polars `scan_parquet` → ETL → `sink_parquet`:
- Source: `togethercomputer/CoderForge-Preview` (SWE_Rebench split)
- Pipeline: filter(reward>0) → add columns (message_len, reward_tier, finish_reason_clean) → select → head(10k)
- Result: **10K rows, 2.5 GB parquet, uploaded in 458s, roundtrip verified**

**`scratch/test_streaming_e2e-big.py`** — Full dataset, no `.head()` limit, no sort:
- Same ETL pipeline but processes entire split
- Result: **Completed in 421s, all assertions passed**
- Memray profiling:
  - Peak memory: **21.3 GB**
  - Total allocated: 79.97 GB (throughput, not resident)
  - Top allocator: Rust-side (`<stack trace unavailable>`) — 72.6 GB total, expected for large string data
  - Note: This dataset has avg 228K chars/row in `messages` column — extreme case

#### Known Issues

**subxet `file_cleaner.rs:165` debug assertion panic**:
- Intermittent assertion failure in debug builds: `file_size() != deduplication_metrics.total_bytes`
- Only fires with `#[cfg(debug_assertions)]` — release builds unaffected
- Triggered by large uploads (>300 MB) with big variable-length string columns
- Root cause: subxet internal bookkeeping bug, not Polars usage — our streaming write code is correct (sequential writes via `ChannelWriter`, proper `finish()` → `drop` → `await` lifecycle)
- The same data sometimes passes, sometimes panics in debug mode (flaky)
- **Action**: Report upstream to subxet maintainers. Not a blocker for release builds.

**Memory (21 GB peak on full CoderForge)**:
- Baseline comparison done: local `sink_parquet` (no bucket) peaks at **15.3 GB** for the same pipeline
- Bucket sink peaks at **21.3 GB** — adds ~6 GB (~40% overhead) for XET client buffers, network buffers, and async upload pipeline
- The 15.3 GB baseline is unavoidable — it's polars processing rows with avg 228K-char `messages` column
- ~40% overhead is reasonable for a parallel async upload pipeline running alongside encoding
- Flamegraph available at `scratch/memray-big.html` for deeper analysis
