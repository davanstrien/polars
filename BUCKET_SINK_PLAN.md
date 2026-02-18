# Polars HF Bucket Sink — Project Plan

## TL;DR

Replace the existing LFS-based HF Hub sink with a dramatically simpler bucket-based sink. The existing sink is ~5000 lines of Rust reimplementing the LFS upload protocol. The bucket approach should be much smaller, with no git semantics, no LFS, no SHA256 pre-hashing, and no multipart upload handling.

Buckets will have a path to become dataset repos, so we get simple uploads + full dataset experience.

**This is a proof of concept** — we accept the bucket API is slightly unstable.

---

## Git Workflow — IMPORTANT

**The existing `feature/hf-hub-sink` branch (225 commits) MUST be preserved.** It represents months of work and is valuable as reference material and as a standalone feature.

### Setup steps (do this FIRST before any implementation):

```bash
cd /Users/davanstrien/Documents/code/polars

# 1. Make sure the existing branch is safe
git stash  # if any uncommitted work
git checkout feature/hf-hub-sink
git log --oneline -3  # verify it's intact

# 2. Sync main with upstream Polars
git checkout main
git fetch origin  # origin = upstream pola-rs/polars
git merge origin/main  # or rebase, as appropriate

# 3. Create new branch FROM synced main
git checkout -b feature/hf-bucket-sink main

# 4. Verify: new branch is up to date with upstream, old branch untouched
git log --oneline -3  # should show latest upstream commits
git branch  # should show both feature/hf-hub-sink and feature/hf-bucket-sink
```

### Branch layout:
```
main (synced with upstream pola-rs/polars)
├── feature/hf-hub-sink       ← PRESERVE, do not modify (LFS-based sink, reference)
└── feature/hf-bucket-sink    ← NEW, all bucket work goes here
```

### Rules:
- **NEVER** delete, rebase, or force-push `feature/hf-hub-sink`
- **ALWAYS** work on `feature/hf-bucket-sink`
- Before starting implementation, verify `main` is synced with upstream
- The old branch is a **read-only reference** — check it out to study patterns, but commit nothing to it

---

## Critical Context: OpenDAL PR #7185

**URL**: https://github.com/apache/opendal/pull/7185

Someone (kszucs) is already adding HF bucket write support + XET protocol to OpenDAL. This is extremely relevant because:

1. **Polars uses `object_store` for cloud IO** (not OpenDAL currently), but OpenDAL is the major alternative
2. **The OpenDAL PR shows exactly how to integrate `xet-core` as a Rust dependency** — including which crates are needed and how the streaming write works
3. **The writer pattern is clean**: `XetWriter` from `xet-data::streaming` provides a streaming write interface (write bytes → close → get hash)
4. **The PR depends on a fork of xet-core** (`kszucs/xet-core`, branch `download_bytes`) which adds `streaming::XetClient` and `streaming::XetWriter` — these are NOT in the main xet-core repo yet

### Key patterns from OpenDAL PR:

**Dependencies** (from `core/services/huggingface/Cargo.toml`):
```toml
[features]
xet = ["dep:xet-data", "dep:cas_types", "dep:xet-utils", "dep:futures", "dep:async-trait"]

[dependencies]
xet-data = { package = "data", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
xet-utils = { package = "utils", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
cas_types = { git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
```

**Streaming write flow** (from `writer.rs`):
```rust
// 1. Get XET client
let client = core.xet_client("write").await?;

// 2. Create streaming writer
let writer = client.write(None).await?;

// 3. Write bytes incrementally (this is the streaming part!)
writer.write(bytes).await?;

// 4. Close and get file info (hash + size)
let file_info: XetFileInfo = writer.close().await?;
let xet_hash = file_info.hash().to_string();

// 5. Register in bucket via batch API
let operation = BucketOperation::AddFile { path, xet_hash };
core.bucket_batch(vec![operation]).await?;
```

**Bucket batch API call** (from `core.rs`):
```rust
pub async fn bucket_batch(endpoint: &str, bucket_id: &str, ops: Vec<BucketOperation>) -> Result<()> {
    // Serialize ops as NDJSON
    // POST to /api/buckets/{bucket_id}/batch
}
```

**XET token endpoint**:
```
GET /api/buckets/{namespace}/{name}/xet-write-token
Authorization: Bearer {token}
Response: { "accessToken": "...", "casUrl": "...", "exp": 123456 }
```

### Decision: Use the OpenDAL patterns as primary Rust reference

The OpenDAL PR is a better reference than the Python huggingface_hub because:
- It uses the **streaming** `XetWriter` API (write bytes incrementally) rather than batch `upload_bytes_async`
- This means we can **stream parquet bytes directly to XET** — no need to buffer the entire shard
- It shows the exact Cargo dependency setup for xet-core
- It's already Rust

---

## Why Buckets Instead of LFS

| Concern | LFS Sink (current ~5000 lines) | Bucket Sink (target) |
|---|---|---|
| Upload protocol | LFS batch API → presigned S3 → multipart upload | `XetWriter::write()` — streaming bytes |
| File hashing | Custom SHA256 streaming (hashing_writer.rs) | XET handles internally |
| Commit model | Atomic git commit via NDJSON API (commit.rs, 879 lines) | POST to `/api/buckets/{id}/batch` |
| Resume on failure | Custom checkpoint system (checkpoint.rs) | Bucket has what landed; upload the rest |
| Multipart uploads | Custom implementation handling HF-specific format | Handled by XET |
| Publishing as dataset | Immediate (it's a git repo) | Promote bucket → dataset repo (API coming) |

---

## Architecture Overview

```
Polars streaming engine
  → morsel arrives (RecordBatch)
  → ShardWriter encodes to parquet bytes
  → XetWriter streams bytes to XET storage as shard is written
  → On shard complete: XetWriter.close() → XetFileInfo { hash, size }
  → Accumulate file info
  → On finalize:
      POST /api/buckets/{id}/batch with NDJSON addFile entries
```

The key insight from OpenDAL: we can **pipe parquet encoding directly into XetWriter** without buffering the entire shard. This gives us true streaming with minimal memory.

---

## Phase 1: Research & Integration Map

**Objective**: Before writing any code, produce a detailed map of exactly which files in upstream Polars need changes, what the minimal sink node interface looks like, and validate the xet-core dependency works.

### 1.1 Map the Polars Streaming Sink Interface

Study the existing `feature/hf-hub-sink` branch (READ ONLY — do not modify it) to answer:

- [ ] What trait/interface must a streaming sink node implement? (look at `hf_sink/mod.rs` and other sinks like `parquet_sink`)
- [ ] Where in `lower_ir.rs` does sink dispatch happen? What match arms were added?
- [ ] Where in `to_graph.rs` does the sink node get inserted into the compute graph?
- [ ] What changes were needed in `polars-plan` (logical plan) to support the new sink target?
- [ ] How does `UnifiedSinkArgs` work? Where is it defined and how does it flow from Python?
- [ ] What other sinks exist in `crates/polars-stream/src/nodes/io_sinks/`? Which is simplest to use as a template?

**To study the existing branch without modifying it:**
```bash
git stash  # save any work
git checkout feature/hf-hub-sink
# ... read files, take notes ...
git checkout feature/hf-bucket-sink  # return to working branch
git stash pop  # restore work if needed
```

Also study **upstream Polars main** (on the `main` branch) to understand what sink infrastructure already exists without the HF changes.

**Deliverable**: Document titled "PHASE1_SINK_INTERFACE.md" in the repo root containing the minimal changeset — exact files and functions that need modification to add a new sink target, with code snippets.

### 1.2 Study the OpenDAL XET Integration

The OpenDAL PR #7185 is the primary reference for the Rust XET integration:

- [ ] How does `xet-data::streaming::XetClient` get created? What config does it need?
- [ ] How does `XetWriter` work? What's the write → close flow?
- [ ] What does `XetFileInfo` contain after close? (hash, size, sha256?)
- [ ] How does token refresh work with the `TokenRefresher` trait?
- [ ] What's in the `kszucs/xet-core` fork's `download_bytes` branch that isn't in main xet-core?
- [ ] Is there a way to use main xet-core instead, or do we need the fork?

**Key files to fetch from GitHub:**
```
# OpenDAL PR #7185
apache/opendal: core/services/huggingface/src/writer.rs
apache/opendal: core/services/huggingface/src/core.rs
apache/opendal: core/services/huggingface/src/uri.rs
apache/opendal: core/services/huggingface/Cargo.toml

# xet-core fork (check for streaming module)
kszucs/xet-core: data/src/streaming/
```

**Deliverable**: Document titled "PHASE1_XET_REFERENCE.md" with exact Rust types, function signatures, and data flow.

### 1.3 Map the Bucket API from huggingface_hub

The Python `huggingface_hub` implementation is a secondary reference:

**Key files** (at `/Users/davanstrien/Documents/code/huggingface_hub/`, branch `origin/buckets-api`):
```
src/huggingface_hub/hf_api.py  (lines 11398-11930: all bucket methods)
src/huggingface_hub/cli/bucket.py  (sync implementation, ~1400 lines)
src/huggingface_hub/utils/_xet.py  (XET connection info)
```

**Deliverable**: Section in "PHASE1_XET_REFERENCE.md" documenting HTTP endpoints and payload formats.

### 1.4 Evaluate Dependency Strategy

- [ ] Can we use `xet-core` main branch, or do we need the `kszucs/xet-core` fork with streaming support?
- [ ] Which xet-core crates are needed? (likely: `data`, `cas_types`, `utils`)
- [ ] Any version conflicts with Polars' existing dependencies?
- [ ] Feature flag strategy: `hf_bucket_sink` feature flag in polars-io?

**Deliverable**: Section in "PHASE1_SINK_INTERFACE.md" with recommended Cargo.toml additions.

---

## Phase 2: Implementation — Status

**All work on `feature/hf-bucket-sink` branch.**

### Completed

- [x] **2.1** Module structure + deps + feature flags (polars-io, polars-stream)
- [x] **2.1a** Standalone XET upload test — validated xet-core fork, token flow, batch API
- [x] **2.2** XET upload path in `polars-io/src/cloud/hf_bucket/xet_upload.rs`
- [x] **2.3** Bucket batch client in `polars-io/src/cloud/hf_bucket/batch.rs`
- [x] **2.4** Buffered parquet encoding in sink node (vstack all morsels → encode → upload)
- [x] **2.5** `HfBucketSinkNode` implementing `SinkNode` trait
- [x] **2.6a** Pipeline wiring: `PhysNodeKind::HfBucketSink`, `lower_ir.rs`, `to_graph.rs`, `fmt.rs`
- [x] **2.6b** Feature flag wiring: polars-lazy → polars → polars-python → polars-runtime-32
- [x] **2.7a** Python e2e test: `sink_parquet("hf://buckets/...")` uploads 1000 rows in 1.7s ✓

---

## Phase 3: Hardening & Optimization — Next Steps

The PoC works end-to-end. These are the remaining tasks to make it production-ready, roughly in priority order.

### 3.1 Migrate from xet-core fork to `subxet` (HIGH PRIORITY)
**Why**: Our current deps (`xet-data`, `xet-utils`, `cas_types` from `kszucs/xet-core`) pull ~420 transitive crates. OpenDAL has migrated to [`subxet`](https://github.com/kszucs/subxet), a tree-shaken single crate that cuts this by ~75%.
**What**: Replace the three git deps in `crates/polars-io/Cargo.toml` with one `subxet` dep. Update import paths (`xet_data::` → `subxet::data::`). API surface is identical — no logic changes.
**Estimated scope**: 2 files (Cargo.toml + xet_upload.rs import paths)

### 3.2 Streaming XET upload ~~(HIGH PRIORITY)~~ DONE
**Why**: Current implementation buffers all morsels into one DataFrame, encodes the full parquet file in memory, then uploads. This defeats the purpose of streaming for large datasets.
**What**: Added `StreamingBucketUploader` in polars-io that owns a `BatchedWriter<ChannelWriter>` for incremental parquet encoding and an async upload task that streams bytes to `XetWriter`. Memory stays at O(row_group_size).
**Scope**: 1 new file (`streaming_upload.rs`), 2 modified files (`mod.rs`, `hf_bucket_sink.rs`)

### 3.3 Add token refresh (MEDIUM PRIORITY)
**Why**: We pass `None` for the `TokenRefresher` when creating `XetClient`. XET tokens expire (~1hr). Long-running uploads on large datasets will fail.
**What**: Implement the `TokenRefresher` trait (re-fetch from `/api/buckets/{id}/xet-write-token`), pass to `XetClient::new()`. Follow OpenDAL's pattern in `core.rs:179-217`.
**Estimated scope**: ~30 lines in `xet_upload.rs`

### 3.4 Multi-file / sharded output (MEDIUM PRIORITY)
**Why**: Currently writes a single parquet file. Large datasets should be sharded (e.g. `part-00000.parquet`, `part-00001.parquet`) to enable parallel reads and avoid huge files.
**What**: Add shard size threshold. When exceeded, close current XetWriter, start new one with next filename. Accumulate all file infos, register all in one `bucket_batch()` call at finalize.
**Estimated scope**: ~50 lines in `hf_bucket_sink.rs`

### 3.5 Add unit tests (MEDIUM PRIORITY)
**Why**: Only have the Python e2e script. Need Rust-level tests for `xet_upload.rs`, `batch.rs`, and URL parsing.
**What**: Add `#[cfg(test)]` modules. Mock HTTP for batch API tests. Integration tests (behind feature flag + env var) for real uploads.
**Estimated scope**: ~200 lines across 2-3 files

### 3.6 Error handling & user experience (LOW PRIORITY)
**Why**: Current errors are raw (HTTP status codes, xet-core errors). Users need actionable messages.
**What**: Wrap errors with context (bucket name, file path, operation). Handle common failures: bucket doesn't exist (404), bad token (401), rate limit (429).
**Estimated scope**: ~50 lines

### Out of scope (for now): Read support for `hf://buckets/`
Reading from buckets (`pl.read_parquet("hf://buckets/...")`) is a separate concern from the write path we're building. [huggingface/huggingface_hub#3807](https://github.com/huggingface/huggingface_hub/pull/3807) adds bucket support to HfFileSystem/fsspec — once landed, worth checking if it works with polars' existing cloud read path out of the box. Tracked separately from this write-focused project.

---

## Key References

### Local Code
| Path | What | Branch |
|---|---|---|
| `/Users/davanstrien/Documents/code/polars/` | Polars with existing LFS sink | `feature/hf-hub-sink` (READ ONLY) |
| `/Users/davanstrien/Documents/code/polars/` | New bucket sink work | `feature/hf-bucket-sink` (WORK HERE) |
| `/Users/davanstrien/Documents/code/huggingface_hub/` | huggingface_hub with bucket API | `origin/buckets-api` |

### Remote Repos & PRs
| URL | What |
|---|---|
| https://github.com/pola-rs/polars | Upstream Polars (sync main from here) |
| https://github.com/huggingface/xet-core | XET storage client (Rust) — main repo |
| https://github.com/kszucs/xet-core/tree/download_bytes | XET fork with streaming API (used by OpenDAL) |
| https://github.com/huggingface/huggingface_hub/pull/3673 | Bucket API PR (Python reference) |
| https://github.com/apache/opendal/pull/7185 | **OpenDAL HF write + XET support (PRIMARY Rust reference)** |

### What to Reuse from Existing Branch

| Component | Location on `feature/hf-hub-sink` | Reuse? |
|---|---|---|
| Streaming engine integration | `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs` | **Study patterns** |
| Shard writer (parquet encoding) | `crates/polars-io/src/cloud/hf/shard_writer.rs` | **Adapt** — replace buffer with XetWriter |
| Sink options + PyO3 bindings | `crates/polars-io/src/cloud/hf/options.rs` | **Simplify** |
| URL parsing | `crates/polars-io/src/cloud/hf/url.rs` | **Simplify** |
| IR lowering / physical plan | `lower_ir.rs`, `to_graph.rs` | **Study** integration points |
| Auth/token handling | `crates/polars-io/src/cloud/hf/auth.rs` | **Reuse** |
| LFS client, upload, hashing, checkpoint, commit | Various | **Not needed** — replaced by XET + bucket batch |

---

## Design Decisions

### Why not a Polars plugin?
Polars plugins support custom expressions/functions but NOT custom sinks. Sink nodes are deeply integrated into the streaming engine's physical plan. A fork is necessary, but the diff should be much smaller than the LFS approach.

### Why start fresh from upstream (new branch)?
- Existing branch is 225 commits ahead — rebase impractical
- ~3000 lines of LFS code would be deleted anyway
- Clean git history from upstream main makes a future PR viable
- Old branch preserved as reference

### Memory model
True streaming: parquet bytes flow through XetWriter to XET storage. Memory ≈ O(row_group_size), not O(shard_size). Better than the existing sink.

### Error handling
If upload fails mid-stream, bucket has whatever shards completed. User re-runs and we skip already-uploaded files (compare with `list_bucket_tree`). No checkpoint file needed.

### Core polars footprint (audited 2026-02-18)
92% of code (666 of 727 lines) is in HF-specific modules (`polars-io/cloud/hf_bucket/` and `hf_bucket_sink.rs`). The remaining 52 lines touch 7 core files — all `#[cfg(feature = "hf_bucket_sink")]` gated except one 1-line change adding `"buckets"` to the allowed HF URL schemes. The core touches are the minimum wiring any new sink type requires: enum variant, IR routing, graph wiring, fmt. Nothing to refactor.

---

## Session Log

**Agents MUST append to this section after each session.** This is critical for continuity between sessions. Include enough detail that a new agent can pick up exactly where you left off.

### Format
```markdown
### YYYY-MM-DD HH:MM — [Phase X.Y] Brief description
**Branch**: feature/hf-bucket-sink
**Duration**: ~X mins
**Status**: completed | in-progress | blocked
**What was done**:
- Specific files read, patterns identified, code written
**Key findings**:
- Important discoveries, gotchas, decisions made
**Artifacts produced**:
- Files created or modified (with paths)
**Next steps**:
- Specific tasks for next session
**Open questions**:
- Unresolved issues needing human input
```

---

### 2026-02-13 — Project kickoff and planning
**Status**: planning complete
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
**Next steps**:
- Phase 1.1: Map Polars streaming sink interface from existing branch
- Phase 1.2: Study OpenDAL XET integration in detail (fetch full source of writer.rs, core.rs)
- Phase 1.3: Document bucket API endpoints from huggingface_hub

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
- OpenDAL writer shows the exact XetWriter lifecycle: `write(bytes)` streaming → `close()` → `XetFileInfo` → `bucket_batch()`.
- Token auto-refresh via `TokenRefresher` trait means long uploads won't fail from expiry.
- NDJSON format for batch API: one JSON object per line, Content-Type `application/x-ndjson`.
- `kszucs/xet-core` fork `download_bytes` branch required — `streaming` module not in main xet-core yet.
**Artifacts produced**:
- `PHASE1_SINK_INTERFACE.md` — Complete integration map for wiring a new sink into Polars (SinkNode trait, type chain, 6-file minimal diff, deps, design decisions)
- `PHASE1_XET_REFERENCE.md` — XET upload + bucket batch API reference (XetClient, XetWriter, BucketOperation, token management, API endpoints, data flow diagram)
- Updated `BUCKET_SINK_PLAN.md` — This session log entry
**Next steps**:
- Phase 2.1: Set up module structure (`crates/polars-io/src/cloud/hf_bucket/` and `crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink/`)
- Phase 2.2: Add xet-core dependencies and feature flags to Cargo.toml files
- Phase 2.3: Implement XET upload path (token fetcher, client creation, streaming writer)
- Phase 2.4: Implement bucket batch client
- Phase 2.5: Implement `HfBucketSinkNode` (SinkNode trait)
- Phase 2.6: Wire into Polars pipeline (PhysNodeKind variant, lower_ir dispatch, to_graph match arm)
**Open questions**:
- When will `streaming` module merge to main `huggingface/xet-core`? (Currently need fork)
- Should bucket sink support partitioned writes, or single-file only for PoC?
- How to pass HF token from Python — via `CloudOptions::config` or custom `hf_options` field?

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
- `async-trait` already exists as an optional dep in `polars-io/Cargo.toml` (workspace). Using `dep:async-trait` in a feature flag suppresses the implicit feature name, breaking the existing `async` feature that references `"async-trait"`. Removed `dep:async-trait` from `hf_bucket_sink` — it's transitively enabled via `cloud` → `async` → `async-trait`.
- `cargo update -p tempfile` was needed to resolve lockfile conflict (xet-core deps need tempfile >= 3.25). This updated many transitive deps.
- Pre-existing `polars-core` compilation error (`GroupsIndicator` undeclared) when building `polars-io` with default features — this is NOT caused by our changes (verified by stashing changes and testing). Building through `polars-stream --features parquet` works fine (different feature set).
- xet-core deps pinned to commit `cc271895` from `download_bytes` branch.
**Verification**:
- `cargo metadata` ✅ — features and deps parse correctly in both `polars-io` and `polars-stream`
- `cargo check -p polars-stream --features parquet,hf_bucket_sink` ✅ — xet-core deps resolve, download, and compile successfully
- BUCKETS change is purely additive — existing `"hf://bucket/a/b/c"` test (singular, not plural) still correctly fails
**Artifacts produced**:
- Modified `crates/polars-io/Cargo.toml` — feature flag + 3 xet-core deps
- Modified `crates/polars-stream/Cargo.toml` — feature flag
- Modified `crates/polars-io/src/path_utils/hugging_face.rs` — BUCKETS const
- Updated `Cargo.lock` — xet-core deps resolved
- Updated `BUCKET_SINK_PLAN.md` — this session log
- Updated `PHASE1_SINK_INTERFACE.md` — marked Steps 1-3 as done in checklist
**Next steps**:
- **Next session: Standalone XET upload test** (Phase 2.1a) — validate xet-core fork, token flow, and batch API before writing Polars integration code. See new section below.
- Then: Session 2 (polars-io modules), Session 3 (sink node + wiring)
- Rebase onto latest `main` before Session 2 to fix pre-existing `polars-core` build issue
**Open questions**:
- Pre-existing `polars-core` compilation issue on this branch — needs rebase from latest `main` before full integration (does NOT block standalone test)

### 2026-02-13 — [Phase 2.1a] Standalone XET upload test binary created
**Branch**: feature/hf-bucket-sink
**Status**: completed (all 5 steps passed end-to-end)
**What was done**:
- Created standalone Rust project at `scratch/xet_upload_test/` (outside polars workspace via empty `[workspace]` table)
- Wrote 5-step test binary: (1) fetch XET write token, (2) create XetClient, (3) upload data via XetWriter, (4) register file via bucket batch API, (5) verify file exists
- Binary reads config from env vars: `HF_TOKEN`, `HF_BUCKET_NAMESPACE`, `HF_BUCKET_NAME`, `HF_ENDPOINT`
- Each step prints clear progress/failure output with HTTP response bodies for debugging
**Key findings**:
- xet-core fork (`kszucs/xet-core` branch `download_bytes`) compiles successfully as a standalone dependency
- Confirmed import paths that work:
  - `xet_data::streaming::XetClient` — client construction
  - `xet_data::streaming::XetWriter` — streaming write (via `client.write(None).await?`)
  - `xet_data::XetFileInfo` — returned from `writer.close().await?`, has `.hash()` and `.file_size()` methods
- `XetClient::new()` signature confirmed: `(Option<String>, Option<(String, u64)>, Option<Arc<dyn TokenRefresher>>, String) -> Result<Self>`
- No `cas_types` dependency needed for the upload path — only `xet-data` and `xet-utils`
- Build pulls 420 transitive crates; compiles in ~2.5 minutes from clean
- Zero compilation errors on first attempt — the API matches the OpenDAL PR reference exactly
**Artifacts produced**:
- `scratch/xet_upload_test/Cargo.toml` — standalone project with xet-core deps
- `scratch/xet_upload_test/src/main.rs` — 270-line test binary (5 steps)
- Updated `BUCKET_SINK_PLAN.md` — this session log
**Next steps**:
- User creates an HF bucket (e.g. `davanstrien/test-bucket`)
- Run: `cd scratch/xet_upload_test && HF_TOKEN="hf_..." HF_BUCKET_NAMESPACE="davanstrien" HF_BUCKET_NAME="test-bucket" cargo run`
- Iterate on any runtime failures (token format, API endpoints, payload format)
- Document confirmed `XetFileInfo` field values (hash format, size) for use in Polars integration
- After validation: proceed to Phase 2.2 (implement XET upload path in polars-io)
**Runtime results** (all passed first attempt against `davanstrien/test-bucket`):
- Step 1: XET write token fetched. CAS URL = `https://cas-server.xethub.hf.co`. Token is JWT (`eyJhbGciOi...`). Expiry is Unix timestamp (e.g. `1770995540`).
- Step 2: `XetClient::new()` succeeds with the token info. No issues.
- Step 3: Upload of 3500 bytes succeeds. Hash = `1be2b0ff9b1bca5969ef2c765b33f8bc291b76ec2be00d71c992e9fdffa5af6e` (64-char hex = SHA256). `file_size()` returns exact byte count.
- Step 4: Batch API returns `{"success":true,"processed":1,"succeeded":1,"failed":[]}`. NDJSON `AddFile` with `type`/`path`/`xetHash` fields confirmed working.
- Step 5: Paths-info returns `[{"type":"file","path":"test_upload.txt","size":3500,"xetHash":"...","uploadedAt":"2026-02-13T14:58:01.399Z"}]`. POST with `{"paths":["test_upload.txt"]}` body confirmed.
**Confirmed for Polars integration**:
- Import paths: `xet_data::streaming::XetClient`, `xet_data::streaming::XetWriter`, `xet_data::XetFileInfo`
- `XetFileInfo.hash()` returns a 64-char hex SHA256 string
- `XetFileInfo.file_size()` returns exact byte count (u64)
- Batch API: POST NDJSON with `Content-Type: application/x-ndjson`, each line `{"type":"addFile","path":"...","xetHash":"..."}`
- Paths-info: POST JSON `{"paths":["..."]}` returns array of file objects with `type`, `path`, `size`, `xetHash`, `uploadedAt`
- No `cas_types` dep needed for the upload path
**Next steps**:
- Proceed to Phase 2.2: Implement XET upload path in `crates/polars-io/src/cloud/hf_bucket/`
- Use confirmed import paths and API formats directly
- Consider token refresh for long-running uploads (expiry was ~1hr from request time)

### 2026-02-13 — [Phase 2.2] polars-io HF bucket module created
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Created `crates/polars-io/src/cloud/hf_bucket/` module with three files:
  - `mod.rs` (~45 lines) — Module root, exports, and `HfBucketConfig` struct with builder pattern
  - `xet_upload.rs` (~100 lines) — `XetToken`, `fetch_xet_write_token()`, `create_xet_client()`, `BucketWriter` with `new_writer()` and `upload_bytes()` helpers
  - `batch.rs` (~70 lines) — `BucketOperation` enum (AddFile/DeleteFile with serde NDJSON serialization), `bucket_batch()` function
- Registered module in `crates/polars-io/src/cloud/mod.rs` with `#[cfg(feature = "hf_bucket_sink")]`
- Updated stale status markers in `BUCKET_SINK_PLAN.md` (2.1a: NEXT → DONE) and `PHASE1_SINK_INTERFACE.md` (revised execution order, added checklist row 3a)
**Key findings**:
- `polars_bail!` macro needs explicit import (`use polars_error::polars_bail;`) — not automatically in scope in new modules
- All dependencies needed (`reqwest`, `serde`, `serde_json`, `bytes`, `tokio`) are transitively enabled via the `cloud` feature, which `hf_bucket_sink` already depends on
- Error handling pattern: `to_compute_err` for converting external errors (reqwest, serde_json, xet-core), `polars_bail!(ComputeError: ...)` for custom error messages
- Zero warnings from new code
**Compiler issues**:
- Initial: `cannot find macro polars_bail in this scope` in both `xet_upload.rs` and `batch.rs` — fixed by adding `use polars_error::polars_bail;`
- After fix: clean compilation with no errors or warnings from new code
**Verification**:
- `cargo check -p polars-stream --features parquet,hf_bucket_sink` ✅ — passes with zero errors
- No warnings from `hf_bucket` module files
- All new code gated behind `#[cfg(feature = "hf_bucket_sink")]` — zero impact on normal builds
**Artifacts produced**:
- Created `crates/polars-io/src/cloud/hf_bucket/mod.rs`
- Created `crates/polars-io/src/cloud/hf_bucket/xet_upload.rs`
- Created `crates/polars-io/src/cloud/hf_bucket/batch.rs`
- Modified `crates/polars-io/src/cloud/mod.rs` — added module declaration
- Updated `BUCKET_SINK_PLAN.md` — status markers + this session log
- Updated `PHASE1_SINK_INTERFACE.md` — execution order + checklist
**Next steps**:
- Phase 2.4: Shard writer — pipe parquet encoder output to `BucketWriter::new_writer()` for streaming upload
- Phase 2.5: Sink node — implement `SinkNode` trait for `HfBucketSinkNode` in `polars-stream`
- Phase 2.6: Pipeline wiring — `PhysNodeKind::HfBucketSink` variant, `lower_ir.rs` dispatch, `to_graph.rs` match arm
- Rebase onto latest `main` before Phase 2.5 to fix pre-existing `polars-core` build issue

### 2026-02-13 — [Phase 2.5] Stub sink node + pipeline wiring
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Created stub `HfBucketSinkNode` at `crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink.rs` (~58 lines)
  - Implements `SinkNode` trait with `spawn_sink` that consumes and discards morsels
  - Uses `FileSinkOptions` directly — no new options type needed
  - `initialize()` and `finalize()` use default no-op implementations
- Registered module in `crates/polars-stream/src/nodes/io_sinks/mod.rs`
- Added `PhysNodeKind::HfBucketSink` variant in `crates/polars-stream/src/physical_plan/mod.rs`
  - Also added match arm in `visit_node_inputs_mut` for graph traversal
- Added visualization match arm in `crates/polars-stream/src/physical_plan/fmt.rs`
- Added `hf://buckets/` URL routing in `crates/polars-stream/src/physical_plan/lower_ir.rs`
  - Checks `SinkTarget::Path` for `hf://buckets/` prefix, routes to `HfBucketSink` instead of `FileSink`
- Wired graph node in `crates/polars-stream/src/physical_plan/to_graph.rs`
  - Creates `SinkComputeNode::from(HfBucketSinkNode::new(options))` — same pattern as other sinks
**Key findings**:
- Cannot use `#[cfg(...)]` on `|` arms in Rust match patterns — needed separate match arm for `HfBucketSink` in `visit_node_inputs_mut`
- `fmt.rs` (`visualize_plan_rec`) also has exhaustive match on `PhysNodeKind` — needed arm there too (not in original plan)
- The `lower_ir.rs` routing returns early with `PhysStream::first(phys_sm.insert(...))` since the match arm is inside the `lower_ir!` macro expansion
**Verification**:
- `cargo check -p polars-stream --features parquet,hf_bucket_sink` ✅ — zero errors
- `cargo check -p polars-stream --features parquet` ✅ — zero errors (no leakage without feature flag)
- All new code gated behind `#[cfg(feature = "hf_bucket_sink")]`
**Artifacts produced**:
- Created `crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink.rs`
- Modified `crates/polars-stream/src/nodes/io_sinks/mod.rs`
- Modified `crates/polars-stream/src/physical_plan/mod.rs`
- Modified `crates/polars-stream/src/physical_plan/fmt.rs`
- Modified `crates/polars-stream/src/physical_plan/lower_ir.rs`
- Modified `crates/polars-stream/src/physical_plan/to_graph.rs`
- Updated `PHASE1_SINK_INTERFACE.md` — marked Steps 4-8 as DONE
- Updated `BUCKET_SINK_PLAN.md` — this session log
**Next steps**:
- Phase 2.4: Shard writer — pipe parquet encoder output to `BucketWriter::new_writer()` for streaming XET upload
- Fill in `initialize()`: parse URL, fetch XET token, create `BucketWriter`
- Fill in `spawn_sink()`: encode morsels to parquet bytes, stream to `XetWriter`
- Fill in `finalize()`: call `bucket_batch()` to register uploaded files
- Python end-to-end test: `df.sink_parquet("hf://buckets/ns/name/file.parquet")`

---

### Session 4 — Phase 3: Fill in HfBucketSinkNode with real parquet + XET upload

**Date**: 2026-02-13

**Goal**: Replace the stub `HfBucketSinkNode` with real logic: parquet encoding of incoming
DataFrames, XET upload of the encoded bytes, and batch API registration.

**Approach**: Simple buffered PoC — consume all morsels serially, vstack into one DataFrame,
encode as a single parquet file, then upload via the XET protocol and register via batch API.

**Changes**:
1. `crates/polars-io/src/cloud/hf_bucket/mod.rs`:
   - Added `parse_hf_bucket_url()` — parses `hf://buckets/ns/name/path` into components
   - Added `extract_hf_token()` — resolves HF token from CloudOptions, HF_TOKEN env, or cached file
   - Added `upload_and_register_file()` — high-level async helper: XET upload + batch registration
2. `crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink.rs`:
   - Full `SinkNode` implementation with `initialize()`, `spawn_sink()`, `finalize()`
   - `initialize()`: parses URL, extracts token, creates `HfBucketConfig`
   - `spawn_sink()`: serial consumer, vstacks all morsels, encodes to parquet
   - `finalize()`: uploads encoded bytes via tokio runtime using `upload_and_register_file()`
3. `crates/polars-stream/src/physical_plan/to_graph.rs`:
   - Updated `HfBucketSink` match arm to pass `input_schema` to constructor

**Verification**:
- `cargo check -p polars-stream --features hf_bucket_sink,parquet` — PASS
- `cargo check -p polars-stream --features parquet` — PASS (no regression)

**Architecture notes**:
- Used serial consumption (`is_sink_input_parallel = false`) for simplicity
- Each morsel is vstacked into a single combined DataFrame, then encoded as one parquet file
- Upload logic lives in polars-io to avoid adding reqwest/bytes deps to polars-stream
- Shared `Arc<Mutex<Option<Vec<u8>>>>` bridges spawn_sink (encoding) → finalize (upload)

**Next steps**:
- Python end-to-end test: `df.sink_parquet("hf://buckets/ns/name/file.parquet")`
- Streaming XET upload (write parquet row groups incrementally instead of buffering all)
- Parallel morsel encoding with batched parquet writer

---

### 2026-02-18 — [Phase 2.6] Feature flag wiring + Python e2e test
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Wired `hf_bucket_sink` feature flag through the full crate chain (4 files):
  - `crates/polars-lazy/Cargo.toml`: `hf_bucket_sink = ["polars-stream?/hf_bucket_sink"]`
  - `crates/polars/Cargo.toml`: `hf_bucket_sink = ["polars-lazy?/hf_bucket_sink"]`
  - `crates/polars-python/Cargo.toml`: `hf_bucket_sink = ["polars/hf_bucket_sink"]`
  - `py-polars/runtime/polars-runtime-32/Cargo.toml`: `hf_bucket_sink = ["polars-python/hf_bucket_sink"]`
- Built local Python wheel: `maturin develop -m py-polars/runtime/polars-runtime-32/Cargo.toml --features hf_bucket_sink`
- Created e2e test script at `scratch/test_hf_bucket_sink.py`
- Created HF bucket `davanstrien/test-polars-bucket` via `hf buckets create` CLI (from `huggingface_hub@buckets-api` branch)
- Ran e2e test successfully:
  - `sink_parquet("hf://buckets/davanstrien/test-polars-bucket/test-f22bceae.parquet")` uploaded 1000 rows in 1.7s
  - File confirmed on HF (5,885 bytes) via `hf buckets tree`
  - Read-back not yet supported (`hf://buckets/` read path not wired in polars-io) — expected
**Verification**:
- `cargo check --manifest-path py-polars/runtime/polars-runtime-32/Cargo.toml --features hf_bucket_sink` — PASS
- `HF_TOKEN=... python scratch/test_hf_bucket_sink.py` — PASS (upload succeeds, file appears in bucket)
**Artifacts produced**:
- Modified 4 Cargo.toml files (feature flag wiring)
- Created `scratch/test_hf_bucket_sink.py` — e2e test script
- Updated `BUCKET_SINK_PLAN.md` — this session log
**Next steps**:
- Streaming XET upload (write parquet row groups incrementally instead of buffering all)
- Parallel morsel encoding
- Wire `hf://buckets/` read support so round-trip works
- Commit all changes

---

## Comparison: Rust-native Sink vs HfFileSystem/fsspec (PR #3807)

**Context**: [huggingface/huggingface_hub#3807](https://github.com/huggingface/huggingface_hub/pull/3807) (draft) adds `hf://buckets/` support to `HfFileSystem`, the Python fsspec backend. [huggingface/huggingface_hub#3796](https://github.com/huggingface/huggingface_hub/issues/3796) documents the full Buckets API (CLI + Python). These are complementary to our approach, not competing.

### How PR #3807 works (fsspec approach)
- `HfFileSystemFile._upload_chunk()` writes data to a **local temp file**, then on `final=True` calls `api.batch_bucket_files(add=[(temp_path, remote_path)])` to upload
- Upload goes through the Python `huggingface_hub` client (which internally uses XET via `hf-xet`)
- All data must be buffered to disk before upload begins
- Standard fsspec interface: `open()`, `read()`, `write()`, `glob()`, `ls()`

### How our Rust-native sink works
- Parquet encoding happens **in-memory** inside the polars streaming pipeline
- Encoded bytes go directly to `XetWriter` via xet-core Rust crate — no temp files, no Python
- `bucket_batch()` registers files after XET upload completes
- Runs inside the streaming engine, processing data morsel-by-morsel

### Key efficiency advantages of Rust-native sink
| Aspect | fsspec (PR #3807) | Rust-native sink (ours) |
|--------|-------------------|------------------------|
| **Encoding** | Python-level (or delegates to polars then copies) | In-engine, zero-copy from streaming pipeline |
| **Temp files** | Yes — writes to disk, then uploads | No — parquet bytes go straight to XET |
| **Memory** | Must buffer full file before upload | O(row_group_size), streams morsel-by-morsel |
| **GIL** | Held during encoding/coordination | No Python involvement — pure Rust |
| **XET deduplication** | Via hf-xet Python wrapper | Direct xet-core Rust — block-level dedup |
| **Large datasets** | Limited by disk space for temp files | Arbitrarily large lazy frames, constant memory |

### When to use which
- **fsspec (PR #3807)**: General-purpose access — read, list, glob, small writes, interactive use. Great for convenience and ecosystem interop.
- **Rust-native sink (ours)**: Performance-critical bulk writes via `sink_parquet()`. Designed for large-scale data pipelines where streaming and memory efficiency matter.

They are complementary: fsspec for the read path and general interop, our sink for the write-heavy data engineering path.

---

## TODO: Review OpenDAL HF Service Changes (Feb 2026)

OpenDAL's HF service (`opendal/core/services/hf/`) has been updated since our initial reference analysis. Key changes identified on 2026-02-18:

### Migration to `subxet` (HIGH PRIORITY)
OpenDAL has migrated from the `kszucs/xet-core` fork (3 crates: `xet-data`, `xet-utils`, `cas_types`) to [`subxet`](https://github.com/kszucs/subxet) — a tree-shaken single-crate version of xet-core. This reduced their Cargo.lock from 511 to 127 entries (~75% reduction). Our Polars deps still pull ~420 transitive crates from the xet-core fork.

**Action**: Migrate `crates/polars-io/Cargo.toml` from `xet-data`/`xet-utils`/`cas_types` to `subxet`. Import paths change from `xet_data::streaming::XetClient` → `subxet::data::streaming::XetClient` (etc). The API surface is identical — this is a dependency swap, not a rewrite.

### Token refresh support (MEDIUM PRIORITY)
OpenDAL now implements `TokenRefresher` trait for automatic XET token renewal during long uploads. We currently pass `None` for the refresher, which works for short uploads but may fail on multi-hour jobs.

**Action**: Implement `TokenRefresher` for long-running sink operations.

### No breaking API changes
The core APIs are confirmed unchanged:
- `XetClient::new()` — same signature
- `XetWriter::write()` / `close()` — same lifecycle
- `BucketOperation` / `bucket_batch()` — same NDJSON format
- HTTP endpoints — unchanged

### Other improvements to consider
- OpenDAL has comprehensive tests (16 test cases for writer) — we should add unit tests for `xet_upload.rs` and `batch.rs`
- OpenDAL's `HfWriter` enum supports both regular (base64 inline) and XET modes — not needed for buckets (always XET) but relevant if we ever support repo writes

---

### 2026-02-18 — [Phase 3.2] Streaming XET upload
**Branch**: feature/hf-bucket-sink
**Status**: completed
**What was done**:
- Created `crates/polars-io/src/cloud/hf_bucket/streaming_upload.rs` (~160 lines):
  - `ChannelWriter`: sync `Write` impl that sends byte chunks over a bounded `std::sync::mpsc::sync_channel(16)` for backpressure
  - `StreamingBucketUploader`: owns `BatchedWriter<ChannelWriter>` + async upload task. Uses bridge pattern: `spawn_blocking` drains std channel → tokio mpsc channel → async XET writes
  - `UploadedFileInfo`: returned from `finish()` with xet_hash + file_size
- Added `register_file()` helper to `crates/polars-io/src/cloud/hf_bucket/mod.rs` — wraps `bucket_batch()` so polars-stream doesn't need reqwest dependency
- Rewrote `crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink.rs`:
  - Replaced `encoded_bytes: Arc<Mutex<Option<Vec<u8>>>>` (buffer everything) with `file_info: Arc<Mutex<Option<UploadedFileInfo>>>` (streaming)
  - `spawn_sink()`: creates `StreamingBucketUploader`, calls `write_batch(&df)` per morsel, calls `finish()` at end
  - `finalize()`: just calls `register_file()` to register the XET hash with the bucket API
- Registered `mod streaming_upload` + `pub use streaming_upload::*` in `mod.rs`
**Key design decisions**:
- All HF/XET logic stays in polars-io — sink node in polars-stream is thin glue
- `StreamingBucketUploader::new()` takes owned values (not refs) so the future is `'static` for `tokio::spawn`
- Bridge pattern (std::sync channel → spawn_blocking → tokio channel) avoids unsafe code and works regardless of caller's thread context
- `ParquetWriteOptions::to_writer(channel_writer).batched(&schema)` reuses existing polars API
**Verification**:
- `cargo check -p polars-io --features hf_bucket_sink,parquet` — PASS
- `cargo check -p polars-stream --features parquet,hf_bucket_sink` — PASS
- `cargo check -p polars-stream --features parquet` — PASS (no regression without feature)
**Memory model**:
- Before: O(total_dataset) — vstack all morsels, encode full parquet, then upload
- After: O(row_group_size) — each morsel encoded as row group(s), bytes streamed to XET via channel
**Artifacts produced**:
- Created `crates/polars-io/src/cloud/hf_bucket/streaming_upload.rs`
- Modified `crates/polars-io/src/cloud/hf_bucket/mod.rs` (module registration + `register_file()`)
- Rewritten `crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink.rs`
- Updated `BUCKET_SINK_PLAN.md` — Phase 3.2 marked done + this session log
**Next steps**:
- Build wheel + e2e test with `scratch/test_hf_bucket_sink.py`
- Test with larger dataset (1M+ rows) to confirm memory doesn't spike
- Phase 3.1: Migrate from xet-core fork to `subxet` (reduce 420 transitive crates)
- Phase 3.3: Token refresh for long-running uploads
- Phase 3.4: Multi-file / sharded output
