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

## Phase 2: Implementation

**Prerequisite**: Phase 1 deliverables complete and reviewed.

**All work on `feature/hf-bucket-sink` branch** (created from synced `main`).

### 2.1 Set Up Module Structure *(DONE — deps, feature flags, and module files)*

Create minimal module structure:
```
crates/polars-io/src/cloud/hf_bucket/
├── mod.rs              # Module exports
├── options.rs          # BucketSinkOptions (simplified)
├── auth.rs             # Token handling
├── url.rs              # Parse hf://buckets/ URLs
├── xet_upload.rs       # XetClient + XetWriter wrapper
└── batch.rs            # Bucket batch API (simple HTTP POST, ~50 lines)

crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink/
└── mod.rs              # Streaming sink node
```

### 2.1a Standalone XET Upload Test *(DONE — all 5 steps passed)*

**Why**: The xet-core fork (`kszucs/xet-core` branch `download_bytes`) is the biggest unknown. Its `streaming` module doesn't exist in main xet-core. If the API has changed, if the token endpoint returns something unexpected, or if `bucket_batch()` needs a different payload — we want to discover that in a ~100-line test, not after writing ~520 lines of Polars integration code.

**What**: A standalone Rust integration test (binary or `#[test]`) that exercises the full upload path end-to-end:

1. **Auth**: Fetch XET write token from `GET /api/buckets/{namespace}/{name}/xet-write-token` using `HF_TOKEN`
2. **XET client**: Create `XetClient` with the token and CAS URL
3. **Upload**: Create `XetWriter`, write a small parquet buffer (~100 rows), call `close()` → `XetFileInfo`
4. **Register**: Call `bucket_batch()` with `AddFile { path, xet_hash }` to register the file in the bucket
5. **Verify**: Confirm the file appears (e.g. `GET /api/buckets/{namespace}/{name}/tree` or similar)

**Location**: `scratch/xet_upload_test/` (standalone Cargo project, not part of polars workspace)

**Validates**:
- xet-core fork compiles and links correctly
- `XetClient` construction with our token works
- `XetWriter::write()` → `close()` produces a valid XET hash
- Bucket batch API accepts the hash and registers the file
- Token format/refresh assumptions are correct

**Prerequisites**:
- An HF bucket to test with (create via `huggingface_hub` CLI or API)
- `HF_TOKEN` environment variable set

**Does NOT require**:
- Polars compilation (bypasses `polars-core` build issue)
- Rebase onto main
- Any changes to polars-stream

### 2.2 Implement XET Upload Path

Following the OpenDAL pattern:
1. XET token fetcher: GET `/api/buckets/{id}/xet-write-token`
2. XET client creation with token refresh
3. Streaming write wrapper: `XetWriter::write(bytes)` + `close() → XetFileInfo`
4. Unit test: upload a small file to a test bucket

### 2.3 Implement Bucket Batch Client

Simple HTTP POST — ~50 lines:
```rust
async fn bucket_batch(endpoint: &str, bucket_id: &str, token: &str, ops: Vec<BucketOp>) -> Result<()> {
    // Serialize ops as NDJSON, POST to /api/buckets/{bucket_id}/batch
}
```

### 2.4 Implement Shard Writer with XET Streaming

Adapt shard_writer.rs from existing branch (study on `feature/hf-hub-sink`, implement on `feature/hf-bucket-sink`):
- Pipe parquet encoder output directly to `XetWriter::write()`
- On shard complete: `XetWriter::close()` → `XetFileInfo`
- No local buffering, no SHA256, no temp files

### 2.5 Implement Streaming Sink Node

Build `HfBucketSinkNode`:
- Receive morsels from streaming engine
- Feed to shard writer → XetWriter
- When shard completes: record XetFileInfo
- On finalize: call bucket batch API

### 2.6 Wire Into Polars Pipeline

- Add sink target to logical plan
- Add dispatch in `lower_ir.rs` and `to_graph.rs`
- Add Python bindings for `sink_parquet("hf://buckets/...")`

### 2.7 Testing

- E2E: create bucket → stream data → verify files
- Test with real Hub bucket
- Test failure recovery (partial upload)
- Memory profiling: verify constant-memory streaming

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
