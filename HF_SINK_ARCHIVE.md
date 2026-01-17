# HF Hub Sink - Completed Work Archive

This file contains detailed documentation for completed phases of the HF sink implementation.
For active work, see [HF_SINK_IMPLEMENTATION_PLAN.md](./HF_SINK_IMPLEMENTATION_PLAN.md).

---

## Session Log

| Date | Tasks | Status | Notes |
|------|-------|--------|-------|
| 2026-01-13 | 0.1, 0.3, 0.4 | Complete | Branch created, build verified |
| 2026-01-14 | 1.1 | Complete | Module structure created |
| 2026-01-14 | 1.2 | Complete | HfSinkOptions with builder pattern |
| 2026-01-14 | 1.3 | Complete | URL parsing for writes |
| 2026-01-14 | 1.4 | Complete | Token/auth handling |
| 2026-01-14 | 2.1 | Complete | HashingWriter |
| 2026-01-14 | 2.2 | Complete | MmapBuffer |
| 2026-01-14 | 2.3 | Complete | HfShardWriter |
| 2026-01-14 | 3.1 | Complete | LFS Types |
| 2026-01-14 | 3.2 | Complete | LFS Client |
| 2026-01-14 | 3.3 | Complete | Upload Executor |
| 2026-01-15 | 3.4 | Complete | Commit API Client |
| 2026-01-15 | 4.1 | Complete | HfSinkNode skeleton |
| 2026-01-15 | 4.2 | Complete | Shard Writer Task |
| 2026-01-15 | Fix | Complete | Upstream feature-gate bugs |
| 2026-01-16 | 4.3-4.4 | Complete | finalize(), IOSinkNode integration |
| 2026-01-17 | 5.1 | Complete | Mode handling (ErrorIfExists, Overwrite, Append) |
| 2026-01-17 | 5.2 | Complete | Dataset card updates (README generation) |

---

## Phase 0: Development Setup (Complete)

### Task 0.1: Fork and Branch Setup ✅
- Created branch `feature/hf-hub-sink`
- Pushed to remote (davanstrien/polars)

### Task 0.3: Local Development Build ✅
- `make build-release` succeeds
- Python import verified (v1.37.1)

### Task 0.4: Git Install Test Setup ✅
```bash
pip install "git+https://github.com/davanstrien/polars.git@feature/hf-hub-sink#subdirectory=py-polars"
```

### Task 0.6: Sync with Upstream ✅
- Rebased onto pola-rs/polars main
- Fixed upstream feature-gate bugs (GroupsIndicator, unique_id)

---

## Phase 1: Foundation (Complete)

### Task 1.1: Module Structure ✅
**Files created:**
```
crates/polars-io/src/cloud/hf/
├── mod.rs          # Module exports
├── url.rs          # HFPathParts, HFRepoLocation
├── glob.rs         # expand_paths_hf
├── options.rs      # HfSinkOptions
├── auth.rs         # Token handling
└── error.rs        # Error types stub
```

### Task 1.2: Configuration Types ✅
**File:** `options.rs` (504 lines)

Types:
- `RepoType` enum (Dataset, Model, Space)
- `HfWriteMode` enum (ErrorIfExists, Overwrite, Append)
- `HfSinkOptions` struct (14 fields)
- `HfSinkOptionsBuilder` with fluent API

Tests: 13 unit tests

### Task 1.3: URL Parsing ✅
**File:** `url.rs`

Added:
- `get_lfs_batch_uri()` for LFS API
- `get_commit_uri()` for commit API
- `RepoType::from_bucket_str()`
- `HFPathParts::repo_type()`

Tests: 4 new tests

### Task 1.4: Token/Auth ✅
**File:** `auth.rs` (110 lines)

`get_hf_token(explicit, required)` with priority:
1. Explicit token parameter
2. `HF_TOKEN` env var
3. `~/.cache/huggingface/token` file

Tests: 5 unit tests

---

## Phase 2: Core Writer (Complete)

### Task 2.1: HashingWriter ✅
**File:** `hashing_writer.rs` (205 lines)

```rust
pub struct HashingWriter<W> {
    inner: W,
    hasher: Sha256,
    bytes_written: u64,
}
```

- Implements `std::io::Write`
- SHA256 computed incrementally
- `finish()` returns (inner, hash, size)
- `sha256_to_hex()` helper

Tests: 7 unit tests

### Task 2.2: MmapBuffer ✅
**File:** `mmap_buffer.rs` (334 lines)

```rust
pub struct MmapBuffer {
    file: NamedTempFile,
    mmap: Option<MmapMut>,
    len: usize,
    capacity: usize,
}
```

- Memory-mapped temp file
- Dynamic growth (doubling, min 1MB)
- `into_read_handle()` for zero-copy upload

Tests: 9 unit tests

### Task 2.3: HfShardWriter ✅
**File:** `shard_writer.rs` (403 lines)

Writer chain:
```rust
FileWriter<BufWriter<HashingWriter<MmapBuffer>>>
```

- `write_batch(RecordBatch)` - writes row groups
- `finish()` returns `FinishedShard` with SHA256, size, rows

Tests: 6 unit tests

---

## Phase 3: LFS Protocol (Complete)

### Task 3.1: LFS Types ✅
**File:** `lfs/types.rs` (586 lines)

Request types:
- `LfsBatchRequest`, `LfsOperation`, `LfsObjectRequest`

Response types:
- `LfsBatchResponse`, `LfsObject`, `LfsActions`, `LfsAction`
- `LfsPartInfo`, `LfsObjectError`

Helper:
- `LfsTransfer` enum (AlreadyExists, Basic, Multipart)
- `into_transfer()` conversion

Tests: 11 unit tests

### Task 3.2: LFS Client ✅
**File:** `lfs/client.rs` (394 lines)

```rust
pub struct LfsClient {
    client: reqwest::Client,
    repo_location: HFRepoLocation,
    token: String,
}
```

Methods:
- `request_upload()` - single file
- `request_uploads()` - batch
- `verify_upload()`, `complete_multipart()`
- Smart rate limit retry (parses `RateLimit` header)

Tests: 5 unit tests

### Task 3.3: Upload Executor ✅
**File:** `lfs/upload.rs` (409 lines)

```rust
pub struct UploadExecutor {
    client: reqwest::Client,
}
```

- Basic upload: single PUT with retry (3 attempts, exponential backoff)
- Multipart: sequential part uploads with ETag capture
- `UploadProgress` trait for callbacks

Tests: 5 unit tests

### Task 3.4: Commit API Client ✅
**File:** `commit.rs` (879 lines)

Types:
- `CommitOperationAdd`, `CommitOperationDelete`, `CommitOperation`
- `CommitInfo` response
- NDJSON serialization types

```rust
pub struct CommitClient {
    client: reqwest::Client,
    repo_location: HFRepoLocation,
    token: String,
}
```

Methods:
- `build_ndjson_payload()` - constructs commit body
- `create_commit()` - async with rate limit retry
- Supports `create_pr=true`

Tests: 22 unit tests

---

## Phase 4: Streaming Integration (Complete)

### Task 4.1-4.3: HfSinkNode Core ✅
**File:** `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs`

- HfSinkNode skeleton with SinkNode trait implementation
- Shard Writer Task with `buffer_and_write_task()` and `upload_shard_task()`
- `finalize()` for atomic commit coordination

### Task 4.4: IOSinkNode Integration ✅

#### 4.4.1 PhysNodeKind::HfSink variant
```rust
// crates/polars-stream/src/physical_plan/mod.rs
pub enum PhysNodeKind {
    // ...
    HfSink { input: PhysNodeKey },
}
```

#### 4.4.2 URL detection in lower_ir.rs
```rust
// crates/polars-stream/src/physical_plan/lower_ir.rs
SinkTypeIR::File(options) => {
    let options = options.clone();
    let input = lower_ir!(*input)?;

    #[cfg(feature = "hf_sink")]
    {
        if options.target.cloud_scheme() == Some(CloudScheme::Hf) {
            PhysNodeKind::HfSink { input, options }
        } else {
            PhysNodeKind::FileSink { input, options }
        }
    }

    #[cfg(not(feature = "hf_sink"))]
    PhysNodeKind::FileSink { input, options }
},
```

#### 4.4.3 Graph conversion in to_graph.rs
```rust
// crates/polars-stream/src/physical_plan/to_graph.rs
#[cfg(feature = "hf_sink")]
HfSink { input, options } => {
    use polars_io::cloud::hf::HfSinkOptions;
    use polars_plan::dsl::{SinkOptions, SinkTarget};

    let FileSinkOptions { target, unified_sink_args, .. } = options;
    let input_schema = ctx.phys_sm[input.node].output_schema.clone();
    let input_key = to_graph_rec(input.node, ctx)?;

    let url = match target {
        SinkTarget::Path(path) => path.as_str(),
        SinkTarget::Dyn(_) => polars_bail!(ComputeError: "HF sink does not support dynamic targets"),
    };

    let hf_options = HfSinkOptions::from_url(url)?;
    let sink_options = SinkOptions {
        sync_on_close: unified_sink_args.sync_on_close,
        maintain_order: unified_sink_args.maintain_order,
        mkdir: unified_sink_args.mkdir,
    };

    let node = HfSinkNode::new(hf_options, input_schema, sink_options)?;
    ctx.graph.add_node(SinkComputeNode::from(node), [(input_key, input.port)])
}
```

#### 4.4.4 HfSinkOptions::from_url()
**File:** `crates/polars-io/src/cloud/hf/options.rs`
```rust
pub fn from_url(url: &str) -> PolarsResult<Self> {
    let parts = super::url::HFPathParts::try_from_uri(url)?;
    HfSinkOptions::builder(&parts.repository)
        .with_repo_type(parts.repo_type())
        .with_revision(parts.revision)
        .with_path_in_repo(parts.path)
        .build()
}
```

#### 4.4.5 Integration Tests
**File:** `crates/polars-stream/src/physical_plan/lower_ir.rs`
```rust
#[cfg(all(test, feature = "hf_sink", feature = "parquet"))]
mod hf_sink_integration_tests {
    fn test_hf_url_creates_hf_sink_node();      // hf:// → HfSink
    fn test_non_hf_url_creates_file_sink_node(); // local → FileSink
    fn test_hf_url_with_revision();             // @revision syntax
    fn test_hf_url_spaces_repo_type();          // hf://spaces/
    fn test_hf_url_options_parsing();           // HfSinkOptions::from_url()
    fn test_invalid_hf_url_errors();            // Error handling
}
```

**Run tests:** `cargo test -p polars-stream --features hf_sink,parquet hf_sink_integration_tests`

---

## Phase 5: Commit Coordination (Complete)

### Task 5.1: Mode Handling ✅
**Files:**
- `crates/polars-io/src/cloud/hf/api.rs` - `list_existing_files()`
- `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs` - mode logic in `finalize()`
- `crates/polars-io/src/cloud/hf/options.rs` - `HfWriteMode` enum

**Implemented:**
- [x] **5.1.1** `list_existing_files()` helper in api.rs
- [x] **5.1.2** ErrorIfExists mode check in finalize()
- [x] **5.1.3** Overwrite mode (delete + add) in finalize()
- [x] **5.1.4** Append mode (renumber shards) in finalize()
- [x] **5.1.5** 6 integration tests for all modes

### Task 5.2: Dataset Card Updates ✅
**File:** `crates/polars-io/src/cloud/hf/dataset_card.rs`

**5.2.1** Added `update_card` option to HfSinkOptions (default: true)

**5.2.2** Regular file support in commit.rs:
- Refactored `CommitOperationAdd` to enum with `Lfs` and `Regular` variants
- Added `NdjsonRegularFile` for base64-encoded files
- Added `base64` dependency to polars-io
- 6 new tests

**5.2.3** dataset_card.rs module:
- `SplitInfo` struct (name, num_bytes, num_examples)
- `DatasetInfo` struct with `update_split()`, `update_splits()` methods
- `ExtractedFrontmatter` for YAML parsing
- `extract_frontmatter()` function
- `generate_updated_readme()`, `generate_new_readme()` functions
- `CardYaml` with `#[serde(flatten)]` to preserve unknown fields
- 22+ unit tests

**5.2.4** `fetch_readme()` in api.rs - handles 404 gracefully

**5.2.5** mod.rs exports complete

**5.2.6** Integration in `HfSinkNode::finalize()`:
- Fetches existing README
- Parses/updates DatasetInfo
- Includes README as regular file in atomic commit

**5.2.7** Integration tests (6 tests):
- `build_readme_operation()` helper function
- Tests: update_card=true/false, missing README, no frontmatter, preserve splits, preserve YAML fields

---

## Development Workflow Reference

### Build Commands
```bash
# Debug build
maturin develop

# Release build
maturin develop --release

# With features
maturin develop --release --features "hf_sink"
```

### Test Commands
```bash
# Rust tests
cargo test -p polars-io hf --features hf_sink

# Python tests
pytest tests/unit/io/test_hf_sink.py -v

# E2E tests (requires token)
HF_TOKEN=hf_xxx pytest -v -m "requires_hf_token"
```

### Git Workflow
```bash
git status
git log --oneline -5
git add -A
git commit -m "feat(hf-sink): <description>"
git push origin feature/hf-hub-sink
```

---

## Known Issues (Resolved)

### Upstream Feature-Gate Bugs (Fixed 2026-01-15)
- `GroupsIndicator` used without `algorithm_group_by` gate
- `ChunkUnique::unique_id` missing feature gate
- Fixed in commit `5b774a6324`
- 9 files modified with minimal `#[cfg(...)]` additions
