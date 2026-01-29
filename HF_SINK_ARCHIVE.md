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

## Phase 6: Advanced Features (Complete)

### Task 6.1: Checkpoint System ✅ COMPLETE

#### Subtasks
| Subtask | Description | Status |
|---------|-------------|--------|
| **6.1.1** | Create `checkpoint.rs` with struct definitions | ✅ Complete |
| **6.1.2** | Implement `new()`, `add_shard()`, `completed_indices()` | ✅ Complete |
| **6.1.3** | Implement `save()`, `load()`, `delete()` with atomic writes | ✅ Complete |
| **6.1.4** | Export checkpoint module from `mod.rs` | ✅ Complete |
| **6.1.5** | Unit tests for checkpoint.rs (6 tests) | ✅ Complete |
| **6.1.6** | Integration: Load checkpoint on startup | ✅ Complete |
| **6.1.7** | Integration: Skip completed shards in buffer_and_write_task | ✅ Complete |
| **6.1.8** | Integration: Save checkpoint after each upload | ✅ Complete |
| **6.1.9** | Integration: Delete checkpoint on success | ✅ Complete |
| **6.1.10** | Integration tests (5 tests) | ✅ Complete |

#### Overview
Enable resume-on-failure by persisting upload state to a local JSON checkpoint file. If a write fails mid-stream, users can resume without re-uploading already-completed shards.

#### Existing Infrastructure
- **`HfSinkOptions.checkpoint_path: Option<PathBuf>`** - Already exists in `options.rs:122-123`
- **`ShardCompletion`** struct in `hf_sink/mod.rs:84-95` - Contains all needed shard metadata:
  ```rust
  pub struct ShardCompletion {
      pub index: usize,           // Shard index (0, 1, 2, ...)
      pub path_in_repo: String,   // "data/train-00000.parquet"
      pub sha256: String,         // Lowercase hex, 64 chars
      pub size: u64,              // Bytes
      pub num_rows: usize,        // Row count
  }
  ```

#### Files to Create/Modify
| File | Action | Purpose |
|------|--------|---------|
| `crates/polars-io/src/cloud/hf/checkpoint.rs` | Create | CheckpointState struct, save/load logic |
| `crates/polars-io/src/cloud/hf/mod.rs` | Modify | Export checkpoint module |
| `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs` | Modify | Integrate into upload_shard_task() and finalize() |

#### CheckpointState Definition
```rust
// crates/polars-io/src/cloud/hf/checkpoint.rs
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

/// Checkpoint format version for forward compatibility
const CHECKPOINT_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// Format version (for future migrations)
    pub version: u32,

    /// Repository identifier for validation on resume
    pub repo_id: String,

    /// Path in repo being written to
    pub path_in_repo: String,

    /// Shards that have been uploaded to LFS (index → completion data)
    pub completed_shards: Vec<ShardCheckpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardCheckpoint {
    pub index: usize,
    pub path_in_repo: String,
    pub sha256: String,
    pub size: u64,
    pub num_rows: usize,
}

impl CheckpointState {
    pub fn new(repo_id: &str, path_in_repo: &str) -> Self { ... }
    pub fn add_shard(&mut self, completion: &ShardCompletion) { ... }
    pub fn completed_indices(&self) -> HashSet<usize> { ... }
    pub fn save(&self, path: &Path) -> PolarsResult<()> { ... }
    pub fn load(path: &Path) -> PolarsResult<Option<Self>> { ... }
    pub fn delete(path: &Path) -> PolarsResult<()> { ... }
}
```

#### Integration Points in hf_sink/mod.rs

**1. During upload (upload_shard_task, ~line 600):**
```rust
// After successful LFS upload, before sending to completion channel:
if let Some(ref checkpoint_path) = options.checkpoint_path {
    // Load existing checkpoint or create new
    let mut checkpoint = CheckpointState::load(checkpoint_path)?
        .unwrap_or_else(|| CheckpointState::new(&options.repo_id, &options.path_in_repo));

    // Add this shard
    checkpoint.add_shard(&completion);

    // Save atomically (write to .tmp, rename)
    checkpoint.save(checkpoint_path)?;
}
```

**2. On resume (initialize or new, ~line 700):**
```rust
// Load existing checkpoint if present
let resumed_shards: HashSet<usize> = if let Some(ref checkpoint_path) = options.checkpoint_path {
    if let Some(checkpoint) = CheckpointState::load(checkpoint_path)? {
        // Validate repo_id and path_in_repo match
        if checkpoint.repo_id != options.repo_id || checkpoint.path_in_repo != options.path_in_repo {
            polars_bail!(ComputeError: "Checkpoint mismatch: was for {}/{}, now {}/{}",
                checkpoint.repo_id, checkpoint.path_in_repo,
                options.repo_id, options.path_in_repo);
        }
        checkpoint.completed_indices()
    } else {
        HashSet::new()
    }
} else {
    HashSet::new()
};
```

**3. In buffer_and_write_task (~line 450):**
```rust
// Skip shard if already in checkpoint
if resumed_shards.contains(&shard_index) {
    if config::verbose() {
        eprintln!("HF sink: skipping shard {} (already uploaded)", shard_index);
    }
    continue;
}
```

**4. On success (finalize, ~line 900):**
```rust
// After successful commit, delete checkpoint
if let Some(ref checkpoint_path) = options.checkpoint_path {
    CheckpointState::delete(checkpoint_path)?;
}
```

#### Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage location | Local filesystem | Simpler than HF Hub; user controls path |
| Checkpoint scope | Single sink instance | No multi-process coordination needed |
| Atomic writes | Write .tmp + rename | Prevents corruption on crash |
| Validation | repo_id + path_in_repo | Prevents resuming wrong job |
| Versioning | version field | Future format migrations |

#### Concurrency Model
- **Single-writer assumption**: One HfSinkNode writes to checkpoint at a time
- **No file locking needed**: Polars streaming engine is single-threaded per sink
- **Thread-safe within node**: upload_shard_task runs sequentially per shard

#### Error Handling

| Scenario | Behavior |
|----------|----------|
| Checkpoint file missing | Start fresh (no resume) |
| Checkpoint parse error | Return error, user must delete manually |
| repo_id/path mismatch | Return error with clear message |
| Write failure mid-save | .tmp file left behind, original intact |
| Commit success | Delete checkpoint file |
| Commit failure | Keep checkpoint for next resume |

#### Acceptance Criteria

**Unit Tests (checkpoint.rs):**
1. `test_checkpoint_roundtrip` - save → load preserves all fields
2. `test_checkpoint_add_shard` - adds shard, updates indices
3. `test_checkpoint_completed_indices` - returns correct HashSet
4. `test_checkpoint_missing_file` - load returns None
5. `test_checkpoint_version_field` - version=1 in JSON
6. `test_checkpoint_atomic_save` - uses .tmp + rename

**Integration Tests (hf_sink/mod.rs):**
1. `test_checkpoint_created_during_upload` - checkpoint file created after first shard
2. `test_checkpoint_deleted_on_success` - file removed after commit
3. `test_checkpoint_resume_skips_shards` - resumed upload skips completed shards
4. `test_checkpoint_mismatch_error` - wrong repo_id fails with clear error
5. `test_no_checkpoint_when_path_none` - no file created if checkpoint_path=None

#### Example Checkpoint JSON
```json
{
  "version": 1,
  "repo_id": "username/my-dataset",
  "path_in_repo": "data/train",
  "completed_shards": [
    {
      "index": 0,
      "path_in_repo": "data/train-00000.parquet",
      "sha256": "abc123...",
      "size": 104857600,
      "num_rows": 500000
    },
    {
      "index": 1,
      "path_in_repo": "data/train-00001.parquet",
      "sha256": "def456...",
      "size": 104857600,
      "num_rows": 500000
    }
  ]
}
```

---

### Task 6.2.S: Smoke Test (Real HF Hub Push) ✅ COMPLETE

#### Overview
Validated the core write pipeline works end-to-end by pushing real Parquet data to HF Hub.

#### Test Repository
- **Repo:** `davanstrien/test-polars-streaming`
- **URL:** https://huggingface.co/datasets/davanstrien/test-polars-streaming

#### What Was Tested
- Created 100-row DataFrame with 3 columns (id, text, timestamp)
- Wrote to Parquet format using `ParquetWriter` + `MmapBuffer`
- Computed SHA256 hash of Parquet bytes
- Requested LFS upload URL via `LfsClient::request_upload()`
- Uploaded to S3 presigned URL via `UploadExecutor::upload()`
- Created atomic commit via `CommitClient::create_commit()`

#### Results
- **Parquet size:** 1,607 bytes
- **Transfer type:** Basic (small file, single PUT)
- **Commit:** https://huggingface.co/datasets/davanstrien/test-polars-streaming/commit/867df2b12bd19a81fa2b7aba861bf92726283cf0

#### Bug Found & Fixed
`CommitInfo.oid` field needed `#[serde(rename = "commitOid")]` - HF Hub API returns `commitOid` not `oid` in the JSON response.

#### Success Criteria (All Met)
- [x] No panics during execution
- [x] LFS upload succeeds (presigned URL works)
- [x] Commit API returns success
- [x] File appears on HF Hub with correct content

---

### Task 6.2: Partitioned Write Support ✅ COMPLETE

#### Overview
Support Hive-style partitioned writes: `data/{partition_col}={value}/train-00000.parquet`

**Scope:** Simple single-column partitioning only (covers most HF dataset use cases).

#### Subtasks
| Subtask | Description | Status |
|---------|-------------|--------|
| **6.2.1** | Add `partition_col` field to `HfSinkOptions` | ✅ Complete |
| **6.2.2** | Create `partitioned_shard_path()` helper function | ✅ Complete |
| **6.2.3** | Create `PartitionWriterState` struct | ✅ Complete |
| **6.2.4** | Implement partition extraction from DataFrame | ✅ Complete |
| **6.2.5** | Implement partitioned `buffer_and_write_task` | ✅ Complete (9/9 sub-tasks) |
|   6.2.5a | Extend `PartitionWriterState` with global shard counter | ✅ Complete |
|   6.2.5b | Add `ShardToUpload` struct for channel messages | ✅ Complete |
|   6.2.5c | Implement `partitioned_buffer_and_write_task()` function | ✅ Complete |
|   6.2.5c.1 | Function signature and initialization | ✅ Complete |
|   6.2.5c.2 | Main loop: morsel partitioning and buffer accumulation | ✅ Complete |
|   6.2.5c.3 | Per-partition batch writing to shards | ✅ Complete |
|   6.2.5c.4 | Per-partition shard rotation (finish, upload, reset) | ✅ Complete |
|   6.2.5c.5 | Final flush for all partitions at end of stream | ✅ Complete |
|   6.2.5d | Modify `spawn_sink()` to dispatch based on `partition_col` | ✅ Complete |
| **6.2.6** | Update `upload_shard_task` for partitioned paths | ✅ Complete (uses pre-computed paths) |
| **6.2.7** | Update `finalize()` for partitioned commits | ✅ Complete |
| **6.2.8** | Update checkpoint for partitioned writes | ✅ Complete |
|   6.2.8a | Add `partition_value` to `ShardCheckpoint` | ✅ Complete |
|   6.2.8b | Add `partition_col` to `CheckpointState` | ✅ Complete |
|   6.2.8c | Add `resumed_indices_for_partition()` API | ✅ Complete |
|   6.2.8d | Update `load_checkpoint_state()` return type | ✅ Complete |
|   6.2.8e | Update non-partitioned buffer task resume check | ✅ Complete |
|   6.2.8f | Update partitioned buffer task resume check | ✅ Complete |
|   6.2.8g | Update `upload_shard_task()` to save partition info | ✅ Complete |
|   6.2.8h | Add partition_col mismatch validation | ✅ Complete |
|   6.2.8i | Unit tests for partition-aware checkpoint | ✅ Complete |
|   6.2.8j | Integration tests for partitioned checkpoint | ✅ Complete |
| **6.2.9** | Wire partitioned path in `HfSinkNode::spawn_sink()` | ✅ Complete |
| **6.2.10** | Unit tests for partitioned paths and state | ✅ Complete |
| **6.2.11** | Integration tests for partitioned writes | ✅ Complete |
|   6.2.11a | Multi-partition progress callbacks test | ✅ Complete |
|   6.2.11b | PartitionWriterState finalize test (3 tests) | ✅ Complete |
|   6.2.11c | Checkpoint with partition_col deletion test | ✅ Complete |

#### Key Design Points
- **Single-column partitioning**: `partition_col: Option<String>` in options
- **Per-partition sharding**: Each partition value gets its own shard counter
- **Atomic commit**: All partitions committed together in single commit
- **Standalone implementation**: Logic contained in HfSinkNode, not io_sinks2

#### Files to Modify
| File | Change |
|------|--------|
| `cloud/hf/options.rs` | Add `partition_col` field ✅ |
| `hf_sink/mod.rs` | `PartitionWriterState`, partitioned buffer task, finalize updates |
| `cloud/hf/checkpoint.rs` | Add partition awareness |

#### Path Format
```
Non-partitioned: data/train-00000.parquet
Partitioned:     data/split=train/train-00000.parquet
                 data/split=test/test-00000.parquet
```

---

### Task 6.3: Progress Reporting ✅ COMPLETE

#### Subtasks
| Subtask | Description | Status |
|---------|-------------|--------|
| **6.3.1** | Add metrics field to HfSinkNode, store completions in finalize | ✅ Complete |
| **6.3.2** | Implement get_metrics() returning shard-level WriteMetrics | ✅ Complete |
| **6.3.3** | Add HfSinkProgress trait for higher-level callbacks | ✅ Complete |
| **6.3.4a** | Add progress field to HfSinkOptions (merged w/ 6.3.5) | ✅ Complete |
| **6.3.4b** | Wire on_shard_start callback in buffer_and_write_task | ✅ Complete |
| **6.3.4c** | Wire on_shard_complete callback in upload_shard_task | ✅ Complete |
| **6.3.4d** | Wire on_commit_start/complete callbacks in finalize | ✅ Complete |
| **6.3.4e** | Implement byte-level upload progress (ProgressBody + UploadExecutor) | ✅ Complete |
| **6.3.6a** | Create TestProgress helper struct for tests | ✅ Complete |
| **6.3.6b** | Test basic callback sequence (single shard) | ✅ Complete |
| **6.3.6c** | Test multiple shard progress tracking | ✅ Complete |
| **6.3.6d** | Test upload progress byte increments | ✅ Complete |
| **6.3.6e** | Test progress with checkpoint resume | ✅ Complete |

#### Overview
Add upload progress callbacks for user-facing progress bars and metrics collection.

Two aspects:
1. **get_metrics()** - Return WriteMetrics after upload (statistics for user)
2. **UploadProgress callbacks** - Real-time progress during upload (for progress bars)

#### Existing Infrastructure
- **`get_metrics()` stub** already exists in `hf_sink/mod.rs` - returns `Ok(None)`
- **`HfSinkProgress` trait** in `progress.rs` - now wired to `upload()` in `lfs/upload.rs`
- **`WriteMetrics` struct** in `metrics.rs` - expects column-level stats
- **`ShardCompletion`** data available in finalize() - path, size, num_rows, sha256

#### Key Design Points
```rust
// Extend existing trait or create new one
pub trait SinkProgress: Send + Sync {
    fn on_shard_start(&self, index: usize, path: &str);
    fn on_shard_upload_progress(&self, index: usize, bytes: u64, total: u64);
    fn on_shard_complete(&self, index: usize, path: &str);
    fn on_commit_start(&self, num_shards: usize);
    fn on_commit_complete(&self, commit_url: &str);
}
```

#### Files Modified/Created
| File | Change | Status |
|------|--------|--------|
| `cloud/hf/progress.rs` | NEW: HfSinkProgress trait, NoOpSinkProgress, SinkProgressRef | ✅ 6.3.3 |
| `cloud/hf/mod.rs` | Export progress module | ✅ 6.3.3 |
| `cloud/hf/options.rs` | Add `progress: Option<SinkProgressRef>` | ✅ 6.3.4a |
| `hf_sink/mod.rs` | Add `peek_next_index()`, wire `on_shard_start` in buffer_and_write_task | ✅ 6.3.4b |
| `hf_sink/mod.rs` | Wire `on_shard_complete` in upload_shard_task, `on_commit_*` in finalize | ✅ 6.3.4c / ✅ 6.3.4d |
| `cloud/hf/lfs/upload.rs` | Add shard_index+progress params to upload(), per-part progress | ✅ 6.3.4e |
| `hf_sink/mod.rs` (tests) | Add TestProgress helper struct and test | ✅ 6.3.6a |

#### Integration Points
1. **Shard start**: In `buffer_and_write_task()` when starting new shard ✅ Done (6.3.4b)
2. **Upload progress**: In `upload_shard_task()` during LFS upload ✅ Done (6.3.4e)
3. **Shard complete**: After upload in `upload_shard_task()` ✅ Done (6.3.4c)
4. **Commit**: In `finalize()` before and after commit API call ✅ Done (6.3.4d)

#### Acceptance Criteria
- `get_metrics()` returns `Some(WriteMetrics { ... })` with shard data
- Progress trait called at all documented points
- No performance regression when progress=None

---

## Known Issues (Resolved)

### Upstream Feature-Gate Bugs (Fixed 2026-01-15)
- `GroupsIndicator` used without `algorithm_group_by` gate
- `ChunkUnique::unique_id` missing feature gate
- Fixed in commit `5b774a6324`
- 9 files modified with minimal `#[cfg(...)]` additions
