# HF Hub Sink Implementation Plan

Native HF Hub write support for Polars via `sink_parquet("hf://datasets/user/repo/...")`.

**Archive:** Detailed completed work in [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md)

---

## Current Status (2026-01-17)

```
✅ Phases 0-5 complete (Foundation, Core Writer, LFS Protocol, Streaming, Coordination)
🔄 Phase 6 in progress: Task 6.1 (Checkpoint) - subtasks 6.1.1-6.1.3 complete
```

**Build Status:**
```bash
✅ cargo check -p polars-io --features hf_sink     # PASSES
✅ cargo check -p polars-stream --features hf_sink # PASSES
✅ cargo test -p polars-stream --features hf_sink hf_sink  # 27 tests pass
```

**Branch:** `feature/hf-hub-sink` (145 commits ahead of main, local only)

---

## What's Next

### Phase 6: Advanced Features (Current Priority)

**Task 6.1: Checkpoint System** ← Start Here
- **File:** `cloud/hf/checkpoint.rs` (new)
- Persist checkpoint state to JSON for resume on failure
- Skip already-uploaded shards on resume
- Delete checkpoint on successful commit

**Task 6.2: Partitioned Write Support**
- Hive-style paths: `data/{col}={val}/train-00000.parquet`
- Per-partition shard limits
- Atomic commit across partitions

**Task 6.3: Progress Reporting**
- `UploadProgress` trait for callbacks
- Note: `get_metrics()` stub exists in hf_sink/mod.rs

### Phase 7: Python Bindings (After Phase 6)

Expose `sink_parquet("hf://...")` to users:
- **File:** `py-polars/src/cloud/hf.rs` (new)
- PyO3 bindings for HfSinkOptions
- Detect `hf://` prefix in sink_parquet
- Pass storage_options for auth

---

## Architecture

```
User: lf.sink_parquet("hf://datasets/user/repo/data/train.parquet")
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    HfSinkNode (Streaming Engine)                 │
│         Receives morsels, writes shards, uploads to HF Hub       │
└─────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Shard Writer: ParquetEncoder → HashingWriter → MmapBuffer      │
│  On shard full: LFS upload → notify coordinator                  │
└─────────────────────────────────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Atomic Commit via CommitClient                │
│     Collects all shards, single commit to HF Hub                 │
└─────────────────────────────────────────────────────────────────┘
```

**Memory:** O(shard_size) per worker (~500MB default)
**Disk:** O(shard_size) temp file per shard

---

## Completed Phases (Summary)

| Phase | Description | Key Files | Status |
|-------|-------------|-----------|--------|
| **0: Dev Setup** | Branch, build, tests | - | ✅ Complete |
| **1: Foundation** | Options, URL, auth | `cloud/hf/{options,url,auth}.rs` | ✅ Complete |
| **2: Core Writer** | Hash+buffer+parquet | `{hashing_writer,mmap_buffer,shard_writer}.rs` | ✅ Complete |
| **3: LFS Protocol** | Upload + commit APIs | `lfs/{types,client,upload}.rs`, `commit.rs` | ✅ Complete |
| **4: Streaming** | HfSinkNode integration | `hf_sink/mod.rs`, `to_graph.rs` | ✅ Complete |
| **5: Coordination** | Mode handling, README | `api.rs`, `dataset_card.rs` | ✅ Complete |

See [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md) for implementation details.

---

## Phase 4: Streaming Integration ✅ COMPLETE

HfSinkNode (4.1-4.3) and IOSinkNode integration (4.4) fully implemented.
- SinkNode trait implementation with `buffer_and_write_task()`, `upload_shard_task()`
- PhysNodeKind::HfSink variant with URL detection in lower_ir.rs
- Graph conversion in to_graph.rs with `HfSinkOptions::from_url()`
- 6 integration tests in lower_ir.rs

**Key Files:** `hf_sink/mod.rs`, `physical_plan/{mod,lower_ir,to_graph}.rs`, `options.rs`

**Tests:** `cargo test -p polars-stream --features hf_sink,parquet hf_sink_integration_tests`

---

## Phase 5: Commit Coordination ✅ COMPLETE

### Task 5.1: Mode Handling ✅ COMPLETE
ErrorIfExists, Overwrite (delete + add), and Append (renumber shards) modes in `finalize()`.
6 integration tests for all modes.

**Key Files:** `api.rs` (list_existing_files), `hf_sink/mod.rs` (finalize), `options.rs` (HfWriteMode)

### Task 5.2: Dataset Card Updates ✅ COMPLETE
README.md generation with YAML frontmatter (SplitInfo, DatasetInfo). All 7 subtasks complete.
- `update_card` option, regular file support in commit.rs (base64)
- `dataset_card.rs`: extract_frontmatter(), generate_updated_readme()
- `fetch_readme()` in api.rs, integration in finalize()
- 6 integration tests for README scenarios

**Key Files:** `dataset_card.rs`, `api.rs`, `commit.rs`, `hf_sink/mod.rs`

---

## Phase 6: Advanced Features

### Task 6.1: Checkpoint System [IN PROGRESS]

#### Subtasks
| Subtask | Description | Status |
|---------|-------------|--------|
| **6.1.1** | Create `checkpoint.rs` with struct definitions | ✅ Complete |
| **6.1.2** | Implement `new()`, `add_shard()`, `completed_indices()` | ✅ Complete |
| **6.1.3** | Implement `save()`, `load()`, `delete()` with atomic writes | ✅ Complete |
| 6.1.4 | Export checkpoint module from `mod.rs` | Pending |
| 6.1.5 | Unit tests for checkpoint.rs (6 tests) | Pending |
| 6.1.6 | Integration: Load checkpoint on startup | Pending |
| 6.1.7 | Integration: Skip completed shards in buffer_and_write_task | Pending |
| 6.1.8 | Integration: Save checkpoint after each upload | Pending |
| 6.1.9 | Integration: Delete checkpoint on success | Pending |
| 6.1.10 | Integration tests (5 tests) | Pending |

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

**Manual E2E Test:**
```bash
# 1. Start large upload, kill mid-way
# 2. Verify checkpoint file exists with partial shards
# 3. Resume same command
# 4. Verify skipped shards in verbose output
# 5. Verify checkpoint deleted after success
```

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

### Task 6.2: Partitioned Write Support [ ]

#### Overview
Support Hive-style partitioned writes: `data/{col}={val}/train-00000.parquet`

#### Key Design Points
- **Partition detection**: Parse partition columns from path template
- **Per-partition sharding**: Each partition gets its own shard counter
- **Atomic commit**: All partitions committed together in single commit
- **Integration**: Extend `HfSinkOptions` with `partition_cols: Vec<String>`

#### Files to Modify
| File | Change |
|------|--------|
| `options.rs` | Add `partition_cols` field |
| `hf_sink/mod.rs` | Route rows to partition-specific writers |
| `shard_writer.rs` | Support dynamic path based on partition values |

#### Complexity Note
This is the most complex Phase 6 task. Consider implementing after 6.1 and 6.3.

---

### Task 6.3: Progress Reporting [ ]

#### Overview
Add upload progress callbacks for user-facing progress bars.

#### Existing Infrastructure
- **`get_metrics()` stub** already exists in `hf_sink/mod.rs` - returns `Ok(None)`
- **`UploadProgress` trait** already defined in `lfs/upload.rs`

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

#### Files to Modify
| File | Change |
|------|--------|
| `options.rs` | Add `progress: Option<Arc<dyn SinkProgress>>` |
| `hf_sink/mod.rs` | Call progress methods at appropriate points |
| `lfs/upload.rs` | Wire existing `UploadProgress` to new trait |

#### Integration Points
1. **Shard start**: In `buffer_and_write_task()` when starting new shard
2. **Upload progress**: In `upload_shard_task()` during LFS upload
3. **Shard complete**: After sending to completion channel
4. **Commit**: In `finalize()` before and after commit API call

#### Acceptance Criteria
- `get_metrics()` returns `Some(SinkMetrics { shards_written, bytes_uploaded, ... })`
- Progress trait called at all documented points
- No performance regression when progress=None

---

## Phase 7: Python Bindings

### Task 7.1: PyO3 Bindings [ ]
**File:** `py-polars/src/cloud/hf.rs`

```python
lf.sink_parquet(
    "hf://datasets/user/repo/data/train.parquet",
    hf_options={"split": "train", "max_shard_size": "500MB"},
    storage_options={"token": "hf_xxx"},
)
```

### Task 7.2: sink_parquet Integration [ ]
- Detect `hf://` prefix
- Pass options to HfSinkNode

### Task 7.3: write_parquet Integration [ ]
- `df.write_parquet("hf://...")` convenience

---

## Phase 8: Testing

### Task 8.1: Unit Tests [ ]
Ongoing - included in component work

### Task 8.2: Integration Tests (Mock) [ ]
```rust
#[tokio::test]
async fn test_single_shard_upload() { ... }
async fn test_multi_shard_upload() { ... }
async fn test_overwrite_mode() { ... }
```

### Task 8.3: E2E Tests (Real HF) [ ]
```python
@pytest.mark.requires_hf_token
def test_streaming_upload(hf_test_repo):
    df.write_parquet(f"hf://datasets/{hf_test_repo}/data/test.parquet")
```

### Task 8.4: Performance Benchmarks [ ]
- HashingWriter overhead < 5%
- Full pipeline saturates network

---

## Phase 9: Documentation

### Task 9.1: User Guide [ ]
### Task 9.2: API Reference [ ]
### Task 9.3: Error Message Polish [ ]
### Task 9.4: Release Notes [ ]

---

## Progress Checklist

- [x] **Phases 0-5:** Foundation through Commit Coordination ✅
- [ ] **Phase 6:** Advanced Features (0/3) ← Current Priority
- [ ] **Phase 7:** Python Bindings (0/3) ← After Phase 6
- [ ] **Phase 8:** Testing (0/4)
- [ ] **Phase 9:** Documentation (0/4)

**Status:** 6/9 phases complete. Rust implementation functional.
**Next:** Phase 6.1 (Checkpoint System) for production reliability.

---

## Quick Reference

### Build
```bash
cargo check -p polars-io --features hf_sink
cargo check -p polars-stream --features hf_sink
maturin develop --release
```

### Test
```bash
cargo test -p polars-io hf --features hf_sink
```

### Key Paths
```
crates/polars-io/src/cloud/hf/           # Core HF types
crates/polars-stream/src/nodes/io_sinks/hf_sink/  # Streaming node
```

---

## Risk Assessment

| Risk | Impact | Mitigation |
|------|--------|------------|
| HF API changes | High | Pin to known API version |
| Streaming integration | High | Early POC (done) |
| Memory issues | Medium | Configurable shard size |

---

## Dependencies

```
Phase 0 → Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7
```

**Current Path:** Phase 6 (Checkpoint, Partitioned Writes, Metrics) → Phase 7 (Python API)
