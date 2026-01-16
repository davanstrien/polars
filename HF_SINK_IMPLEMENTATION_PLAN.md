# HF Hub Sink Implementation Plan

Native HF Hub write support for Polars via `sink_parquet("hf://datasets/user/repo/...")`.

**Archive:** Detailed completed work in [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md)

---

## Current Status (2026-01-16)

```
✅ Phases 0-4 complete (Foundation, Core Writer, LFS Protocol, Streaming Integration)
✅ Task 5.1 complete: Mode Handling (ErrorIfExists, Overwrite, Append) with tests
🔄 Phase 5: Commit Coordination - Task 5.2 in progress (Dataset Card Updates)
  ✅ Task 5.2.1: Add update_card option to HfSinkOptions
  ✅ Task 5.2.2: Add regular file support to commit.rs (NdjsonFile, base64)
  ✅ Task 5.2.3.1: Add serde_yaml dependency to polars-io
  ✅ Task 5.2.3.2: Create dataset_card.rs with SplitInfo struct
  ✅ Task 5.2.3.3: Add YAML frontmatter extraction (parse README)
  ✅ Task 5.2.3.4: Add dataset_info update logic (update_split, update_splits)
  ✅ Task 5.2.3.5: Add generate_updated_readme() function
  ✅ Task 5.2.4: Add fetch_readme() to api.rs
```

**Latest Commit:** `ae7efae28b feat(hf-sink): add fetch_readme() function (Task 5.2.4)`

**Build Status:**
```bash
✅ cargo check -p polars-io --features hf_sink     # PASSES
✅ cargo check -p polars-stream --features hf_sink # PASSES
✅ cargo test -p polars-stream --features hf_sink hf_sink  # 21 tests pass
```

**Branch:** `feature/hf-hub-sink` (72 commits ahead, local only)

---

## What's Next

### Task 5.2.5: Update mod.rs exports
**File:** `crates/polars-io/src/cloud/hf/mod.rs`

Ensure all dataset_card functions are properly exported for use in polars-stream.

### Task 5.1: Mode Handling for CommitCoordinator ✅ COMPLETE
The coordinator logic already exists in `HfSinkNode::finalize()` (lines 624-714).
Mode handling (ErrorIfExists, Overwrite, Append) is now fully implemented and tested.

**Key Files:**
- `crates/polars-io/src/cloud/hf/api.rs` - Shared API types (GetPages, list_existing_files)
- `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs` - finalize() where mode logic goes
- `crates/polars-io/src/cloud/hf/options.rs` - HfWriteMode enum

**Subtasks:**
- [x] **5.1.1** Add `list_existing_files()` helper in api.rs
- [x] **5.1.2** Implement ErrorIfExists mode check in finalize()
- [x] **5.1.3** Implement Overwrite mode (delete + add) in finalize()
- [x] **5.1.4** Implement Append mode (renumber shards) in finalize()
- [x] **5.1.5** Integration tests for all modes (6 tests added)

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

| Phase | Description | Files |
|-------|-------------|-------|
| **1: Foundation** | Options, URL parsing, auth | `cloud/hf/{options,url,auth}.rs` |
| **2: Core Writer** | Hash+buffer+parquet | `{hashing_writer,mmap_buffer,shard_writer}.rs` |
| **3: LFS Protocol** | Upload + commit APIs | `lfs/{types,client,upload}.rs`, `commit.rs` |

See [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md) for details.

---

## Phase 4: Streaming Integration (In Progress)

### Task 4.1-4.3 ✅ Complete
- HfSinkNode skeleton implemented
- Shard Writer Task with buffer_and_write_task() and upload_shard_task()
- finalize() for atomic commit

### Task 4.4: IOSinkNode Integration ✅
**Status:** Complete (4/4 subtasks)

#### 4.4.1 ✅ Add PhysNodeKind::HfSink variant
```rust
// crates/polars-stream/src/physical_plan/mod.rs
pub enum PhysNodeKind {
    // ...
    HfSink { input: PhysNodeKey },
}
```

#### 4.4.2 ✅ Add URL detection in lower_ir.rs
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

#### 4.4.3 ✅ Add graph conversion in to_graph.rs
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

Also added `HfSinkOptions::from_url()` in `crates/polars-io/src/cloud/hf/options.rs`:
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

#### 4.4.4 ✅ End-to-end integration test
**File:** `crates/polars-stream/src/physical_plan/lower_ir.rs` (test module at end)

```rust
#[cfg(all(test, feature = "hf_sink", feature = "parquet"))]
mod hf_sink_integration_tests {
    // Tests verify the IR → PhysNodeKind::HfSink pipeline
    fn test_hf_url_creates_hf_sink_node();      // hf:// → HfSink
    fn test_non_hf_url_creates_file_sink_node(); // local → FileSink
    fn test_hf_url_with_revision();             // @revision syntax
    fn test_hf_url_spaces_repo_type();          // hf://spaces/
    fn test_hf_url_options_parsing();           // HfSinkOptions::from_url()
    fn test_invalid_hf_url_errors();            // Error handling
}
```

**Run tests:**
```bash
cargo test -p polars-stream --features hf_sink,parquet hf_sink_integration_tests
```

---

## Phase 5: Commit Coordination

### Task 5.1: CommitCoordinator [ ]
**File:** `hf_sink/coordinator.rs`

```rust
pub struct CommitCoordinator {
    options: Arc<HfSinkOptions>,
    commit_client: CommitClient,
    additions: Vec<CommitOperationAdd>,
}

impl CommitCoordinator {
    pub fn register_completion(&mut self, completion: ShardCompletion);
    pub async fn execute_commit(&mut self) -> PolarsResult<CommitInfo>;
    async fn handle_overwrite(&self) -> PolarsResult<Vec<CommitOperationDelete>>;
}
```

**Acceptance:**
- [ ] Collects all shard completions
- [ ] Overwrite: deletes existing files first
- [ ] Append: renumbers shards
- [ ] ErrorIfExists: fails if files exist

### Task 5.2: Dataset Card Updates [ ]
**File:** `cloud/hf/dataset_card.rs`

- Update README.md YAML metadata
- Add split info (rows, bytes)
- Preserve existing content

**Subtasks:**
- [x] **5.2.1** Add `update_card` option to HfSinkOptions (default: true)
- [x] **5.2.2** Add regular file support to commit.rs (NdjsonFile, base64)
  - Added `base64` dependency to polars-io
  - Refactored `CommitOperationAdd` from struct to enum with `Lfs` and `Regular` variants
  - Added `NdjsonRegularFile` serialization type for base64-encoded files
  - Updated `build_ndjson_payload()` to handle both LFS and regular files
  - Added helper methods: `CommitOperationAdd::lfs()`, `CommitOperationAdd::regular()`, `path_in_repo()`
  - Added 6 new tests for regular file support
- [x] **5.2.3** Create dataset_card.rs module (YAML parsing, SplitInfo)
  - [x] **5.2.3.1** Add `serde_yaml` dependency to polars-io (version 0.9, optional, in hf_sink feature)
  - [x] **5.2.3.2** Create dataset_card.rs with SplitInfo struct
    - Added `SplitInfo` struct with `name`, `num_bytes`, `num_examples` fields
    - Derives: `Debug, Clone, PartialEq, Eq, Serialize, Deserialize`
    - Added `SplitInfo::new()` constructor
    - Exported from `mod.rs` under `hf_sink` feature
    - Added unit tests for basic functionality and serde round-trips
  - [x] **5.2.3.3** Add YAML frontmatter extraction (parse README)
    - Added `DatasetInfo` struct with `config_name`, `splits`, `download_size`, `dataset_size`
    - Added `ExtractedFrontmatter` struct for extracted YAML and body
    - Added `extract_frontmatter()` function to parse `---` delimited YAML
    - Exported new types from `mod.rs`
    - Added 10 unit tests for frontmatter extraction and DatasetInfo
  - [x] **5.2.3.4** Add dataset_info update logic
    - Added `DatasetInfo::recalculate_dataset_size()` private helper
    - Added `DatasetInfo::update_split()` - update or add single split by name
    - Added `DatasetInfo::update_splits()` - bulk update multiple splits
    - Auto-recalculates `dataset_size` after updates
    - Added 6 unit tests for update scenarios
  - [x] **5.2.3.5** Add generate_updated_readme() function
    - Added `CardYaml` internal struct with `#[serde(flatten)]` to preserve unknown YAML fields
    - Added `generate_updated_readme()` - reconstructs README with updated dataset_info
    - Added `generate_new_readme()` - creates minimal README for repos without one
    - Preserves all existing YAML fields (license, task_categories, etc.)
    - Added 6 unit tests including round-trip verification
  - [x] **5.2.3.6** Add unit tests (included in 5.2.3.5)
- [x] **5.2.4** Add fetch_readme() to api.rs
  - Added async `fetch_readme()` function to api.rs
  - Handles 404 (no README exists) → returns `Ok(None)`
  - Returns `Ok(Some(content))` on success
  - Uses same HTTP client pattern as `check_existing_files()`
  - Exported from mod.rs
- [ ] **5.2.5** Update mod.rs exports
- [ ] **5.2.6** Integrate into HfSinkNode::finalize()
- [ ] **5.2.7** Integration tests

---

## Phase 6: Advanced Features

### Task 6.1: Checkpoint System [ ]
**File:** `cloud/hf/checkpoint.rs`

```rust
pub struct CheckpointState {
    completed_shards: HashSet<usize>,
    pending_additions: Vec<SerializedAddition>,
}
```

- Persist to JSON for resume
- Skip already-uploaded shards
- Delete checkpoint on success

### Task 6.2: Partitioned Write Support [ ]
- Hive-style paths: `data/{col}={val}/train-00000.parquet`
- Per-partition shard limits
- Atomic commit across partitions

### Task 6.3: Progress Reporting [ ]
```rust
pub trait UploadProgress: Send + Sync {
    fn on_shard_progress(&self, index: usize, bytes: u64, total: u64);
    fn on_commit_complete(&self, info: &CommitInfo);
}
```

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

- [x] **Phase 0:** Dev Setup (5/6)
- [x] **Phase 1:** Foundation (4/4)
- [x] **Phase 2:** Core Writer (3/3)
- [x] **Phase 3:** LFS Protocol (4/4)
- [x] **Phase 4:** Streaming Integration (4/4)
- [ ] **Phase 5:** Commit Coordination (1/2) ← **Current** (5.1 Mode Handling complete)
- [ ] **Phase 6:** Advanced Features (0/3)
- [ ] **Phase 7:** Python Bindings (0/3)
- [ ] **Phase 8:** Testing (0/4)
- [ ] **Phase 9:** Documentation (0/4)

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
Phase 0 → Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 7
                                          ↓
                                       Phase 6
```

**Critical Path:** 4.4 → 5.1 → 7.2 (minimum for working Python API)
