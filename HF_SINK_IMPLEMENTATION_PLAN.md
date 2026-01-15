# HF Hub Sink Implementation Plan

Native HF Hub write support for Polars via `sink_parquet("hf://datasets/user/repo/...")`.

**Archive:** Detailed completed work in [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md)

---

## Current Status (2026-01-15)

```
✅ Phases 0-3 complete (Foundation, Core Writer, LFS Protocol)
✅ Phase 4 nearly complete (3/4 tasks - HfSinkNode compiles!)
🔄 Task 4.4: Wire HfSinkNode into PhysNodeKind (2/4 subtasks done)
```

**Build Status:**
```bash
✅ cargo check -p polars-io --features hf_sink     # PASSES
✅ cargo check -p polars-stream --features hf_sink # PASSES
```

**Branch:** `feature/hf-hub-sink` (41 commits ahead)

---

## What's Next

### Task 4.4: IOSinkNode Integration
Wire `sink_parquet("hf://...")` to create `HfSinkNode`.

**Subtasks:**
- [x] 4.4.1: Add `PhysNodeKind::HfSink` variant
- [x] 4.4.2: Add URL detection in `lower_ir.rs`
- [ ] 4.4.3: Add graph conversion in `to_graph.rs`
- [ ] 4.4.4: End-to-end integration test

**Key Files:**
- `crates/polars-stream/src/physical_plan/mod.rs` - PhysNodeKind
- `crates/polars-stream/src/physical_plan/lower_ir.rs` - URL detection
- `crates/polars-stream/src/physical_plan/lower_sink.rs` - Graph conversion

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

### Task 4.4: IOSinkNode Integration [ ]
**Status:** In progress (2/4 subtasks)

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

#### 4.4.3 [ ] Add graph conversion in lower_sink.rs
```rust
PhysNodeKind::HfSink { input } => {
    let hf_options = HfSinkOptions::from_file_sink_options(&options)?;
    let node = HfSinkNode::new(hf_options, input_schema, sink_options)?;
    ctx.graph.add_node(SinkComputeNode::from(node), [(input_key, input.port)])
}
```

#### 4.4.4 [ ] End-to-end integration test
- Verify `sink_parquet("hf://...")` creates HfSinkNode
- Test URL parsing, options passing

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
**File:** `hf_sink/dataset_card.rs`

- Update README.md YAML metadata
- Add split info (rows, bytes)
- Preserve existing content

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
- [ ] **Phase 4:** Streaming Integration (3/4) ← **Current**
- [ ] **Phase 5:** Commit Coordination (0/2)
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
