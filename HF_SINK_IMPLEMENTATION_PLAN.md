# HF Hub Sink Implementation Plan

Native HF Hub write support for Polars via `sink_parquet("hf://datasets/user/repo/...")`.

**Archive:** Detailed completed work in [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md)

---

## Current Status (2026-01-29)

```
✅ Phases 0-7 complete (Foundation → Python Bindings)
✅ Phase 7 complete - Python Bindings (sink_parquet, write_parquet)
✅ Task 8.2.1 complete - Add wiremock mock HTTP infrastructure
```

**Build Status:**
```bash
✅ cargo check -p polars-io --features hf_sink     # PASSES
✅ cargo check -p polars-stream --features hf_sink # PASSES
✅ cargo check -p polars-python                    # PASSES
✅ cargo test -p polars-io checkpoint --features hf_sink  # 10 checkpoint tests pass
✅ cargo test -p polars-io hf_token --features hf_sink,http  # 4 token extraction tests pass
✅ cargo test -p polars-stream --features hf_sink hf_sink # 69 tests pass (68 + 1 mock)
✅ cargo test -p polars-io apply_key_value --features hf_sink  # 12 apply_key_value tests pass
```

**Branch:** `feature/hf-hub-sink` (246 commits ahead of main)

---

## What's Next

### Phase 8: Testing (Current Priority)

**Next Task:** 8.2.2 - Add base URL injection to HFRepoLocation for mock testing

**Task 8.2.1: Mock HTTP Infrastructure** ✅ Complete
- Added `wiremock = "0.6"` to polars-stream dev-dependencies
- Created `mock_tests.rs` with skeleton test verifying wiremock setup
- Test passes: `cargo test -p polars-stream --features hf_sink mock_server_setup`

**Task 7.1.1: Wire HF Token** ✅ Complete
- Added `CloudOptions::hf_token()` helper method in `cloud/options.rs`
- Updated `to_graph.rs` to extract token from `cloud_options` and pass to `HfSinkOptions`
- 4 unit tests for token extraction, 68 HF sink tests pass

**Task 7.1.2d: Add hf_options to UnifiedSinkArgs** ✅ Complete
- Added `hf_options: Option<Vec<(String, String)>>` field to `UnifiedSinkArgs` struct
- Updated Default impl with `hf_options: None`
- Updated pattern matches in 3 files: lower_ir.rs, partition_by.rs, single_file.rs
- Updated struct construction in sink_options.rs

### Phase 6: Advanced Features ✅ Complete

**Task 6.1: Checkpoint System** ✅ Complete
**Task 6.2: Partitioned Write Support** ✅ Complete (24/24 subtasks)
**Task 6.2.S: Smoke Test** ✅ Complete (real HF Hub push validated)
**Task 6.3: Progress Reporting** ✅ Complete

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

## Phases 4-6: Complete (Details in Archive)

See [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md) for full implementation details.

| Phase | Tasks | Summary |
|-------|-------|---------|
| **4: Streaming** | 4.1-4.4 | HfSinkNode, IOSinkNode integration, URL detection |
| **5: Coordination** | 5.1-5.2 | Write modes (Error/Overwrite/Append), README generation |
| **6: Advanced** | 6.1-6.3 | Checkpoint resume, partitioned writes, progress callbacks |

**Test counts:** 10 checkpoint tests, 68 HF sink tests, 4 token tests

---

| Task | Description | Status |
|------|-------------|--------|
| **6.1 Checkpoint** | Resume interrupted uploads via local JSON file | ✅ 10 subtasks, 10 tests |
| **6.2.S Smoke Test** | Validated real HF Hub push end-to-end | ✅ [Commit](https://huggingface.co/datasets/davanstrien/test-polars-streaming/commit/867df2b12bd19a81fa2b7aba861bf92726283cf0) |
| **6.2 Partitioned** | Hive-style `col=value/` directories | ✅ 24 subtasks complete |
| **6.3 Progress** | Upload callbacks for progress bars | ✅ 13 subtasks, 5 tests |

See [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md) for full implementation details.

---

## Phase 7: Python Bindings

### Task 7.1: Wire Python Options to HfSinkOptions ✅ Complete

#### Overview
Enable `storage_options` and HF-specific options to flow from Python through to `HfSinkOptions`.

#### Subtasks
| Subtask | Description | Status |
|---------|-------------|--------|
| **7.1.1** | Wire HF token from CloudOptions to HfSinkOptions in to_graph.rs | ✅ Complete |
| **7.1.2** | Add `hf_options` parameter for HF-specific options | ✅ Complete |
|   7.1.2d | Add `hf_options` field to `UnifiedSinkArgs` struct | ✅ Complete |
|   7.1.2a | Add hf_options to Python `_SinkOptions` dataclass | ✅ Complete |
|   7.1.2b | Add hf_options param to `sink_parquet` | ✅ Complete |
|   7.1.2c | Extract hf_options in Rust PyO3 bridge | ✅ Complete |
|   7.1.2e | Wire hf_options to HfSinkOptions in to_graph.rs | ✅ Complete |
|   7.1.2f | Add unit tests (12 tests for apply_key_value_options) | ✅ Complete |
|   7.1.2g | Update implementation plan | ✅ Complete |
| **7.1.3** | Update sink_parquet docstrings to document HF support | ✅ Complete |

#### Task 7.1.1: Wire HF Token
**Goal:** Enable `storage_options={"token": "hf_xxx"}` to work with `sink_parquet("hf://...")`

**Files to Modify:**
- `crates/polars-io/src/cloud/options.rs` - Add `CloudOptions::hf_token()` helper
- `crates/polars-stream/src/physical_plan/to_graph.rs` - Extract token and pass to HfSinkOptions

**Current Gap:** `unified_sink_args.cloud_options` contains the token but isn't being passed to `HfSinkOptions`.

### Task 7.2: sink_parquet Integration ✅ Complete
- Detect `hf://` prefix (done in lower_ir.rs)
- Pass options to HfSinkNode (7.1.1 handles token, 7.1.2 handles hf_options)
- Full flow: Python → PyO3 bridge → lower_ir.rs → to_graph.rs → HfSinkNode

### Task 7.3: write_parquet Integration ✅ Complete
- Added `hf_options` parameter to `write_parquet()` in dataframe/frame.py
- Parameter passed through to `sink_parquet()` (which already supports HF)
- Added PyArrow + hf:// validation (raises ValueError)
- Added docstring documentation and HF Hub example

---

## Phase 8: Testing

### Task 8.1: Unit Tests [ ]
Ongoing - included in component work (114 tests across modules)

### Task 8.2: Integration Tests (Mock)

Mock HTTP integration tests using wiremock to test the full upload pipeline.

| Subtask | Description | Status |
|---------|-------------|--------|
| **8.2.1** | Add wiremock dev-dependency + skeleton test file | ✅ Complete |
| 8.2.2 | Add base URL injection to HFRepoLocation | [ ] |
| 8.2.3 | Create reusable mock fixtures (LFS, commit, tree APIs) | [ ] |
| 8.2.4 | Implement test_single_shard_upload | [ ] |
| 8.2.5 | Implement test_multi_shard_upload | [ ] |
| 8.2.6 | Implement test_overwrite_mode | [ ] |

**Files added:**
- `crates/polars-stream/Cargo.toml` - Added `wiremock = "0.6"` dev-dependency
- `crates/polars-stream/src/nodes/io_sinks/hf_sink/mock_tests.rs` - Mock test infrastructure

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
- [x] **Phase 6:** Advanced Features ✅ Complete
  - [x] Task 6.1: Checkpoint System ✅
  - [x] Task 6.2.S: Smoke Test (Real HF Hub Push) ✅
  - [x] Task 6.2: Partitioned Write Support ✅
  - [x] Task 6.3: Progress Reporting ✅
- [x] **Phase 7:** Python Bindings ✅ Complete
  - [x] Task 7.1: Wire Python Options to HfSinkOptions ✅
  - [x] Task 7.2: sink_parquet Integration ✅
  - [x] Task 7.3: write_parquet Integration ✅
- [ ] **Phase 8:** Testing (1/4 subtasks) ← Current Priority
  - [x] Task 8.2.1: Mock HTTP infrastructure ✅
  - [ ] Task 8.2.2-8.2.6: Remaining mock integration tests
  - [ ] Task 8.3: E2E Tests (Real HF)
  - [ ] Task 8.4: Performance Benchmarks
- [ ] **Phase 9:** Documentation (0/4)

**Status:** 7/9 phases complete. Mock testing infrastructure added.

**Next:** Task 8.2.2 - Add base URL injection to HFRepoLocation for mock testing

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
