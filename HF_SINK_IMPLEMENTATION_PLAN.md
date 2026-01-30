# HF Hub Sink Implementation Plan

Native HF Hub write support for Polars via `sink_parquet("hf://datasets/user/repo/...")`.

**Archive:** Detailed completed work in [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md)

---

## Current Status (2026-01-30)

```
✅ Phases 0-7 complete (Foundation → Python Bindings)
✅ Task 7.4: Feature Flag Wiring COMPLETE - Python E2E test PASSES!
✅ Task 8.2.P: Python E2E Smoke Test COMPLETE
✅ Task 8.2.3: Mock fixtures COMPLETE (15 tests: LFS, upload, commit, tree)
✅ Task 8.2.4: test_single_shard_upload COMPLETE (16 mock tests total)
🔄 Phase 8 in progress - Testing (8.2.5: test_multi_shard_upload next)
```

**Python E2E Test Result:** Successfully uploaded file to HF Hub:
- `hf://datasets/davanstrien/test-polars-streaming/data/python-e2e-7.4.5.parquet`
- LFS OID: `48100f9f117d2d6661d2ac21b0421e4111dcdc7713db07352c1d42808430dd39`

**Build Status:**
```bash
✅ cargo check -p polars-io --features hf_sink     # PASSES
✅ cargo check -p polars-stream --features hf_sink # PASSES
✅ cargo check -p polars-python                    # PASSES (hf_sink enabled via io feature)
✅ cargo test -p polars-io checkpoint --features hf_sink  # 10 checkpoint tests pass
✅ cargo test -p polars-io hf_token --features hf_sink,http  # 4 token extraction tests pass
✅ cargo test -p polars-stream --features hf_sink hf_sink # 85 tests pass (69 + 16 mock)
✅ cargo test -p polars-io apply_key_value --features hf_sink  # 12 apply_key_value tests pass
✅ Python E2E test - PASSES (Task 7.4.5 + 8.2.P complete)
```

**Branch:** `feature/hf-hub-sink` (262 commits ahead of main)

---

## What's Next

### Phase 8: Testing

**Next Task:** 8.2.5 - Implement test_multi_shard_upload

With Task 8.2.4 complete, the next step is testing multi-shard upload flow using MockHfHub.

### Task 7.4: Feature Flag Wiring ✅ COMPLETE

The `hf_sink` feature is now fully wired through the dependency chain:
1. ✅ `polars-lazy/Cargo.toml` - `hf_sink = ["polars-stream?/hf_sink"]` (DONE)
2. ✅ `polars/Cargo.toml` - `hf_sink = ["polars-lazy?/hf_sink", "new_streaming", "cloud"]` (DONE)
3. ✅ `polars-python/Cargo.toml` - `hf_sink = ["polars/hf_sink"]` + added to `io` feature (DONE)
4. ✅ Python E2E test passes (Task 7.4.5)

**Bugs Fixed During Task 7.4.5:**
1. **Tokio Runtime Issue** - `upload_shard_task` was spawned in polars' custom executor but needed
   Tokio for HTTP operations. Fixed by using `pl_async::get_runtime().spawn()` for the upload loop.
   - File: `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs`

2. **API 404 Handling** - When checking existing files, a 404 response was being parsed as JSON array,
   causing "expected a sequence" error. Fixed by handling 404 status in `GetPages::next()`.
   - File: `crates/polars-io/src/cloud/hf/api.rs`

**Task 8.2.3a/b: MockHfHub + mock_lfs_batch + mock_presigned_upload** ✅ Complete
- Added `MockHfHub` struct wrapping wiremock `MockServer` with builder pattern
- Implemented `mock_lfs_batch()` for LFS batch API (`POST .../.git/info/lfs/objects/batch`)
- Implemented `mock_presigned_upload()` for S3 presigned URL uploads
- Added `MockLfsObject` helper for configuring mock LFS responses
- Added helper `build_lfs_batch_response()` to generate JSON responses
- Added `reqwest = { workspace = true }` to polars-stream dev-dependencies
- 6 new tests: mock_hf_hub_start, mock_lfs_batch_single_upload, mock_lfs_batch_already_exists, mock_lfs_batch_multiple_objects, mock_presigned_upload, mock_lfs_batch_and_upload_flow
- Total mock tests: 10 (4 existing + 6 new)

**Task 8.2.2: Base URL Injection** ✅ Complete
- Added `base_url: String` field to `HFRepoLocation` struct
- Modified `HFRepoLocation::new()` to accept `Option<&str>` base_url parameter
- Updated all URL-building methods to use `self.base_url` instead of hardcoded `https://huggingface.co`
- Added `api_base_url: Option<String>` field to `HfSinkOptions` with builder method
- Threaded `api_base_url` through all callers: `LfsClient`, `CommitClient`, `check_existing_files`, `fetch_readme`, `expand_paths_hf`
- HTTP client now allows http:// (not just https://) when custom base_url provided (for mock server testing)
- Added tests: `test_custom_base_url`, `test_options_with_custom_base_url`, `test_repo_location_custom_base_url`, `test_mock_server_with_options_pattern`
- Files modified: `url.rs`, `options.rs`, `lfs/client.rs`, `commit.rs`, `api.rs`, `glob.rs`, `path_utils/mod.rs`, `hf_sink/mod.rs`, `mock_tests.rs`

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

### Task 7.4: Feature Flag Wiring [ ] ← **BLOCKING PYTHON SUPPORT**

**Problem:** The `hf_sink` feature is defined in `polars-io` and `polars-stream`, but NOT propagated through the dependency chain to `polars-python`. This means `hf://` URLs panic when used from Python.

**Current Feature Chain (PARTIALLY FIXED):**
```
polars-io/hf_sink ✅ (defined)
polars-stream/hf_sink ✅ (defined, enables polars-io/hf_sink)
polars-lazy/hf_sink ✅ (Task 7.4.1 - DONE)
polars/hf_sink ✅ (Task 7.4.2 - DONE)
polars-python/hf_sink ✅ (Task 7.4.3 - DONE)
```

**Files to Modify:**

| File | Change | Status |
|------|--------|--------|
| `crates/polars-lazy/Cargo.toml` | `hf_sink = ["polars-stream?/hf_sink"]` | ✅ Done |
| `crates/polars/Cargo.toml` | `hf_sink = ["polars-lazy?/hf_sink", "new_streaming", "cloud"]` | ✅ Done |
| `crates/polars-python/Cargo.toml` | `hf_sink = ["polars/hf_sink"]` + added to `io` feature | ✅ Done |
| `py-polars/pyproject.toml` | N/A (features flow through Cargo.toml) | N/A |

**Subtasks:**

| Subtask | Description | Status |
|---------|-------------|--------|
| **7.4.1** | Add `hf_sink` feature to `polars-lazy/Cargo.toml` | ✅ Complete |
| **7.4.2** | Add `hf_sink` feature to `polars/Cargo.toml` | ✅ Complete |
| **7.4.3** | Add `hf_sink` feature to `polars-python/Cargo.toml` | ✅ Complete |
| **7.4.4** | Enable `hf_sink` in py-polars build (pyproject.toml) | N/A (flows via `io` feature) |
| **7.4.5** | Re-run Python E2E smoke test (Task 8.2.P) | ✅ Complete |

**Verification:**
```bash
# After changes, rebuild and test:
cd py-polars && maturin develop --release
python -c "import polars as pl; pl.DataFrame({'a': [1]}).write_parquet('hf://datasets/user/repo/test.parquet')"
```

---

## Phase 8: Testing

### Task 8.1: Unit Tests [ ]
Ongoing - included in component work (114 tests across modules)

### Task 8.2: Integration Tests (Mock)

Mock HTTP integration tests using wiremock to test the full upload pipeline.

| Subtask | Description | Status |
|---------|-------------|--------|
| **8.2.1** | Add wiremock dev-dependency + skeleton test file | ✅ Complete |
| **8.2.2** | Add base URL injection to HFRepoLocation | ✅ Complete |
| **8.2.3** | Create reusable mock fixtures (LFS, commit, tree APIs) | ✅ Complete |
|   8.2.3a | Create MockHfHub struct + mock_lfs_batch() | ✅ Complete |
|   8.2.3b | Create mock_presigned_upload() | ✅ Complete |
|   8.2.3c | Create mock_commit() | ✅ Complete |
|   8.2.3d | Create mock_tree() | ✅ Complete |
| **8.2.4** | Implement test_single_shard_upload | ✅ Complete |
| 8.2.5 | Implement test_multi_shard_upload | [ ] |
| 8.2.6 | Implement test_overwrite_mode | [ ] |

**Task 8.2.4: test_single_shard_upload** ✅ Complete
- Added `new_with_base_url()` to `UploadExecutor` for HTTP testing (mirrors `LfsClient` pattern)
- Added `sha2 = { workspace = true }` to polars-stream dev-dependencies
- Added `compute_sha256()` helper function
- Added `test_single_shard_upload` test: LFS batch → presigned upload flow with mock HTTP
- Total mock tests: 16 (15 existing + 1 new)

**Task 8.2.3c: mock_commit()** ✅ Complete
- Added `mock_commit(commit_oid)` method to `MockHfHub`
- Matches `POST /api/datasets/.*/commit/.*` with `content-type: application/x-ndjson`
- Returns JSON response with `commitUrl` and `commitOid` fields
- Added 2 tests: `test_mock_commit_success`, `test_mock_full_upload_flow`
- Total mock tests: 12 (10 existing + 2 new)

**Task 8.2.3d: mock_tree()** ✅ Complete
- Added `MockTreeEntry` helper struct with `file()` and `directory()` constructors
- Added `mock_tree(entries)` method to `MockHfHub`
- Matches `GET /api/datasets/.*/tree/.*`
- Returns JSON array matching `HFAPIResponse` format: `[{"type": "file"|"directory", "path": "...", "size": N}]`
- Added 3 tests: `test_mock_tree_lists_files`, `test_mock_tree_empty`, `test_mock_tree_with_directories`
- Total mock tests: 15 (12 existing + 3 new)

**Files added:**
- `crates/polars-stream/Cargo.toml` - Added `wiremock = "0.6"` dev-dependency
- `crates/polars-stream/src/nodes/io_sinks/hf_sink/mock_tests.rs` - Mock test infrastructure

### Task 8.2.P: Python E2E Smoke Test (Priority) ✅ COMPLETE

**Status:** ✅ PASSED (2026-01-29)

**Test Result:**
```python
import polars as pl

df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
df.write_parquet(
    "hf://datasets/davanstrien/test-polars-streaming/data/python-e2e-7.4.5.parquet",
    storage_options={"token": "hf_xxx"}
)
# ✅ SUCCESS - File uploaded to HF Hub
```

**Uploaded File:**
- Path: `data/python-e2e-7.4.5.parquet/train-00000.parquet`
- Size: 711 bytes
- LFS OID: `48100f9f117d2d6661d2ac21b0421e4111dcdc7713db07352c1d42808430dd39`

**Issues Fixed:**
1. Feature flag wiring (Task 7.4) - `hf_sink` now flows through dependency chain
2. Tokio runtime issue - HTTP tasks now spawn in Tokio, not polars executor
3. API 404 handling - `GetPages::next()` now returns `None` for 404 responses

---

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
- [x] **Phase 7:** Python Bindings ✅ COMPLETE
  - [x] Task 7.1: Wire Python Options to HfSinkOptions ✅
  - [x] Task 7.2: sink_parquet Integration ✅
  - [x] Task 7.3: write_parquet Integration ✅
  - [x] Task 7.4: Feature Flag Wiring ✅
- [ ] **Phase 8:** Testing 🔄 IN PROGRESS
  - [x] Task 8.2.1: Mock HTTP infrastructure ✅
  - [x] Task 8.2.2: Base URL injection ✅
  - [x] Task 8.2.3a/b: MockHfHub + mock_lfs_batch + mock_presigned_upload ✅
  - [x] Task 8.2.P: Python E2E Smoke Test ✅ PASSED
  - [x] Task 8.2.3c: mock_commit() ✅ (12 mock tests)
  - [x] Task 8.2.3d: mock_tree() ✅ (15 mock tests)
  - [x] Task 8.2.4: test_single_shard_upload ✅ (16 mock tests)
  - [ ] Task 8.2.5-8.2.6: Mock integration tests
  - [ ] Task 8.3: E2E Tests (Real HF)
  - [ ] Task 8.4: Performance Benchmarks
- [ ] **Phase 9:** Documentation (0/4)

**Status:** 7/9 phases complete. Python E2E test passes!

**Next:** Task 8.2.5 - test_multi_shard_upload integration test

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
