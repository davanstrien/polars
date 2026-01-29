# HF Hub Sink Implementation Plan

Native HF Hub write support for Polars via `sink_parquet("hf://datasets/user/repo/...")`.

**Archive:** Detailed completed work in [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md)

---

## Current Status (2026-01-29)

```
✅ Phases 0-6 complete (Foundation → Advanced Features)
🔄 Phase 7 in progress - Python Bindings (Tasks 7.1-7.3 complete, Task 7.4 BLOCKING)
❌ Task 8.2.P FAILED - Python E2E test revealed hf_sink feature not enabled in py-polars!
```

**Critical Finding:** `hf_sink` feature flag is NOT propagated to `polars-python`, causing
`hf://` URLs to panic with "impl error: unresolved hf:// path".

**Build Status:**
```bash
✅ cargo check -p polars-io --features hf_sink     # PASSES
✅ cargo check -p polars-stream --features hf_sink # PASSES
✅ cargo check -p polars-python                    # PASSES (but hf_sink NOT enabled!)
✅ cargo test -p polars-io checkpoint --features hf_sink  # 10 checkpoint tests pass
✅ cargo test -p polars-io hf_token --features hf_sink,http  # 4 token extraction tests pass
✅ cargo test -p polars-stream --features hf_sink hf_sink # 79 tests pass (69 + 10 mock)
✅ cargo test -p polars-io apply_key_value --features hf_sink  # 12 apply_key_value tests pass
❌ Python E2E test - FAILS (hf_sink feature not wired through)
```

**Branch:** `feature/hf-hub-sink` (248 commits ahead of main)

---

## What's Next

### Task 7.4: Feature Flag Wiring (PRIORITY - BLOCKING PYTHON SUPPORT)

**Next Task:** 7.4.1 - Add `hf_sink` feature to `polars-lazy/Cargo.toml`

The `hf_sink` feature must be propagated through the dependency chain:
1. `polars-lazy/Cargo.toml` - Add `hf_sink = ["polars-stream/hf_sink"]`
2. `polars/Cargo.toml` - Add `hf_sink = ["polars-lazy/hf_sink"]`
3. `polars-python/Cargo.toml` - Add `hf_sink = ["polars/hf_sink"]`
4. `py-polars/pyproject.toml` - Enable `hf_sink` in default build

See Task 7.4 section below for details.

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

**Current Feature Chain (BROKEN):**
```
polars-io/hf_sink ✅ (defined)
polars-stream/hf_sink ✅ (defined, enables polars-io/hf_sink)
polars-lazy/hf_sink ❌ (NOT defined)
polars/hf_sink ❌ (NOT defined)
polars-python/hf_sink ❌ (NOT defined)
```

**Files to Modify:**

| File | Change |
|------|--------|
| `crates/polars-lazy/Cargo.toml` | Add `hf_sink = ["polars-stream/hf_sink"]` feature |
| `crates/polars/Cargo.toml` | Add `hf_sink = ["polars-lazy/hf_sink"]` feature |
| `crates/polars-python/Cargo.toml` | Add `hf_sink = ["polars/hf_sink"]` feature |
| `py-polars/pyproject.toml` | Add `hf_sink` to default features |

**Subtasks:**

| Subtask | Description | Status |
|---------|-------------|--------|
| **7.4.1** | Add `hf_sink` feature to `polars-lazy/Cargo.toml` | [ ] |
| **7.4.2** | Add `hf_sink` feature to `polars/Cargo.toml` | [ ] |
| **7.4.3** | Add `hf_sink` feature to `polars-python/Cargo.toml` | [ ] |
| **7.4.4** | Enable `hf_sink` in py-polars build (pyproject.toml) | [ ] |
| **7.4.5** | Re-run Python E2E smoke test (Task 8.2.P) | [ ] |

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
| **8.2.3** | Create reusable mock fixtures (LFS, commit, tree APIs) | 🔄 In Progress |
|   8.2.3a | Create MockHfHub struct + mock_lfs_batch() | ✅ Complete |
|   8.2.3b | Create mock_presigned_upload() | ✅ Complete |
|   8.2.3c | Create mock_commit() | [ ] |
|   8.2.3d | Create mock_tree() | [ ] |
| 8.2.4 | Implement test_single_shard_upload | [ ] |
| 8.2.5 | Implement test_multi_shard_upload | [ ] |
| 8.2.6 | Implement test_overwrite_mode | [ ] |

**Files added:**
- `crates/polars-stream/Cargo.toml` - Added `wiremock = "0.6"` dev-dependency
- `crates/polars-stream/src/nodes/io_sinks/hf_sink/mock_tests.rs` - Mock test infrastructure

### Task 8.2.P: Python E2E Smoke Test (Priority) 🔄 BLOCKED

**Status:** Blocked by Task 7.4 (Feature Flag Wiring)

**Why:** Verify the full Python → Rust → HF Hub pipeline works before building more infrastructure.

**Test:**
```python
import polars as pl

df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
df.write_parquet(
    "hf://datasets/davanstrien/test-polars-streaming/data/python-e2e-test.parquet",
    storage_options={"token": "hf_xxx"}
)
```

**Result (2026-01-29):** ❌ FAILED with `panic!("impl error: unresolved hf:// path")`

**Root Cause:** The `hf_sink` feature is NOT enabled when building py-polars!

The feature is defined in `polars-io` and `polars-stream`, but not propagated through the dependency chain to `polars-python`. This causes the code at `lower_ir.rs:279` to take the `#[cfg(not(feature = "hf_sink"))]` path, routing `hf://` URLs to `FileSink` instead of `HfSink`.

**Fix Required:** Task 7.4 (Feature Flag Wiring)

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
- [ ] **Phase 7:** Python Bindings 🔄 (3/4 tasks complete)
  - [x] Task 7.1: Wire Python Options to HfSinkOptions ✅
  - [x] Task 7.2: sink_parquet Integration ✅
  - [x] Task 7.3: write_parquet Integration ✅
  - [ ] **Task 7.4: Feature Flag Wiring** ← BLOCKING PYTHON SUPPORT!
- [ ] **Phase 8:** Testing (blocked by 7.4)
  - [x] Task 8.2.1: Mock HTTP infrastructure ✅
  - [x] Task 8.2.2: Base URL injection ✅
  - [x] Task 8.2.3a/b: MockHfHub + mock_lfs_batch + mock_presigned_upload ✅
  - [ ] **Task 8.2.P: Python E2E Smoke Test** ← BLOCKED by 7.4
  - [ ] Task 8.2.3c/d: Remaining mock fixtures
  - [ ] Task 8.2.4-8.2.6: Mock integration tests
  - [ ] Task 8.3: E2E Tests (Real HF)
  - [ ] Task 8.4: Performance Benchmarks
- [ ] **Phase 9:** Documentation (0/4)

**Status:** 6/9 phases complete. Phase 7 blocked by missing feature flag wiring (Task 7.4).

**Next:** Task 7.4 - Feature Flag Wiring (PRIORITY - enables Python support for hf:// URLs)

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
