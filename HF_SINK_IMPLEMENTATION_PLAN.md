# HF Hub Sink Implementation Plan

Native HF Hub write support for Polars via `sink_parquet("hf://datasets/user/repo/...")`.

**Archive:** Detailed completed work in [HF_SINK_ARCHIVE.md](./HF_SINK_ARCHIVE.md)

---

## Current Status (2026-01-31)

```
✅ Phases 0-8 complete (Foundation → Python Bindings → Core Testing)
✅ Python E2E test PASSES - sink_parquet("hf://...") works for small files!
✅ 86 Rust tests pass (unit + mock integration)
✅ BUG-001 FIXED - error propagation now shows actual errors
✅ BUG-002 FIXED - multipart uploads now use correct HF Hub response format
🔄 Phase 9 in progress - Distribution & Demo
```

**Python E2E Test Result:** Successfully uploaded file to HF Hub:
- `hf://datasets/davanstrien/test-polars-streaming/data/python-e2e-7.4.5.parquet`
- LFS OID: `48100f9f117d2d6661d2ac21b0421e4111dcdc7713db07352c1d42808430dd39`

**Build Status:**
```bash
✅ cargo check -p polars-io --features hf_sink     # PASSES
✅ cargo check -p polars-stream --features hf_sink # PASSES
✅ cargo check -p polars-python                    # PASSES (hf_sink enabled via io feature)
✅ cargo test -p polars-stream --features hf_sink hf_sink # 86 tests pass
✅ Python E2E test - PASSES
```

**Branch:** `feature/hf-hub-sink` (214 commits ahead of main)

---

## What's Next

### ~~BLOCKER: Fix BUG-002 (Multipart Upload 404)~~ ✅ FIXED

**Status:** Fixed in commit (branch: feature/hf-hub-sink)

**Root Cause:** Polars was parsing the wrong response format for multipart uploads. HF Hub returns:
```json
{
  "actions": {
    "upload": {
      "href": "https://huggingface.co/api/complete_multipart?...",  // Completion URL
      "header": {
        "chunk_size": "16000000",
        "00001": "https://s3.../part1?...",  // Part URLs as numeric keys
        "00002": "https://s3.../part2?..."
      }
    }
  }
}
```
But Polars expected `actions.parts[]` array format (which doesn't exist).

**Fix Applied:**
1. `types.rs` - Updated `LfsTransfer::Multipart` to include `completion_url`, `chunk_size`, `part_urls`
2. `types.rs` - Updated `into_transfer()` to parse numeric header keys for part URLs
3. `types.rs` - Fixed `LfsPartCompletion` to use `partNumber` (camelCase) as HF expects
4. `client.rs` - Updated `complete_multipart()` to accept provided completion URL
5. `upload.rs` - Updated multipart upload to use new format
6. `hf_sink/mod.rs` - Thread completion URL through upload flow

### Phase 9: Distribution & Demo

**Goal:** Make it easy for people to test without compiling. Not a formal release.

1. ~~Clean up repo (remove scratch files)~~ ✅
2. ~~Push to davanstrien/polars fork~~ ✅
3. ~~Set up GitHub Actions to build wheels (Linux x64)~~ ✅ (Task 9.2.1)
4. Add macOS ARM64 to wheel build (Task 9.2.2) - *helps with local debugging*
5. ~~Smoke test install in Colab~~ ✅ (small files work)
6. **🔄 Test large file upload (>100MB)** - Task 9.3.4 - verify BUG-002 fix with real HF Hub
7. Create demo notebook

**Recent Fixes:**
- BUG-001 ✅ FIXED - error propagation now shows actual errors
- BUG-002 ✅ FIXED - multipart uploads now use correct HF Hub format (completion URL + numeric header keys)

### Future Enhancements (Low Priority)

- **Better debug logging for rate limits**: Distinguish "waiting 30s (fallback - header missing)" vs "waiting 42s (per RateLimit header)" in verbose output. Would help verify BUG-001 fix is working as expected.

---

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
| **8.2.5** | Implement test_multi_shard_upload | ✅ Complete |
| **8.2.6** | Implement test_overwrite_mode | ✅ Complete |

**Task 8.2.6: test_overwrite_mode** ✅ Complete
- Added `mock_commit_with_deletes()` method to `MockHfHub` - validates commit body contains `deletedFile` entries
- Added `test_overwrite_mode` integration test: verifies delete + add operations in atomic commit
- Tests overwrite flow: mock tree (existing files) → LFS batch → upload → commit with deletes
- Uses `CommitOperationDelete` to verify correct operation types
- Total mock tests: 18 (17 existing + 1 new)

**Task 8.2.4: test_single_shard_upload** ✅ Complete
- Added `new_with_base_url()` to `UploadExecutor` for HTTP testing (mirrors `LfsClient` pattern)
- Added `sha2 = { workspace = true }` to polars-stream dev-dependencies
- Added `compute_sha256()` helper function
- Added `test_single_shard_upload` test: LFS batch → presigned upload flow with mock HTTP
- Total mock tests: 16 (15 existing + 1 new)

**Task 8.2.5: test_multi_shard_upload** ✅ Complete
- Added `test_multi_shard_upload` test: full multi-shard upload + commit flow
- Tests 3 shards: LFS batch → presigned upload (x3) → atomic commit
- Uses `LfsClient`, `UploadExecutor`, and `CommitClient` with MockHfHub
- Total mock tests: 17 (16 existing + 1 new)

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

### Task 8.3: E2E Tests (Real HF) 🔄 IN PROGRESS

Python E2E tests using a real HF Hub repository (`davanstrien/test-polars-streaming`).

| Subtask | Description | Status |
|---------|-------------|--------|
| **8.3.1** | First E2E test (basic upload) | ✅ Complete |
|   8.3.1a | Add `hf_hub` pytest marker to pyproject.toml | ✅ Complete |
|   8.3.1b | Create conftest.py with HF fixtures | ✅ Complete |
|   8.3.1c | Create test_hf_sink.py with test_basic_upload | ✅ Complete |
| **8.3.2** | test_overwrite_mode | [ ] Pending |
| **8.3.3** | test_append_mode | [ ] Pending |
| **8.3.4** | test_partitioned_write | [ ] Pending |

**Files Created:**
- `py-polars/tests/unit/io/cloud/conftest.py` - HF fixtures (hf_token, hf_test_repo)
- `py-polars/tests/unit/io/cloud/test_hf_sink.py` - E2E tests

**Run Tests:**
```bash
HF_TOKEN=hf_xxx pytest -m "hf_hub" py-polars/tests/unit/io/cloud/test_hf_sink.py -v
```

### Task 8.4: Performance Benchmarks [ ]
- HashingWriter overhead < 5%
- Full pipeline saturates network

---

## Phase 9: Distribution & Demo

**Goal:** Make the feature easy for people to test without compiling from source. This is NOT a formal release - just a way to share a working prototype and gather feedback.

### Task 9.1: Clean Branch for Fork ✅ Complete

| Subtask | Description | Status |
|---------|-------------|--------|
| **9.1.1** | Create clean branch from feature/hf-hub-sink | ✅ Complete |
| **9.1.2** | Remove test files / scratch work from repo root | ✅ Complete (moved to scratch/) |
| **9.1.3** | Push to davanstrien/polars fork | ✅ Complete |

### Task 9.2: GitHub Actions for Wheels 🔄 IN PROGRESS

| Subtask | Description | Status |
|---------|-------------|--------|
| **9.2.1** | Create `.github/workflows/build-hf-sink-wheels.yml` | ✅ Complete |
| **9.2.2** | Target platforms: Linux x64, macOS ARM64 | [ ] (Linux x64 done, macOS pending) |
| **9.2.3** | Attach wheels to GitHub Release | [ ] |

**Task 9.2.1 Details:**
- Created `build-hf-sink-wheels.yml` with Linux x64 wheel build
- Uses `maturin-action@v1` with `dist-release` profile
- Builds `polars-runtime-32` (default runtime)
- Manual trigger via `workflow_dispatch` with dry-run option
- 10GB swap space to avoid OOM during compilation
- Tests wheel installation before uploading

### Task 9.3: Smoke Test Install 🔄 IN PROGRESS

Verify the wheels install and work in a clean environment.

| Subtask | Description | Status |
|---------|-------------|--------|
| **9.3.1** | Test `uv pip install <wheel-url>` in Colab | ✅ Complete |
| **9.3.2** | Verify basic `sink_parquet("hf://...")` works | ✅ Complete |
| **9.3.3** | Test streaming read → filter → write | ✅ Works for small data |
| **9.3.4** | Test large file upload (>100MB) to verify BUG-002 multipart fix | 🔄 IN PROGRESS |

**Task 9.3.4 Subtasks:**

| Subtask | Description | Status |
|---------|-------------|--------|
| **9.3.4a** | Build Python wheel with latest BUG-002 fix | [ ] |
| **9.3.4b** | Create test script for large DataFrame (~1M rows, >100MB) | [ ] |
| **9.3.4c** | Run test and verify multipart upload succeeds | [ ] |
| **9.3.4d** | Read back and verify data integrity | [ ] |
| **9.3.4e** | Update implementation plan with results | [ ] |

**Notes:**
- Must install BOTH `polars-*.whl` (base) AND `polars_runtime_32-*.whl` (runtime)
- Small streaming writes work (single file, head(N))
- BUG-001 ✅ FIXED: Error propagation now shows actual errors
- BUG-002 ✅ FIXED: Multipart uploads use correct HF Hub format (needs real-world validation)

### Task 9.4: Demo Notebook [ ]

| Subtask | Description | Status |
|---------|-------------|--------|
| **9.4.1** | Create notebook: streaming read → filter → streaming write | [ ] |
| **9.4.2** | Show "Hub is your disk" workflow (TBs with minimal RAM) | [ ] |
| **9.4.3** | Upload to HF Hub or include in repo | [ ] |

### Task 9.5: Document Limitations [ ]

| Subtask | Description | Status |
|---------|-------------|--------|
| **9.5.1** | List known limitations / caveats | [ ] |
| **9.5.2** | Note this is experimental / WIP | [ ] |
| **9.5.3** | Add install + usage examples to README | [ ] |

---

## Known Issues / Bugs to Investigate

| Issue | Description | Status | Priority |
|-------|-------------|--------|----------|
| **BUG-001** | "upload channel closed unexpectedly" on large streaming writes | ✅ Fixed | High |
| **BUG-002** | Multipart upload fails with 404 on `/api/complete_multipart` | ✅ Fixed | High |

### BUG-001: Upload Channel Closed Unexpectedly ✅ FIXED

**Root Cause Identified:** The issue had two components:
1. **`.unwrap()` panic** at line 1398 in `upload_shard_task` masked the real network error
2. **Immediate retry on 429** - when `RateLimit` header was missing, `wait_secs.unwrap_or(0)` caused 0-second waits between retries

**Symptom:** `ComputeError: upload channel closed unexpectedly` when streaming larger datasets.

**Fix Applied (2026-01-30):**

| Change | File | Line |
|--------|------|------|
| Replace `.unwrap()` with `map_err()` | `hf_sink/mod.rs` | 1398 |
| Change fallback wait from 0s to 30s | `lfs/client.rs` | 293 |
| Increase rate limit retries from 3 to 5 | `lfs/client.rs` | 19 |

**Files Modified:**
- `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs`
- `crates/polars-io/src/cloud/hf/lfs/client.rs`

**Tests:** All 86 HF sink tests pass. Error messages now show actual network failures instead of "channel closed".

### BUG-002: Multipart Upload 404 Error ✅ FIXED

**Discovered:** 2026-01-31 during Colab testing

**Symptom:** Large files (triggering multipart upload) fail with:
```
ComputeError: upload failed after 3 retries: S3 upload failed (HTTP 404):
<pre>Cannot PUT /api/complete_multipart</pre>
```

**Root Cause:** Polars was parsing the wrong response format for multipart uploads. HF Hub returns:
```json
{
  "actions": {
    "upload": {
      "href": "https://huggingface.co/api/complete_multipart?...",  // Completion URL
      "header": {
        "chunk_size": "16000000",
        "00001": "https://s3.../part1?...",  // Part URLs as numeric keys
        "00002": "https://s3.../part2?..."
      }
    }
  }
}
```
But Polars expected `actions.parts[]` array format (which doesn't exist).

**Fix Applied:**
1. `types.rs` - Updated `LfsTransfer::Multipart` to include `completion_url`, `chunk_size`, `part_urls`
2. `types.rs` - Updated `into_transfer()` to parse numeric header keys for part URLs
3. `types.rs` - Fixed `LfsPartCompletion` to use `partNumber` (camelCase) as HF expects
4. `client.rs` - Updated `complete_multipart()` to accept provided completion URL
5. `upload.rs` - Updated multipart upload to use new format
6. `hf_sink/mod.rs` - Thread completion URL through upload flow

**Files Modified:**
- `crates/polars-io/src/cloud/hf/lfs/types.rs`
- `crates/polars-io/src/cloud/hf/lfs/upload.rs`
- `crates/polars-io/src/cloud/hf/lfs/client.rs`
- `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs`

**Tests:** All 86 HF sink tests pass. Unit tests added for multipart format parsing.

**Validation Pending:** Task 9.3.4 - test large file upload (>100MB) to verify fix with real HF Hub

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
- [x] **Phase 8:** Testing ✅ (core testing complete, additional E2E tests can continue in parallel)
  - [x] Task 8.2.1: Mock HTTP infrastructure ✅
  - [x] Task 8.2.2: Base URL injection ✅
  - [x] Task 8.2.3a/b: MockHfHub + mock_lfs_batch + mock_presigned_upload ✅
  - [x] Task 8.2.P: Python E2E Smoke Test ✅ PASSED
  - [x] Task 8.2.3c: mock_commit() ✅ (12 mock tests)
  - [x] Task 8.2.3d: mock_tree() ✅ (15 mock tests)
  - [x] Task 8.2.4: test_single_shard_upload ✅ (16 mock tests)
  - [x] Task 8.2.5: test_multi_shard_upload ✅ (17 mock tests)
  - [x] Task 8.2.6: test_overwrite_mode ✅ (18 mock tests)
  - [x] Task 8.3.1: First E2E test (test_basic_upload) ✅
  - [ ] Task 8.3.2-8.3.4: Additional E2E tests (can continue in parallel)
  - [ ] Task 8.4: Performance Benchmarks (optional)
- [ ] **Phase 9:** Distribution & Demo 🔄 CURRENT FOCUS
  - [x] Task 9.1: Clean branch for fork ✅
  - [ ] Task 9.2: GitHub Actions for wheels (9.2.1 Linux x64 ✅, 9.2.2-9.2.3 pending)
  - 🔄 Task 9.3: Smoke test install (9.3.1-9.3.3 ✅, 9.3.4 in progress)
  - [ ] Task 9.4: Demo notebook
  - [ ] Task 9.5: Document limitations

**Status:** 8/9 phases complete. Python E2E works! Branch cleaned and pushed.

**Next:** Task 9.3.4 - Test large file upload (>100MB) to verify BUG-002 multipart fix

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
