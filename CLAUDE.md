# Polars HF Hub Integration Notes

## Active Projects

### 1. HF Hub Sink (Write Support) - IN PROGRESS
**Plan**: [HF_SINK_IMPLEMENTATION_PLAN.md](./HF_SINK_IMPLEMENTATION_PLAN.md)

**Goal**: Native streaming writes to HF Hub via `sink_parquet("hf://datasets/user/repo/...")`

**Branch**: `feature/hf-hub-sink` (76 commits ahead of main)

**Status**: Phases 0-5 complete (Rust implementation functional). Phase 6 next.

**Implemented Features**:
- ✅ True streaming with O(shard_size) memory
- ✅ LFS uploads with SHA256 hashing
- ✅ Atomic commits via NDJSON API
- ✅ Write modes: ErrorIfExists, Overwrite, Append
- ✅ Dataset card (README.md) auto-generation
- 🔄 Resume from checkpoint (Phase 6.1)
- 🔄 Partitioned write support (Phase 6.2)

**Quick Commands**:
```bash
# Build
cargo check -p polars-io --features hf_sink
cargo check -p polars-stream --features hf_sink

# Run tests (27 tests)
cargo test -p polars-stream --features hf_sink hf_sink

# Python build
cd py-polars && maturin develop --release
```

### 2. HF Hub Glob Expansion Rate Limiting - COMPLETED
**Issue**: https://github.com/pola-rs/polars/issues/25389

**Problem**: One Tree API call per directory causing 429 errors.

**Solution**: Use `?recursive=true` on Tree API.

**Key File**: `crates/polars-io/src/path_utils/hugging_face.rs`

---

## HF Hub API Reference

### Tree API (Recommended)
```
GET https://huggingface.co/api/{datasets|spaces}/{repo}/tree/{revision}/{path}?recursive=true
```
- Returns all files under path recursively
- Includes file sizes (`size` field)
- Paginated via `Link` header (1000 items per page)
- **This is the safer/more reliable option** per HF team

### Siblings API (Faster but less reliable)
```
GET https://huggingface.co/api/{datasets|spaces}/{repo}?expand=siblings
```
- Returns all files in repo in single call
- **May return incomplete list for repos >1000 files**
- No file sizes
- huggingface_hub library uses this with fallback to tree when unreliable

---

## Reference: huggingface_hub Library

The official `huggingface_hub` Python library is a useful reference for how HF recommends handling these APIs. We don't need to copy it exactly, but it shows best practices.

**Local clone**: `/Users/davanstrien/Documents/code/huggingface_hub`

### Key files:

1. **`src/huggingface_hub/hf_api.py`** (~line 3080)
   - `list_repo_tree()` function
   - Shows URL construction with `?recursive=true`
   ```python
   tree_url = f"{self.endpoint}/api/{repo_type}s/{repo_id}/tree/{revision}{encoded_path_in_repo}"
   params={"recursive": recursive, "expand": expand}
   ```

2. **`src/huggingface_hub/_snapshot_download.py`** (lines 337-348)
   - Shows siblings → tree fallback logic
   - `LARGE_REPO_THRESHOLD = 1000` - repos with more files considered "unreliable" for siblings
   ```python
   unreliable_nb_files = (
       repo_info.siblings is None or
       len(repo_info.siblings) == 0 or
       len(repo_info.siblings) > LARGE_REPO_THRESHOLD
   )
   if unreliable_nb_files:
       # Fall back to list_repo_tree with recursive=True
   ```

---

## Benchmark Results

Benchmark script: `benchmark_hf_api.py` in repo root.

**fineweb-2** (`data/*/train/*.parquet`, 8,257 files):
| Approach | API Calls | Time |
|----------|-----------|------|
| Current (tree per dir) | 379 | 53.8s (hit 429) |
| Recursive Tree | 19 | 8.2s |
| Siblings | 1 | 374ms |

**finepdfs-edu** (`data/*_Latn/train/*.parquet`, 429 files):
| Approach | API Calls | Time |
|----------|-----------|------|
| Current | 139 | 18.7s |
| Recursive Tree | 1 | 246ms |
| Siblings | 1 | 172ms |

---

## HF Team Contacts
- **Quentin Lhoest** - Suggested `?recursive=true` approach in GitHub issue
- **Lucain** - Confirmed recursive tree is "safer/more reliable", explained siblings limitations

---

## Code Structure

### hugging_face.rs key components:
- `HFPathParts` - Parses `hf://` URLs
- `HFRepoLocation` - Builds API/download URLs
- `GetPages` - Handles pagination via Link headers
- `expand_paths_hf()` - Main function to expand glob patterns (THIS IS WHAT WE'RE MODIFYING)
- `HFAPIResponse` - Deserializes tree API response `{type, path, size}`

### Current glob expansion flow (problematic):
1. Extract prefix from glob pattern
2. Stack-based traversal: for each directory, call Tree API
3. Filter results with Matcher regex
4. Push subdirectories to stack
5. **Problem**: One API call per directory level

### New flow (recursive):
1. Extract prefix from glob pattern
2. Single call: `tree/{revision}/{prefix}?recursive=true`
3. Paginate through all results
4. Filter with Matcher regex
5. **Result**: ~20x fewer API calls

---

## HF Hub Sink Architecture (Write Support)

See [HF_SINK_IMPLEMENTATION_PLAN.md](./HF_SINK_IMPLEMENTATION_PLAN.md) for full details.

### High-Level Flow
```
sink_parquet("hf://datasets/user/repo/data/train.parquet")
    │
    ▼
HfSinkNode (streaming engine)
    │
    ├── ShardWriter[0] ──┐
    ├── ShardWriter[1] ──┼── Each: ParquetEncoder → HashingWriter → MmapBuffer
    └── ShardWriter[N] ──┘        On shard full: LFS upload
    │
    ▼
CommitCoordinator → Atomic commit of all shards
```

### Key HF Hub APIs for Writes

**1. LFS Batch API** (get upload URLs):
```
POST /{repo_type}s/{repo_id}.git/info/lfs/objects/batch
{
  "operation": "upload",
  "transfers": ["basic", "multipart"],
  "objects": [{"oid": "sha256...", "size": 12345}]
}
```

**2. Commit API** (atomic commit):
```
POST /api/{repo_type}s/{repo_id}/commit/{revision}
Content-Type: application/x-ndjson

{"key":"header","value":{"summary":"Upload via Polars"}}
{"key":"lfsFile","value":{"path":"data/train-00000.parquet","oid":"sha256...","size":12345}}
```

### Key Files (All Implemented)
```
crates/polars-io/src/cloud/hf/
├── mod.rs              # Module exports
├── options.rs          # HfSinkOptions, HfWriteMode
├── auth.rs             # Token handling (env, file, explicit)
├── api.rs              # fetch_readme(), list_existing_files()
├── hashing_writer.rs   # SHA256 streaming
├── mmap_buffer.rs      # Temp file buffer
├── shard_writer.rs     # Parquet + hash + buffer
├── dataset_card.rs     # README generation, SplitInfo
├── commit.rs           # CommitClient, NDJSON payload
└── lfs/
    ├── types.rs        # LFS request/response types
    ├── client.rs       # LfsClient
    └── upload.rs       # UploadExecutor

crates/polars-stream/src/nodes/io_sinks/hf_sink/
└── mod.rs              # HfSinkNode (1660 lines, 27 tests)
                        # - buffer_and_write_task()
                        # - upload_shard_task()
                        # - finalize() with mode handling
```

### Why SHA256 Before Upload?
HF Hub's LFS requires the file hash before providing upload URLs:
1. Compute SHA256 of entire file
2. POST /lfs/batch with {sha256, size} → get presigned S3 URLs
3. Upload to those URLs
4. Commit references files by SHA256

This means we buffer to temp file while computing hash, then upload.

### Reference Implementations
- `pyspark_huggingface`: https://github.com/huggingface/pyspark_huggingface
  - Key file: `huggingface_sink.py` - shows preupload + atomic commit pattern
- `datasets` library: `/Users/davanstrien/Documents/code/datasets`
  - Key file: `src/datasets/iterable_dataset.py` - `_push_parquet_shards_to_hub_single()`
- `huggingface_hub`: `/Users/davanstrien/Documents/code/huggingface_hub`
  - Key file: `src/huggingface_hub/lfs.py` - LFS upload implementation
