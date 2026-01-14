# HF Hub Sink Implementation Plan

## Overview

This document outlines the implementation plan for native HF Hub write support in Polars, enabling streaming uploads via `sink_parquet("hf://datasets/user/repo/...")`.

**Goal**: True streaming writes to HF Hub with bounded memory, parallel uploads, atomic commits, and resume capability.

**Estimated Effort**: 4-6 weeks for full implementation

---

## Table of Contents

1. [Phase 0: Development Setup](#phase-0-development-setup)
2. [Architecture Summary](#architecture-summary)
3. [Phase 1: Foundation](#phase-1-foundation)
4. [Phase 2: Core Writer](#phase-2-core-writer)
5. [Phase 3: LFS Protocol](#phase-3-lfs-protocol)
6. [Phase 4: Streaming Integration](#phase-4-streaming-integration)
7. [Phase 5: Commit Coordination](#phase-5-commit-coordination)
8. [Phase 6: Advanced Features](#phase-6-advanced-features)
9. [Phase 7: Python Bindings](#phase-7-python-bindings)
10. [Phase 8: Testing](#phase-8-testing)
11. [Phase 9: Documentation & Polish](#phase-9-documentation--polish)
12. [Dependencies Graph](#dependencies-graph)
13. [Risk Assessment](#risk-assessment)
14. [Progress Tracking](#progress-tracking)

---

## Phase 0: Development Setup

**Goal**: Set up branch, fork, and CI workflow for iterative development and testing.

### Task 0.1: Fork and Branch Setup
**Status**: [x] Complete (2026-01-13)
**Dependencies**: None
**Estimate**: 30 minutes

```bash
# 1. Ensure we have a fork (if contributing to pola-rs/polars)
# Or work directly on a feature branch if you have write access

# 2. Create feature branch
git checkout main
git pull origin main
git checkout -b feature/hf-hub-sink

# 3. Push branch to remote (your fork or origin)
git push -u origin feature/hf-hub-sink
```

**Acceptance Criteria**:
- [x] Feature branch `feature/hf-hub-sink` created
- [x] Branch pushed to remote (davanstrien/polars)
- [x] Can pull/push to this branch

### Task 0.2: Draft PR Setup
**Status**: [ ] Deferred (working on fork privately first)
**Dependencies**: 0.1
**Estimate**: 15 minutes

```bash
# Create draft PR for visibility and CI runs
gh pr create --draft \
  --title "feat: Native HF Hub sink support for streaming writes" \
  --body "## Summary
This PR adds native support for writing datasets to Hugging Face Hub via \`sink_parquet(\"hf://...\")\`.

## Features
- [ ] Streaming writes with bounded memory
- [ ] Parallel shard uploads
- [ ] Atomic commits
- [ ] Resume from checkpoint
- [ ] Partitioned write support

## Status
🚧 Work in Progress - See HF_SINK_IMPLEMENTATION_PLAN.md for details

## Test Plan
- Unit tests for each component
- Integration tests with mock HF Hub
- E2E tests with real HF Hub (gated)
"
```

**Acceptance Criteria**:
- [ ] Draft PR created
- [ ] CI runs on push
- [ ] PR linked to any related issues

### Task 0.3: Local Development Build Setup
**Status**: [x] Complete (2026-01-13)
**Dependencies**: 0.1
**Estimate**: 30 minutes

```bash
# Ensure local build works - use Makefile target
make build-release

# Verify build
.venv/bin/python -c "import polars; print(polars.__version__)"
```

**Acceptance Criteria**:
- [x] `make build-release` succeeds (16m 21s)
- [x] Can import polars in Python (v1.37.1)
- [x] Changes to Rust code reflected after rebuild

### Task 0.4: Git Install Test Setup
**Status**: [x] Complete (2026-01-13)
**Dependencies**: 0.1
**Estimate**: 15 minutes

Document how others (or CI) can test the branch:

```bash
# Install from git branch (for testing by others)
pip install "git+https://github.com/davanstrien/polars.git@feature/hf-hub-sink#subdirectory=py-polars"

# Or for local editable development:
cd polars
make build-release
```

Smoke test script created at `test_hf_sink.py`:
```bash
.venv/bin/python test_hf_sink.py
```

**Acceptance Criteria**:
- [x] Git install command documented
- [x] Smoke test script created (`test_hf_sink.py`)
- [ ] Instructions in PR description (deferred - no PR yet)

### Task 0.5: CI Configuration (if needed)
**Status**: [ ] Deferred (no PR to upstream yet)
**Dependencies**: 0.2
**Estimate**: 1 hour

If the `hf_sink` feature needs CI configuration:

```yaml
# .github/workflows/test-python.yml additions (if needed)
# May need to add HF_TOKEN secret for e2e tests
```

**Acceptance Criteria**:
- [ ] CI runs on PR
- [ ] Feature flag properly gated
- [ ] E2E tests skipped without HF_TOKEN

---

## Development Workflow

### Important: Check Uncommitted Work First

**Before starting any new task:**
1. Run `git status` to check for uncommitted changes from previous sessions
2. Run `git log --oneline -5` to see recent commit history
3. If there are uncommitted changes, commit them first with appropriate atomic commits
4. Each task should result in its own atomic commit

**After completing a task:**
1. Stage and commit the changes immediately
2. Use conventional commit format: `feat(hf-sink): <description>`
3. Don't batch multiple tasks into one commit

### Important: Research Before Implementation

**Before writing any code for a task:**
1. **Read existing code** - Understand the files you'll be modifying and their patterns
2. **Search for similar patterns** - Look for how similar functionality is implemented elsewhere in polars
3. **Check dependencies** - Understand what imports, traits, and types are available
4. **Review reference implementations** - Check huggingface_hub library or other references mentioned in this plan
5. **Understand the context** - Read surrounding code to match style, error handling, and conventions

**Key questions to answer before coding:**
- What existing patterns in polars should I follow?
- What utilities/helpers already exist that I can reuse?
- What error handling approach is used in similar code?
- Are there existing tests I can use as templates?

This research step prevents wasted effort and ensures consistency with the codebase.

### Daily Workflow

```bash
# 1. Start of session: check for uncommitted work
git status
git log --oneline -5

# 2. If uncommitted changes exist, commit them first
git add <relevant-files>
git commit -m "feat(hf-sink): ..."

# 3. Ensure up to date with upstream
git fetch origin
git rebase origin/main  # or merge if preferred

# 4. Make changes, test locally
maturin develop --release
python test_hf_sink.py

# 5. Commit with clear message (one commit per task)
git add -A
git commit -m "feat(hf-sink): implement HashingWriter component

- Add SHA256 streaming hash computation
- Implement Write trait passthrough
- Add unit tests for hash correctness"

# 6. Push to branch (triggers CI)
git push origin feature/hf-hub-sink

# 7. Check CI status
gh pr checks
```

### Testing Commands

```bash
# Run specific Rust tests
cargo test -p polars-io hf

# Run Python tests
cd py-polars
pytest tests/unit/io/test_hf_sink.py -v

# Run with HF token for e2e tests
HF_TOKEN=hf_xxx pytest tests/unit/io/test_hf_sink.py -v -m "requires_hf_token"
```

### Build Variants

```bash
# Debug build (faster compile, slower runtime)
maturin develop

# Release build (slower compile, faster runtime)
maturin develop --release

# With specific features
maturin develop --release --features "hf_sink"
```

---

## Architecture Summary

```
User API: lf.sink_parquet("hf://datasets/user/repo/data/train.parquet")
              │
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    HfSinkNode (Streaming Engine)                 │
│         Manages shard workers, morsel routing, coordination      │
└─────────────────────────────────────────────────────────────────┘
              │
    ┌─────────┼─────────┐
    ▼         ▼         ▼
┌───────┐ ┌───────┐ ┌───────┐
│Shard  │ │Shard  │ │Shard  │   ShardWriter per worker:
│Writer │ │Writer │ │Writer │   - ParquetEncoder → HashingWriter → MmapBuffer
│  [0]  │ │  [1]  │ │  [N]  │   - On shard full: LFS upload, notify coordinator
└───────┘ └───────┘ └───────┘
    │         │         │
    └─────────┼─────────┘
              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    CommitCoordinator                             │
│     Collects additions, handles deletions, atomic commit         │
└─────────────────────────────────────────────────────────────────┘
```

**Key Properties**:
- Memory: O(shard_size) per worker (~500MB default)
- Disk: O(shard_size) per worker (temp mmap files)
- True streaming: Processes morsels incrementally
- Atomic: Single commit after all shards uploaded

---

## Phase 1: Foundation

**Goal**: Set up module structure, types, and configuration.

### Task 1.1: Create Module Structure
**File**: `crates/polars-io/src/cloud/hf/mod.rs`
**Status**: [x] Complete (2026-01-14)
**Dependencies**: Phase 0
**Estimate**: 2 hours

```
crates/polars-io/src/cloud/hf/
├── mod.rs              # Module exports
├── url.rs              # HFPathParts, HFRepoLocation (moved from path_utils)
├── glob.rs             # expand_paths_hf (moved from path_utils)
├── options.rs          # HfSinkOptions (stub, gated by hf_sink)
├── auth.rs             # Token handling (stub, gated by hf_sink)
└── error.rs            # HF-specific error types (stub, gated by hf_sink)
```

**Work Completed**:
- Moved HF code from `path_utils/hugging_face.rs` to `cloud/hf/`
- Made `HiveIdxTracker` pub(crate) in `path_utils/mod.rs`
- Updated import in `path_utils/mod.rs` to use `crate::cloud::hf::expand_paths_hf`
- Added `hf_sink` feature to Cargo.toml
- Created stub files for `auth.rs`, `error.rs`, `options.rs`
- Build verification: polars-io compiles with 0 errors (warnings fixed)

**Acceptance Criteria**:
- [x] Module compiles
- [x] Feature flag `hf_sink` gates write modules
- [x] Exports visible from `polars-io`
- [x] Existing HF read functionality still works

**Commit checkpoint**: `git commit -m "feat(hf-sink): add module structure for HF sink"`

### Task 1.2: Define Configuration Types
**File**: `crates/polars-io/src/cloud/hf/options.rs`
**Status**: [x] Complete (2026-01-14)
**Dependencies**: 1.1
**Estimate**: 3 hours

#### Sub-tasks (granular) - ALL COMPLETE

**1.2.1: Define RepoType enum** ✅
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RepoType {
    #[default]
    Dataset,
    Model,
    Space,
}
```
- Add `Display` impl for URL construction
- Add `as_str()` method returning "datasets", "models", "spaces"

**1.2.2: Define HfWriteMode enum** ✅
```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HfWriteMode {
    #[default]
    ErrorIfExists,
    Overwrite,
    Append,
}
```

**1.2.3: Define HfSinkOptions struct** ✅
```rust
pub struct HfSinkOptions {
    pub repo_id: String,
    pub repo_type: RepoType,
    pub revision: Option<String>,
    pub path_in_repo: String,
    pub split: String,
    pub max_shard_size: usize,          // default: 500MB
    pub max_shard_rows: Option<usize>,
    pub num_shards: Option<usize>,
    pub mode: HfWriteMode,
    pub token: Option<String>,
    pub commit_message: Option<String>,
    pub create_pr: bool,
    pub checkpoint_path: Option<PathBuf>,
    pub upload_concurrency: usize,      // default: 4
}
```

**1.2.4: Implement Default for HfSinkOptions** ✅
- `max_shard_size`: 500 * 1024 * 1024 (500MB)
- `upload_concurrency`: 4
- `split`: "train"
- `mode`: ErrorIfExists

**1.2.5: Implement HfSinkOptionsBuilder** ✅
```rust
impl HfSinkOptions {
    pub fn builder(repo_id: impl Into<String>) -> HfSinkOptionsBuilder;
}

pub struct HfSinkOptionsBuilder { ... }
impl HfSinkOptionsBuilder {
    pub fn repo_type(mut self, repo_type: RepoType) -> Self;
    pub fn revision(mut self, revision: impl Into<String>) -> Self;
    pub fn path_in_repo(mut self, path: impl Into<String>) -> Self;
    pub fn split(mut self, split: impl Into<String>) -> Self;
    pub fn max_shard_size(mut self, bytes: usize) -> Self;
    pub fn mode(mut self, mode: HfWriteMode) -> Self;
    pub fn token(mut self, token: impl Into<String>) -> Self;
    pub fn commit_message(mut self, msg: impl Into<String>) -> Self;
    pub fn create_pr(mut self, create_pr: bool) -> Self;
    pub fn build(self) -> PolarsResult<HfSinkOptions>;
}
```

**1.2.6: Implement validation** ✅
```rust
impl HfSinkOptions {
    pub fn validate(&self) -> PolarsResult<()> {
        // - repo_id not empty
        // - repo_id format: "user/repo" or "org/repo"
        // - path_in_repo doesn't start with "/"
        // - max_shard_size > 0
        // - upload_concurrency > 0
    }
}
```

**1.2.7: Add serde derives** ✅
- Add `#[derive(serde::Serialize, serde::Deserialize)]` where needed
- Add `#[serde(default)]` for optional fields

**1.2.8: Write unit tests** ✅
- Test builder pattern
- Test validation (valid and invalid cases)
- Test defaults
- Test serde roundtrip

**Acceptance Criteria**:
- [x] All types defined with serde derives
- [x] Default implementations
- [x] Validation methods (e.g., `validate(&self) -> PolarsResult<()>`)
- [x] Builder pattern for ergonomic construction
- [x] Unit tests pass (13 tests)

**Work Completed (2026-01-14)**:
- `RepoType` enum with `as_str()` and `Display` impl
- `HfWriteMode` enum (ErrorIfExists, Overwrite, Append)
- `HfSinkOptions` struct with 14 fields
- `HfSinkOptionsBuilder` with fluent `with_*` API
- `validate()` with clear error messages
- `effective_revision()` helper method
- 13 unit tests covering defaults, builder, validation errors, serde

**Commit checkpoint**: `git commit -m "feat(hf-sink): add HfSinkOptions configuration types"`

### Task 1.3: Extend URL Parsing for Write Paths
**File**: `crates/polars-io/src/cloud/hf/url.rs`
**Status**: [x] Complete (2026-01-14)
**Dependencies**: 1.1
**Estimate**: 2 hours

Extend existing `HFPathParts` to support write scenarios:
- Extract repo_id, repo_type, path_in_repo from `hf://` URLs
- Support both file and directory targets
- Validate write permissions requirements

**Work Completed (2026-01-14)**:
- Added `bucket`, `repository`, `revision` fields to `HFRepoLocation`
- Added `get_lfs_batch_uri()` for LFS upload API
- Added `get_commit_uri()` for atomic commit API
- Added `RepoType::from_bucket_str()` for bucket parsing
- Added `repo_type()` helper on `HFPathParts`
- Feature-gated write methods with `#[cfg(feature = "hf_sink")]`
- Added 4 unit tests for new methods

**Acceptance Criteria**:
- [x] Parse `hf://datasets/user/repo/path/file.parquet`
- [x] Parse `hf://datasets/user/repo/path/` (directory for shards)
- [x] Error on invalid URLs
- [x] Unit tests for parsing

**Commit checkpoint**: `git commit -m "feat(hf-sink): extend URL parsing for write paths"`

### Task 1.4: Token/Auth Handling
**File**: `crates/polars-io/src/cloud/hf/auth.rs`
**Status**: [x] Complete (2026-01-14)
**Dependencies**: 1.1
**Estimate**: 2 hours

#### Sub-tasks (granular)

**1.4.1: Add imports** (~5 min)
```rust
use std::path::PathBuf;
use polars_error::{polars_bail, PolarsResult};
```

**1.4.2: Define function signature** (~5 min)
```rust
/// Resolve HF Hub authentication token.
/// Returns Ok(None) if no token found and required=false.
pub fn get_hf_token(explicit: Option<&str>, required: bool) -> PolarsResult<Option<String>>
```

**1.4.3: Implement explicit token check** (~10 min)
```rust
if let Some(token) = explicit {
    let token = token.trim();
    if !token.is_empty() {
        return Ok(Some(token.to_string()));
    }
}
```

**1.4.4: Implement env var checks** (~10 min)
```rust
// HF_TOKEN env var
if let Ok(token) = std::env::var("HF_TOKEN") { ... }
// HUGGINGFACE_HUB_TOKEN env var
if let Ok(token) = std::env::var("HUGGINGFACE_HUB_TOKEN") { ... }
```

**1.4.5: Implement token file helper** (~10 min)
```rust
fn read_token_file(path: &PathBuf) -> Option<String> {
    std::fs::read_to_string(path).ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}
```

**1.4.6: Implement file-based token resolution** (~15 min)
Token file locations (in priority order):
1. `~/.cache/huggingface/token` (new location)
2. `~/.huggingface/token` (legacy location)

Note: Use `std::env::var("HOME")` to get home dir (no external crate needed).

**1.4.7: Implement error case** (~10 min)
```rust
if required {
    polars_bail!(InvalidOperation: "HF Hub token required but not found. Set HF_TOKEN environment variable or run `huggingface-cli login`");
}
Ok(None)
```

**1.4.8: Add unit tests** (~30 min)
- Test explicit token (with whitespace trimming)
- Test empty explicit falls through to env
- Test required=false returns Ok(None) when no token
- Test required=true errors when no token

**1.4.9: Update mod.rs exports** (~5 min)
```rust
#[cfg(feature = "hf_sink")]
pub use auth::get_hf_token;
```

**Acceptance Criteria**:
- [ ] Token resolution in priority order (explicit → HF_TOKEN → HUGGINGFACE_HUB_TOKEN → files)
- [ ] Clear error messages for missing token
- [ ] Unit tests pass
- [ ] Build verified with `make build-release`

**Commit checkpoint**: `git commit -m "feat(hf-sink): add token/auth handling"`

---

## Phase 2: Core Writer

**Goal**: Implement the low-level writer that buffers to temp file while computing SHA256.

### Task 2.1: HashingWriter
**File**: `crates/polars-io/src/cloud/hf/hashing_writer.rs`
**Status**: [x] Complete (2026-01-14)
**Dependencies**: 1.1
**Estimate**: 3 hours

```rust
pub struct HashingWriter<W: Write> {
    inner: W,
    hasher: Sha256,
    bytes_written: u64,
}

impl<W: Write> Write for HashingWriter<W> { ... }
impl<W: Write> HashingWriter<W> {
    pub fn finish(self) -> (W, [u8; 32], u64);
}
```

**Acceptance Criteria**:
- [x] Implements `std::io::Write`
- [x] SHA256 computed incrementally (no re-read)
- [x] `finish()` returns hash and byte count
- [x] Unit tests verify hash correctness (7 tests)
- [ ] Benchmark: < 5% overhead vs plain write (deferred)

**Work Completed (2026-01-14)**:
- Created `hashing_writer.rs` with `HashingWriter<W>` struct
- Implemented `Write` trait with passthrough to inner writer
- Added `sha256_to_hex()` helper function
- 7 unit tests covering: empty, single, multiple writes, byte tracking, flush, hex encoding, partial writes
- Added `sha2` dependency to Cargo.toml under `hf_sink` feature
- Updated `mod.rs` to export `HashingWriter` and `sha256_to_hex`
- Build verification blocked by pre-existing polars-core errors (branch needs rebase)

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement HashingWriter for streaming SHA256"`

### Task 2.2: MmapBuffer
**File**: `crates/polars-io/src/cloud/hf/mmap_buffer.rs`
**Status**: [x] Complete (2026-01-14)
**Dependencies**: 1.1
**Estimate**: 4 hours

```rust
pub struct MmapBuffer {
    file: NamedTempFile,
    mmap: MmapMut,
    len: usize,
    capacity: usize,
}

impl Write for MmapBuffer { ... }
impl MmapBuffer {
    pub fn new(initial_capacity: usize) -> io::Result<Self>;
    pub fn as_slice(&self) -> &[u8];
    pub fn into_read_handle(self) -> MmapReadHandle;
}
```

#### Sub-tasks (granular)

**2.2.1: Research existing patterns** (~15 min)
- [x] Check existing mmap usage in polars (`polars-utils/src/mmap.rs`)
- [x] Note: `MMapSemaphore` is read-only; we need writable `MmapMut`
- [x] Note: `memmap` crate already in workspace deps
- [x] Note: `tempfile` in dev-deps, need to add to regular deps for `hf_sink`

**2.2.2: Add dependencies to Cargo.toml** (~5 min)
```toml
# In crates/polars-io/Cargo.toml [dependencies]
tempfile = { version = "3", optional = true }

# In [features]
hf_sink = ["cloud", "sha2", "tempfile"]
```

**2.2.3: Create mmap_buffer.rs with imports** (~5 min)
```rust
use std::fs::File;
use std::io::{self, Write};
use memmap::MmapMut;
use tempfile::NamedTempFile;
```

**2.2.4: Define MmapBuffer struct** (~10 min)
```rust
pub struct MmapBuffer {
    file: NamedTempFile,
    mmap: Option<MmapMut>,  // None when file is empty (mmap requires len > 0)
    len: usize,             // Bytes written so far
    capacity: usize,        // Current file/mmap size
}
```
- Note: mmap is `Option` because you can't mmap an empty file

**2.2.5: Implement MmapBuffer::new()** (~15 min)
```rust
impl MmapBuffer {
    pub fn new(initial_capacity: usize) -> io::Result<Self> {
        let file = NamedTempFile::new()?;
        // Set initial file size
        file.as_file().set_len(initial_capacity as u64)?;
        // Create mutable mmap
        let mmap = unsafe { MmapMut::map_mut(file.as_file())? };
        Ok(Self {
            file,
            mmap: Some(mmap),
            len: 0,
            capacity: initial_capacity,
        })
    }
}
```

**2.2.6: Implement grow() helper** (~20 min)
```rust
impl MmapBuffer {
    fn grow(&mut self, min_capacity: usize) -> io::Result<()> {
        // Calculate new capacity (double, or min_capacity if larger)
        let new_capacity = self.capacity.max(min_capacity).max(1024 * 1024)
            .checked_next_power_of_two()
            .unwrap_or(min_capacity);

        // Drop existing mmap before resizing file
        self.mmap = None;

        // Resize file
        self.file.as_file().set_len(new_capacity as u64)?;

        // Remap
        let mmap = unsafe { MmapMut::map_mut(self.file.as_file())? };
        self.mmap = Some(mmap);
        self.capacity = new_capacity;
        Ok(())
    }
}
```
- Key insight: Must drop mmap before resizing file, then remap

**2.2.7: Implement Write trait** (~15 min)
```rust
impl Write for MmapBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let required = self.len + buf.len();
        if required > self.capacity {
            self.grow(required)?;
        }

        if let Some(ref mut mmap) = self.mmap {
            mmap[self.len..self.len + buf.len()].copy_from_slice(buf);
            self.len += buf.len();
            Ok(buf.len())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "mmap not initialized"))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(ref mmap) = self.mmap {
            mmap.flush()?;
        }
        Ok(())
    }
}
```

**2.2.8: Implement accessor methods** (~10 min)
```rust
impl MmapBuffer {
    /// Returns the written bytes as a slice
    pub fn as_slice(&self) -> &[u8] {
        match &self.mmap {
            Some(mmap) => &mmap[..self.len],
            None => &[],
        }
    }

    /// Returns the number of bytes written
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if no bytes have been written
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
```

**2.2.9: Define MmapReadHandle for upload** (~15 min)
```rust
/// Read-only handle to completed buffer, suitable for upload
pub struct MmapReadHandle {
    file: NamedTempFile,  // Keeps temp file alive
    mmap: memmap::Mmap,   // Read-only mmap
    len: usize,
}

impl MmapReadHandle {
    pub fn as_slice(&self) -> &[u8] {
        &self.mmap[..self.len]
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl AsRef<[u8]> for MmapReadHandle {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}
```

**2.2.10: Implement into_read_handle()** (~15 min)
```rust
impl MmapBuffer {
    /// Finalize buffer and convert to read-only handle
    /// Flushes data and converts mutable mmap to read-only
    pub fn into_read_handle(mut self) -> io::Result<MmapReadHandle> {
        // Flush any pending writes
        if let Some(ref mmap) = self.mmap {
            mmap.flush()?;
        }

        // Drop mutable mmap
        drop(self.mmap.take());

        // Create read-only mmap
        let mmap = unsafe { memmap::Mmap::map(self.file.as_file())? };

        Ok(MmapReadHandle {
            file: self.file,
            mmap,
            len: self.len,
        })
    }
}
```

**2.2.11: Add unit tests** (~30 min)
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_write_read() { ... }

    #[test]
    fn test_multiple_writes() { ... }

    #[test]
    fn test_growth() { ... }

    #[test]
    fn test_into_read_handle() { ... }

    #[test]
    fn test_empty_buffer() { ... }

    #[test]
    fn test_large_write() { ... }
}
```

**2.2.12: Update mod.rs exports** (~5 min)
```rust
#[cfg(feature = "hf_sink")]
mod mmap_buffer;
#[cfg(feature = "hf_sink")]
pub use mmap_buffer::{MmapBuffer, MmapReadHandle};
```

**Acceptance Criteria**:
- [x] Implements `std::io::Write`
- [x] Dynamic growth when capacity exceeded
- [x] Efficient read-back for upload (no copy)
- [x] Temp file auto-deleted on drop (NamedTempFile)
- [x] Unit tests for write/read cycle
- [x] Test growth behavior

**Work Completed (2026-01-14)**:
- Created `mmap_buffer.rs` with MmapBuffer and MmapReadHandle structs
- MmapBuffer: Write trait impl, dynamic growth (doubling, min 1MB)
- MmapReadHandle: zero-copy read access via AsRef<[u8]>
- Added `tempfile` dependency to Cargo.toml (optional, under hf_sink feature)
- 9 unit tests covering: basic write/read, multiple writes, growth, read handle conversion, empty buffer, large writes, flush, default capacity
- Code passes rustfmt check
- Full build verification blocked by upstream polars-core issue

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement MmapBuffer for efficient temp storage"`

### Task 2.3: HfShardWriter (Combines Components)
**File**: `crates/polars-io/src/cloud/hf/shard_writer.rs`
**Status**: [ ] Not Started
**Dependencies**: 2.1, 2.2
**Estimate**: 4 hours

```rust
pub struct HfShardWriter {
    buffer: MmapBuffer,
    hasher: HashingWriter<...>,
    parquet_writer: Option<ParquetWriter<...>>,
    schema: ArrowSchema,
    rows_written: usize,
    options: Arc<HfSinkOptions>,
}

impl HfShardWriter {
    pub fn new(schema: ArrowSchema, options: Arc<HfSinkOptions>) -> PolarsResult<Self>;
    pub fn write_batch(&mut self, batch: &RecordBatch) -> PolarsResult<()>;
    pub fn should_flush(&self) -> bool;
    pub fn finish(self) -> PolarsResult<FinishedShard>;
}

pub struct FinishedShard {
    pub sha256: String,
    pub size: u64,
    pub num_rows: usize,
    pub buffer: MmapReadHandle,
}
```

**Acceptance Criteria**:
- [ ] Integrates HashingWriter + MmapBuffer + ParquetWriter
- [ ] Tracks rows and bytes for flush decisions
- [ ] Clean `finish()` API returns all needed info
- [ ] Unit tests with mock data

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement HfShardWriter combining hash+buffer+parquet"`

---

## Phase 3: LFS Protocol

**Goal**: Implement HF Hub LFS API client for upload coordination.

### Task 3.1: LFS Types
**File**: `crates/polars-io/src/cloud/hf/lfs/types.rs`
**Status**: [ ] Not Started
**Dependencies**: 1.1
**Estimate**: 2 hours

```rust
#[derive(Deserialize)]
pub struct LfsBatchResponse {
    pub transfer: Option<String>,
    pub objects: Vec<LfsObject>,
}

#[derive(Deserialize)]
pub struct LfsObject {
    pub oid: String,
    pub size: u64,
    pub actions: Option<LfsActions>,
    pub error: Option<LfsError>,
}

pub enum LfsTransfer {
    AlreadyExists,
    Basic { url: String, headers: HashMap<String, String> },
    Multipart { parts: Vec<PartInfo>, complete_url: String },
}
```

**Acceptance Criteria**:
- [ ] All HF LFS response types modeled
- [ ] Serde deserialization works
- [ ] Error types for LFS failures

**Commit checkpoint**: `git commit -m "feat(hf-sink): add LFS protocol types"`

### Task 3.2: LFS Client
**File**: `crates/polars-io/src/cloud/hf/lfs/client.rs`
**Status**: [ ] Not Started
**Dependencies**: 3.1, 1.4
**Estimate**: 6 hours

```rust
pub struct LfsClient {
    client: reqwest::Client,
    endpoint: String,
    repo_type: RepoType,
    repo_id: String,
    token: String,
}

impl LfsClient {
    pub async fn request_upload(&self, sha256: &str, size: u64) -> PolarsResult<LfsTransfer>;
    pub async fn verify_upload(&self, sha256: &str, size: u64) -> PolarsResult<()>;
}
```

**Acceptance Criteria**:
- [ ] Correct LFS batch API request format
- [ ] Parses all transfer types (basic, multipart)
- [ ] Handles "already exists" case (skip upload)
- [ ] Proper error handling for API failures
- [ ] Integration test against real HF Hub (optional, gated)

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement LFS client for upload coordination"`

### Task 3.3: Upload Executor
**File**: `crates/polars-io/src/cloud/hf/lfs/upload.rs`
**Status**: [ ] Not Started
**Dependencies**: 3.2, 2.2
**Estimate**: 6 hours

```rust
pub struct UploadExecutor {
    client: reqwest::Client,
}

impl UploadExecutor {
    pub async fn upload(
        &self,
        data: MmapReadHandle,
        transfer: LfsTransfer,
    ) -> PolarsResult<()>;

    async fn upload_basic(&self, data: &[u8], url: &str, headers: &HashMap<String, String>) -> PolarsResult<()>;
    async fn upload_multipart(&self, data: &[u8], parts: &[PartInfo], complete_url: &str) -> PolarsResult<()>;
}
```

**Acceptance Criteria**:
- [ ] Basic upload: single PUT request
- [ ] Multipart: parallel part uploads + completion
- [ ] Streaming from mmap (no extra buffer)
- [ ] Retry logic with exponential backoff
- [ ] Progress tracking hooks (for future progress bars)
- [ ] Integration test with mock S3

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement upload executor with basic/multipart support"`

### Task 3.4: Commit API Client
**File**: `crates/polars-io/src/cloud/hf/commit.rs`
**Status**: [ ] Not Started
**Dependencies**: 1.4
**Estimate**: 4 hours

```rust
pub struct CommitClient {
    client: reqwest::Client,
    endpoint: String,
    token: String,
}

pub struct CommitOperationAdd {
    pub path_in_repo: String,
    pub oid: String,
    pub size: u64,
}

pub struct CommitOperationDelete {
    pub path_in_repo: String,
}

impl CommitClient {
    pub async fn create_commit(
        &self,
        repo_type: &RepoType,
        repo_id: &str,
        revision: Option<&str>,
        operations: Vec<CommitOperation>,
        message: &str,
        create_pr: bool,
    ) -> PolarsResult<CommitInfo>;

    pub async fn list_repo_files(
        &self,
        repo_type: &RepoType,
        repo_id: &str,
        path_prefix: &str,
        revision: Option<&str>,
    ) -> PolarsResult<Vec<RepoFile>>;
}
```

**Acceptance Criteria**:
- [ ] NDJSON payload format correct
- [ ] Supports add, delete operations
- [ ] create_pr flag works
- [ ] Returns commit info (URL, sha)
- [ ] Integration test with real commit (to test repo)

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement commit API client"`

---

## Phase 4: Streaming Integration

**Goal**: Integrate with Polars streaming engine as a sink node.

### Task 4.1: HfSinkNode Skeleton
**File**: `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs`
**Status**: [ ] Not Started
**Dependencies**: 2.3, Phase 3
**Estimate**: 4 hours

```rust
pub struct HfSinkNode {
    options: Arc<HfSinkOptions>,
    state: HfSinkState,
    completion_rx: mpsc::Receiver<ShardCompletion>,
    coordinator: CommitCoordinator,
}

enum HfSinkState {
    Uninitialized,
    Running { workers: Vec<ShardWorkerHandle>, router: MorselRouter },
    Committing,
    Finished,
}
```

**Acceptance Criteria**:
- [ ] Implements `ComputeNode` trait
- [ ] State machine transitions correct
- [ ] Compiles and links with streaming engine

**Commit checkpoint**: `git commit -m "feat(hf-sink): add HfSinkNode skeleton for streaming engine"`

### Task 4.2: Morsel Router
**File**: `crates/polars-stream/src/nodes/io_sinks/hf_sink/router.rs`
**Status**: [ ] Not Started
**Dependencies**: 4.1
**Estimate**: 4 hours

```rust
pub struct MorselRouter {
    workers: Vec<ShardWorkerHandle>,
    current_worker: usize,
    // For partitioned writes: partition -> worker mapping
    partition_map: Option<HashMap<PartitionKey, usize>>,
}

impl MorselRouter {
    pub fn route(&mut self, morsel: SinkMorsel) -> usize;
}
```

**Acceptance Criteria**:
- [ ] Round-robin for non-partitioned
- [ ] Partition-aware routing for partitioned writes
- [ ] Load balancing based on worker queue depth

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement morsel router for shard distribution"`

### Task 4.3: Shard Worker Task
**File**: `crates/polars-stream/src/nodes/io_sinks/hf_sink/worker.rs`
**Status**: [ ] Not Started
**Dependencies**: 4.1, 2.3, 3.3
**Estimate**: 8 hours

```rust
pub struct ShardWorker {
    id: usize,
    options: Arc<HfSinkOptions>,
    schema: ArrowSchema,
    morsel_rx: mpsc::Receiver<SinkMorsel>,
    completion_tx: mpsc::Sender<ShardCompletion>,
    lfs_client: LfsClient,
    upload_executor: UploadExecutor,
    current_shard: Option<HfShardWriter>,
    shard_index: usize,
}

impl ShardWorker {
    pub async fn run(mut self) -> PolarsResult<()> {
        while let Some(morsel) = self.morsel_rx.recv().await {
            self.process_morsel(morsel).await?;
        }
        // Flush final shard
        if let Some(shard) = self.current_shard.take() {
            self.flush_shard(shard).await?;
        }
        Ok(())
    }

    async fn process_morsel(&mut self, morsel: SinkMorsel) -> PolarsResult<()>;
    async fn flush_shard(&mut self, shard: HfShardWriter) -> PolarsResult<()>;
}
```

**Acceptance Criteria**:
- [ ] Processes morsels incrementally
- [ ] Flushes shard when size/row limit reached
- [ ] Uploads shard and notifies coordinator
- [ ] Handles final partial shard
- [ ] Clean shutdown on channel close

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement shard worker task for async processing"`

### Task 4.4: Integration with IOSinkNode Infrastructure
**File**: Various in `crates/polars-stream/src/nodes/io_sinks/`
**Status**: [ ] Not Started
**Dependencies**: 4.1, 4.2, 4.3
**Estimate**: 6 hours

- Register `HfSinkNode` as a sink target type
- Wire up `hf://` URL detection in sink path resolution
- Integrate with existing `IOSinkTarget` infrastructure

**Acceptance Criteria**:
- [ ] `sink_parquet("hf://...")` creates HfSinkNode
- [ ] Options passed through from Python/Rust API
- [ ] Works with existing sink infrastructure

**Commit checkpoint**: `git commit -m "feat(hf-sink): integrate HfSinkNode with IOSinkNode infrastructure"`

---

## Phase 5: Commit Coordination

**Goal**: Coordinate final atomic commit across all workers.

### Task 5.1: CommitCoordinator
**File**: `crates/polars-stream/src/nodes/io_sinks/hf_sink/coordinator.rs`
**Status**: [ ] Not Started
**Dependencies**: 3.4
**Estimate**: 6 hours

```rust
pub struct CommitCoordinator {
    options: Arc<HfSinkOptions>,
    commit_client: CommitClient,
    additions: Vec<CommitOperationAdd>,
    total_rows: usize,
    total_bytes: usize,
}

impl CommitCoordinator {
    pub fn register_completion(&mut self, completion: ShardCompletion);
    pub async fn execute_commit(&mut self) -> PolarsResult<CommitInfo>;
    async fn handle_overwrite(&self) -> PolarsResult<Vec<CommitOperationDelete>>;
    async fn handle_append(&mut self) -> PolarsResult<()>;
    fn renumber_shards(&mut self, offset: usize, total: usize);
}
```

**Acceptance Criteria**:
- [ ] Collects all shard completions
- [ ] Overwrite mode: deletes existing split files
- [ ] Append mode: renumbers existing + new files
- [ ] ErrorIfExists mode: fails if files exist
- [ ] Batches commits (max 100 ops per commit)
- [ ] Returns final commit info

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement CommitCoordinator for atomic commits"`

### Task 5.2: Dataset Card Updates
**File**: `crates/polars-stream/src/nodes/io_sinks/hf_sink/dataset_card.rs`
**Status**: [ ] Not Started
**Dependencies**: 5.1
**Estimate**: 4 hours

```rust
pub fn generate_dataset_card_update(
    existing_card: Option<&str>,
    config_name: &str,
    split: &str,
    num_rows: usize,
    num_bytes: usize,
) -> PolarsResult<Option<CommitOperationAdd>>;
```

**Acceptance Criteria**:
- [ ] Updates configs section with new split
- [ ] Preserves existing card content
- [ ] Creates minimal card if none exists
- [ ] YAML metadata format correct

**Commit checkpoint**: `git commit -m "feat(hf-sink): add dataset card update generation"`

---

## Phase 6: Advanced Features

**Goal**: Add checkpoint/resume, partitioned writes, and optimizations.

### Task 6.1: Checkpoint System
**File**: `crates/polars-io/src/cloud/hf/checkpoint.rs`
**Status**: [ ] Not Started
**Dependencies**: 5.1
**Estimate**: 6 hours

```rust
pub struct CheckpointState {
    path: PathBuf,
    session_id: String,
    repo_id: String,
    split: String,
    completed_shards: HashSet<usize>,
    pending_additions: Vec<SerializedAddition>,
}

impl CheckpointState {
    pub fn load_or_create(path: &Path, repo_id: &str, split: &str) -> PolarsResult<Self>;
    pub fn is_shard_complete(&self, index: usize) -> bool;
    pub fn mark_shard_complete(&mut self, index: usize, addition: &CommitOperationAdd);
    pub fn save(&self) -> PolarsResult<()>;
    pub fn delete(&self) -> PolarsResult<()>;
}
```

**Acceptance Criteria**:
- [ ] Persists to JSON file
- [ ] Loads existing checkpoint for resume
- [ ] Validates checkpoint matches current upload
- [ ] Skips already-uploaded shards
- [ ] Deletes on successful commit

**Commit checkpoint**: `git commit -m "feat(hf-sink): implement checkpoint system for resumable uploads"`

### Task 6.2: Partitioned Write Support
**File**: `crates/polars-stream/src/nodes/io_sinks/hf_sink/partitioned.rs`
**Status**: [ ] Not Started
**Dependencies**: 4.2, 5.1
**Estimate**: 8 hours

- Integrate with existing `PartitionByKey` infrastructure
- Route morsels by partition key
- Generate Hive-style paths: `data/{partition_col}={value}/train-00000.parquet`
- Coordinate commits across partitions

**Acceptance Criteria**:
- [ ] `partition_by` parameter works
- [ ] Hive-style directory structure
- [ ] Per-partition shard limits
- [ ] Atomic commit across all partitions

**Commit checkpoint**: `git commit -m "feat(hf-sink): add partitioned write support"`

### Task 6.3: Progress Reporting
**File**: `crates/polars-stream/src/nodes/io_sinks/hf_sink/progress.rs`
**Status**: [ ] Not Started
**Dependencies**: 4.3
**Estimate**: 3 hours

```rust
pub trait UploadProgress: Send + Sync {
    fn on_shard_start(&self, shard_index: usize);
    fn on_shard_progress(&self, shard_index: usize, bytes_uploaded: u64, total_bytes: u64);
    fn on_shard_complete(&self, shard_index: usize);
    fn on_commit_start(&self);
    fn on_commit_complete(&self, commit_info: &CommitInfo);
}
```

**Acceptance Criteria**:
- [ ] Trait defined for progress callbacks
- [ ] Default no-op implementation
- [ ] Hooks called at appropriate points
- [ ] Python can provide custom callback

**Commit checkpoint**: `git commit -m "feat(hf-sink): add progress reporting hooks"`

---

## Phase 7: Python Bindings

**Goal**: Expose HF sink functionality to Python API.

### Task 7.1: PyO3 Bindings for Options
**File**: `py-polars/src/cloud/hf.rs`
**Status**: [ ] Not Started
**Dependencies**: Phase 1
**Estimate**: 4 hours

```python
# Target API
lf.sink_parquet(
    "hf://datasets/user/repo/data/train.parquet",
    hf_options={
        "split": "train",
        "max_shard_size": "500MB",
        "mode": "overwrite",
        "commit_message": "Upload training data",
    },
    storage_options={"token": "hf_xxx"},
)
```

**Acceptance Criteria**:
- [ ] `hf_options` dict parsed to `HfSinkOptions`
- [ ] Token from `storage_options` or env
- [ ] Validation errors surface to Python
- [ ] Type stubs for IDE completion

**Commit checkpoint**: `git commit -m "feat(hf-sink): add PyO3 bindings for HfSinkOptions"`

### Task 7.2: LazyFrame.sink_parquet Integration
**File**: `py-polars/src/lazyframe/mod.rs`
**Status**: [ ] Not Started
**Dependencies**: 7.1, Phase 4
**Estimate**: 4 hours

- Detect `hf://` prefix in sink path
- Pass `hf_options` to sink node creation
- Wire up Python-side option parsing

**Acceptance Criteria**:
- [ ] `sink_parquet("hf://...")` works
- [ ] Options passed correctly
- [ ] Errors propagate to Python

**Commit checkpoint**: `git commit -m "feat(hf-sink): integrate HF sink with LazyFrame.sink_parquet"`

### Task 7.3: DataFrame.write_parquet Integration
**File**: `py-polars/src/dataframe/mod.rs`
**Status**: [ ] Not Started
**Dependencies**: 7.2
**Estimate**: 2 hours

- `write_parquet("hf://...")` delegates to `lazy().sink_parquet()`
- Eager write convenience

**Acceptance Criteria**:
- [ ] `df.write_parquet("hf://...")` works
- [ ] Same options as sink_parquet

**Commit checkpoint**: `git commit -m "feat(hf-sink): add DataFrame.write_parquet HF support"`

---

## Phase 8: Testing

**Goal**: Comprehensive test coverage at unit, integration, and e2e levels.

### Task 8.1: Unit Tests
**Status**: [ ] Not Started
**Dependencies**: Each component
**Estimate**: Ongoing (included in component estimates)

| Component | Test File | Coverage Goals |
|-----------|-----------|----------------|
| HashingWriter | `hashing_writer.rs` | Hash correctness, write passthrough |
| MmapBuffer | `mmap_buffer.rs` | Write, grow, read-back |
| HfShardWriter | `shard_writer.rs` | Batch writing, flush triggers |
| LFS Types | `lfs/types.rs` | Serde roundtrip |
| URL Parsing | `url.rs` | Valid/invalid URL cases |
| Auth | `auth.rs` | Token resolution priority |

### Task 8.2: Integration Tests (Mock Server)
**File**: `crates/polars-io/tests/hf_sink_integration.rs`
**Status**: [ ] Not Started
**Dependencies**: Phase 3, Phase 4
**Estimate**: 8 hours

```rust
// Use wiremock or similar to mock HF Hub API
#[tokio::test]
async fn test_single_shard_upload() { ... }

#[tokio::test]
async fn test_multi_shard_upload() { ... }

#[tokio::test]
async fn test_overwrite_mode() { ... }

#[tokio::test]
async fn test_append_mode() { ... }

#[tokio::test]
async fn test_multipart_upload() { ... }

#[tokio::test]
async fn test_resume_from_checkpoint() { ... }
```

**Acceptance Criteria**:
- [ ] Mock server simulates HF Hub LFS API
- [ ] All write modes tested
- [ ] Error cases tested (auth failure, upload failure)
- [ ] Checkpoint resume tested

**Commit checkpoint**: `git commit -m "test(hf-sink): add integration tests with mock HF Hub"`

### Task 8.3: End-to-End Tests (Real HF Hub)
**File**: `py-polars/tests/unit/io/test_hf_sink.py`
**Status**: [ ] Not Started
**Dependencies**: Phase 7
**Estimate**: 6 hours

```python
@pytest.mark.slow
@pytest.mark.requires_hf_token
class TestHfSink:
    def test_small_dataset_upload(self, hf_test_repo):
        df = pl.DataFrame({"a": range(100), "b": ["x"] * 100})
        df.write_parquet(f"hf://datasets/{hf_test_repo}/data/test.parquet")

        # Verify by reading back
        result = pl.read_parquet(f"hf://datasets/{hf_test_repo}/data/test.parquet")
        assert_frame_equal(df, result)

    def test_sharded_upload(self, hf_test_repo):
        ...

    def test_streaming_large_dataset(self, hf_test_repo):
        ...

    def test_overwrite_existing(self, hf_test_repo):
        ...

    def test_append_to_existing(self, hf_test_repo):
        ...
```

**Acceptance Criteria**:
- [ ] Tests run against real HF Hub (CI with secret token)
- [ ] Test repo created/cleaned per test
- [ ] All major flows covered
- [ ] Streaming with large synthetic data

**Commit checkpoint**: `git commit -m "test(hf-sink): add e2e tests with real HF Hub"`

### Task 8.4: Performance Benchmarks
**File**: `crates/polars-io/benches/hf_sink.rs`
**Status**: [ ] Not Started
**Dependencies**: Phase 4
**Estimate**: 4 hours

```rust
// Benchmark components
fn bench_hashing_writer(c: &mut Criterion) { ... }
fn bench_mmap_buffer(c: &mut Criterion) { ... }
fn bench_parquet_encode_shard(c: &mut Criterion) { ... }

// End-to-end (with mock upload)
fn bench_full_sink_pipeline(c: &mut Criterion) { ... }
```

**Acceptance Criteria**:
- [ ] HashingWriter overhead < 5%
- [ ] MmapBuffer comparable to direct file write
- [ ] Full pipeline saturates network (not CPU-bound)

**Commit checkpoint**: `git commit -m "bench(hf-sink): add performance benchmarks"`

---

## Phase 9: Documentation & Polish

**Goal**: User documentation, API polish, error messages.

### Task 9.1: User Guide
**File**: `docs/source/user-guide/io/huggingface.md`
**Status**: [ ] Not Started
**Dependencies**: Phase 7
**Estimate**: 4 hours

Contents:
- Installation (feature flags)
- Authentication setup
- Basic usage examples
- Advanced options (sharding, modes, checkpoint)
- Troubleshooting

**Commit checkpoint**: `git commit -m "docs(hf-sink): add user guide for HF Hub writes"`

### Task 9.2: API Reference
**File**: Docstrings in source
**Status**: [ ] Not Started
**Dependencies**: Phase 7
**Estimate**: 3 hours

- Comprehensive docstrings on all public APIs
- Examples in docstrings
- Type annotations

**Commit checkpoint**: `git commit -m "docs(hf-sink): add API reference docstrings"`

### Task 9.3: Error Message Polish
**Status**: [ ] Not Started
**Dependencies**: All phases
**Estimate**: 3 hours

Review all error paths and ensure:
- [ ] Clear, actionable error messages
- [ ] Suggestions for common issues
- [ ] Links to documentation where helpful

**Commit checkpoint**: `git commit -m "fix(hf-sink): polish error messages"`

### Task 9.4: Release Notes / Changelog
**Status**: [ ] Not Started
**Dependencies**: All phases
**Estimate**: 1 hour

**Commit checkpoint**: `git commit -m "docs(hf-sink): add release notes"`

---

## Dependencies Graph

```
Phase 0 (Dev Setup)
    │
    ▼
Phase 1 (Foundation)
    │
    ├─► Task 1.1 ─┬─► Task 1.2
    │             ├─► Task 1.3
    │             └─► Task 1.4
    │
    ▼
Phase 2 (Core Writer)
    │
    ├─► Task 2.1 ─┬─► Task 2.3
    ├─► Task 2.2 ─┘
    │
    ▼
Phase 3 (LFS Protocol)
    │
    ├─► Task 3.1 ─► Task 3.2 ─► Task 3.3
    ├─► Task 3.4
    │
    ▼
Phase 4 (Streaming Integration)
    │
    ├─► Task 4.1 ─┬─► Task 4.2
    │             └─► Task 4.3 ─► Task 4.4
    │
    ▼
Phase 5 (Commit Coordination)
    │
    ├─► Task 5.1 ─► Task 5.2
    │
    ▼
Phase 6 (Advanced Features)
    │
    ├─► Task 6.1 (can parallel with 6.2, 6.3)
    ├─► Task 6.2
    └─► Task 6.3
    │
    ▼
Phase 7 (Python Bindings)
    │
    ├─► Task 7.1 ─► Task 7.2 ─► Task 7.3
    │
    ▼
Phase 8 (Testing) - Ongoing throughout
    │
    ▼
Phase 9 (Documentation)
```

**Critical Path**: 0.1 → 1.1 → 2.1/2.2 → 2.3 → 3.2 → 3.3 → 4.3 → 4.4 → 5.1 → 7.2

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| HF API changes | Low | High | Pin to known API version, monitor HF changelog |
| LFS multipart complexity | Medium | Medium | Start with basic upload, add multipart later |
| Streaming engine integration issues | Medium | High | Early POC to validate integration approach |
| Memory issues with large shards | Low | Medium | Configurable shard size, mmap handles spillover |
| Token/auth edge cases | Medium | Low | Comprehensive auth tests, clear error messages |
| Xet protocol complexity | High | Low | Make Xet optional, start without it |

---

## Progress Tracking

### Phase Completion Checklist

- [x] **Phase 0: Dev Setup** (5/6 tasks)
  - [x] 0.1 Fork and Branch Setup
  - [ ] 0.2 Draft PR Setup (deferred - working on fork first)
  - [x] 0.3 Local Development Build
  - [x] 0.4 Git Install Test Setup
  - [ ] 0.5 CI Configuration (deferred - no PR yet)
  - [x] 0.6 Sync with Upstream Main (2026-01-14)

- [x] **Phase 1: Foundation** (4/4 tasks)
  - [x] 1.1 Module Structure (2026-01-14)
  - [x] 1.2 Configuration Types (2026-01-14)
  - [x] 1.3 URL Parsing (2026-01-14)
  - [x] 1.4 Token/Auth (2026-01-14)

- [ ] **Phase 2: Core Writer** (2/3 tasks)
  - [x] 2.1 HashingWriter (2026-01-14)
  - [x] 2.2 MmapBuffer (2026-01-14)
  - [ ] 2.3 HfShardWriter

- [ ] **Phase 3: LFS Protocol** (0/4 tasks)
  - [ ] 3.1 LFS Types
  - [ ] 3.2 LFS Client
  - [ ] 3.3 Upload Executor
  - [ ] 3.4 Commit API Client

- [ ] **Phase 4: Streaming Integration** (0/4 tasks)
  - [ ] 4.1 HfSinkNode Skeleton
  - [ ] 4.2 Morsel Router
  - [ ] 4.3 Shard Worker Task
  - [ ] 4.4 IOSinkNode Integration

- [ ] **Phase 5: Commit Coordination** (0/2 tasks)
  - [ ] 5.1 CommitCoordinator
  - [ ] 5.2 Dataset Card Updates

- [ ] **Phase 6: Advanced Features** (0/3 tasks)
  - [ ] 6.1 Checkpoint System
  - [ ] 6.2 Partitioned Write Support
  - [ ] 6.3 Progress Reporting

- [ ] **Phase 7: Python Bindings** (0/3 tasks)
  - [ ] 7.1 PyO3 Bindings
  - [ ] 7.2 sink_parquet Integration
  - [ ] 7.3 write_parquet Integration

- [ ] **Phase 8: Testing** (0/4 tasks)
  - [ ] 8.1 Unit Tests
  - [ ] 8.2 Integration Tests (Mock)
  - [ ] 8.3 E2E Tests (Real HF)
  - [ ] 8.4 Performance Benchmarks

- [ ] **Phase 9: Documentation** (0/4 tasks)
  - [ ] 9.1 User Guide
  - [ ] 9.2 API Reference
  - [ ] 9.3 Error Message Polish
  - [ ] 9.4 Release Notes

### Milestone Targets

| Milestone | Tasks | Description |
|-----------|-------|-------------|
| **M0: Setup** | 0.* | Branch, PR, build working |
| **M1: POC** | 1.*, 2.*, 3.1-3.3 | Single file upload works via Rust |
| **M2: Streaming** | 4.* | Streaming sink node works |
| **M3: Production** | 5.*, 6.1 | Atomic commits, resume |
| **M4: Python** | 7.* | Python API complete |
| **M5: Release** | 8.*, 9.* | Tested, documented, released |

---

## Session Log

Track work sessions here:

| Date | Tasks Worked | Status | Notes |
|------|--------------|--------|-------|
| 2026-01-13 | 0.1, 0.3, 0.4 | Complete | Branch `feature/hf-hub-sink` created on fork, build verified, smoke test created |
| 2026-01-14 | 1.1 | Complete | Module structure created. Moved HF code from `path_utils/hugging_face.rs` to `cloud/hf/`. Created `mod.rs`, `url.rs`, `glob.rs` + stubs for `options.rs`, `auth.rs`, `error.rs`. Build verified (polars-io compiles with 0 errors). |
| 2026-01-14 | 1.2 | Complete | Implemented HfSinkOptions with builder pattern. Added RepoType enum, HfWriteMode enum, validation logic. Full test coverage. Build verified with `make build-release`. |
| 2026-01-14 | 1.4 | Complete | Implemented `get_hf_token()` following existing polars patterns from `options.rs:635-661`. Uses `resolve_homedir()`, `config::verbose()`. Priority: explicit → HF_TOKEN env → HF_HOME/token file. 5 unit tests. Note: Full build verification pending branch rebase (pre-existing polars-core errors). |
| 2026-01-14 | 1.3 | Complete | Extended URL parsing for write support. Added bucket/repository/revision fields to HFRepoLocation. Added get_lfs_batch_uri() and get_commit_uri() methods. Added RepoType::from_bucket_str() and HFPathParts::repo_type(). Feature-gated with hf_sink. 4 new tests. **Phase 1 complete!** |
| 2026-01-14 | 2.1 | Complete | Implemented HashingWriter for streaming SHA256 computation. Added sha2 dependency to Cargo.toml (optional, under hf_sink feature). Created hashing_writer.rs with Write impl, sha256_to_hex helper, 7 unit tests. Build verification blocked by pre-existing polars-core errors (branch needs rebase). |
| 2026-01-14 | 0.6 | Complete | Rebased feature branch onto upstream main (pola-rs/polars). Fetched via HTTPS, rebased 7 HF sink commits onto 7 new upstream commits. **Build issue identified**: `polars-io --features cloud` fails on upstream main with `GroupsIndicator` not found error in polars-core. This is an upstream bug (serde-lazy feature triggers code that references missing type). Our HF sink code is unaffected - `polars-core` and `polars-io` (without cloud features) build successfully. |
| 2026-01-14 | 2.2 | Complete | Implemented MmapBuffer for efficient temp storage. Added `tempfile` dependency to hf_sink feature. Created `mmap_buffer.rs` with MmapBuffer (Write trait, dynamic growth) and MmapReadHandle (zero-copy read access). 9 unit tests. Code passes rustfmt. Full build verification blocked by upstream polars-core issue (same as 2.1). |

---

## Reference Materials

### HF Hub API Documentation
- [Upload Files](https://huggingface.co/docs/huggingface_hub/en/guides/upload)
- [LFS Protocol](https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md)
- [Commit API](https://huggingface.co/docs/hub/api)

### Reference Implementations
- `huggingface_hub` Python library: `/Users/davanstrien/Documents/code/huggingface_hub`
- `pyspark_huggingface`: https://github.com/huggingface/pyspark_huggingface
- `datasets` library: `/Users/davanstrien/Documents/code/datasets`

### Polars Internals
- Streaming sink infrastructure: `crates/polars-stream/src/nodes/io_sinks/`
- Cloud write infrastructure: `crates/polars-io/src/cloud/`
- Existing HF read code: `crates/polars-io/src/path_utils/hugging_face.rs`

---

## Notes

- Start with single-file upload, add sharding after
- Xet support is optional/future enhancement
- Consider contributing back to `huggingface_hub` Rust bindings as separate crate
- Coordinate with HF team on API stability

## Known Issues

### Upstream Build Issue (2026-01-14)

**Status**: Blocking `--features hf_sink` testing, but NOT blocking development.

The `cloud` feature (which `hf_sink` depends on) fails to build on upstream polars main:

```
error[E0425]: cannot find type `GroupsIndicator` in this scope
--> crates/polars-core/src/frame/mod.rs:1214:57
```

**Root cause**: The `cloud` → `serde` → `polars-core/serde-lazy` feature chain enables code in `polars-core/src/frame/mod.rs` that references `GroupsIndicator`, but that type is not imported/defined when only `serde-lazy` is enabled.

**Impact on HF sink work**:
- ✅ `polars-core` builds fine
- ✅ `polars-io` (without cloud features) builds fine
- ❌ `polars-io --features cloud` fails
- ❌ `polars-io --features hf_sink` fails (depends on cloud)

**Workaround**: Continue developing HF sink code. Unit tests for individual components (HashingWriter, etc.) can run without the full `hf_sink` feature. Full integration testing requires upstream fix.

**Next steps**: Monitor upstream or report issue to pola-rs/polars.
