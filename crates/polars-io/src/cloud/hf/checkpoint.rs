//! Checkpoint support for HF Hub sink resumable uploads.
//!
//! Persists upload state to a local JSON file, enabling resume on failure.

use std::collections::HashSet;
use std::path::Path;

use polars_error::PolarsResult;
use serde::{Deserialize, Serialize};

/// Checkpoint format version for forward compatibility.
pub const CHECKPOINT_VERSION: u32 = 1;

/// Metadata for a single uploaded shard.
///
/// Mirrors `ShardCompletion` from hf_sink but is serde-enabled for persistence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardCheckpoint {
    /// Shard index (0, 1, 2, ...)
    pub index: usize,
    /// Path in the repository (e.g., "data/train-00000.parquet")
    pub path_in_repo: String,
    /// SHA256 hash of the file (lowercase hex, 64 characters)
    pub sha256: String,
    /// Size in bytes
    pub size: u64,
    /// Number of rows in this shard
    pub num_rows: usize,
}

/// Checkpoint state for resumable HF Hub uploads.
///
/// Stored as JSON at the path specified by `HfSinkOptions.checkpoint_path`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    /// Format version (for future migrations)
    pub version: u32,
    /// Repository identifier for validation on resume (e.g., "username/dataset")
    pub repo_id: String,
    /// Path in repo being written to (e.g., "data/train")
    pub path_in_repo: String,
    /// Shards that have been successfully uploaded to LFS
    pub completed_shards: Vec<ShardCheckpoint>,
}

impl CheckpointState {
    /// Create a new empty checkpoint for the given repository and path.
    pub fn new(repo_id: &str, path_in_repo: &str) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            repo_id: repo_id.to_string(),
            path_in_repo: path_in_repo.to_string(),
            completed_shards: Vec::new(),
        }
    }

    /// Add a completed shard to the checkpoint.
    pub fn add_shard(&mut self, shard: ShardCheckpoint) {
        self.completed_shards.push(shard);
    }

    /// Get the set of completed shard indices.
    pub fn completed_indices(&self) -> HashSet<usize> {
        self.completed_shards.iter().map(|s| s.index).collect()
    }

    /// Save checkpoint to disk atomically (write .tmp, then rename).
    pub fn save(&self, _path: &Path) -> PolarsResult<()> {
        todo!("Implement in subtask 6.1.3")
    }

    /// Load checkpoint from disk, returning None if file doesn't exist.
    pub fn load(_path: &Path) -> PolarsResult<Option<Self>> {
        todo!("Implement in subtask 6.1.3")
    }

    /// Delete checkpoint file if it exists.
    pub fn delete(_path: &Path) -> PolarsResult<()> {
        todo!("Implement in subtask 6.1.3")
    }
}
