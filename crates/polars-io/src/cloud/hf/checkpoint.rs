//! Checkpoint support for HF Hub sink resumable uploads.
//!
//! Persists upload state to a local JSON file, enabling resume on failure.

use std::collections::HashSet;
use std::path::Path;

use polars_error::{PolarsResult, polars_err};
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
    pub fn save(&self, path: &Path) -> PolarsResult<()> {
        use std::fs;
        use std::io::Write;

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|e| {
                    polars_err!(ComputeError: "Failed to create checkpoint directory: {}", e)
                })?;
            }
        }

        // Write to temporary file first (atomic write pattern)
        let tmp_path = path.with_extension("tmp");
        let json = serde_json::to_vec_pretty(self)
            .map_err(|e| polars_err!(ComputeError: "Failed to serialize checkpoint: {}", e))?;

        let mut file = fs::File::create(&tmp_path).map_err(|e| {
            polars_err!(ComputeError: "Failed to create checkpoint temp file: {}", e)
        })?;
        file.write_all(&json).map_err(|e| {
            polars_err!(ComputeError: "Failed to write checkpoint: {}", e)
        })?;
        file.sync_all().map_err(|e| {
            polars_err!(ComputeError: "Failed to sync checkpoint: {}", e)
        })?;

        // Atomic rename
        fs::rename(&tmp_path, path).map_err(|e| {
            polars_err!(ComputeError: "Failed to rename checkpoint file: {}", e)
        })?;

        Ok(())
    }

    /// Load checkpoint from disk, returning None if file doesn't exist.
    pub fn load(path: &Path) -> PolarsResult<Option<Self>> {
        use std::fs::File;
        use std::io::BufReader;

        // Return None if file doesn't exist (not an error)
        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(path).map_err(|e| {
            polars_err!(ComputeError: "Failed to open checkpoint file: {}", e)
        })?;
        let reader = BufReader::new(file);
        let checkpoint: Self = serde_json::from_reader(reader).map_err(|e| {
            polars_err!(ComputeError: "Failed to parse checkpoint file: {}", e)
        })?;

        Ok(Some(checkpoint))
    }

    /// Delete checkpoint file if it exists.
    pub fn delete(path: &Path) -> PolarsResult<()> {
        use std::fs;

        // Silently succeed if file doesn't exist
        if path.exists() {
            fs::remove_file(path).map_err(|e| {
                polars_err!(ComputeError: "Failed to delete checkpoint file: {}", e)
            })?;
        }

        // Also clean up any leftover .tmp file
        let tmp_path = path.with_extension("tmp");
        if tmp_path.exists() {
            let _ = fs::remove_file(&tmp_path); // Best effort, don't fail
        }

        Ok(())
    }
}
