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
    /// Shard index (0, 1, 2, ...) - per-partition for partitioned writes
    pub index: usize,
    /// Partition value for partitioned writes (e.g., "train" for split=train).
    /// None for non-partitioned writes.
    #[serde(default)]
    pub partition_value: Option<String>,
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
    /// Partition column for partitioned writes (e.g., "split").
    /// None for non-partitioned writes.
    #[serde(default)]
    pub partition_col: Option<String>,
    /// Shards that have been successfully uploaded to LFS
    pub completed_shards: Vec<ShardCheckpoint>,
}

impl CheckpointState {
    /// Create a new empty checkpoint for the given repository and path.
    pub fn new(repo_id: &str, path_in_repo: &str, partition_col: Option<String>) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            repo_id: repo_id.to_string(),
            path_in_repo: path_in_repo.to_string(),
            partition_col,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_checkpoint_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        // Create checkpoint with some shards
        let mut checkpoint = CheckpointState::new("user/test-repo", "data/train", None);
        checkpoint.add_shard(ShardCheckpoint {
            index: 0,
            partition_value: None,
            path_in_repo: "data/train-00000.parquet".to_string(),
            sha256: "abc123".to_string(),
            size: 1024,
            num_rows: 100,
        });
        checkpoint.add_shard(ShardCheckpoint {
            index: 1,
            partition_value: None,
            path_in_repo: "data/train-00001.parquet".to_string(),
            sha256: "def456".to_string(),
            size: 2048,
            num_rows: 200,
        });

        // Save and reload
        checkpoint.save(&path).unwrap();
        let loaded = CheckpointState::load(&path).unwrap().unwrap();

        // Verify all fields preserved
        assert_eq!(loaded.version, CHECKPOINT_VERSION);
        assert_eq!(loaded.repo_id, "user/test-repo");
        assert_eq!(loaded.path_in_repo, "data/train");
        assert_eq!(loaded.completed_shards.len(), 2);
        assert_eq!(loaded.completed_shards[0].index, 0);
        assert_eq!(loaded.completed_shards[0].sha256, "abc123");
        assert_eq!(loaded.completed_shards[1].index, 1);
        assert_eq!(loaded.completed_shards[1].num_rows, 200);
    }

    #[test]
    fn test_checkpoint_add_shard() {
        let mut checkpoint = CheckpointState::new("user/repo", "data", None);
        assert!(checkpoint.completed_shards.is_empty());

        checkpoint.add_shard(ShardCheckpoint {
            index: 5,
            partition_value: None,
            path_in_repo: "data/train-00005.parquet".to_string(),
            sha256: "abc".to_string(),
            size: 100,
            num_rows: 10,
        });

        assert_eq!(checkpoint.completed_shards.len(), 1);
        assert_eq!(checkpoint.completed_shards[0].index, 5);
    }

    #[test]
    fn test_checkpoint_completed_indices() {
        let mut checkpoint = CheckpointState::new("user/repo", "data", None);
        checkpoint.add_shard(ShardCheckpoint {
            index: 0,
            partition_value: None,
            path_in_repo: "p0".to_string(),
            sha256: "a".to_string(),
            size: 1,
            num_rows: 1,
        });
        checkpoint.add_shard(ShardCheckpoint {
            index: 2,
            partition_value: None,
            path_in_repo: "p2".to_string(),
            sha256: "b".to_string(),
            size: 2,
            num_rows: 2,
        });
        checkpoint.add_shard(ShardCheckpoint {
            index: 5,
            partition_value: None,
            path_in_repo: "p5".to_string(),
            sha256: "c".to_string(),
            size: 3,
            num_rows: 3,
        });

        let indices = checkpoint.completed_indices();
        assert_eq!(indices.len(), 3);
        assert!(indices.contains(&0));
        assert!(indices.contains(&2));
        assert!(indices.contains(&5));
        assert!(!indices.contains(&1));
        assert!(!indices.contains(&3));
    }

    #[test]
    fn test_checkpoint_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let result = CheckpointState::load(&path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_checkpoint_version_field() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let checkpoint = CheckpointState::new("user/repo", "data", None);
        checkpoint.save(&path).unwrap();

        // Read raw JSON and verify version field
        let json_str = fs::read_to_string(&path).unwrap();
        let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(json["version"], 1);
    }

    #[test]
    fn test_checkpoint_atomic_save() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let checkpoint = CheckpointState::new("user/repo", "data", None);
        checkpoint.save(&path).unwrap();

        // Verify main file exists
        assert!(path.exists());

        // Verify no .tmp file left behind
        let tmp_path = path.with_extension("tmp");
        assert!(!tmp_path.exists());
    }

    #[test]
    fn test_checkpoint_partition_value_backward_compat() {
        // Test that old JSON without partition_value still deserializes (backward compat)
        let json = r#"{
            "index": 0,
            "path_in_repo": "data/train-00000.parquet",
            "sha256": "abc123",
            "size": 1024,
            "num_rows": 100
        }"#;
        let shard: ShardCheckpoint = serde_json::from_str(json).unwrap();
        assert!(shard.partition_value.is_none());
        assert_eq!(shard.index, 0);
        assert_eq!(shard.path_in_repo, "data/train-00000.parquet");
    }

    #[test]
    fn test_checkpoint_partition_value_roundtrip() {
        // Test that partition_value is preserved through serialization
        let shard = ShardCheckpoint {
            index: 0,
            partition_value: Some("train".to_string()),
            path_in_repo: "data/split=train/train-00000.parquet".to_string(),
            sha256: "abc123".to_string(),
            size: 1024,
            num_rows: 100,
        };
        let json = serde_json::to_string(&shard).unwrap();
        let loaded: ShardCheckpoint = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.partition_value, Some("train".to_string()));
        assert_eq!(loaded.index, 0);
        assert_eq!(loaded.path_in_repo, "data/split=train/train-00000.parquet");
    }

    #[test]
    fn test_checkpoint_full_roundtrip_with_partition() {
        // Test full checkpoint save/load with partitioned shards
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint.json");

        let mut checkpoint = CheckpointState::new("user/dataset", "data", None);

        // Add a non-partitioned shard
        checkpoint.add_shard(ShardCheckpoint {
            index: 0,
            partition_value: None,
            path_in_repo: "data/train-00000.parquet".to_string(),
            sha256: "aaa".to_string(),
            size: 100,
            num_rows: 10,
        });

        // Add partitioned shards
        checkpoint.add_shard(ShardCheckpoint {
            index: 0,
            partition_value: Some("train".to_string()),
            path_in_repo: "data/split=train/train-00000.parquet".to_string(),
            sha256: "bbb".to_string(),
            size: 200,
            num_rows: 20,
        });
        checkpoint.add_shard(ShardCheckpoint {
            index: 0,
            partition_value: Some("test".to_string()),
            path_in_repo: "data/split=test/test-00000.parquet".to_string(),
            sha256: "ccc".to_string(),
            size: 300,
            num_rows: 30,
        });

        // Save and reload
        checkpoint.save(&path).unwrap();
        let loaded = CheckpointState::load(&path).unwrap().unwrap();

        // Verify all shards preserved with correct partition values
        assert_eq!(loaded.completed_shards.len(), 3);
        assert_eq!(loaded.completed_shards[0].partition_value, None);
        assert_eq!(
            loaded.completed_shards[1].partition_value,
            Some("train".to_string())
        );
        assert_eq!(
            loaded.completed_shards[2].partition_value,
            Some("test".to_string())
        );
    }
}
