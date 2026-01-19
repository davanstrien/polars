//! Progress tracking for HF Hub sink operations.
//!
//! This module provides the [`HfSinkProgress`] trait for receiving real-time
//! notifications during HF Hub uploads. Implement this trait to add progress
//! bars, logging, or metrics collection to your uploads.
//!
//! # Example
//!
//! ```ignore
//! use polars_io::cloud::hf::{HfSinkProgress, NoOpSinkProgress};
//!
//! struct MyProgress;
//!
//! impl HfSinkProgress for MyProgress {
//!     fn on_shard_complete(&self, index: usize, path: &str, size: u64) {
//!         println!("Uploaded shard {}: {} ({} bytes)", index, path, size);
//!     }
//! }
//! ```

use std::sync::Arc;

/// Callback trait for monitoring HF sink progress.
///
/// Implement this trait to receive real-time notifications during uploads.
/// All methods have default no-op implementations so you can override only
/// the events you care about.
///
/// # Thread Safety
///
/// This trait requires `Send + Sync` because callbacks may be invoked from
/// multiple async tasks during parallel uploads.
pub trait HfSinkProgress: Send + Sync {
    /// Called when a shard begins writing (before upload).
    ///
    /// # Arguments
    /// * `index` - Zero-based shard index (0, 1, 2, ...)
    /// * `path` - Path in repository (e.g., "data/train-00000.parquet")
    fn on_shard_start(&self, index: usize, path: &str) {
        let _ = (index, path);
    }

    /// Called periodically during shard upload with byte progress.
    ///
    /// # Arguments
    /// * `index` - Zero-based shard index
    /// * `bytes_uploaded` - Bytes uploaded so far
    /// * `total_bytes` - Total bytes to upload
    fn on_shard_upload_progress(&self, index: usize, bytes_uploaded: u64, total_bytes: u64) {
        let _ = (index, bytes_uploaded, total_bytes);
    }

    /// Called when a shard upload completes successfully.
    ///
    /// # Arguments
    /// * `index` - Zero-based shard index
    /// * `path` - Path in repository
    /// * `size` - Total size in bytes
    fn on_shard_complete(&self, index: usize, path: &str, size: u64) {
        let _ = (index, path, size);
    }

    /// Called when commit to HF Hub begins.
    ///
    /// # Arguments
    /// * `num_shards` - Total number of shards being committed
    fn on_commit_start(&self, num_shards: usize) {
        let _ = num_shards;
    }

    /// Called when commit completes successfully.
    ///
    /// # Arguments
    /// * `commit_url` - URL to the commit (None if dry-run or unavailable)
    fn on_commit_complete(&self, commit_url: Option<&str>) {
        let _ = commit_url;
    }
}

/// No-op progress handler (default).
///
/// Use this when you don't need progress tracking. All callback methods
/// do nothing.
pub struct NoOpSinkProgress;

impl HfSinkProgress for NoOpSinkProgress {}

/// Type alias for progress handler wrapped in Arc.
///
/// Use this type when storing or passing progress handlers, as they need
/// to be shared across async tasks.
pub type SinkProgressRef = Arc<dyn HfSinkProgress>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_op_progress_compiles() {
        let progress = NoOpSinkProgress;
        progress.on_shard_start(0, "data/train-00000.parquet");
        progress.on_shard_upload_progress(0, 100, 1000);
        progress.on_shard_complete(0, "data/train-00000.parquet", 1000);
        progress.on_commit_start(1);
        progress.on_commit_complete(Some("https://huggingface.co/datasets/user/repo/commit/abc123"));
    }

    #[test]
    fn test_progress_ref_type() {
        let progress: SinkProgressRef = Arc::new(NoOpSinkProgress);
        progress.on_shard_start(0, "data/train-00000.parquet");
    }

    #[test]
    fn test_custom_progress_implementation() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingProgress {
            shard_starts: AtomicUsize,
            shard_completes: AtomicUsize,
        }

        impl HfSinkProgress for CountingProgress {
            fn on_shard_start(&self, _index: usize, _path: &str) {
                self.shard_starts.fetch_add(1, Ordering::SeqCst);
            }

            fn on_shard_complete(&self, _index: usize, _path: &str, _size: u64) {
                self.shard_completes.fetch_add(1, Ordering::SeqCst);
            }
        }

        let progress = CountingProgress {
            shard_starts: AtomicUsize::new(0),
            shard_completes: AtomicUsize::new(0),
        };

        progress.on_shard_start(0, "data/train-00000.parquet");
        progress.on_shard_start(1, "data/train-00001.parquet");
        progress.on_shard_complete(0, "data/train-00000.parquet", 1000);

        assert_eq!(progress.shard_starts.load(Ordering::SeqCst), 2);
        assert_eq!(progress.shard_completes.load(Ordering::SeqCst), 1);
    }
}
