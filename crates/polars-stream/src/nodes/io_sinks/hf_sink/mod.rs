//! HF Hub sink node for streaming writes to Hugging Face Hub.
//!
//! This module provides the [`HfSinkNode`] which integrates with Polars' streaming
//! execution engine to write datasets to HF Hub with:
//! - Streaming writes with bounded memory
//! - Parallel shard uploads
//! - Atomic commits
//!
//! # Usage
//! ```ignore
//! lf.sink_parquet("hf://datasets/user/repo/data/train.parquet", options)
//! ```

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;

use polars_core::schema::SchemaRef;
use polars_error::PolarsResult;
use polars_io::cloud::hf::options::HfSinkOptions;
use polars_io::cloud::hf::shard_writer::FinishedShard;
use polars_plan::dsl::SinkOptions;

use super::phase::PhaseOutcome;
use super::{SinkInputPort, SinkNode};
use crate::async_executor::spawn;
use crate::async_primitives::connector::Receiver;
use crate::execute::StreamingExecutionState;
use crate::nodes::{JoinHandle, TaskPriority};
use crate::utils::tokio_handle_ext::AbortOnDropHandle;

// ============================================================================
// Constants
// ============================================================================

/// Default chunk size for buffering rows before encoding (256K rows).
///
/// This matches the ParquetSinkNode pattern where rows are accumulated
/// before being written as a row group.
#[allow(dead_code)]
const DEFAULT_CHUNK_SIZE: usize = 256 * 1024;

/// Default buffer capacity for shard completion channel.
///
/// Allows up to 16 shards to be queued for commit tracking before
/// blocking the upload task.
#[allow(dead_code)]
const COMPLETION_CHANNEL_SIZE: usize = 16;

// ============================================================================
// Types for Shard Writer Task
// ============================================================================

/// Information about a completed shard ready for commit.
///
/// Contains all the metadata needed to create a `CommitOperationAdd`
/// for the atomic commit to HF Hub.
#[derive(Debug, Clone)]
pub struct ShardCompletion {
    /// Shard index (0, 1, 2, ...).
    pub index: usize,
    /// Path in the repository (e.g., "data/train-00000.parquet").
    pub path_in_repo: String,
    /// SHA256 hash of the file (lowercase hex, 64 characters).
    pub sha256: String,
    /// Size in bytes.
    pub size: u64,
    /// Number of rows in this shard.
    pub num_rows: usize,
}

impl ShardCompletion {
    /// Create from a `FinishedShard` and metadata.
    ///
    /// # Arguments
    /// * `index` - Shard index number
    /// * `path_in_repo` - Full path in the repository
    /// * `shard` - Completed shard data from `HfShardWriter::finish()`
    pub fn from_finished(index: usize, path_in_repo: String, shard: &FinishedShard) -> Self {
        Self {
            index,
            path_in_repo,
            sha256: shard.sha256.clone(),
            size: shard.size,
            num_rows: shard.num_rows,
        }
    }
}

/// Internal state for tracking shard writer progress.
///
/// Used by the writer task to track completed shards and maintain
/// counters for metrics reporting.
#[derive(Debug, Default)]
pub struct WriterState {
    /// Completed shards awaiting final commit.
    pub completed: VecDeque<ShardCompletion>,
    /// Current shard index (increments on each flush).
    pub current_shard_index: usize,
    /// Total rows written across all shards.
    pub total_rows: usize,
    /// Total bytes written across all shards.
    pub total_bytes: u64,
}

impl WriterState {
    /// Create a new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a completed shard.
    ///
    /// Updates total counters and adds the shard to the completion queue.
    pub fn record_completion(&mut self, completion: ShardCompletion) {
        self.total_rows += completion.num_rows;
        self.total_bytes += completion.size;
        self.completed.push_back(completion);
    }

    /// Get the next shard index and increment the counter.
    ///
    /// Returns the current index before incrementing.
    pub fn next_shard_index(&mut self) -> usize {
        let index = self.current_shard_index;
        self.current_shard_index += 1;
        index
    }

    /// Get the number of completed shards.
    pub fn num_completed(&self) -> usize {
        self.completed.len()
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Generate a shard file path.
///
/// Format: `{path_in_repo}/{split}-{index:05}.parquet`
///
/// # Examples
/// ```ignore
/// assert_eq!(shard_path("data", "train", 0), "data/train-00000.parquet");
/// assert_eq!(shard_path("data", "train", 42), "data/train-00042.parquet");
/// ```
pub fn shard_path(path_in_repo: &str, split: &str, index: usize) -> String {
    let path = path_in_repo.trim_end_matches('/');
    format!("{}/{}-{:05}.parquet", path, split, index)
}

// ============================================================================
// HfSinkNode
// ============================================================================

/// Streaming sink node for writing to Hugging Face Hub.
///
/// Implements the [`SinkNode`] trait to integrate with Polars' streaming
/// execution engine. Manages shard writers, LFS uploads, and atomic commits.
///
/// # State Machine
/// - Uninitialized: Before `initialize()` is called
/// - Running: After `spawn_sink()`, workers processing morsels
/// - Committing: After all data written, executing atomic commit
/// - Finished: After successful commit
pub struct HfSinkNode {
    /// HF Hub specific configuration (repo, sharding, etc.)
    options: Arc<HfSinkOptions>,
    /// Input data schema
    input_schema: SchemaRef,
    /// General sink options (maintain_order, mkdir, etc.)
    sink_options: SinkOptions,
    /// Background IO/upload task handle
    io_task: Option<AbortOnDropHandle<PolarsResult<()>>>,
}

impl HfSinkNode {
    /// Create a new HF sink node.
    ///
    /// # Arguments
    /// * `options` - HF Hub specific options (repo_id, sharding config, etc.)
    /// * `input_schema` - Schema of the input data
    /// * `sink_options` - General sink options
    ///
    /// # Errors
    /// Returns an error if the options validation fails.
    pub fn new(
        options: HfSinkOptions,
        input_schema: SchemaRef,
        sink_options: SinkOptions,
    ) -> PolarsResult<Self> {
        // Validate options
        options.validate()?;

        Ok(Self {
            options: Arc::new(options),
            input_schema,
            sink_options,
            io_task: None,
        })
    }

    /// Get a reference to the HF sink options.
    pub fn options(&self) -> &HfSinkOptions {
        &self.options
    }

    /// Get a reference to the input schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}

impl SinkNode for HfSinkNode {
    fn name(&self) -> &str {
        "hf-sink"
    }

    /// HF sink uses serial input for shard batching.
    ///
    /// This allows us to accumulate rows into shards before encoding,
    /// similar to how ParquetSinkNode works.
    fn is_sink_input_parallel(&self) -> bool {
        false
    }

    /// Maintain order based on sink options.
    fn do_maintain_order(&self) -> bool {
        self.sink_options.maintain_order
    }

    fn initialize(&mut self, _state: &StreamingExecutionState) -> PolarsResult<()> {
        // TODO (Task 4.3): Set up channels and spawn coordinator task
        // - Create channel for shard completions
        // - Spawn commit coordinator task
        // - Initialize LFS client
        Ok(())
    }

    fn spawn_sink(
        &mut self,
        recv_port_rx: Receiver<(PhaseOutcome, SinkInputPort)>,
        _state: &StreamingExecutionState,
        join_handles: &mut Vec<JoinHandle<PolarsResult<()>>>,
    ) {
        // TODO (Task 4.3): Spawn actual shard worker tasks
        // For now, spawn a placeholder task that drains input to satisfy the interface
        join_handles.push(spawn(TaskPriority::High, async move {
            let mut recv_port_rx = recv_port_rx;

            // Drain all incoming morsels
            while let Ok((outcome, sink_input)) = recv_port_rx.recv().await {
                let mut rx = sink_input.serial();

                // Consume all morsels from this phase
                while let Ok(morsel) = rx.recv().await {
                    let (_df, _seq, _, consume_token) = morsel.into_inner();
                    // TODO: Process morsel through shard writer
                    drop(consume_token);
                }

                outcome.stopped();
            }

            PolarsResult::Ok(())
        }));
    }

    fn finalize(
        &mut self,
        _state: &StreamingExecutionState,
    ) -> Option<Pin<Box<dyn Future<Output = PolarsResult<()>> + Send>>> {
        // TODO (Task 5.1): Execute atomic commit via CommitCoordinator
        // For now, return None (no finalization needed)
        None
    }

    fn get_metrics(&self) -> PolarsResult<Option<super::metrics::WriteMetrics>> {
        // TODO (Task 6.3): Implement metrics collection
        Ok(None)
    }
}

// Required for the finalize return type
use std::future::Future;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hf_sink_node_creation() {
        use polars_core::prelude::*;

        let schema = Arc::new(Schema::from_iter([
            Field::new("id".into(), DataType::Int64),
            Field::new("text".into(), DataType::String),
        ]));

        let hf_options = HfSinkOptions::builder("user/test-repo")
            .with_path_in_repo("data")
            .with_split("train")
            .build()
            .unwrap();

        let sink_options = SinkOptions {
            maintain_order: true,
            ..Default::default()
        };

        let node = HfSinkNode::new(hf_options, schema, sink_options);
        assert!(node.is_ok());

        let node = node.unwrap();
        assert_eq!(node.name(), "hf-sink");
        assert!(!node.is_sink_input_parallel());
        assert!(node.do_maintain_order());
    }

    #[test]
    fn test_hf_sink_node_invalid_options() {
        use polars_core::prelude::*;

        let schema = Arc::new(Schema::from_iter([Field::new(
            "id".into(),
            DataType::Int64,
        )]));

        // Invalid: empty repo_id
        let hf_options = HfSinkOptions {
            repo_id: String::new(),
            path_in_repo: "data".to_string(),
            ..Default::default()
        };

        let sink_options = SinkOptions::default();

        let result = HfSinkNode::new(hf_options, schema, sink_options);
        assert!(result.is_err());
    }

    #[test]
    fn test_shard_path_generation() {
        // Basic case
        assert_eq!(shard_path("data", "train", 0), "data/train-00000.parquet");

        // With trailing slash (should be trimmed)
        assert_eq!(shard_path("data/", "train", 5), "data/train-00005.parquet");

        // Different split name
        assert_eq!(
            shard_path("output", "validation", 123),
            "output/validation-00123.parquet"
        );

        // Nested path
        assert_eq!(
            shard_path("data/processed", "test", 42),
            "data/processed/test-00042.parquet"
        );

        // Large index (5 digits)
        assert_eq!(
            shard_path("data", "train", 99999),
            "data/train-99999.parquet"
        );
    }

    #[test]
    fn test_writer_state_default() {
        let state = WriterState::default();
        assert_eq!(state.current_shard_index, 0);
        assert_eq!(state.total_rows, 0);
        assert_eq!(state.total_bytes, 0);
        assert_eq!(state.num_completed(), 0);
        assert!(state.completed.is_empty());
    }

    #[test]
    fn test_writer_state_next_shard_index() {
        let mut state = WriterState::new();

        // First call returns 0
        assert_eq!(state.next_shard_index(), 0);

        // Subsequent calls increment
        assert_eq!(state.next_shard_index(), 1);
        assert_eq!(state.next_shard_index(), 2);
        assert_eq!(state.next_shard_index(), 3);

        // Counter is now at 4
        assert_eq!(state.current_shard_index, 4);
    }

    #[test]
    fn test_writer_state_record_completion() {
        let mut state = WriterState::default();

        // Record first shard
        state.record_completion(ShardCompletion {
            index: 0,
            path_in_repo: "data/train-00000.parquet".to_string(),
            sha256: "abc123".repeat(10), // 60 chars
            size: 1000,
            num_rows: 100,
        });

        assert_eq!(state.total_rows, 100);
        assert_eq!(state.total_bytes, 1000);
        assert_eq!(state.num_completed(), 1);

        // Record second shard
        state.record_completion(ShardCompletion {
            index: 1,
            path_in_repo: "data/train-00001.parquet".to_string(),
            sha256: "def456".repeat(10),
            size: 2000,
            num_rows: 200,
        });

        assert_eq!(state.total_rows, 300);
        assert_eq!(state.total_bytes, 3000);
        assert_eq!(state.num_completed(), 2);

        // Verify order (FIFO)
        assert_eq!(state.completed[0].index, 0);
        assert_eq!(state.completed[1].index, 1);
    }

    #[test]
    fn test_shard_completion_clone() {
        let completion = ShardCompletion {
            index: 42,
            path_in_repo: "data/train-00042.parquet".to_string(),
            sha256: "a".repeat(64),
            size: 12345,
            num_rows: 500,
        };

        let cloned = completion.clone();
        assert_eq!(cloned.index, 42);
        assert_eq!(cloned.path_in_repo, "data/train-00042.parquet");
        assert_eq!(cloned.sha256.len(), 64);
        assert_eq!(cloned.size, 12345);
        assert_eq!(cloned.num_rows, 500);
    }
}
