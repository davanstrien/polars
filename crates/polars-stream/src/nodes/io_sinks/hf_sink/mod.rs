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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use polars_core::config;
use polars_core::frame::DataFrame;
use polars_core::prelude::CompatLevel;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_err};
use polars_io::cloud::hf::commit::{CommitClient, CommitOperation, CommitOperationAdd};
use polars_io::cloud::hf::get_hf_token;
use polars_io::cloud::hf::lfs::client::LfsClient;
use polars_io::cloud::hf::lfs::upload::UploadExecutor;
use polars_io::cloud::hf::options::HfSinkOptions;
use polars_io::cloud::hf::shard_writer::{FinishedShard, HfShardWriter};
use polars_io::schema_to_arrow_checked;
use polars_parquet::write::{
    ColumnWriteOptions, CompressionOptions, StatisticsOptions, Version, WriteOptions,
};
use polars_plan::dsl::SinkOptions;

use super::phase::PhaseOutcome;
use super::{SinkInputPort, SinkNode};
use crate::async_executor::spawn;
use crate::async_primitives::connector::{Receiver, Sender, connector};
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

/// Default maximum rows per shard (500K rows).
///
/// This is a reasonable default for datasets - approximately 500MB assuming
/// ~1KB per row on average. Can be overridden via `max_shard_rows` option.
#[allow(dead_code)]
const DEFAULT_SHARD_ROWS: usize = 500_000;

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

/// Convert a Polars DataFrame to an Arrow RecordBatch.
///
/// This rechunks the DataFrame to ensure each column has a single chunk,
/// then converts each column to Arrow format and creates a RecordBatch.
///
/// # Arguments
/// * `df` - The DataFrame to convert
/// * `schema` - The Polars schema (used to generate Arrow schema)
///
/// # Errors
/// Returns an error if schema conversion fails or if column conversion fails.
fn df_to_record_batch(df: DataFrame, schema: &SchemaRef) -> PolarsResult<RecordBatch> {
    // 1. Rechunk so each column has a single chunk
    let df = df.rechunk();

    // 2. Convert Polars schema to Arrow schema
    let arrow_schema = schema_to_arrow_checked(schema, CompatLevel::newest(), "parquet")?;

    // 3. Convert each column to Arrow
    let arrays: Vec<_> = df
        .get_columns()
        .iter()
        .map(|col| {
            col.as_materialized_series()
                .to_arrow(0, CompatLevel::newest())
        })
        .collect();

    // 4. Create RecordBatch
    RecordBatch::try_new(Arc::new(arrow_schema), arrays)
        .map_err(|e| polars_err!(ComputeError: "failed to create RecordBatch: {}", e))
}

/// Create an HfShardWriter for the given schema and options.
///
/// Converts the Polars schema to Arrow, creates write options for Parquet,
/// and initializes a new shard writer with the configured buffer capacity.
///
/// # Arguments
/// * `schema` - The Polars schema for the data
/// * `options` - HF sink options (contains max_shard_size, etc.)
///
/// # Errors
/// Returns an error if schema conversion fails or buffer creation fails.
fn create_shard_writer(schema: &SchemaRef, options: &HfSinkOptions) -> PolarsResult<HfShardWriter> {
    // 1. Convert Polars schema to Arrow schema
    let arrow_schema = schema_to_arrow_checked(schema, CompatLevel::newest(), "parquet")?;

    // 2. Create write options (using reasonable defaults for HF Hub)
    let write_options = WriteOptions {
        statistics: StatisticsOptions::full(),
        compression: CompressionOptions::Snappy,
        version: Version::V2,
        data_page_size: None, // Use default
    };

    // 3. Create column options (default for each column)
    let column_options: Vec<ColumnWriteOptions> = arrow_schema
        .iter_values()
        .map(|_| ColumnWriteOptions::default())
        .collect();

    // 4. Create shard writer with max_shard_size as initial capacity
    HfShardWriter::new(
        arrow_schema,
        options.max_shard_size,
        write_options,
        column_options,
    )
}

/// Determine if the current shard should be rotated based on row count.
///
/// Uses `max_shard_rows` from options if set, otherwise falls back to
/// the default of 500K rows.
///
/// # Arguments
/// * `current_rows` - Number of rows in the current shard
/// * `options` - HF sink options
fn should_rotate_shard(current_rows: usize, options: &HfSinkOptions) -> bool {
    let max_rows = options.max_shard_rows.unwrap_or(DEFAULT_SHARD_ROWS);
    current_rows >= max_rows
}

// ============================================================================
// Shard Writer Task
// ============================================================================

/// Spawn a task that buffers morsels and writes them to HfShardWriter.
///
/// This task:
/// 1. Receives morsels from the streaming engine
/// 2. Buffers rows until chunk_size is reached
/// 3. Converts buffered rows to Arrow RecordBatch
/// 4. Writes to HfShardWriter
/// 5. Rotates shards when row limit is reached
/// 6. Sends finished shards to upload task via channel
///
/// # Arguments
/// * `recv_port_rx` - Receiver for incoming morsel phases
/// * `shard_tx` - Sender for completed shards (to upload task)
/// * `options` - HF sink options
/// * `schema` - Input data schema
///
/// # Errors
/// Returns an error if:
/// - Schema conversion fails
/// - Writing to shard fails
/// - Upload channel is closed (fail-fast behavior)
#[allow(dead_code)]
fn buffer_and_write_task(
    recv_port_rx: Receiver<(PhaseOutcome, SinkInputPort)>,
    shard_tx: Sender<FinishedShard>,
    options: Arc<HfSinkOptions>,
    schema: SchemaRef,
) -> JoinHandle<PolarsResult<()>> {
    spawn(TaskPriority::High, async move {
        let chunk_size = DEFAULT_CHUNK_SIZE;
        let mut buffer = DataFrame::empty_with_schema(schema.as_ref());
        let mut current_writer: Option<HfShardWriter> = None;
        let mut shard_rows: usize = 0;
        let mut state = WriterState::new();

        let mut recv_port_rx = recv_port_rx;

        while let Ok((outcome, rx)) = recv_port_rx.recv().await {
            let mut rx = rx.serial();

            while let Ok(morsel) = rx.recv().await {
                let (df, _, _, consume_token) = morsel.into_inner();

                // 1. Buffer the incoming DataFrame
                buffer.vstack_mut_owned(df)?;

                // 2. Process when buffer >= chunk_size
                while buffer.height() >= chunk_size {
                    let (batch_df, remainder) = buffer.split_at(chunk_size as i64);
                    buffer = remainder;

                    // 3. Ensure we have a shard writer
                    if current_writer.is_none() {
                        current_writer = Some(create_shard_writer(&schema, &options)?);
                    }
                    let writer = current_writer.as_mut().unwrap();

                    // 4. Convert and write
                    let batch = df_to_record_batch(batch_df, &schema)?;
                    writer.write_batch(batch)?;
                    shard_rows += chunk_size;

                    // 5. Check if shard is full
                    if should_rotate_shard(shard_rows, &options) {
                        let finished = current_writer.take().unwrap().finish()?;
                        let shard_idx = state.next_shard_index();
                        let path = shard_path(&options.path_in_repo, &options.split, shard_idx);

                        // Record completion for tracking
                        state.record_completion(ShardCompletion::from_finished(
                            shard_idx, path, &finished,
                        ));

                        // Send for upload (fail-fast if channel closed)
                        shard_tx.send(finished).await.map_err(
                            |_| polars_err!(ComputeError: "upload channel closed unexpectedly"),
                        )?;

                        shard_rows = 0;
                    }
                }

                // Drop consume token after processing (for backpressure)
                drop(consume_token);
            }

            outcome.stopped();
        }

        // Flush any remaining data in buffer or current shard
        if buffer.height() > 0 {
            // Write remaining buffer to current (or new) shard
            if current_writer.is_none() {
                current_writer = Some(create_shard_writer(&schema, &options)?);
            }
            let writer = current_writer.as_mut().unwrap();
            let batch = df_to_record_batch(buffer, &schema)?;
            writer.write_batch(batch)?;
        }

        // Finish the last shard if it has data
        if let Some(writer) = current_writer.take() {
            if writer.rows_written() > 0 {
                let finished = writer.finish()?;
                let shard_idx = state.next_shard_index();
                let path = shard_path(&options.path_in_repo, &options.split, shard_idx);

                state.record_completion(ShardCompletion::from_finished(shard_idx, path, &finished));

                // Send final shard for upload
                let _ = shard_tx.send(finished).await;
            }
        }

        PolarsResult::Ok(())
    })
}

/// Spawn a task that receives finished shards and uploads them to HF Hub.
///
/// This task:
/// 1. Receives `FinishedShard` from buffer_and_write_task via channel
/// 2. Requests upload URLs from LFS API
/// 3. Uploads data to presigned S3 URLs
/// 4. Sends `ShardCompletion` to coordinator for atomic commit
///
/// # Arguments
/// * `shard_rx` - Receiver for finished shards (from buffer_and_write_task)
/// * `completion_tx` - Sender for upload completions (to coordinator)
/// * `path_in_repo` - Base path in repository (e.g., "data")
/// * `split` - Split name (e.g., "train")
/// * `lfs_client` - LFS client for requesting upload URLs
/// * `upload_executor` - Executor for uploading data
///
/// # Errors
/// Returns an error if:
/// - LFS API request fails
/// - Upload fails after retries
/// - Completion channel is closed
#[allow(dead_code)]
fn upload_shard_task(
    shard_rx: Receiver<FinishedShard>,
    completion_tx: Sender<ShardCompletion>,
    path_in_repo: String,
    split: String,
    lfs_client: LfsClient,
    upload_executor: UploadExecutor,
) -> JoinHandle<PolarsResult<()>> {
    spawn(TaskPriority::Low, async move {
        let mut shard_rx = shard_rx;
        let mut shard_index = 0usize;

        while let Ok(finished_shard) = shard_rx.recv().await {
            // Log upload start if verbose
            if config::verbose() {
                eprintln!(
                    "HF sink: uploading shard {} ({} bytes, {} rows)",
                    shard_index, finished_shard.size, finished_shard.num_rows
                );
            }

            // 1. Generate path for this shard
            let path = shard_path(&path_in_repo, &split, shard_index);

            // 2. Request upload URL from LFS
            let transfer = lfs_client
                .request_upload(&finished_shard.sha256, finished_shard.size)
                .await?;

            // 3. Upload data (handles AlreadyExists, Basic, Multipart)
            let maybe_completions = upload_executor
                .upload(finished_shard.buffer, transfer, &finished_shard.sha256)
                .await?;

            // 4. Complete multipart if needed
            if let Some(completions) = maybe_completions {
                lfs_client
                    .complete_multipart(&finished_shard.sha256, completions)
                    .await?;
            }

            // 5. Create and send completion
            let completion = ShardCompletion {
                index: shard_index,
                path_in_repo: path,
                sha256: finished_shard.sha256,
                size: finished_shard.size,
                num_rows: finished_shard.num_rows,
            };

            completion_tx
                .send(completion)
                .await
                .map_err(|_| polars_err!(ComputeError: "completion channel closed unexpectedly"))?;

            // Log upload complete if verbose
            if config::verbose() {
                eprintln!("HF sink: shard {} uploaded successfully", shard_index);
            }

            shard_index += 1;
        }

        PolarsResult::Ok(())
    })
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
    /// Background IO/upload task handle (legacy, unused)
    #[allow(dead_code)]
    io_task: Option<AbortOnDropHandle<PolarsResult<()>>>,
    /// Channel sender for finished shards (buffer_and_write_task → upload_shard_task)
    shard_tx: Option<Sender<FinishedShard>>,
    /// Channel receiver for shard completions (for finalize/commit)
    completion_rx: Option<Receiver<ShardCompletion>>,
    /// Handle to await upload task completion
    upload_task: Option<JoinHandle<PolarsResult<()>>>,
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
            shard_tx: None,
            completion_rx: None,
            upload_task: None,
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
        // 1. Resolve HF token (required for writes)
        let token = get_hf_token(self.options.token.as_deref(), true)?
            .expect("token required=true guarantees Some");

        // 2. Get bucket (repo type) and revision from options
        let bucket = self.options.repo_type.as_str();
        let revision = self.options.effective_revision();

        // 3. Create LFS client and upload executor
        let lfs_client = LfsClient::new(bucket, &self.options.repo_id, &revision, token)?;
        let upload_executor = UploadExecutor::new()?;

        // 4. Create channels for shard pipeline:
        //    buffer_and_write_task → shard_tx/shard_rx → upload_shard_task → completion_tx/rx
        let (shard_tx, shard_rx) = connector::<FinishedShard>();
        let (completion_tx, completion_rx) = connector::<ShardCompletion>();

        // 5. Spawn upload task (background) - receives finished shards and uploads to HF Hub
        let path_in_repo = self.options.path_in_repo.clone();
        let split = self.options.split.clone();
        let upload_task = upload_shard_task(
            shard_rx,
            completion_tx,
            path_in_repo,
            split,
            lfs_client,
            upload_executor,
        );

        // 6. Store channels and task handle for use in spawn_sink() and finalize()
        self.shard_tx = Some(shard_tx);
        self.completion_rx = Some(completion_rx);
        self.upload_task = Some(upload_task);

        Ok(())
    }

    fn spawn_sink(
        &mut self,
        recv_port_rx: Receiver<(PhaseOutcome, SinkInputPort)>,
        _state: &StreamingExecutionState,
        join_handles: &mut Vec<JoinHandle<PolarsResult<()>>>,
    ) {
        // Take the shard sender from initialize() - this connects to upload_shard_task
        let shard_tx = self
            .shard_tx
            .take()
            .expect("initialize() must be called before spawn_sink()");

        // Spawn the buffer-and-write task that processes morsels and writes to shards
        let task = buffer_and_write_task(
            recv_port_rx,
            shard_tx,
            Arc::clone(&self.options),
            self.input_schema.clone(),
        );

        join_handles.push(task);
    }

    fn finalize(
        &mut self,
        _state: &StreamingExecutionState,
    ) -> Option<Pin<Box<dyn Future<Output = PolarsResult<()>> + Send>>> {
        // 1. Take upload task handle - must be awaited before collecting completions
        let upload_task = self.upload_task.take()?;

        // 2. Take completion receiver - used to collect all shard completions
        let completion_rx = self.completion_rx.take()?;

        // 3. Clone options for use in async block (Arc clone is cheap)
        let options = Arc::clone(&self.options);

        Some(Box::pin(async move {
            // Step A: Wait for upload task to complete
            // This ensures all shards are uploaded before we commit
            upload_task.await?;

            // Step B: Collect all ShardCompletions from the channel
            let mut completions = Vec::new();
            let mut completion_rx = completion_rx;
            while let Ok(completion) = completion_rx.recv().await {
                completions.push(completion);
            }

            // Step C: Early return if no shards were written
            if completions.is_empty() {
                if config::verbose() {
                    eprintln!("HF sink: no shards to commit (empty dataset)");
                }
                return Ok(());
            }

            // Step D: Resolve token for commit (required for write access)
            let token = get_hf_token(options.token.as_deref(), true)?
                .expect("token required=true guarantees Some");

            // Step E: Create CommitClient
            let commit_client = CommitClient::new(
                options.repo_type.as_str(),
                &options.repo_id,
                &options.effective_revision(),
                token,
            )?;

            // Step F: Build commit operations from ShardCompletions
            let operations: Vec<CommitOperation> = completions
                .iter()
                .map(|c| {
                    CommitOperation::Add(CommitOperationAdd {
                        path_in_repo: c.path_in_repo.clone(),
                        oid: c.sha256.clone(),
                        size: c.size,
                    })
                })
                .collect();

            // Step G: Build commit message
            let total_rows: usize = completions.iter().map(|c| c.num_rows).sum();
            let total_bytes: u64 = completions.iter().map(|c| c.size).sum();
            let summary = options
                .commit_message
                .clone()
                .unwrap_or_else(|| "Upload via Polars".to_string());
            let description = format!(
                "Uploaded {} shard(s) with {} rows ({:.2} MB)",
                operations.len(),
                total_rows,
                total_bytes as f64 / (1024.0 * 1024.0)
            );

            // Step H: Execute atomic commit
            let commit_info = commit_client
                .create_commit(&summary, Some(&description), &operations, options.create_pr)
                .await?;

            // Step I: Log success
            if config::verbose() {
                eprintln!(
                    "HF sink: committed {} shards to {}",
                    operations.len(),
                    commit_info.commit_url
                );
                if let Some(pr_url) = &commit_info.pr_url {
                    eprintln!("HF sink: created PR at {}", pr_url);
                }
            }

            Ok(())
        }))
    }

    fn get_metrics(&self) -> PolarsResult<Option<super::metrics::WriteMetrics>> {
        // TODO (Task 6.3): Implement metrics collection
        Ok(None)
    }
}

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

    // ========================================================================
    // Tests for Task 4.2.2: buffer_and_write_task helpers
    // ========================================================================

    #[test]
    fn test_should_rotate_shard_with_explicit_max() {
        // Options with explicit max_shard_rows
        let options = HfSinkOptions {
            repo_id: "user/repo".to_string(),
            path_in_repo: "data".to_string(),
            max_shard_rows: Some(1000),
            ..Default::default()
        };

        // Below limit - should not rotate
        assert!(!should_rotate_shard(999, &options));

        // At limit - should rotate
        assert!(should_rotate_shard(1000, &options));

        // Above limit - should rotate
        assert!(should_rotate_shard(1001, &options));
    }

    #[test]
    fn test_should_rotate_shard_uses_default() {
        // Options without max_shard_rows (uses DEFAULT_SHARD_ROWS = 500_000)
        let options = HfSinkOptions {
            repo_id: "user/repo".to_string(),
            path_in_repo: "data".to_string(),
            max_shard_rows: None,
            ..Default::default()
        };

        // Below default limit
        assert!(!should_rotate_shard(499_999, &options));

        // At default limit
        assert!(should_rotate_shard(500_000, &options));

        // Above default limit
        assert!(should_rotate_shard(500_001, &options));
    }

    #[test]
    fn test_should_rotate_shard_zero_rows() {
        let options = HfSinkOptions {
            repo_id: "user/repo".to_string(),
            path_in_repo: "data".to_string(),
            max_shard_rows: Some(1000),
            ..Default::default()
        };

        // Zero rows should never trigger rotation
        assert!(!should_rotate_shard(0, &options));
    }

    #[test]
    fn test_default_constants() {
        // Verify our constants have reasonable values
        assert_eq!(DEFAULT_CHUNK_SIZE, 256 * 1024); // 256K rows
        assert_eq!(DEFAULT_SHARD_ROWS, 500_000); // 500K rows
        assert_eq!(COMPLETION_CHANNEL_SIZE, 16); // 16 shards buffer
    }

    // ========================================================================
    // Tests for Task 4.2.3: upload_shard_task
    // ========================================================================

    #[test]
    fn test_upload_task_types_available() {
        // Verify that the LFS client and upload executor types are importable
        // and that upload_shard_task signature compiles correctly.
        // This is a compile-time check - the function exists and types align.
        fn _assert_types_compile(
            shard_rx: Receiver<FinishedShard>,
            completion_tx: Sender<ShardCompletion>,
            path_in_repo: String,
            split: String,
            lfs_client: LfsClient,
            upload_executor: UploadExecutor,
        ) -> JoinHandle<PolarsResult<()>> {
            upload_shard_task(
                shard_rx,
                completion_tx,
                path_in_repo,
                split,
                lfs_client,
                upload_executor,
            )
        }
        // If this compiles, the test passes
    }

    #[test]
    fn test_shard_path_for_upload() {
        // Test that shard_path generates correct paths for upload task
        // This is the same function used by both buffer_and_write_task and upload_shard_task
        let path = shard_path("data", "train", 0);
        assert_eq!(path, "data/train-00000.parquet");

        let path = shard_path("output/processed", "validation", 15);
        assert_eq!(path, "output/processed/validation-00015.parquet");
    }

    #[test]
    fn test_shard_completion_fields_for_upload() {
        // Verify ShardCompletion has all fields needed for commit operations
        let completion = ShardCompletion {
            index: 5,
            path_in_repo: "data/train-00005.parquet".to_string(),
            sha256: "abc123def456".repeat(5) + "abcd", // 64 chars
            size: 500_000_000,                         // 500MB
            num_rows: 1_000_000,
        };

        // These are the fields needed by CommitOperationAdd
        assert_eq!(completion.path_in_repo, "data/train-00005.parquet");
        assert_eq!(completion.sha256.len(), 64); // SHA256 hex length
        assert_eq!(completion.size, 500_000_000);

        // Also verify index and rows are tracked for metrics
        assert_eq!(completion.index, 5);
        assert_eq!(completion.num_rows, 1_000_000);
    }
}
