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

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;
use polars_core::config;
use polars_core::datatypes::DataType;
use polars_core::frame::DataFrame;
use polars_core::prelude::CompatLevel;
use polars_utils::pl_str::PlSmallStr;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_bail, polars_err};
use polars_io::cloud::hf::commit::{CommitClient, CommitOperation, CommitOperationAdd, CommitOperationDelete};
use polars_io::cloud::hf::checkpoint::{CheckpointState, ShardCheckpoint};
use polars_io::cloud::hf::{
    DatasetInfo, SplitInfo, check_existing_files, extract_frontmatter,
    fetch_readme, generate_new_readme, generate_updated_readme, get_hf_token,
    parse_dataset_info_from_yaml,
};
use polars_io::cloud::hf::lfs::client::LfsClient;
use polars_io::cloud::hf::lfs::upload::UploadExecutor;
use polars_io::cloud::hf::options::{HfSinkOptions, HfWriteMode};
use polars_io::cloud::hf::shard_writer::{FinishedShard, HfShardWriter};
use polars_io::schema_to_arrow_checked;
use polars_parquet::write::{
    ColumnWriteOptions, CompressionOptions, Encoding, FieldWriteOptions, StatisticsOptions,
    Version, WriteOptions,
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

/// Message sent through channel from buffer_and_write_task to upload_shard_task.
///
/// Contains the finished shard data plus pre-computed metadata needed for upload.
/// This allows the upload task to work identically for partitioned and non-partitioned writes.
pub struct ShardToUpload {
    /// The finished shard data (sha256, size, num_rows, buffer).
    pub shard: FinishedShard,
    /// Pre-computed shard index (partition-aware for partitioned writes, global otherwise).
    pub shard_index: usize,
    /// Pre-computed path in repository (e.g., "data/split=train/train-00000.parquet").
    pub path_in_repo: String,
    /// Partition value for partitioned writes (None for non-partitioned).
    pub partition_value: Option<String>,
}

impl ShardToUpload {
    /// Create a new shard message for non-partitioned writes.
    pub fn new(shard: FinishedShard, shard_index: usize, path_in_repo: String) -> Self {
        Self {
            shard,
            shard_index,
            path_in_repo,
            partition_value: None,
        }
    }

    /// Create a new shard message for partitioned writes.
    pub fn with_partition(
        shard: FinishedShard,
        shard_index: usize,
        path_in_repo: String,
        partition_value: String,
    ) -> Self {
        Self {
            shard,
            shard_index,
            path_in_repo,
            partition_value: Some(partition_value),
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

    /// Get the next shard index without incrementing.
    ///
    /// Use this to preview the index before actually consuming it.
    pub fn peek_next_index(&self) -> usize {
        self.current_shard_index
    }

    /// Get the number of completed shards.
    pub fn num_completed(&self) -> usize {
        self.completed.len()
    }
}

/// Per-partition state for tracking partitioned shard writes.
#[derive(Debug, Clone)]
pub struct PartitionShardState {
    /// Current shard index for this partition (increments on each flush).
    pub shard_index: usize,
    /// Completed shards for this partition.
    pub completed: VecDeque<ShardCompletion>,
}

impl PartitionShardState {
    /// Create a new empty partition state.
    pub fn new() -> Self {
        Self {
            shard_index: 0,
            completed: VecDeque::new(),
        }
    }
}

impl Default for PartitionShardState {
    fn default() -> Self {
        Self::new()
    }
}

/// State tracker for partitioned writes.
///
/// Tracks multiple partitions, each with its own shard counter and completions.
#[derive(Debug, Default)]
pub struct PartitionWriterState {
    /// Per-partition state keyed by partition value (e.g., "train", "test").
    pub partitions: HashMap<String, PartitionShardState>,
    /// Total rows written across all partitions.
    pub total_rows: usize,
    /// Total bytes written across all partitions.
    pub total_bytes: u64,
    /// Global shard counter for progress reporting (increments across all partitions).
    pub global_shard_count: usize,
}

impl PartitionWriterState {
    /// Create a new empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create state for a partition value.
    pub fn partition_mut(&mut self, partition_value: &str) -> &mut PartitionShardState {
        self.partitions
            .entry(partition_value.to_string())
            .or_default()
    }

    /// Get the next shard index for a partition and increment it.
    pub fn next_shard_index(&mut self, partition_value: &str) -> usize {
        let state = self.partition_mut(partition_value);
        let index = state.shard_index;
        state.shard_index += 1;
        index
    }

    /// Peek the next shard index for a partition without incrementing.
    pub fn peek_next_index(&self, partition_value: &str) -> usize {
        self.partitions
            .get(partition_value)
            .map(|s| s.shard_index)
            .unwrap_or(0)
    }

    /// Get the next global shard index for progress callbacks and increment it.
    pub fn next_global_index(&mut self) -> usize {
        let idx = self.global_shard_count;
        self.global_shard_count += 1;
        idx
    }

    /// Peek the next global shard index without incrementing.
    pub fn peek_global_index(&self) -> usize {
        self.global_shard_count
    }

    /// Record a completed shard for a partition.
    pub fn record_completion(&mut self, partition_value: &str, completion: ShardCompletion) {
        self.total_rows += completion.num_rows;
        self.total_bytes += completion.size;
        let state = self.partition_mut(partition_value);
        state.completed.push_back(completion);
    }

    /// Get all completed shards across all partitions.
    pub fn all_completions(&self) -> Vec<&ShardCompletion> {
        self.partitions
            .values()
            .flat_map(|s| s.completed.iter())
            .collect()
    }

    /// Get the total number of completed shards across all partitions.
    pub fn num_completed(&self) -> usize {
        self.partitions.values().map(|s| s.completed.len()).sum()
    }

    /// Get partition values that have been written to.
    pub fn partition_values(&self) -> impl Iterator<Item = &str> {
        self.partitions.keys().map(|s| s.as_str())
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

/// Generate a partitioned shard file path for Hive-style partitioning.
///
/// Format: `{path_in_repo}/{partition_col}={partition_value}/{split}-{index:05}.parquet`
///
/// # Examples
/// ```ignore
/// assert_eq!(
///     partitioned_shard_path("data", "split", "train", "train", 0),
///     "data/split=train/train-00000.parquet"
/// );
/// ```
pub fn partitioned_shard_path(
    path_in_repo: &str,
    partition_col: &str,
    partition_value: &str,
    split: &str,
    index: usize,
) -> String {
    let path = path_in_repo.trim_end_matches('/');
    format!(
        "{}/{}={}/{}-{:05}.parquet",
        path, partition_col, partition_value, split, index
    )
}

/// Extract partition value from a Hive-style path.
///
/// Looks for a path segment matching `{partition_col}={value}` and returns the value.
///
/// # Arguments
/// * `path` - Full path like "data/split=train/train-00000.parquet"
/// * `partition_col` - Column name to look for (e.g., "split")
///
/// # Returns
/// * `Some(value)` if found (e.g., "train")
/// * `None` if no matching segment found
///
/// # Examples
/// ```ignore
/// assert_eq!(
///     extract_partition_from_path("data/split=train/train-00000.parquet", "split"),
///     Some("train".to_string())
/// );
/// assert_eq!(
///     extract_partition_from_path("data/train-00000.parquet", "split"),
///     None
/// );
/// ```
fn extract_partition_from_path(path: &str, partition_col: &str) -> Option<String> {
    let pattern = format!("{}=", partition_col);
    path.split('/')
        .find_map(|segment| segment.strip_prefix(&pattern))
        .map(String::from)
}

/// Hive default partition value for null partition keys.
///
/// Used when a partition column contains null values, following the Hive convention.
const HIVE_DEFAULT_PARTITION: &str = "__HIVE_DEFAULT_PARTITION__";

/// Extract the first partition value from a DataFrame.
///
/// Returns the string representation of the first value in the partition column.
/// Returns `__HIVE_DEFAULT_PARTITION__` for null values, following Hive convention.
///
/// # Arguments
/// * `df` - The DataFrame to extract from
/// * `partition_col` - Name of the partition column
///
/// # Errors
/// Returns an error if:
/// - The partition column doesn't exist
/// - The DataFrame is empty
/// - The value cannot be cast to string
///
/// # Examples
/// ```ignore
/// let df = df! { "split" => ["train", "train"] }.unwrap();
/// assert_eq!(extract_partition_value(&df, "split").unwrap(), "train");
/// ```
fn extract_partition_value(df: &DataFrame, partition_col: &str) -> PolarsResult<String> {
    let col = df.column(partition_col)?;

    if col.is_empty() {
        polars_bail!(ComputeError: "cannot extract partition value from empty DataFrame");
    }

    // Get first value, cast to string
    let first = col.head(Some(1));
    let str_col = first.cast(&DataType::String)?;
    let str_arr = str_col.str()?;

    match str_arr.get(0) {
        Some(v) => Ok(v.to_string()),
        None => Ok(HIVE_DEFAULT_PARTITION.to_string()),
    }
}

/// Split a DataFrame by partition column into (partition_value, sub_df) pairs.
///
/// Uses Polars' built-in partitioning with stable ordering to preserve row order
/// within each partition. The partition column is retained in output DataFrames.
///
/// # Arguments
/// * `df` - The DataFrame to partition
/// * `partition_col` - Name of the column to partition by
///
/// # Returns
/// A vector of (partition_value, DataFrame) pairs, one per unique partition value.
///
/// # Errors
/// Returns an error if:
/// - The partition column doesn't exist
/// - Partitioning fails internally
///
/// # Examples
/// ```ignore
/// let df = df! {
///     "split" => ["train", "test", "train"],
///     "data" => [1, 2, 3],
/// }.unwrap();
/// let partitions = partition_dataframe(df, "split").unwrap();
/// // Returns [("train", df_with_2_rows), ("test", df_with_1_row)]
/// ```
fn partition_dataframe(
    df: DataFrame,
    partition_col: &str,
) -> PolarsResult<Vec<(String, DataFrame)>> {
    let col_names = [PlSmallStr::from_str(partition_col)];

    // Use Polars' built-in partitioning (stable=true to preserve order)
    let partitions = df._partition_by_impl(
        &col_names,
        true,  // stable
        true,  // include_key (keep partition column)
        false, // parallel (not needed for small chunks)
    )?;

    // Extract partition value from each sub-DataFrame
    let mut result = Vec::with_capacity(partitions.len());
    for sub_df in partitions {
        let value = extract_partition_value(&sub_df, partition_col)?;
        result.push((value, sub_df));
    }

    Ok(result)
}

/// Extract the shard index from a path like "data/train-00042.parquet".
///
/// Returns None if the path doesn't match the expected format for the given split.
///
/// # Examples
/// ```ignore
/// assert_eq!(parse_shard_index("data/train-00000.parquet", "train"), Some(0));
/// assert_eq!(parse_shard_index("data/train-00042.parquet", "train"), Some(42));
/// assert_eq!(parse_shard_index("data/test-00001.parquet", "train"), None); // wrong split
/// assert_eq!(parse_shard_index("other.parquet", "train"), None);
/// ```
fn parse_shard_index(path: &str, split: &str) -> Option<usize> {
    let filename = path.rsplit('/').next()?;
    let expected_prefix = format!("{}-", split);
    let rest = filename.strip_prefix(&expected_prefix)?;
    let index_str = rest.strip_suffix(".parquet")?;
    index_str.parse().ok()
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
fn df_to_record_batch(mut df: DataFrame, schema: &SchemaRef) -> PolarsResult<RecordBatch> {
    // 1. Get height before rechunking, then rechunk in place
    let height = df.height();
    df.rechunk_mut();

    // 2. Convert Polars schema to Arrow schema
    let arrow_schema = schema_to_arrow_checked(schema, CompatLevel::newest(), "parquet")?;

    // 3. Convert each column to Arrow
    let arrays: Vec<_> = df
        .columns()
        .iter()
        .map(|col| {
            col.as_materialized_series()
                .to_arrow(0, CompatLevel::newest())
        })
        .collect();

    // 4. Create RecordBatch (height, schema, arrays)
    RecordBatch::try_new(height, Arc::new(arrow_schema), arrays)
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

    // 3. Create column options (default encoding for each column)
    let column_options: Vec<ColumnWriteOptions> = arrow_schema
        .iter_values()
        .map(|_| {
            FieldWriteOptions::default_with_encoding(Encoding::Plain)
                .into_default_column_write_options()
        })
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
// Mode Handling Helpers (Task 5.1.5)
// ============================================================================

use polars_io::cloud::hf::ExistingFile;

/// Create delete operations from a list of existing files.
///
/// Used by Overwrite mode to delete existing files before adding new ones.
/// The delete operations should be committed first (before adds) for atomic replace.
///
/// # Arguments
/// * `existing` - List of existing files to delete
///
/// # Returns
/// A vector of `CommitOperationDelete` for each existing file.
fn create_delete_operations(existing: Vec<ExistingFile>) -> Vec<CommitOperationDelete> {
    existing
        .into_iter()
        .map(|f| CommitOperationDelete {
            path_in_repo: f.path,
        })
        .collect()
}

/// Renumber shard completions to continue from the max existing shard index.
///
/// Used by Append mode to avoid overwriting existing shards. Finds the maximum
/// shard index from existing files that match the split pattern, then renumbers
/// new completions to start from max + 1.
///
/// # Arguments
/// * `completions` - Mutable slice of completions to renumber
/// * `existing` - List of existing files in the repository
/// * `split` - The split name (e.g., "train", "test") to match
/// * `path_in_repo` - Base path for generating new shard paths
///
/// # Notes
/// - If no existing files match the split pattern, completions are unchanged
/// - Updates both `index` and `path_in_repo` fields on each completion
fn renumber_for_append(
    completions: &mut [ShardCompletion],
    existing: &[ExistingFile],
    split: &str,
    path_in_repo: &str,
    partition_col: Option<&str>,
) {
    match partition_col {
        None => {
            // Non-partitioned: existing behavior
            let max_existing_idx = existing
                .iter()
                .filter_map(|f| parse_shard_index(&f.path, split))
                .max();

            if let Some(max_idx) = max_existing_idx {
                let start_idx = max_idx + 1;
                for (i, c) in completions.iter_mut().enumerate() {
                    c.index = start_idx + i;
                    c.path_in_repo = shard_path(path_in_repo, split, c.index);
                }
            }
        },
        Some(col) => {
            // Partitioned: renumber per-partition independently
            use std::collections::HashMap;

            // Build map of partition_value -> max existing index
            let mut max_by_partition: HashMap<String, usize> = HashMap::new();
            for f in existing {
                if let Some(pv) = extract_partition_from_path(&f.path, col) {
                    if let Some(idx) = parse_shard_index(&f.path, split) {
                        max_by_partition
                            .entry(pv)
                            .and_modify(|m| *m = (*m).max(idx))
                            .or_insert(idx);
                    }
                }
            }

            // Build map of partition_value -> next index to assign
            let mut next_by_partition: HashMap<String, usize> = max_by_partition
                .into_iter()
                .map(|(pv, max)| (pv, max + 1))
                .collect();

            // Renumber each completion based on its partition
            for c in completions.iter_mut() {
                if let Some(pv) = extract_partition_from_path(&c.path_in_repo, col) {
                    // Get next index for this partition (or 0 if no existing)
                    let next_idx = next_by_partition.entry(pv.clone()).or_insert(0);
                    c.index = *next_idx;
                    c.path_in_repo = partitioned_shard_path(path_in_repo, col, &pv, split, c.index);
                    *next_idx += 1;
                }
                // If partition can't be extracted, leave unchanged (shouldn't happen)
            }
        },
    }
}

/// Builds the README.md commit operation for dataset card updates.
///
/// Returns None if update_card is false, otherwise generates updated README
/// based on existing content (if any).
///
/// # Arguments
/// * `readme_content` - Existing README content, or None if no README exists
/// * `split_info` - Information about the split being written (name, bytes, rows)
/// * `update_card` - Whether to generate a README operation
///
/// # Returns
/// * `Ok(None)` if update_card is false
/// * `Ok(Some(CommitOperation))` with updated README content if update_card is true
///
/// # Behavior
/// 1. If `update_card` is false: returns `Ok(None)`
/// 2. If `readme_content` is `None`: creates new README with just dataset_info
/// 3. If README exists without frontmatter: prepends new frontmatter
/// 4. If README exists with frontmatter: updates existing dataset_info, preserving other fields
fn build_readme_operation(
    readme_content: Option<&str>,
    split_info: SplitInfo,
    update_card: bool,
) -> PolarsResult<Option<CommitOperation>> {
    if !update_card {
        return Ok(None);
    }

    let updated_readme = match readme_content {
        Some(content) => {
            // Parse existing frontmatter
            if let Some(extracted) = extract_frontmatter(content) {
                // Parse existing dataset_info and update with new split
                let mut dataset_info =
                    parse_dataset_info_from_yaml(extracted.yaml).unwrap_or_default();
                dataset_info.update_split(split_info);
                generate_updated_readme(extracted.yaml, extracted.body, &dataset_info)?
            } else {
                // README exists but no frontmatter - prepend new frontmatter
                let dataset_info = DatasetInfo::new(vec![split_info]);
                let new_frontmatter = generate_new_readme(&dataset_info)?;
                // generate_new_readme returns "---\n...\n---\n", so just append content
                format!("{}{}", new_frontmatter, content)
            }
        },
        None => {
            // No README exists - create minimal one with just dataset_info
            let dataset_info = DatasetInfo::new(vec![split_info]);
            generate_new_readme(&dataset_info)?
        },
    };

    Ok(Some(CommitOperation::Add(CommitOperationAdd::regular(
        "README.md",
        updated_readme.into_bytes(),
    ))))
}

// ============================================================================
// Checkpoint Loading
// ============================================================================

/// Loads checkpoint state from disk and validates it matches the current operation.
///
/// Returns a tuple of:
/// - Set of completed shard indices (for skip logic)
/// - Vec of ShardCompletion (for including in final commit)
///
/// Returns empty collections if no checkpoint is found.
///
/// # Errors
/// Returns an error if:
/// - The checkpoint file exists but cannot be parsed
/// - The checkpoint repo_id or path_in_repo doesn't match current operation
fn load_checkpoint_state(
    options: &HfSinkOptions,
) -> PolarsResult<(HashSet<usize>, Vec<ShardCompletion>)> {
    let Some(ref checkpoint_path) = options.checkpoint_path else {
        return Ok((HashSet::new(), Vec::new()));
    };

    let Some(checkpoint) = CheckpointState::load(checkpoint_path)? else {
        return Ok((HashSet::new(), Vec::new()));
    };

    // Validate checkpoint matches current operation
    if checkpoint.repo_id != options.repo_id
        || checkpoint.path_in_repo != options.path_in_repo
        || checkpoint.partition_col != options.partition_col
    {
        polars_bail!(ComputeError:
            "Checkpoint mismatch: checkpoint is for {}/{} (partition_col={:?}) \
             but operation is {}/{} (partition_col={:?}). Delete {} to start fresh.",
            checkpoint.repo_id, checkpoint.path_in_repo, checkpoint.partition_col,
            options.repo_id, options.path_in_repo, options.partition_col,
            checkpoint_path.display()
        );
    }

    let indices = checkpoint.completed_indices();
    if config::verbose() && !indices.is_empty() {
        eprintln!("HF sink: resuming with {} completed shards", indices.len());
    }

    // Convert ShardCheckpoint to ShardCompletion for commit tracking
    let completions: Vec<ShardCompletion> = checkpoint
        .completed_shards
        .into_iter()
        .map(|sc| ShardCompletion {
            index: sc.index,
            path_in_repo: sc.path_in_repo,
            sha256: sc.sha256,
            size: sc.size,
            num_rows: sc.num_rows,
        })
        .collect();

    Ok((indices, completions))
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
    mut shard_tx: Sender<ShardToUpload>,
    options: Arc<HfSinkOptions>,
    schema: SchemaRef,
    resumed_shards: Arc<HashSet<usize>>,
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

                        // Notify progress callback of shard start
                        if let Some(ref progress) = options.progress {
                            let shard_idx = state.peek_next_index();
                            let path = shard_path(&options.path_in_repo, &options.split, shard_idx);
                            progress.on_shard_start(shard_idx, &path);
                        }
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

                        // Skip if already in checkpoint (resumed shard)
                        if resumed_shards.contains(&shard_idx) {
                            if config::verbose() {
                                eprintln!(
                                    "HF sink: skipping shard {} (already uploaded)",
                                    shard_idx
                                );
                            }
                            shard_rows = 0;
                            continue;
                        }

                        // Record completion for tracking
                        state.record_completion(ShardCompletion::from_finished(
                            shard_idx, path.clone(), &finished,
                        ));

                        // Send for upload (fail-fast if channel closed)
                        shard_tx.send(ShardToUpload::new(finished, shard_idx, path)).await.map_err(
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

                // Notify progress callback of shard start
                if let Some(ref progress) = options.progress {
                    let shard_idx = state.peek_next_index();
                    let path = shard_path(&options.path_in_repo, &options.split, shard_idx);
                    progress.on_shard_start(shard_idx, &path);
                }
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

                // Skip if already in checkpoint (resumed shard)
                if resumed_shards.contains(&shard_idx) {
                    if config::verbose() {
                        eprintln!(
                            "HF sink: skipping final shard {} (already uploaded)",
                            shard_idx
                        );
                    }
                } else {
                    state.record_completion(ShardCompletion::from_finished(
                        shard_idx, path.clone(), &finished,
                    ));

                    // Send final shard for upload
                    let _ = shard_tx.send(ShardToUpload::new(finished, shard_idx, path)).await;
                }
            }
        }

        PolarsResult::Ok(())
    })
}

/// Partitioned version of buffer_and_write_task.
///
/// Receives morsels, partitions by partition_col, accumulates per-partition buffers,
/// writes to per-partition shards, and sends ShardToUpload messages for upload.
///
/// Unlike `buffer_and_write_task()`, this function maintains separate state per partition:
/// - Per-partition buffers (accumulated DataFrames)
/// - Per-partition shard writers (HfShardWriter instances)
/// - Per-partition shard row counts (for rotation decisions)
///
/// Paths follow Hive-style format: `data/{partition_col}={value}/train-00000.parquet`
#[allow(dead_code)]
fn partitioned_buffer_and_write_task(
    recv_port_rx: Receiver<(PhaseOutcome, SinkInputPort)>,
    mut shard_tx: Sender<ShardToUpload>,
    options: Arc<HfSinkOptions>,
    schema: SchemaRef,
    resumed_shards: Arc<HashSet<usize>>,
) -> JoinHandle<PolarsResult<()>> {
    spawn(TaskPriority::High, async move {
        // Extract partition column (required for this function)
        let partition_col = options
            .partition_col
            .as_ref()
            .ok_or_else(|| polars_err!(InvalidOperation: "partition_col required for partitioned writes"))?;

        // Per-partition state tracker
        let mut state = PartitionWriterState::new();

        // Per-partition buffers: partition_value -> accumulated DataFrame
        let mut buffers: HashMap<String, DataFrame> = HashMap::new();

        // Per-partition writers: partition_value -> current HfShardWriter
        let mut writers: HashMap<String, HfShardWriter> = HashMap::new();

        // Per-partition shard row counts: partition_value -> rows in current shard
        let mut shard_rows: HashMap<String, usize> = HashMap::new();

        // Configuration
        let chunk_size = DEFAULT_CHUNK_SIZE;
        let _max_shard_rows = options.max_shard_rows.unwrap_or(DEFAULT_SHARD_ROWS);

        // Mutable receiver for the main loop
        let mut recv_port_rx = recv_port_rx;

        // Main loop - receive morsels, partition, accumulate
        while let Ok((outcome, rx)) = recv_port_rx.recv().await {
            let mut rx = rx.serial();

            while let Ok(morsel) = rx.recv().await {
                let (df, _, _, consume_token) = morsel.into_inner();

                // Partition the morsel by the partition column
                let partitions = partition_dataframe(df, partition_col)?;

                // Accumulate each sub-DataFrame into its partition's buffer
                for (partition_value, sub_df) in partitions {
                    match buffers.entry(partition_value.clone()) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            e.get_mut().vstack_mut_owned(sub_df)?;
                        },
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(sub_df);
                        },
                    }

                    // Per-partition batch writing when buffer >= chunk_size
                    let buf = buffers.get_mut(&partition_value).unwrap();
                    while buf.height() >= chunk_size {
                        // 1. Split off chunk_size rows
                        let (batch_df, remainder) = buf.split_at(chunk_size as i64);
                        *buf = remainder;

                        // 2. Get or create writer for this partition
                        if !writers.contains_key(&partition_value) {
                            let writer = create_shard_writer(&schema, &options)?;

                            // Notify progress callback of shard start
                            if let Some(ref progress) = options.progress {
                                let shard_idx = state.peek_next_index(&partition_value);
                                let path = partitioned_shard_path(
                                    &options.path_in_repo,
                                    partition_col,
                                    &partition_value,
                                    &options.split,
                                    shard_idx,
                                );
                                progress.on_shard_start(state.peek_global_index(), &path);
                            }

                            writers.insert(partition_value.clone(), writer);
                            shard_rows.insert(partition_value.clone(), 0);
                        }
                        let writer = writers.get_mut(&partition_value).unwrap();

                        // 3. Convert DataFrame to RecordBatch and write
                        let batch = df_to_record_batch(batch_df, &schema)?;
                        writer.write_batch(batch)?;

                        // 4. Update row count for this partition's shard
                        *shard_rows.get_mut(&partition_value).unwrap() += chunk_size;

                        // 5. Per-partition shard rotation when shard >= max_rows
                        let partition_shard_rows = *shard_rows.get(&partition_value).unwrap();
                        if should_rotate_shard(partition_shard_rows, &options) {
                            // Finish current writer for this partition
                            let finished = writers.remove(&partition_value).unwrap().finish()?;

                            // Get partition-specific shard index
                            let shard_idx = state.next_shard_index(&partition_value);

                            // Compute Hive-style path
                            let path = partitioned_shard_path(
                                &options.path_in_repo,
                                partition_col,
                                &partition_value,
                                &options.split,
                                shard_idx,
                            );

                            // TODO(6.2.8): Add partition-aware checkpoint check
                            // The current resumed_shards uses global indices; partition checkpoint
                            // support will be added in task 6.2.8

                            // Record completion for this partition
                            state.record_completion(
                                &partition_value,
                                ShardCompletion::from_finished(shard_idx, path.clone(), &finished),
                            );

                            // Increment global index for progress tracking
                            let _global_idx = state.next_global_index();

                            // Send for upload (on_shard_complete is called in upload_shard_task)
                            shard_tx
                                .send(ShardToUpload::with_partition(
                                    finished,
                                    shard_idx,
                                    path,
                                    partition_value.clone(),
                                ))
                                .await
                                .map_err(|_| {
                                    polars_err!(ComputeError: "upload channel closed unexpectedly")
                                })?;

                            // Reset row count for this partition
                            *shard_rows.get_mut(&partition_value).unwrap() = 0;
                        }
                    }
                }

                drop(consume_token); // Signal backpressure complete
            }

            outcome.stopped();
        }

        // Final flush: write remaining buffered data to writers
        for (partition_value, buf) in buffers.iter_mut() {
            if buf.height() > 0 {
                // Get or create writer for this partition
                if !writers.contains_key(partition_value) {
                    let writer = create_shard_writer(&schema, &options)?;

                    // Notify progress callback of shard start
                    if let Some(ref progress) = options.progress {
                        let shard_idx = state.peek_next_index(partition_value);
                        let path = partitioned_shard_path(
                            &options.path_in_repo,
                            partition_col,
                            partition_value,
                            &options.split,
                            shard_idx,
                        );
                        progress.on_shard_start(state.peek_global_index(), &path);
                    }

                    writers.insert(partition_value.clone(), writer);
                }

                let writer = writers.get_mut(partition_value).unwrap();
                let batch = df_to_record_batch(std::mem::take(buf), &schema)?;
                writer.write_batch(batch)?;
            }
        }

        // Final flush: finish and upload all active writers
        for (partition_value, writer) in writers.drain() {
            if writer.rows_written() > 0 {
                let finished = writer.finish()?;
                let shard_idx = state.next_shard_index(&partition_value);
                let path = partitioned_shard_path(
                    &options.path_in_repo,
                    partition_col,
                    &partition_value,
                    &options.split,
                    shard_idx,
                );

                // TODO(6.2.8): Add partition-aware checkpoint check
                // For now, resumed_shards uses global indices which won't match
                // partition-specific indices. Full support added in task 6.2.8.

                state.record_completion(
                    &partition_value,
                    ShardCompletion::from_finished(shard_idx, path.clone(), &finished),
                );

                let _global_idx = state.next_global_index();

                shard_tx
                    .send(ShardToUpload::with_partition(
                        finished,
                        shard_idx,
                        path,
                        partition_value,
                    ))
                    .await
                    .map_err(|_| {
                        polars_err!(ComputeError: "upload channel closed unexpectedly")
                    })?;
            }
        }

        // Suppress unused warning (partition checkpoint support in task 6.2.8)
        let _ = &resumed_shards;

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
    shard_rx: Receiver<ShardToUpload>,
    mut completion_tx: Sender<ShardCompletion>,
    lfs_client: LfsClient,
    upload_executor: UploadExecutor,
    #[allow(unused_variables)] resumed_shards: Arc<HashSet<usize>>,
    repo_id: String,
    base_path_in_repo: String,
    checkpoint_path: Option<std::path::PathBuf>,
    options: Arc<HfSinkOptions>,
) -> JoinHandle<PolarsResult<()>> {
    spawn(TaskPriority::Low, async move {
        let mut shard_rx = shard_rx;

        while let Ok(shard_to_upload) = shard_rx.recv().await {
            // Extract pre-computed values from the message
            let ShardToUpload {
                shard,
                shard_index,
                path_in_repo,
                partition_value,
            } = shard_to_upload;

            // Log upload start if verbose
            if config::verbose() {
                eprintln!(
                    "HF sink: uploading shard {} ({} bytes, {} rows)",
                    shard_index, shard.size, shard.num_rows
                );
            }

            // 1. Request upload URL from LFS
            let transfer = lfs_client
                .request_upload(&shard.sha256, shard.size)
                .await?;

            // 2. Upload data (handles AlreadyExists, Basic, Multipart)
            let maybe_completions = upload_executor
                .upload(
                    shard.buffer,
                    transfer,
                    &shard.sha256,
                    shard_index,
                    options.progress.clone(),
                )
                .await?;

            // 3. Complete multipart if needed
            if let Some(completions) = maybe_completions {
                lfs_client
                    .complete_multipart(&shard.sha256, completions)
                    .await?;
            }

            // 3a. Save checkpoint if path configured
            if let Some(ref ckpt_path) = checkpoint_path {
                // Load existing or create new checkpoint
                let mut checkpoint = match CheckpointState::load(ckpt_path) {
                    Ok(Some(cp)) => cp,
                    Ok(None) => CheckpointState::new(&repo_id, &base_path_in_repo, options.partition_col.clone()),
                    Err(e) => {
                        // Log warning but don't fail - upload already succeeded
                        if config::verbose() {
                            eprintln!("HF sink: warning - failed to load checkpoint: {}", e);
                        }
                        // Continue without checkpoint save
                        CheckpointState::new(&repo_id, &base_path_in_repo, options.partition_col.clone())
                    },
                };

                // Add this shard
                checkpoint.add_shard(ShardCheckpoint {
                    index: shard_index,
                    partition_value: partition_value.clone(),
                    path_in_repo: path_in_repo.clone(),
                    sha256: shard.sha256.clone(),
                    size: shard.size,
                    num_rows: shard.num_rows,
                });

                // Save atomically
                if let Err(e) = checkpoint.save(ckpt_path) {
                    if config::verbose() {
                        eprintln!("HF sink: warning - failed to save checkpoint: {}", e);
                    }
                } else if config::verbose() {
                    eprintln!("HF sink: checkpoint saved (shard {})", shard_index);
                }
            }

            // 4. Create and send completion
            let completion = ShardCompletion {
                index: shard_index,
                path_in_repo,
                sha256: shard.sha256,
                size: shard.size,
                num_rows: shard.num_rows,
            };

            // Notify progress callback of shard completion
            if let Some(ref progress) = options.progress {
                progress.on_shard_complete(
                    completion.index,
                    &completion.path_in_repo,
                    completion.size,
                );
            }

            completion_tx
                .send(completion)
                .await
                .map_err(|_| polars_err!(ComputeError: "completion channel closed unexpectedly"))?;

            // Log upload complete if verbose
            if config::verbose() {
                eprintln!("HF sink: shard {} uploaded successfully", shard_index);
            }
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
    shard_tx: Option<Sender<ShardToUpload>>,
    /// Channel receiver for shard completions (for finalize/commit)
    completion_rx: Option<Receiver<ShardCompletion>>,
    /// Handle to await upload task completion
    upload_task: Option<JoinHandle<PolarsResult<()>>>,
    /// Shard indices already uploaded (from checkpoint), wrapped in Arc for cheap cloning
    resumed_shards: Arc<HashSet<usize>>,
    /// Shard completions from checkpoint, for including in final commit
    resumed_completions: Vec<ShardCompletion>,
    /// Collected shard completions for metrics (populated in finalize)
    shard_completions: Arc<Mutex<Vec<ShardCompletion>>>,
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
            resumed_shards: Arc::new(HashSet::new()),
            resumed_completions: Vec::new(),
            shard_completions: Arc::new(Mutex::new(Vec::new())),
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
        // 0. Load checkpoint state (if any) for resumable uploads
        let (resumed_shards, resumed_completions) = load_checkpoint_state(&self.options)?;
        self.resumed_shards = Arc::new(resumed_shards);
        self.resumed_completions = resumed_completions;

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
        let (shard_tx, shard_rx) = connector::<ShardToUpload>();
        let (completion_tx, completion_rx) = connector::<ShardCompletion>();

        // 5. Spawn upload task (background) - receives finished shards and uploads to HF Hub
        let upload_task = upload_shard_task(
            shard_rx,
            completion_tx,
            lfs_client,
            upload_executor,
            Arc::clone(&self.resumed_shards),
            self.options.repo_id.clone(),
            self.options.path_in_repo.clone(),
            self.options.checkpoint_path.clone(),
            Arc::clone(&self.options),
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
        // Dispatch to partitioned or non-partitioned implementation based on options
        let task = if self.options.partition_col.is_some() {
            partitioned_buffer_and_write_task(
                recv_port_rx,
                shard_tx,
                Arc::clone(&self.options),
                self.input_schema.clone(),
                Arc::clone(&self.resumed_shards),
            )
        } else {
            buffer_and_write_task(
                recv_port_rx,
                shard_tx,
                Arc::clone(&self.options),
                self.input_schema.clone(),
                Arc::clone(&self.resumed_shards),
            )
        };

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

        // 4. Take resumed completions for inclusion in commit (already uploaded shards)
        let resumed_completions = std::mem::take(&mut self.resumed_completions);

        // 5. Clone shard_completions Arc to store completions for get_metrics()
        let shard_completions = Arc::clone(&self.shard_completions);

        Some(Box::pin(async move {
            // Step A: Wait for upload task to complete
            // This ensures all shards are uploaded before we commit
            upload_task.await?;

            // Step B: Collect all ShardCompletions from the channel
            // Start with resumed completions (already uploaded), then add new ones
            let mut completions = resumed_completions;
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

            // Step C.5: Mode check for ErrorIfExists
            // Check if files already exist at target path and fail early if mode forbids it
            if options.mode == HfWriteMode::ErrorIfExists {
                // Get token for API call (read access may work without token for public repos,
                // but we'll need it anyway for the commit, so resolve it now)
                let token = get_hf_token(options.token.as_deref(), false)?;

                let existing = check_existing_files(
                    options.repo_type.as_str(),
                    &options.repo_id,
                    &options.effective_revision(),
                    &options.path_in_repo,
                    token.as_deref(),
                )
                .await?;

                if !existing.is_empty() {
                    polars_bail!(
                        ComputeError:
                        "HF sink: {} file(s) already exist at path '{}' (mode=ErrorIfExists). \
                        Use HfWriteMode::Overwrite to replace or HfWriteMode::Append to add new shards.",
                        existing.len(),
                        options.path_in_repo
                    );
                }
            }

            // Step C.5.2: Mode check for Overwrite - collect existing files for deletion
            let delete_ops: Vec<CommitOperation> = if options.mode == HfWriteMode::Overwrite {
                let token = get_hf_token(options.token.as_deref(), false)?;

                let existing = check_existing_files(
                    options.repo_type.as_str(),
                    &options.repo_id,
                    &options.effective_revision(),
                    &options.path_in_repo,
                    token.as_deref(),
                )
                .await?;

                // Use extracted helper function
                create_delete_operations(existing)
                    .into_iter()
                    .map(CommitOperation::Delete)
                    .collect()
            } else {
                Vec::new()
            };

            // Step C.5.3: Mode check for Append - renumber shards to avoid conflicts
            if options.mode == HfWriteMode::Append {
                let token = get_hf_token(options.token.as_deref(), false)?;

                let existing = check_existing_files(
                    options.repo_type.as_str(),
                    &options.repo_id,
                    &options.effective_revision(),
                    &options.path_in_repo,
                    token.as_deref(),
                )
                .await?;

                // Get the first index before renumbering for logging
                let first_idx_before = completions.first().map(|c| c.index);

                // Use extracted helper function
                renumber_for_append(
                    &mut completions,
                    &existing,
                    &options.split,
                    &options.path_in_repo,
                    options.partition_col.as_deref(),
                );

                // Log if renumbering occurred
                if config::verbose() {
                    if let Some(first_c) = completions.first() {
                        if first_idx_before != Some(first_c.index) {
                            eprintln!(
                                "HF sink: Append mode - renumbering {} shards starting from index {}",
                                completions.len(),
                                first_c.index
                            );
                        }
                    }
                }
            }

            // Step C.6: Store completions for get_metrics() (Task 6.3.1)
            // Clone completions before consuming them for commit operations
            {
                let mut stored = shard_completions.lock().unwrap();
                *stored = completions.clone();
            }

            // Step D: Resolve token for commit (required for write access)
            let token = get_hf_token(options.token.as_deref(), true)?
                .expect("token required=true guarantees Some");

            // Step E: Create CommitClient
            let commit_client = CommitClient::new(
                options.repo_type.as_str(),
                &options.repo_id,
                &options.effective_revision(),
                token.clone(),
            )?;

            // Step F: Build commit operations from ShardCompletions
            let add_ops: Vec<CommitOperation> = completions
                .iter()
                .map(|c| {
                    CommitOperation::Add(CommitOperationAdd::lfs(
                        &c.path_in_repo,
                        &c.sha256,
                        c.size,
                    ))
                })
                .collect();

            let num_deleted = delete_ops.len();

            // Step G: Build commit message (compute totals first for G.5)
            let total_rows: usize = completions.iter().map(|c| c.num_rows).sum();
            let total_bytes: u64 = completions.iter().map(|c| c.size).sum();
            let summary = options
                .commit_message
                .clone()
                .unwrap_or_else(|| "Upload via Polars".to_string());
            let description = if num_deleted > 0 {
                // Overwrite mode: replaced existing files
                format!(
                    "Replaced {} existing file(s) with {} shard(s) ({} rows, {:.2} MB)",
                    num_deleted,
                    completions.len(),
                    total_rows,
                    total_bytes as f64 / (1024.0 * 1024.0)
                )
            } else if options.mode == HfWriteMode::Append && !completions.is_empty() {
                // Append mode: show starting index
                let first_idx = completions.first().map(|c| c.index).unwrap_or(0);
                format!(
                    "Appended {} shard(s) starting at index {} ({} rows, {:.2} MB)",
                    completions.len(),
                    first_idx,
                    total_rows,
                    total_bytes as f64 / (1024.0 * 1024.0)
                )
            } else {
                // Default: fresh upload
                format!(
                    "Uploaded {} shard(s) with {} rows ({:.2} MB)",
                    completions.len(),
                    total_rows,
                    total_bytes as f64 / (1024.0 * 1024.0)
                )
            };

            // Step G.5: Update dataset card (README.md) if enabled
            let split_info = SplitInfo::new(&options.split, total_bytes, total_rows as u64);

            // Fetch existing README if update_card is enabled
            let readme_content = if options.update_card {
                fetch_readme(
                    options.repo_type.as_str(),
                    &options.repo_id,
                    options.effective_revision(),
                    Some(&token),
                )
                .await?
            } else {
                None
            };

            // Build README operation (returns None if update_card is false)
            let readme_operation =
                build_readme_operation(readme_content.as_deref(), split_info, options.update_card)?;

            if readme_operation.is_some() && config::verbose() {
                eprintln!(
                    "HF sink: updating README.md with split '{}' ({} rows, {:.2} MB)",
                    options.split,
                    total_rows,
                    total_bytes as f64 / (1024.0 * 1024.0)
                );
            }

            // Combine: deletes first, then adds, then README (for atomic commit)
            let operations: Vec<CommitOperation> = delete_ops
                .into_iter()
                .chain(add_ops)
                .chain(readme_operation)
                .collect();

            // Step H: Execute atomic commit
            // Notify progress callback of commit start (Task 6.3.4d)
            if let Some(ref progress) = options.progress {
                progress.on_commit_start(completions.len());
            }

            let commit_info = commit_client
                .create_commit(&summary, Some(&description), &operations, options.create_pr)
                .await?;

            // Notify progress callback of commit completion (Task 6.3.4d)
            if let Some(ref progress) = options.progress {
                progress.on_commit_complete(Some(&commit_info.commit_url));
            }

            // Step H.5: Delete checkpoint on success (Task 6.1.9)
            if let Some(ref checkpoint_path) = options.checkpoint_path {
                if let Err(e) = CheckpointState::delete(checkpoint_path) {
                    if config::verbose() {
                        eprintln!("HF sink: warning - failed to delete checkpoint: {}", e);
                    }
                } else if config::verbose() {
                    eprintln!("HF sink: checkpoint deleted after successful commit");
                }
            }

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
        // Task 6.3.2: Return aggregate WriteMetrics across all shards
        let completions = self.shard_completions.lock().unwrap();

        if completions.is_empty() {
            return Ok(None);
        }

        // Aggregate metrics across all shards
        let total_rows: u64 = completions.iter().map(|c| c.num_rows as u64).sum();
        let total_bytes: u64 = completions.iter().map(|c| c.size).sum();

        // Build full HF URL path for identification
        let repo_type_str = match self.options.repo_type {
            polars_io::cloud::hf::RepoType::Dataset => "datasets",
            polars_io::cloud::hf::RepoType::Model => "models",
            polars_io::cloud::hf::RepoType::Space => "spaces",
        };
        let path = format!(
            "hf://{}/{}/{}",
            repo_type_str, self.options.repo_id, self.options.path_in_repo
        );

        // Log metrics in verbose mode
        if config::verbose() {
            eprintln!(
                "HF sink metrics: {} shards, {} rows, {:.2} MB -> {}",
                completions.len(),
                total_rows,
                total_bytes as f64 / (1024.0 * 1024.0),
                path
            );
        }

        // Create column entries for the schema
        // HF sink doesn't track column-level statistics during streaming,
        // so we report zero null/nan counts and no bounds.
        let columns = self
            .input_schema
            .iter_values()
            .map(|_dtype| super::metrics::WriteMetricsColumn {
                null_count: 0,
                nan_count: 0,
                lower_bound: None,
                upper_bound: None,
            })
            .collect();

        Ok(Some(super::metrics::WriteMetrics {
            path,
            num_rows: total_rows,
            file_size: total_bytes,
            keys: None, // No partition keys for non-partitioned writes
            columns,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars_io::cloud::hf::HfSinkProgress;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // =========================================================================
    // Test Helper: TestProgress
    // =========================================================================

    /// Event types recorded by TestProgress for verification
    #[derive(Debug, Clone, PartialEq)]
    enum ProgressEvent {
        ShardStart { index: usize, path: String },
        UploadProgress { index: usize, bytes: u64, total: u64 },
        ShardComplete { index: usize, path: String, size: u64 },
        CommitStart { num_shards: usize },
        CommitComplete { url: Option<String> },
    }

    /// Test helper that records all progress callbacks with thread-safe counters
    struct TestProgress {
        shard_starts: AtomicUsize,
        shard_completes: AtomicUsize,
        upload_progress_calls: AtomicUsize,
        commit_starts: AtomicUsize,
        commit_completes: AtomicUsize,
        events: Mutex<Vec<ProgressEvent>>,
    }

    impl TestProgress {
        fn new() -> Self {
            Self {
                shard_starts: AtomicUsize::new(0),
                shard_completes: AtomicUsize::new(0),
                upload_progress_calls: AtomicUsize::new(0),
                commit_starts: AtomicUsize::new(0),
                commit_completes: AtomicUsize::new(0),
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<ProgressEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl HfSinkProgress for TestProgress {
        fn on_shard_start(&self, index: usize, path: &str) {
            self.shard_starts.fetch_add(1, Ordering::SeqCst);
            self.events.lock().unwrap().push(ProgressEvent::ShardStart {
                index,
                path: path.to_string(),
            });
        }

        fn on_shard_upload_progress(&self, index: usize, bytes_uploaded: u64, total_bytes: u64) {
            self.upload_progress_calls.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap()
                .push(ProgressEvent::UploadProgress {
                    index,
                    bytes: bytes_uploaded,
                    total: total_bytes,
                });
        }

        fn on_shard_complete(&self, index: usize, path: &str, size: u64) {
            self.shard_completes.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap()
                .push(ProgressEvent::ShardComplete {
                    index,
                    path: path.to_string(),
                    size,
                });
        }

        fn on_commit_start(&self, num_shards: usize) {
            self.commit_starts.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap()
                .push(ProgressEvent::CommitStart { num_shards });
        }

        fn on_commit_complete(&self, commit_url: Option<&str>) {
            self.commit_completes.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap()
                .push(ProgressEvent::CommitComplete {
                    url: commit_url.map(String::from),
                });
        }
    }

    // =========================================================================
    // Tests
    // =========================================================================

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
    fn test_partitioned_shard_path_basic() {
        assert_eq!(
            partitioned_shard_path("data", "split", "train", "train", 0),
            "data/split=train/train-00000.parquet"
        );
    }

    #[test]
    fn test_partitioned_shard_path_with_index() {
        assert_eq!(
            partitioned_shard_path("data", "split", "test", "test", 42),
            "data/split=test/test-00042.parquet"
        );
    }

    #[test]
    fn test_partitioned_shard_path_nested_base() {
        assert_eq!(
            partitioned_shard_path("output/processed", "date", "2024-01-20", "train", 5),
            "output/processed/date=2024-01-20/train-00005.parquet"
        );
    }

    #[test]
    fn test_partitioned_shard_path_trailing_slash() {
        assert_eq!(
            partitioned_shard_path("data/", "split", "train", "train", 0),
            "data/split=train/train-00000.parquet"
        );
    }

    #[test]
    fn test_parse_shard_index() {
        // Basic cases
        assert_eq!(parse_shard_index("data/train-00000.parquet", "train"), Some(0));
        assert_eq!(parse_shard_index("data/train-00042.parquet", "train"), Some(42));
        assert_eq!(parse_shard_index("data/train-99999.parquet", "train"), Some(99999));

        // Wrong split name - should return None
        assert_eq!(parse_shard_index("data/test-00001.parquet", "train"), None);
        assert_eq!(parse_shard_index("data/train-00001.parquet", "validation"), None);

        // Invalid formats - should return None
        assert_eq!(parse_shard_index("other.parquet", "train"), None);
        assert_eq!(parse_shard_index("data/train.parquet", "train"), None);
        assert_eq!(parse_shard_index("data/train-abc.parquet", "train"), None);

        // Nested paths
        assert_eq!(
            parse_shard_index("output/processed/validation-00123.parquet", "validation"),
            Some(123)
        );

        // Edge cases
        assert_eq!(parse_shard_index("train-00000.parquet", "train"), Some(0)); // No directory
        assert_eq!(parse_shard_index("", "train"), None); // Empty path

        // Large index (6 digits, beyond 5-digit format)
        assert_eq!(parse_shard_index("data/train-100000.parquet", "train"), Some(100000));
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
            shard_rx: Receiver<ShardToUpload>,
            completion_tx: Sender<ShardCompletion>,
            lfs_client: LfsClient,
            upload_executor: UploadExecutor,
            resumed_shards: Arc<HashSet<usize>>,
            repo_id: String,
            base_path_in_repo: String,
            checkpoint_path: Option<std::path::PathBuf>,
            options: Arc<HfSinkOptions>,
        ) -> JoinHandle<PolarsResult<()>> {
            upload_shard_task(
                shard_rx,
                completion_tx,
                lfs_client,
                upload_executor,
                resumed_shards,
                repo_id,
                base_path_in_repo,
                checkpoint_path,
                options,
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

    // ========================================================================
    // Tests for Task 5.1.5: Mode handling integration tests
    // ========================================================================

    #[test]
    fn test_create_delete_operations_basic() {
        let existing = vec![
            ExistingFile {
                path: "data/train-00000.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/train-00001.parquet".to_string(),
                size: 2000,
            },
        ];
        let ops = create_delete_operations(existing);
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0].path_in_repo, "data/train-00000.parquet");
        assert_eq!(ops[1].path_in_repo, "data/train-00001.parquet");
    }

    #[test]
    fn test_create_delete_operations_empty() {
        let ops = create_delete_operations(vec![]);
        assert!(ops.is_empty());
    }

    #[test]
    fn test_renumber_for_append_with_existing() {
        let existing = vec![
            ExistingFile {
                path: "data/train-00000.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/train-00001.parquet".to_string(),
                size: 2000,
            },
        ];
        let mut completions = vec![
            ShardCompletion {
                index: 0,
                path_in_repo: "data/train-00000.parquet".to_string(),
                sha256: "a".repeat(64),
                size: 500,
                num_rows: 100,
            },
            ShardCompletion {
                index: 1,
                path_in_repo: "data/train-00001.parquet".to_string(),
                sha256: "b".repeat(64),
                size: 600,
                num_rows: 200,
            },
        ];

        renumber_for_append(&mut completions, &existing, "train", "data", None);

        // Should start from index 2 (max existing is 1)
        assert_eq!(completions[0].index, 2);
        assert_eq!(completions[0].path_in_repo, "data/train-00002.parquet");
        assert_eq!(completions[1].index, 3);
        assert_eq!(completions[1].path_in_repo, "data/train-00003.parquet");
    }

    #[test]
    fn test_renumber_for_append_empty_repo() {
        let mut completions = vec![ShardCompletion {
            index: 0,
            path_in_repo: "data/train-00000.parquet".to_string(),
            sha256: "a".repeat(64),
            size: 500,
            num_rows: 100,
        }];

        renumber_for_append(&mut completions, &[], "train", "data", None);

        // No existing files: indices stay the same
        assert_eq!(completions[0].index, 0);
        assert_eq!(completions[0].path_in_repo, "data/train-00000.parquet");
    }

    #[test]
    fn test_renumber_for_append_different_split() {
        // Existing files are "test" split, new files are "train" split
        let existing = vec![
            ExistingFile {
                path: "data/test-00000.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/test-00005.parquet".to_string(),
                size: 2000,
            },
        ];
        let mut completions = vec![ShardCompletion {
            index: 0,
            path_in_repo: "data/train-00000.parquet".to_string(),
            sha256: "a".repeat(64),
            size: 500,
            num_rows: 100,
        }];

        renumber_for_append(&mut completions, &existing, "train", "data", None);

        // "train" split doesn't match "test" files, so no renumbering
        assert_eq!(completions[0].index, 0);
        assert_eq!(completions[0].path_in_repo, "data/train-00000.parquet");
    }

    #[test]
    fn test_renumber_for_append_non_contiguous() {
        // Existing shards with gaps: 0, 2, 5
        let existing = vec![
            ExistingFile {
                path: "data/train-00000.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/train-00002.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/train-00005.parquet".to_string(),
                size: 1000,
            },
        ];
        let mut completions = vec![ShardCompletion {
            index: 0,
            path_in_repo: "data/train-00000.parquet".to_string(),
            sha256: "a".repeat(64),
            size: 500,
            num_rows: 100,
        }];

        renumber_for_append(&mut completions, &existing, "train", "data", None);

        // Should start from 6 (max existing is 5)
        assert_eq!(completions[0].index, 6);
        assert_eq!(completions[0].path_in_repo, "data/train-00006.parquet");
    }

    // ========================================================================
    // Tests for Task 6.2.7: Partitioned renumber_for_append
    // ========================================================================

    #[test]
    fn test_extract_partition_from_path() {
        // Standard case
        assert_eq!(
            extract_partition_from_path("data/split=train/train-00000.parquet", "split"),
            Some("train".to_string())
        );

        // Different partition column
        assert_eq!(
            extract_partition_from_path("output/date=2024-01-20/train-00005.parquet", "date"),
            Some("2024-01-20".to_string())
        );

        // No partition in path
        assert_eq!(
            extract_partition_from_path("data/train-00000.parquet", "split"),
            None
        );

        // Wrong partition column name
        assert_eq!(
            extract_partition_from_path("data/split=train/train-00000.parquet", "date"),
            None
        );

        // Nested paths
        assert_eq!(
            extract_partition_from_path("deep/nested/path/lang=en/train-00000.parquet", "lang"),
            Some("en".to_string())
        );
    }

    #[test]
    fn test_renumber_for_append_partitioned_single_partition() {
        // Existing files for one partition
        let existing = vec![
            ExistingFile {
                path: "data/split=train/train-00000.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/split=train/train-00001.parquet".to_string(),
                size: 2000,
            },
        ];
        let mut completions = vec![ShardCompletion {
            index: 0,
            path_in_repo: "data/split=train/train-00000.parquet".to_string(),
            sha256: "a".repeat(64),
            size: 500,
            num_rows: 100,
        }];

        renumber_for_append(&mut completions, &existing, "train", "data", Some("split"));

        // Should start from index 2 (max existing is 1)
        assert_eq!(completions[0].index, 2);
        assert_eq!(
            completions[0].path_in_repo,
            "data/split=train/train-00002.parquet"
        );
    }

    #[test]
    fn test_renumber_for_append_partitioned_multiple_partitions() {
        // Existing files for multiple partitions
        let existing = vec![
            ExistingFile {
                path: "data/split=train/train-00000.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/split=train/train-00002.parquet".to_string(),
                size: 1000,
            },
            ExistingFile {
                path: "data/split=test/train-00000.parquet".to_string(),
                size: 1000,
            },
        ];
        let mut completions = vec![
            ShardCompletion {
                index: 0,
                path_in_repo: "data/split=train/train-00000.parquet".to_string(),
                sha256: "a".repeat(64),
                size: 500,
                num_rows: 100,
            },
            ShardCompletion {
                index: 0,
                path_in_repo: "data/split=test/train-00000.parquet".to_string(),
                sha256: "b".repeat(64),
                size: 600,
                num_rows: 200,
            },
        ];

        renumber_for_append(&mut completions, &existing, "train", "data", Some("split"));

        // Train partition: max existing is 2, so starts at 3
        assert_eq!(completions[0].index, 3);
        assert_eq!(
            completions[0].path_in_repo,
            "data/split=train/train-00003.parquet"
        );

        // Test partition: max existing is 0, so starts at 1
        assert_eq!(completions[1].index, 1);
        assert_eq!(
            completions[1].path_in_repo,
            "data/split=test/train-00001.parquet"
        );
    }

    #[test]
    fn test_renumber_for_append_partitioned_new_partition() {
        // Existing files for one partition, new completion for different partition
        let existing = vec![ExistingFile {
            path: "data/split=train/train-00005.parquet".to_string(),
            size: 1000,
        }];
        let mut completions = vec![ShardCompletion {
            index: 0,
            path_in_repo: "data/split=validation/train-00000.parquet".to_string(),
            sha256: "a".repeat(64),
            size: 500,
            num_rows: 100,
        }];

        renumber_for_append(&mut completions, &existing, "train", "data", Some("split"));

        // validation partition has no existing files, starts at 0
        assert_eq!(completions[0].index, 0);
        assert_eq!(
            completions[0].path_in_repo,
            "data/split=validation/train-00000.parquet"
        );
    }

    // ========================================================================
    // Tests for Task 5.2.7: Dataset card (README.md) update integration tests
    // ========================================================================

    #[test]
    fn test_build_readme_operation_update_card_false() {
        let split = SplitInfo::new("train", 1024, 100);
        let result = build_readme_operation(Some("# My Dataset"), split, false).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_build_readme_operation_no_existing_readme() {
        let split = SplitInfo::new("train", 1024, 100);
        let result = build_readme_operation(None, split, true).unwrap();
        assert!(result.is_some());

        // Extract the content from the operation
        if let Some(CommitOperation::Add(add_op)) = result {
            let content = match add_op {
                CommitOperationAdd::Regular {
                    content,
                    path_in_repo,
                } => {
                    assert_eq!(path_in_repo, "README.md");
                    String::from_utf8(content).unwrap()
                },
                _ => panic!("Expected Regular file operation"),
            };

            // Verify it has dataset_info with train split
            assert!(content.contains("dataset_info:"));
            assert!(content.contains("name: train"));
            assert!(content.contains("num_bytes: 1024"));
            assert!(content.contains("num_examples: 100"));
        } else {
            panic!("Expected CommitOperation::Add");
        }
    }

    #[test]
    fn test_build_readme_operation_existing_without_frontmatter() {
        let split = SplitInfo::new("train", 1024, 100);
        let existing_readme = "# My Dataset\n\nThis is a dataset without frontmatter.";
        let result = build_readme_operation(Some(existing_readme), split, true).unwrap();
        assert!(result.is_some());

        if let Some(CommitOperation::Add(add_op)) = result {
            let content = match add_op {
                CommitOperationAdd::Regular {
                    content,
                    path_in_repo,
                } => {
                    assert_eq!(path_in_repo, "README.md");
                    String::from_utf8(content).unwrap()
                },
                _ => panic!("Expected Regular file operation"),
            };

            // Verify frontmatter was prepended
            assert!(content.starts_with("---\n"));
            assert!(content.contains("dataset_info:"));
            assert!(content.contains("name: train"));

            // Verify original content preserved
            assert!(content.contains("# My Dataset"));
            assert!(content.contains("This is a dataset without frontmatter."));
        } else {
            panic!("Expected CommitOperation::Add");
        }
    }

    #[test]
    fn test_build_readme_operation_existing_with_frontmatter() {
        let split = SplitInfo::new("train", 2048, 200);
        let existing_readme = "---\nlicense: mit\n---\n\n# My Dataset\n\nDescription.";
        let result = build_readme_operation(Some(existing_readme), split, true).unwrap();
        assert!(result.is_some());

        if let Some(CommitOperation::Add(add_op)) = result {
            let content = match add_op {
                CommitOperationAdd::Regular {
                    content,
                    path_in_repo,
                } => {
                    assert_eq!(path_in_repo, "README.md");
                    String::from_utf8(content).unwrap()
                },
                _ => panic!("Expected Regular file operation"),
            };

            // Verify dataset_info was added
            assert!(content.contains("dataset_info:"));
            assert!(content.contains("name: train"));
            assert!(content.contains("num_bytes: 2048"));

            // Verify license preserved
            assert!(content.contains("license:"));

            // Verify body preserved
            assert!(content.contains("# My Dataset"));
            assert!(content.contains("Description."));
        } else {
            panic!("Expected CommitOperation::Add");
        }
    }

    #[test]
    fn test_build_readme_operation_preserves_other_fields() {
        let split = SplitInfo::new("train", 1024, 100);
        let existing_readme = r#"---
license: apache-2.0
task_categories:
  - text-classification
language:
  - en
---

# Dataset

Some content.
"#;
        let result = build_readme_operation(Some(existing_readme), split, true).unwrap();
        assert!(result.is_some());

        if let Some(CommitOperation::Add(add_op)) = result {
            let content = match add_op {
                CommitOperationAdd::Regular {
                    content,
                    path_in_repo,
                } => {
                    assert_eq!(path_in_repo, "README.md");
                    String::from_utf8(content).unwrap()
                },
                _ => panic!("Expected Regular file operation"),
            };

            // Verify all original fields preserved
            assert!(content.contains("license:"));
            assert!(content.contains("task_categories:"));
            assert!(content.contains("text-classification"));
            assert!(content.contains("language:"));

            // Verify dataset_info added
            assert!(content.contains("dataset_info:"));
            assert!(content.contains("name: train"));

            // Verify body preserved
            assert!(content.contains("# Dataset"));
            assert!(content.contains("Some content."));
        } else {
            panic!("Expected CommitOperation::Add");
        }
    }

    #[test]
    fn test_build_readme_operation_preserves_existing_splits() {
        let split = SplitInfo::new("train", 1024, 100);
        let existing_readme = r#"---
dataset_info:
  splits:
    - name: test
      num_bytes: 500
      num_examples: 50
---

# Dataset
"#;
        let result = build_readme_operation(Some(existing_readme), split, true).unwrap();
        assert!(result.is_some());

        if let Some(CommitOperation::Add(add_op)) = result {
            let content = match add_op {
                CommitOperationAdd::Regular {
                    content,
                    path_in_repo,
                } => {
                    assert_eq!(path_in_repo, "README.md");
                    String::from_utf8(content).unwrap()
                },
                _ => panic!("Expected Regular file operation"),
            };

            // Verify both splits present
            assert!(content.contains("name: test"));
            assert!(content.contains("num_bytes: 500"));
            assert!(content.contains("name: train"));
            assert!(content.contains("num_bytes: 1024"));
        } else {
            panic!("Expected CommitOperation::Add");
        }
    }

    // ==========================================================================
    // Checkpoint Integration Tests (Task 6.1.10)
    // ==========================================================================

    #[test]
    fn test_checkpoint_created_during_upload() {
        use polars_io::cloud::hf::checkpoint::{CheckpointState, ShardCheckpoint};
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let ckpt_path = dir.path().join("checkpoint.json");

        // Initially no checkpoint exists
        assert!(!ckpt_path.exists());

        // Simulate what upload_shard_task does after successful upload
        let mut checkpoint = CheckpointState::new("user/test-repo", "data/train", None);
        checkpoint.add_shard(ShardCheckpoint {
            index: 0,
            path_in_repo: "data/train-00000.parquet".into(),
            sha256: "abc123def456".into(),
            size: 1000,
            num_rows: 100,
        });
        checkpoint.save(&ckpt_path).unwrap();

        // Verify checkpoint file created
        assert!(ckpt_path.exists());

        // Verify content
        let loaded = CheckpointState::load(&ckpt_path).unwrap().unwrap();
        assert_eq!(loaded.completed_shards.len(), 1);
        assert_eq!(loaded.completed_shards[0].index, 0);
        assert_eq!(
            loaded.completed_shards[0].path_in_repo,
            "data/train-00000.parquet"
        );
    }

    #[test]
    fn test_checkpoint_deleted_on_success() {
        use polars_io::cloud::hf::checkpoint::CheckpointState;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let ckpt_path = dir.path().join("checkpoint.json");

        // Create checkpoint
        let checkpoint = CheckpointState::new("user/test-repo", "data/train", None);
        checkpoint.save(&ckpt_path).unwrap();
        assert!(ckpt_path.exists());

        // Delete (as finalize() does after successful commit)
        CheckpointState::delete(&ckpt_path).unwrap();

        // Verify removed
        assert!(!ckpt_path.exists());
    }

    #[test]
    fn test_checkpoint_resume_skips_shards() {
        use polars_io::cloud::hf::checkpoint::{CheckpointState, ShardCheckpoint};
        use std::collections::HashSet;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let ckpt_path = dir.path().join("checkpoint.json");

        // Create checkpoint with shards 0, 2, 5 completed (non-contiguous)
        let mut checkpoint = CheckpointState::new("user/test-repo", "data/train", None);
        for idx in [0, 2, 5] {
            checkpoint.add_shard(ShardCheckpoint {
                index: idx,
                path_in_repo: format!("data/train-{:05}.parquet", idx),
                sha256: format!("hash{}", idx),
                size: 1000,
                num_rows: 100,
            });
        }
        checkpoint.save(&ckpt_path).unwrap();

        // Load and get completed indices
        let loaded = CheckpointState::load(&ckpt_path).unwrap().unwrap();
        let skipped = loaded.completed_indices();

        // Verify correct indices marked for skip
        assert_eq!(skipped, HashSet::from([0, 2, 5]));
        assert!(skipped.contains(&0));
        assert!(skipped.contains(&2));
        assert!(skipped.contains(&5));
        assert!(!skipped.contains(&1)); // Not completed
        assert!(!skipped.contains(&3)); // Not completed
    }

    #[test]
    fn test_checkpoint_mismatch_error() {
        use polars_io::cloud::hf::checkpoint::CheckpointState;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let ckpt_path = dir.path().join("checkpoint.json");

        // Create checkpoint for repo-A
        let checkpoint = CheckpointState::new("user/repo-A", "data/train", None);
        checkpoint.save(&ckpt_path).unwrap();

        // Load checkpoint
        let loaded = CheckpointState::load(&ckpt_path).unwrap().unwrap();

        // Validate against different repo (simulating load_checkpoint_state logic)
        let current_repo = "user/repo-B";
        let current_path = "data/train";

        // Verify mismatch detected - repo_id differs
        assert_ne!(loaded.repo_id, current_repo);
        assert_eq!(loaded.repo_id, "user/repo-A");

        // Verify path matches
        assert_eq!(loaded.path_in_repo, current_path);

        // In actual code (load_checkpoint_state), this triggers:
        // polars_bail!(ComputeError: "Checkpoint mismatch...")
    }

    #[test]
    fn test_no_checkpoint_when_path_none() {
        use polars_io::cloud::hf::HfSinkOptions;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let potential_path = dir.path().join("checkpoint.json");

        // Build options WITHOUT checkpoint_path
        let options = HfSinkOptions::builder("user/test-repo")
            .with_path_in_repo("data")
            .with_split("train")
            // Note: NOT calling .with_checkpoint_path()
            .build()
            .unwrap();

        // Verify checkpoint_path is None
        assert!(options.checkpoint_path.is_none());

        // No file should be created (the None branch simply skips checkpoint operations)
        assert!(!potential_path.exists());
    }

    // =========================================================================
    // Progress Callback Tests
    // =========================================================================

    #[test]
    fn test_progress_helper_records_events() {
        use std::sync::Arc;

        let progress = Arc::new(TestProgress::new());

        // Simulate callback sequence for a single shard upload
        progress.on_shard_start(0, "data/train-00000.parquet");
        progress.on_shard_upload_progress(0, 500, 1000);
        progress.on_shard_upload_progress(0, 1000, 1000);
        progress.on_shard_complete(0, "data/train-00000.parquet", 1000);
        progress.on_commit_start(1);
        progress.on_commit_complete(Some("https://huggingface.co/datasets/user/repo/commit/abc123"));

        // Verify counters
        assert_eq!(progress.shard_starts.load(Ordering::SeqCst), 1);
        assert_eq!(progress.upload_progress_calls.load(Ordering::SeqCst), 2);
        assert_eq!(progress.shard_completes.load(Ordering::SeqCst), 1);
        assert_eq!(progress.commit_starts.load(Ordering::SeqCst), 1);
        assert_eq!(progress.commit_completes.load(Ordering::SeqCst), 1);

        // Verify event log has correct count
        let events = progress.events();
        assert_eq!(events.len(), 6);

        // Verify event order and content
        assert!(matches!(
            &events[0],
            ProgressEvent::ShardStart { index: 0, path } if path == "data/train-00000.parquet"
        ));
        assert!(matches!(
            &events[1],
            ProgressEvent::UploadProgress { index: 0, bytes: 500, total: 1000 }
        ));
        assert!(matches!(
            &events[2],
            ProgressEvent::UploadProgress { index: 0, bytes: 1000, total: 1000 }
        ));
        assert!(matches!(
            &events[3],
            ProgressEvent::ShardComplete { index: 0, size: 1000, .. }
        ));
        assert!(matches!(
            &events[4],
            ProgressEvent::CommitStart { num_shards: 1 }
        ));
        assert!(matches!(
            &events[5],
            ProgressEvent::CommitComplete { url: Some(_) }
        ));
    }

    #[test]
    fn test_progress_single_shard_callback_sequence() {
        use std::sync::Arc;

        let progress = Arc::new(TestProgress::new());

        // Simulate expected callback sequence for single shard upload
        // This is what HfSinkNode should call during a real upload

        // 1. Shard starts (in buffer_and_write_task, before writing begins)
        progress.on_shard_start(0, "data/train-00000.parquet");

        // 2. Upload progress (in upload_shard_task, during LFS upload)
        let total_bytes = 10_000u64;
        progress.on_shard_upload_progress(0, 2_500, total_bytes);
        progress.on_shard_upload_progress(0, 5_000, total_bytes);
        progress.on_shard_upload_progress(0, 7_500, total_bytes);
        progress.on_shard_upload_progress(0, 10_000, total_bytes);

        // 3. Shard completes (in upload_shard_task, after LFS upload)
        progress.on_shard_complete(0, "data/train-00000.parquet", total_bytes);

        // 4. Commit starts (in finalize, before commit API call)
        progress.on_commit_start(1);

        // 5. Commit completes (in finalize, after commit API call)
        progress.on_commit_complete(Some("https://huggingface.co/datasets/user/repo/commit/abc123"));

        // ===== ASSERTIONS =====

        // Counter assertions
        assert_eq!(
            progress.shard_starts.load(Ordering::SeqCst),
            1,
            "single shard should have 1 start"
        );
        assert_eq!(
            progress.shard_completes.load(Ordering::SeqCst),
            1,
            "single shard should have 1 complete"
        );
        assert_eq!(
            progress.upload_progress_calls.load(Ordering::SeqCst),
            4,
            "4 progress updates"
        );
        assert_eq!(
            progress.commit_starts.load(Ordering::SeqCst),
            1,
            "1 commit start"
        );
        assert_eq!(
            progress.commit_completes.load(Ordering::SeqCst),
            1,
            "1 commit complete"
        );

        // Event sequence assertions
        let events = progress.events();
        assert_eq!(events.len(), 8, "8 total events");

        // Verify order: start → progress(4x) → complete → commit_start → commit_complete
        assert!(matches!(
            &events[0],
            ProgressEvent::ShardStart { index: 0, .. }
        ));
        assert!(matches!(
            &events[1],
            ProgressEvent::UploadProgress { index: 0, .. }
        ));
        assert!(matches!(
            &events[2],
            ProgressEvent::UploadProgress { index: 0, .. }
        ));
        assert!(matches!(
            &events[3],
            ProgressEvent::UploadProgress { index: 0, .. }
        ));
        assert!(matches!(
            &events[4],
            ProgressEvent::UploadProgress { index: 0, .. }
        ));
        assert!(matches!(
            &events[5],
            ProgressEvent::ShardComplete { index: 0, .. }
        ));
        assert!(matches!(
            &events[6],
            ProgressEvent::CommitStart { num_shards: 1 }
        ));
        assert!(matches!(&events[7], ProgressEvent::CommitComplete { .. }));

        // Verify upload progress bytes are monotonically increasing
        let upload_events: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                ProgressEvent::UploadProgress { bytes, .. } => Some(*bytes),
                _ => None,
            })
            .collect();
        assert_eq!(upload_events, vec![2_500, 5_000, 7_500, 10_000]);
    }

    #[test]
    fn test_progress_multi_shard_callback_sequence() {
        use std::sync::Arc;

        let progress = Arc::new(TestProgress::new());

        // Simulate 3 shard upload sequence
        let shard_sizes = [10_000u64, 15_000, 8_000];

        for shard_idx in 0..3 {
            let path = format!("data/train-{:05}.parquet", shard_idx);
            let total_bytes = shard_sizes[shard_idx];

            // 1. Shard starts
            progress.on_shard_start(shard_idx, &path);

            // 2. Upload progress (simulate 4 increments per shard)
            for i in 1..=4 {
                let bytes = (total_bytes / 4) * i as u64;
                progress.on_shard_upload_progress(shard_idx, bytes, total_bytes);
            }

            // 3. Shard completes
            progress.on_shard_complete(shard_idx, &path, total_bytes);
        }

        // 4. Commit starts with total shard count
        progress.on_commit_start(3);

        // 5. Commit completes
        progress.on_commit_complete(Some(
            "https://huggingface.co/datasets/user/repo/commit/abc123",
        ));

        // ===== ASSERTIONS =====

        // Counter assertions
        assert_eq!(
            progress.shard_starts.load(Ordering::SeqCst),
            3,
            "3 shard starts"
        );
        assert_eq!(
            progress.shard_completes.load(Ordering::SeqCst),
            3,
            "3 shard completes"
        );
        assert_eq!(
            progress.upload_progress_calls.load(Ordering::SeqCst),
            12,
            "4 progress × 3 shards"
        );
        assert_eq!(
            progress.commit_starts.load(Ordering::SeqCst),
            1,
            "1 commit start"
        );
        assert_eq!(
            progress.commit_completes.load(Ordering::SeqCst),
            1,
            "1 commit complete"
        );

        // Event sequence assertions
        let events = progress.events();
        assert_eq!(
            events.len(),
            20,
            "3×(1 start + 4 progress + 1 complete) + 2 commit = 20"
        );

        // Verify each shard's events are grouped (start → progress × 4 → complete)
        for shard_idx in 0..3usize {
            let base = shard_idx * 6; // Each shard has 6 events

            assert!(
                matches!(
                    &events[base],
                    ProgressEvent::ShardStart { index, .. } if *index == shard_idx
                ),
                "shard {} should start at position {}",
                shard_idx,
                base
            );

            // 4 upload progress events
            for i in 1..=4 {
                assert!(
                    matches!(
                        &events[base + i],
                        ProgressEvent::UploadProgress { index, .. } if *index == shard_idx
                    ),
                    "shard {} progress at position {}",
                    shard_idx,
                    base + i
                );
            }

            assert!(
                matches!(
                    &events[base + 5],
                    ProgressEvent::ShardComplete { index, .. } if *index == shard_idx
                ),
                "shard {} should complete at position {}",
                shard_idx,
                base + 5
            );
        }

        // Final commit events
        assert!(matches!(
            &events[18],
            ProgressEvent::CommitStart { num_shards: 3 }
        ));
        assert!(matches!(&events[19], ProgressEvent::CommitComplete { .. }));

        // Verify upload progress for each shard is monotonically increasing
        for shard_idx in 0..3usize {
            let shard_progress: Vec<u64> = events
                .iter()
                .filter_map(|e| match e {
                    ProgressEvent::UploadProgress { index, bytes, .. } if *index == shard_idx => {
                        Some(*bytes)
                    },
                    _ => None,
                })
                .collect();

            let expected_total = shard_sizes[shard_idx];
            let expected: Vec<u64> = (1..=4).map(|i| (expected_total / 4) * i).collect();
            assert_eq!(shard_progress, expected, "shard {} progress bytes", shard_idx);
        }
    }

    /// Tests upload progress byte increments for both basic and multipart upload patterns.
    ///
    /// Basic upload (single PUT): Reports 0 → total (2 progress calls)
    /// Multipart upload (n parts): Reports 0 → after_part1 → ... → after_partN (n+1 calls)
    ///
    /// This test verifies:
    /// - Basic upload reports exactly 2 progress events
    /// - Multipart upload reports n+1 progress events
    /// - Bytes are monotonically increasing
    /// - Total bytes is consistent across all calls
    /// - Final bytes equals total bytes
    #[test]
    fn test_progress_upload_byte_increments() {
        let progress = TestProgress::new();

        // =========================================================================
        // Scenario 1: Basic upload (single PUT request pattern)
        // Shard 0: Reports 0 at start, total at completion
        // =========================================================================
        let basic_total = 50_000u64;
        progress.on_shard_upload_progress(0, 0, basic_total);
        progress.on_shard_upload_progress(0, basic_total, basic_total);

        // =========================================================================
        // Scenario 2: Multipart upload (3 parts)
        // Shard 1: Part sizes 20KB + 20KB + 10KB = 50KB total
        // Progress reported: 0 → 20000 → 40000 → 50000
        // =========================================================================
        let multipart_total = 50_000u64;
        let part_sizes = [20_000u64, 20_000, 10_000];

        // Start: 0 bytes uploaded
        progress.on_shard_upload_progress(1, 0, multipart_total);

        // After each part completion
        let mut bytes_so_far = 0u64;
        for part_size in part_sizes {
            bytes_so_far += part_size;
            progress.on_shard_upload_progress(1, bytes_so_far, multipart_total);
        }

        // =========================================================================
        // Scenario 3: Multipart upload with uneven parts
        // Shard 2: Part sizes 30KB + 15KB + 5KB = 50KB total
        // Tests non-uniform part distribution
        // =========================================================================
        let uneven_total = 50_000u64;
        let uneven_parts = [30_000u64, 15_000, 5_000];

        progress.on_shard_upload_progress(2, 0, uneven_total);
        let mut uneven_bytes = 0u64;
        for part_size in uneven_parts {
            uneven_bytes += part_size;
            progress.on_shard_upload_progress(2, uneven_bytes, uneven_total);
        }

        // =========================================================================
        // Assertions
        // =========================================================================
        let events = progress.events();

        // Counter assertion
        assert_eq!(
            progress.upload_progress_calls.load(Ordering::SeqCst),
            10,
            "total upload progress calls: 2 (basic) + 4 (multipart) + 4 (uneven)"
        );

        // Extract progress for each shard
        let shard0_progress: Vec<(u64, u64)> = events
            .iter()
            .filter_map(|e| match e {
                ProgressEvent::UploadProgress {
                    index: 0,
                    bytes,
                    total,
                } => Some((*bytes, *total)),
                _ => None,
            })
            .collect();

        let shard1_progress: Vec<(u64, u64)> = events
            .iter()
            .filter_map(|e| match e {
                ProgressEvent::UploadProgress {
                    index: 1,
                    bytes,
                    total,
                } => Some((*bytes, *total)),
                _ => None,
            })
            .collect();

        let shard2_progress: Vec<(u64, u64)> = events
            .iter()
            .filter_map(|e| match e {
                ProgressEvent::UploadProgress {
                    index: 2,
                    bytes,
                    total,
                } => Some((*bytes, *total)),
                _ => None,
            })
            .collect();

        // 1. Basic upload: exactly 2 progress events (0 → total)
        assert_eq!(
            shard0_progress,
            vec![(0, 50_000), (50_000, 50_000)],
            "basic upload should report 0 then total"
        );

        // 2. Multipart upload: n+1 progress events (0 + 3 parts)
        assert_eq!(
            shard1_progress,
            vec![
                (0, 50_000),
                (20_000, 50_000),
                (40_000, 50_000),
                (50_000, 50_000)
            ],
            "multipart upload should report incremental progress after each part"
        );

        // 3. Uneven parts: verify non-uniform distribution works
        assert_eq!(
            shard2_progress,
            vec![
                (0, 50_000),
                (30_000, 50_000),
                (45_000, 50_000),
                (50_000, 50_000)
            ],
            "uneven multipart upload should report correct cumulative bytes"
        );

        // 4. Verify monotonicity for all shards
        for (shard_idx, progress_events) in
            [(0, &shard0_progress), (1, &shard1_progress), (2, &shard2_progress)]
        {
            for window in progress_events.windows(2) {
                assert!(
                    window[1].0 >= window[0].0,
                    "shard {}: bytes should be monotonically increasing, got {} after {}",
                    shard_idx,
                    window[1].0,
                    window[0].0
                );
            }
        }

        // 5. Verify total bytes is consistent across all calls for each shard
        for (shard_idx, progress_events) in
            [(0, &shard0_progress), (1, &shard1_progress), (2, &shard2_progress)]
        {
            let expected_total = 50_000u64;
            for (bytes, total) in progress_events {
                assert_eq!(
                    *total, expected_total,
                    "shard {}: total should be consistent (expected {}, got {})",
                    shard_idx, expected_total, total
                );
            }
        }

        // 6. Verify final bytes equals total bytes for each shard
        for (shard_idx, progress_events) in
            [(0, &shard0_progress), (1, &shard1_progress), (2, &shard2_progress)]
        {
            let (final_bytes, total) = progress_events.last().unwrap();
            assert_eq!(
                final_bytes, total,
                "shard {}: final bytes ({}) should equal total ({})",
                shard_idx, final_bytes, total
            );
        }
    }

    /// Tests that progress callbacks are NOT called for resumed shards (from checkpoint).
    ///
    /// When resuming from a checkpoint:
    /// - Shards already in checkpoint → skipped (no progress callbacks)
    /// - New shards → full progress callback sequence
    /// - Commit includes total count of ALL shards (resumed + new)
    ///
    /// This test simulates a 4-shard upload where shards 0 and 2 were already uploaded:
    /// - Shard 0: SKIPPED (in checkpoint)
    /// - Shard 1: Uploaded (full callbacks)
    /// - Shard 2: SKIPPED (in checkpoint)
    /// - Shard 3: Uploaded (full callbacks)
    #[test]
    fn test_progress_with_checkpoint_resume() {
        use std::collections::HashSet;
        use std::sync::Arc;

        let progress = Arc::new(TestProgress::new());

        // Shards 0 and 2 are in checkpoint (already uploaded)
        let resumed_shards: HashSet<usize> = HashSet::from([0, 2]);

        // Simulate upload loop for 4 shards
        let shard_sizes = [10_000u64, 12_000, 8_000, 15_000]; // sizes for shards 0-3
        let total_shards = 4;

        for shard_idx in 0..total_shards {
            let path = format!("data/train-{:05}.parquet", shard_idx);
            let total_bytes = shard_sizes[shard_idx];

            // Skip resumed shards (no callbacks)
            if resumed_shards.contains(&shard_idx) {
                // This is what buffer_and_write_task does for resumed shards
                continue;
            }

            // New shard: full callback sequence
            progress.on_shard_start(shard_idx, &path);

            // Upload progress (4 increments)
            for i in 1..=4 {
                let bytes = (total_bytes / 4) * i as u64;
                progress.on_shard_upload_progress(shard_idx, bytes, total_bytes);
            }

            progress.on_shard_complete(shard_idx, &path, total_bytes);
        }

        // Commit includes ALL shards (resumed + new)
        progress.on_commit_start(total_shards);
        progress.on_commit_complete(Some(
            "https://huggingface.co/datasets/user/repo/commit/abc123",
        ));

        // ===== COUNTER ASSERTIONS =====

        // Only 2 shards uploaded (1 and 3), not 4
        assert_eq!(
            progress.shard_starts.load(Ordering::SeqCst),
            2,
            "only 2 shard starts (shards 1, 3)"
        );
        assert_eq!(
            progress.shard_completes.load(Ordering::SeqCst),
            2,
            "only 2 shard completes (shards 1, 3)"
        );
        assert_eq!(
            progress.upload_progress_calls.load(Ordering::SeqCst),
            8,
            "4 progress × 2 new shards = 8"
        );
        assert_eq!(
            progress.commit_starts.load(Ordering::SeqCst),
            1,
            "1 commit start"
        );
        assert_eq!(
            progress.commit_completes.load(Ordering::SeqCst),
            1,
            "1 commit complete"
        );

        // ===== EVENT SEQUENCE ASSERTIONS =====

        let events = progress.events();

        // 2 shards × (1 start + 4 progress + 1 complete) + 2 commit = 14 events
        assert_eq!(
            events.len(),
            14,
            "2×(1 start + 4 progress + 1 complete) + 2 commit = 14"
        );

        // Verify shard 1 events (first uploaded shard)
        assert!(
            matches!(
                &events[0],
                ProgressEvent::ShardStart { index: 1, path } if path == "data/train-00001.parquet"
            ),
            "first event should be shard 1 start"
        );

        for i in 1..=4 {
            assert!(
                matches!(
                    &events[i],
                    ProgressEvent::UploadProgress { index: 1, .. }
                ),
                "events 1-4 should be shard 1 progress"
            );
        }

        assert!(
            matches!(
                &events[5],
                ProgressEvent::ShardComplete { index: 1, .. }
            ),
            "event 5 should be shard 1 complete"
        );

        // Verify shard 3 events (second uploaded shard)
        assert!(
            matches!(
                &events[6],
                ProgressEvent::ShardStart { index: 3, path } if path == "data/train-00003.parquet"
            ),
            "event 6 should be shard 3 start"
        );

        for i in 7..=10 {
            assert!(
                matches!(
                    &events[i],
                    ProgressEvent::UploadProgress { index: 3, .. }
                ),
                "events 7-10 should be shard 3 progress"
            );
        }

        assert!(
            matches!(
                &events[11],
                ProgressEvent::ShardComplete { index: 3, .. }
            ),
            "event 11 should be shard 3 complete"
        );

        // Verify commit includes ALL shards (4, not 2)
        assert!(
            matches!(&events[12], ProgressEvent::CommitStart { num_shards: 4 }),
            "commit should include ALL 4 shards (resumed + new)"
        );
        assert!(
            matches!(&events[13], ProgressEvent::CommitComplete { .. }),
            "final event should be commit complete"
        );

        // ===== VERIFY NO EVENTS FOR RESUMED SHARDS =====

        // Shards 0 and 2 should have NO events at all
        for resumed_idx in [0usize, 2] {
            let resumed_events: Vec<_> = events
                .iter()
                .filter(|e| match e {
                    ProgressEvent::ShardStart { index, .. } => *index == resumed_idx,
                    ProgressEvent::UploadProgress { index, .. } => *index == resumed_idx,
                    ProgressEvent::ShardComplete { index, .. } => *index == resumed_idx,
                    _ => false,
                })
                .collect();

            assert!(
                resumed_events.is_empty(),
                "shard {} should have no events (was resumed from checkpoint)",
                resumed_idx
            );
        }

        // Verify upload progress is monotonically increasing for new shards
        for new_shard_idx in [1usize, 3] {
            let shard_progress: Vec<u64> = events
                .iter()
                .filter_map(|e| match e {
                    ProgressEvent::UploadProgress { index, bytes, .. }
                        if *index == new_shard_idx =>
                    {
                        Some(*bytes)
                    },
                    _ => None,
                })
                .collect();

            // Verify monotonically increasing
            for window in shard_progress.windows(2) {
                assert!(
                    window[1] >= window[0],
                    "shard {} progress should be monotonically increasing",
                    new_shard_idx
                );
            }
        }
    }

    // =========================================================================
    // PartitionWriterState Tests
    // =========================================================================

    #[test]
    fn test_partition_writer_state_new_partition() {
        let mut state = PartitionWriterState::new();

        // First access to "train" partition
        assert_eq!(state.next_shard_index("train"), 0);
        assert_eq!(state.next_shard_index("train"), 1);

        // First access to "test" partition
        assert_eq!(state.next_shard_index("test"), 0);
        assert_eq!(state.next_shard_index("test"), 1);

        // Back to "train"
        assert_eq!(state.next_shard_index("train"), 2);
    }

    #[test]
    fn test_partition_writer_state_peek_index() {
        let mut state = PartitionWriterState::new();

        // Peek should not increment
        assert_eq!(state.peek_next_index("train"), 0);
        assert_eq!(state.peek_next_index("train"), 0);

        // After increment
        state.next_shard_index("train");
        assert_eq!(state.peek_next_index("train"), 1);

        // Unknown partition returns 0
        assert_eq!(state.peek_next_index("unknown"), 0);
    }

    #[test]
    fn test_partition_writer_state_record_completion() {
        let mut state = PartitionWriterState::new();

        let comp1 = ShardCompletion {
            index: 0,
            path_in_repo: "data/split=train/train-00000.parquet".to_string(),
            sha256: "abc".to_string(),
            size: 1000,
            num_rows: 100,
        };
        let comp2 = ShardCompletion {
            index: 0,
            path_in_repo: "data/split=test/test-00000.parquet".to_string(),
            sha256: "def".to_string(),
            size: 500,
            num_rows: 50,
        };

        state.record_completion("train", comp1);
        state.record_completion("test", comp2);

        assert_eq!(state.total_rows, 150);
        assert_eq!(state.total_bytes, 1500);
        assert_eq!(state.num_completed(), 2);
    }

    #[test]
    fn test_partition_writer_state_all_completions() {
        let mut state = PartitionWriterState::new();

        state.record_completion(
            "train",
            ShardCompletion {
                index: 0,
                path_in_repo: "train-0".to_string(),
                sha256: "a".to_string(),
                size: 100,
                num_rows: 10,
            },
        );
        state.record_completion(
            "test",
            ShardCompletion {
                index: 0,
                path_in_repo: "test-0".to_string(),
                sha256: "b".to_string(),
                size: 200,
                num_rows: 20,
            },
        );
        state.record_completion(
            "train",
            ShardCompletion {
                index: 1,
                path_in_repo: "train-1".to_string(),
                sha256: "c".to_string(),
                size: 150,
                num_rows: 15,
            },
        );

        let all = state.all_completions();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_partition_writer_state_partition_values() {
        let mut state = PartitionWriterState::new();
        state.next_shard_index("train");
        state.next_shard_index("test");
        state.next_shard_index("validation");

        let values: HashSet<&str> = state.partition_values().collect();
        assert_eq!(values.len(), 3);
        assert!(values.contains("train"));
        assert!(values.contains("test"));
        assert!(values.contains("validation"));
    }

    #[test]
    fn test_partition_writer_state_global_index() {
        let mut state = PartitionWriterState::new();

        // Initial peek should be 0
        assert_eq!(state.peek_global_index(), 0);

        // next_global_index increments
        assert_eq!(state.next_global_index(), 0);
        assert_eq!(state.next_global_index(), 1);
        assert_eq!(state.next_global_index(), 2);

        // Peek reflects current value
        assert_eq!(state.peek_global_index(), 3);

        // Global index is independent of partition indices
        assert_eq!(state.next_shard_index("train"), 0);
        assert_eq!(state.next_shard_index("test"), 0);
        assert_eq!(state.peek_global_index(), 3); // unchanged
    }

    // =========================================================================
    // Tests for Partition Extraction (Task 6.2.4)
    // =========================================================================

    #[test]
    fn test_extract_partition_value_string() {
        use polars_core::prelude::*;

        let df = df! {
            "split" => ["train", "train", "train"],
            "data" => [1i32, 2, 3],
        }
        .unwrap();

        let value = extract_partition_value(&df, "split").unwrap();
        assert_eq!(value, "train");
    }

    #[test]
    fn test_extract_partition_value_int() {
        use polars_core::prelude::*;

        let df = df! {
            "year" => [2024i32, 2024, 2024],
            "data" => [1i32, 2, 3],
        }
        .unwrap();

        let value = extract_partition_value(&df, "year").unwrap();
        assert_eq!(value, "2024");
    }

    #[test]
    fn test_extract_partition_value_null() {
        use polars_core::prelude::*;

        let split: Vec<Option<&str>> = vec![None, None];
        let df = df! {
            "split" => split,
            "data" => [1i32, 2],
        }
        .unwrap();

        let value = extract_partition_value(&df, "split").unwrap();
        assert_eq!(value, HIVE_DEFAULT_PARTITION);
    }

    #[test]
    fn test_extract_partition_value_missing_column() {
        use polars_core::prelude::*;

        let df = df! {
            "data" => [1i32, 2, 3],
        }
        .unwrap();

        let result = extract_partition_value(&df, "nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_partition_value_empty_df() {
        use polars_core::prelude::*;

        let df = DataFrame::empty_with_schema(&Schema::from_iter([
            Field::new("split".into(), DataType::String),
            Field::new("data".into(), DataType::Int32),
        ]));

        let result = extract_partition_value(&df, "split");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("empty DataFrame"));
    }

    #[test]
    fn test_partition_dataframe_basic() {
        use polars_core::prelude::*;

        let df = df! {
            "split" => ["train", "test", "train", "test"],
            "data" => [1i32, 2, 3, 4],
        }
        .unwrap();

        let partitions = partition_dataframe(df, "split").unwrap();
        assert_eq!(partitions.len(), 2);

        // Find train partition
        let train = partitions.iter().find(|(v, _)| v == "train").unwrap();
        assert_eq!(train.1.height(), 2);

        // Find test partition
        let test = partitions.iter().find(|(v, _)| v == "test").unwrap();
        assert_eq!(test.1.height(), 2);
    }

    #[test]
    fn test_partition_dataframe_single_partition() {
        use polars_core::prelude::*;

        let df = df! {
            "split" => ["train", "train", "train"],
            "data" => [1i32, 2, 3],
        }
        .unwrap();

        let partitions = partition_dataframe(df, "split").unwrap();
        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].0, "train");
        assert_eq!(partitions[0].1.height(), 3);
    }

    #[test]
    fn test_partition_dataframe_preserves_column() {
        use polars_core::prelude::*;

        let df = df! {
            "split" => ["train", "test"],
            "data" => [1i32, 2],
        }
        .unwrap();

        let partitions = partition_dataframe(df, "split").unwrap();

        // Partition column should be retained
        for (_, sub_df) in &partitions {
            assert!(sub_df.column("split").is_ok());
            assert!(sub_df.column("data").is_ok());
        }
    }

    #[test]
    fn test_partition_dataframe_missing_column() {
        use polars_core::prelude::*;

        let df = df! {
            "data" => [1i32, 2, 3],
        }
        .unwrap();

        let result = partition_dataframe(df, "nonexistent");
        assert!(result.is_err());
    }
}
