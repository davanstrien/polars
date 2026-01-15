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

use std::pin::Pin;
use std::sync::Arc;

use polars_core::schema::SchemaRef;
use polars_error::PolarsResult;
use polars_io::cloud::hf::options::HfSinkOptions;
use polars_plan::dsl::SinkOptions;

use super::phase::PhaseOutcome;
use super::{SinkInputPort, SinkNode};
use crate::async_executor::spawn;
use crate::async_primitives::connector::Receiver;
use crate::execute::StreamingExecutionState;
use crate::nodes::{JoinHandle, TaskPriority};
use crate::utils::tokio_handle_ext::AbortOnDropHandle;

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
}
