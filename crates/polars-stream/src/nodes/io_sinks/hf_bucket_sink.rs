use polars_error::PolarsResult;
use polars_plan::dsl::FileSinkOptions;

use super::{SinkInputPort, SinkNode};
use crate::async_executor::spawn;
use crate::async_primitives::connector::Receiver;
use crate::execute::StreamingExecutionState;
use crate::nodes::io_sinks::phase::PhaseOutcome;
use crate::nodes::{JoinHandle, TaskPriority};

/// Stub sink node for HF Bucket uploads.
///
/// Currently consumes and discards all incoming morsels.
/// Future phases will add parquet encoding + XET upload in `spawn_sink`,
/// URL/token parsing in `initialize`, and batch API registration in `finalize`.
pub struct HfBucketSinkNode {
    options: FileSinkOptions,
}

impl HfBucketSinkNode {
    pub fn new(options: FileSinkOptions) -> Self {
        Self { options }
    }
}

impl SinkNode for HfBucketSinkNode {
    fn name(&self) -> &str {
        "hf-bucket-sink"
    }

    fn is_sink_input_parallel(&self) -> bool {
        true
    }

    fn do_maintain_order(&self) -> bool {
        self.options.unified_sink_args.maintain_order
    }

    fn spawn_sink(
        &mut self,
        mut recv_port_rx: Receiver<(PhaseOutcome, SinkInputPort)>,
        _state: &StreamingExecutionState,
        join_handles: &mut Vec<JoinHandle<PolarsResult<()>>>,
    ) {
        // Stub: consume and discard all morsels.
        join_handles.push(spawn(TaskPriority::High, async move {
            while let Ok((outcome, rx)) = recv_port_rx.recv().await {
                let mut rx = rx.serial();
                while let Ok(morsel) = rx.recv().await {
                    drop(morsel);
                }
                outcome.stopped();
            }
            PolarsResult::Ok(())
        }));
    }
}
