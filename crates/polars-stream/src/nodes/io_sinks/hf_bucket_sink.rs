use polars_core::frame::DataFrame;
use polars_core::schema::SchemaRef;
use polars_error::{PolarsResult, polars_ensure};
use polars_io::cloud::hf_bucket::{
    StreamingBucketUploader, extract_hf_token, parse_hf_bucket_url, register_file,
};
use polars_io::pl_async;
use polars_plan::dsl::FileSinkOptions;

use crate::async_executor;
use crate::async_primitives::connector;
use crate::execute::StreamingExecutionState;
use crate::morsel::{Morsel, MorselSeq, SourceToken};
use crate::nodes::io_sinks::PortState;
use crate::nodes::{ComputeNode, TaskPriority};
use crate::pipe::PortReceiver;

/// Sink node for HF Bucket uploads.
///
/// Streams parquet row groups incrementally to XET as morsels arrive,
/// keeping memory at O(row_group_size) instead of O(total_dataset).
///
/// Implements the same `ComputeNode` state-machine pattern as `IOSinkNode`:
/// `Uninitialized` → `Initialized` → `Finished`.
pub struct HfBucketSinkNode {
    options: FileSinkOptions,
    input_schema: SchemaRef,
    state: HfBucketSinkState,
}

enum HfBucketSinkState {
    Uninitialized,

    Initialized {
        phase_channel_tx: connector::Sender<PortReceiver>,
        /// Join handle for the background upload task.
        task_handle: async_executor::AbortOnDropHandle<PolarsResult<()>>,
    },

    Finished,
}

impl HfBucketSinkNode {
    pub fn new(options: FileSinkOptions, input_schema: SchemaRef) -> Self {
        Self {
            options,
            input_schema,
            state: HfBucketSinkState::Uninitialized,
        }
    }

    /// Initialize the background upload pipeline if not yet started.
    fn initialize(&mut self) -> PolarsResult<()> {
        if !matches!(self.state, HfBucketSinkState::Uninitialized) {
            return Ok(());
        }

        // Parse the HF bucket URL from sink options.
        let url = match &self.options.target {
            polars_plan::dsl::SinkTarget::Path(p) => p.to_string(),
            _ => polars_error::polars_bail!(
                ComputeError: "HF bucket sink requires a path target"
            ),
        };
        let (namespace, bucket_name, file_path) = parse_hf_bucket_url(&url)?;
        let hf_token = extract_hf_token(self.options.unified_sink_args.cloud_options.as_deref())?;

        let config =
            polars_io::cloud::hf_bucket::HfBucketConfig::new(namespace, bucket_name, hf_token);
        let file_format = self.options.file_format.clone();
        let input_schema = self.input_schema.clone();

        // Set up a channel to bridge per-phase PortReceivers into a single
        // continuous morsel stream, exactly like IOSinkNode.
        let (phase_channel_tx, mut phase_channel_rx) = connector::connector::<PortReceiver>();
        let (mut multi_phase_tx, mut multi_phase_rx) = connector::connector();

        // Send an initial empty morsel (seq 0) so the uploader sees the schema
        // even if there are zero data morsels.
        let _ = multi_phase_tx.try_send(Morsel::new(
            DataFrame::empty_with_arc_schema(input_schema.clone()),
            MorselSeq::new(0),
            SourceToken::default(),
        ));

        // Spawn the phase-bridging task: receives per-phase PortReceivers and
        // re-sequences their morsels into multi_phase_tx.
        async_executor::spawn(TaskPriority::High, async move {
            let mut morsel_seq: u64 = 1;

            while let Ok(mut phase_rx) = phase_channel_rx.recv().await {
                while let Ok(mut morsel) = phase_rx.recv().await {
                    morsel.set_seq(MorselSeq::new(morsel_seq));
                    morsel_seq = morsel_seq.saturating_add(1);

                    if multi_phase_tx.send(morsel).await.is_err() {
                        break;
                    }
                }
            }
        });

        // Spawn the upload task: reads morsels from multi_phase_rx, streams
        // them through StreamingBucketUploader, then registers the file.
        let task_handle = async_executor::AbortOnDropHandle::new(async_executor::spawn(
            TaskPriority::High,
            async move {
                // Extract parquet options (format validated in lower_ir).
                let parquet_opts = match &file_format {
                    polars_plan::dsl::FileWriteFormat::Parquet(opts) => (**opts).clone(),
                    _ => {
                        unreachable!("HF bucket sink only supports parquet (validated in lower_ir)")
                    },
                };

                // Create the streaming uploader (connects to XET, starts upload task).
                let schema = input_schema.as_ref().clone();
                let mut uploader = pl_async::get_runtime()
                    .spawn(StreamingBucketUploader::new(
                        config.clone(),
                        schema,
                        parquet_opts,
                    ))
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::from(e).into()))?;

                // Stream morsels through the uploader.
                while let Ok(morsel) = multi_phase_rx.recv().await {
                    let df = morsel.into_df();
                    if df.height() > 0 {
                        uploader.write_batch(&df)?;
                    }
                }

                // Finalize: write parquet footer + close XET writer.
                let info = pl_async::get_runtime()
                    .spawn(uploader.finish())
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::from(e).into()))?;

                // Register the uploaded file with the HF bucket batch API.
                let xet_hash = info.xet_hash;
                pl_async::get_runtime()
                    .spawn(async move { register_file(&config, file_path, xet_hash).await })
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::from(e).into()))?;

                Ok(())
            },
        ));

        self.state = HfBucketSinkState::Initialized {
            phase_channel_tx,
            task_handle,
        };

        Ok(())
    }
}

impl ComputeNode for HfBucketSinkNode {
    fn name(&self) -> &str {
        "hf-bucket-sink"
    }

    fn update_state(
        &mut self,
        recv: &mut [PortState],
        send: &mut [PortState],
        _state: &StreamingExecutionState,
    ) -> PolarsResult<()> {
        assert_eq!(recv.len(), 1);
        assert!(send.is_empty());

        recv[0] = if recv[0] == PortState::Done {
            // Ensure initialization even for empty output.
            self.initialize()?;

            match std::mem::replace(&mut self.state, HfBucketSinkState::Finished) {
                HfBucketSinkState::Initialized {
                    phase_channel_tx,
                    task_handle,
                } => {
                    drop(phase_channel_tx);
                    pl_async::get_runtime().block_on(task_handle)?;
                },
                HfBucketSinkState::Finished => {},
                HfBucketSinkState::Uninitialized => unreachable!(),
            };

            PortState::Done
        } else {
            polars_ensure!(
                !matches!(self.state, HfBucketSinkState::Finished),
                ComputeError:
                "unreachable: HF bucket sink node state is 'Finished', but recv port \
                state is not 'Done'."
            );

            PortState::Ready
        };

        Ok(())
    }

    fn spawn<'env, 's>(
        &'env mut self,
        scope: &'s crate::async_executor::TaskScope<'s, 'env>,
        recv_ports: &mut [Option<crate::pipe::RecvPort<'_>>],
        send_ports: &mut [Option<crate::pipe::SendPort<'_>>],
        _state: &'s StreamingExecutionState,
        join_handles: &mut Vec<crate::async_executor::JoinHandle<PolarsResult<()>>>,
    ) {
        assert_eq!(recv_ports.len(), 1);
        assert!(send_ports.is_empty());

        let phase_morsel_rx = recv_ports[0].take().unwrap().serial();

        join_handles.push(scope.spawn_task(TaskPriority::Low, async move {
            self.initialize()?;

            let HfBucketSinkState::Initialized {
                phase_channel_tx, ..
            } = &mut self.state
            else {
                unreachable!()
            };

            if phase_channel_tx.send(phase_morsel_rx).await.is_err() {
                let HfBucketSinkState::Initialized {
                    phase_channel_tx,
                    task_handle,
                } = std::mem::replace(&mut self.state, HfBucketSinkState::Finished)
                else {
                    unreachable!()
                };

                drop(phase_channel_tx);
                return Err(task_handle.await.unwrap_err());
            }

            Ok(())
        }));
    }
}
