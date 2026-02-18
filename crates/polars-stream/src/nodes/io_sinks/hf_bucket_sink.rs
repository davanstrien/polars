use std::pin::Pin;
use std::sync::{Arc, Mutex};

use polars_core::schema::SchemaRef;
use polars_error::PolarsResult;
use polars_io::cloud::hf_bucket::{
    HfBucketConfig, StreamingBucketUploader, UploadedFileInfo, extract_hf_token,
    parse_hf_bucket_url, register_file,
};
use polars_plan::dsl::FileSinkOptions;

use super::{SinkInputPort, SinkNode};
use crate::async_executor::spawn;
use crate::async_primitives::connector::Receiver;
use crate::execute::StreamingExecutionState;
use crate::nodes::io_sinks::phase::PhaseOutcome;
use crate::nodes::{JoinHandle, TaskPriority};

/// Sink node for HF Bucket uploads.
///
/// Streams parquet row groups incrementally to XET as morsels arrive,
/// keeping memory at O(row_group_size) instead of O(total_dataset).
pub struct HfBucketSinkNode {
    options: FileSinkOptions,
    input_schema: SchemaRef,

    // Set during initialize(), consumed during finalize()
    config: Option<HfBucketConfig>,
    file_path: Option<String>,
    // Written by spawn_sink (upload result), read by finalize (registration)
    file_info: Arc<Mutex<Option<UploadedFileInfo>>>,
}

impl HfBucketSinkNode {
    pub fn new(options: FileSinkOptions, input_schema: SchemaRef) -> Self {
        Self {
            options,
            input_schema,
            config: None,
            file_path: None,
            file_info: Arc::new(Mutex::new(None)),
        }
    }
}

impl SinkNode for HfBucketSinkNode {
    fn name(&self) -> &str {
        "hf-bucket-sink"
    }

    fn is_sink_input_parallel(&self) -> bool {
        false // Serial consumption — one morsel at a time
    }

    fn do_maintain_order(&self) -> bool {
        self.options.unified_sink_args.maintain_order
    }

    fn initialize(&mut self, _state: &StreamingExecutionState) -> PolarsResult<()> {
        let url = match &self.options.target {
            polars_plan::dsl::SinkTarget::Path(p) => p.to_string(),
            _ => polars_error::polars_bail!(
                ComputeError: "HF bucket sink requires a path target"
            ),
        };

        let (namespace, bucket_name, file_path) = parse_hf_bucket_url(&url)?;

        let hf_token = extract_hf_token(
            self.options
                .unified_sink_args
                .cloud_options
                .as_deref(),
        )?;

        self.config = Some(HfBucketConfig::new(namespace, bucket_name, hf_token));
        self.file_path = Some(file_path);

        Ok(())
    }

    fn spawn_sink(
        &mut self,
        mut recv_port_rx: Receiver<(PhaseOutcome, SinkInputPort)>,
        _state: &StreamingExecutionState,
        join_handles: &mut Vec<JoinHandle<PolarsResult<()>>>,
    ) {
        let input_schema = self.input_schema.clone();
        let file_format = self.options.file_format.clone();
        let config = self.config.clone().expect("initialize() must be called first");
        let file_info_slot = self.file_info.clone();

        join_handles.push(spawn(TaskPriority::High, async move {
            // Extract parquet options from the file format.
            #[cfg(feature = "parquet")]
            let parquet_opts = match &file_format {
                polars_plan::dsl::FileWriteFormat::Parquet(opts) => (**opts).clone(),
                _ => Default::default(),
            };
            #[cfg(not(feature = "parquet"))]
            let parquet_opts = Default::default();

            // Create the streaming uploader (connects to XET, starts upload task).
            // Pass owned values so the future is 'static for tokio::spawn.
            let schema = input_schema.as_ref().clone();
            let mut uploader = polars_io::pl_async::get_runtime()
                .spawn(StreamingBucketUploader::new(
                    config,
                    schema,
                    parquet_opts,
                ))
                .await
                .unwrap_or_else(|e| Err(std::io::Error::from(e).into()))?;

            // Stream morsels through the uploader.
            while let Ok((outcome, rx)) = recv_port_rx.recv().await {
                let mut rx = rx.serial();
                while let Ok(morsel) = rx.recv().await {
                    let (df, _seq, _, consume_token) = morsel.into_inner();
                    if df.height() > 0 {
                        uploader.write_batch(&df)?;
                    }
                    drop(consume_token);
                }
                outcome.stopped();
            }

            // Finalize: write parquet footer + close XET writer.
            let info = polars_io::pl_async::get_runtime()
                .spawn(uploader.finish())
                .await
                .unwrap_or_else(|e| Err(std::io::Error::from(e).into()))?;

            *file_info_slot.lock().unwrap() = Some(info);

            PolarsResult::Ok(())
        }));
    }

    fn finalize(
        &mut self,
        _state: &StreamingExecutionState,
    ) -> Option<Pin<Box<dyn Future<Output = PolarsResult<()>> + Send>>> {
        let config = self.config.take()?;
        let file_path = self.file_path.take()?;
        let file_info_slot = self.file_info.clone();

        Some(Box::pin(async move {
            let info = file_info_slot.lock().unwrap().take();

            if let Some(info) = info {
                let handle = polars_io::pl_async::get_runtime().spawn(async move {
                    register_file(&config, file_path, info.xet_hash).await
                });
                handle
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::from(e).into()))?;
            }

            Ok(())
        }))
    }
}
