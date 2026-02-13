use std::pin::Pin;
use std::sync::{Arc, Mutex};

use polars_core::frame::DataFrame;
use polars_core::schema::SchemaRef;
use polars_error::PolarsResult;
use polars_io::cloud::hf_bucket::{
    HfBucketConfig, extract_hf_token, parse_hf_bucket_url, upload_and_register_file,
};
use polars_io::parquet::write::ParquetWriter;
use polars_plan::dsl::FileSinkOptions;

use super::{SinkInputPort, SinkNode};
use crate::async_executor::spawn;
use crate::async_primitives::connector::Receiver;
use crate::execute::StreamingExecutionState;
use crate::nodes::io_sinks::phase::PhaseOutcome;
use crate::nodes::{JoinHandle, TaskPriority};

/// Sink node for HF Bucket uploads.
///
/// Consumes all incoming morsels, encodes the combined DataFrame as a single
/// parquet file, then uploads via XET and registers via the batch API.
pub struct HfBucketSinkNode {
    options: FileSinkOptions,
    input_schema: SchemaRef,

    // Set during initialize(), consumed during finalize()
    config: Option<HfBucketConfig>,
    file_path: Option<String>,
    // Shared buffer for encoded parquet bytes, written by spawn_sink, read by finalize
    encoded_bytes: Arc<Mutex<Option<Vec<u8>>>>,
}

impl HfBucketSinkNode {
    pub fn new(options: FileSinkOptions, input_schema: SchemaRef) -> Self {
        Self {
            options,
            input_schema,
            config: None,
            file_path: None,
            encoded_bytes: Arc::new(Mutex::new(None)),
        }
    }
}

impl SinkNode for HfBucketSinkNode {
    fn name(&self) -> &str {
        "hf-bucket-sink"
    }

    fn is_sink_input_parallel(&self) -> bool {
        false // Serial consumption — we vstack all morsels into one DataFrame
    }

    fn do_maintain_order(&self) -> bool {
        self.options.unified_sink_args.maintain_order
    }

    fn initialize(&mut self, _state: &StreamingExecutionState) -> PolarsResult<()> {
        // Parse URL
        let url = match &self.options.target {
            polars_plan::dsl::SinkTarget::Path(p) => p.to_string(),
            _ => polars_error::polars_bail!(
                ComputeError: "HF bucket sink requires a path target"
            ),
        };

        let (namespace, bucket_name, file_path) = parse_hf_bucket_url(&url)?;

        // Extract token
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
        let encoded_bytes = self.encoded_bytes.clone();

        // Single serial task: consume all morsels, vstack, encode to parquet
        join_handles.push(spawn(TaskPriority::High, async move {
            let mut combined = DataFrame::empty_with_schema(&input_schema);

            while let Ok((outcome, rx)) = recv_port_rx.recv().await {
                let mut rx = rx.serial();
                while let Ok(morsel) = rx.recv().await {
                    let (df, _seq, _, consume_token) = morsel.into_inner();
                    combined.vstack_mut_owned(df)?;
                    drop(consume_token);
                }
                outcome.stopped();
            }

            if combined.height() > 0 {
                // Encode to parquet
                let mut buffer = Vec::new();
                let mut writer = ParquetWriter::new(&mut buffer);

                #[cfg(feature = "parquet")]
                if let polars_plan::dsl::FileWriteFormat::Parquet(opts) = &file_format {
                    writer = writer
                        .with_compression(opts.compression)
                        .with_statistics(opts.statistics)
                        .with_row_group_size(opts.row_group_size)
                        .with_data_page_size(opts.data_page_size);
                }

                writer.finish(&mut combined)?;

                // Store encoded bytes for finalize to upload
                *encoded_bytes.lock().unwrap() = Some(buffer);
            }

            PolarsResult::Ok(())
        }));
    }

    fn finalize(
        &mut self,
        _state: &StreamingExecutionState,
    ) -> Option<Pin<Box<dyn Future<Output = PolarsResult<()>> + Send>>> {
        let config = self.config.take()?;
        let file_path = self.file_path.take()?;
        let encoded_bytes = self.encoded_bytes.clone();

        Some(Box::pin(async move {
            let data = encoded_bytes.lock().unwrap().take();

            if let Some(data) = data {
                // Upload on the tokio runtime
                let handle = polars_io::pl_async::get_runtime().spawn(async move {
                    upload_and_register_file(&config, file_path, data).await
                });
                handle
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::from(e).into()))?;
            }

            Ok(())
        }))
    }
}
