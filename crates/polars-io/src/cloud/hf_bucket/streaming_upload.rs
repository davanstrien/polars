//! Streaming parquet encode → XET upload pipeline.
//!
//! [`StreamingBucketUploader`] owns a [`BatchedWriter<ChannelWriter>`] for
//! incremental parquet encoding and an async task that streams the encoded
//! bytes to an [`XetWriter`].  Memory usage stays at O(row_group_size)
//! instead of O(total_dataset).

use std::io::{self, Write};
use std::sync::mpsc::{SyncSender, sync_channel};

use polars_core::frame::DataFrame;
use polars_core::schema::Schema;
use polars_error::{PolarsResult, to_compute_err};
use tokio::task::JoinHandle;

use super::HfBucketConfig;
use super::xet_upload::BucketWriter;
use crate::parquet::write::{BatchedWriter, ParquetWriteOptions};

/// Wrapper around [`JoinHandle`] that aborts the task when dropped.
///
/// Prevents orphaned upload tasks when the uploader is dropped early
/// (e.g. on error in `write_batch`). Mirrors the pattern used by
/// `async_executor::AbortOnDropHandle` in `polars-stream`.
struct AbortOnDropHandle<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropHandle<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// Consume the wrapper and await the inner task.
    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        self.handle.take().unwrap().await
    }
}

impl<T> Drop for AbortOnDropHandle<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

/// Information about a completed XET upload (hash + size).
pub struct UploadedFileInfo {
    pub xet_hash: String,
    pub file_size: u64,
}

/// Sync [`Write`] adapter that sends byte chunks over a bounded channel.
///
/// The receiving end is an async task that forwards bytes to an
/// [`XetWriter`].  The bounded channel (capacity 16) provides backpressure:
/// when the XET upload falls behind, `write()` blocks the encoding thread.
struct ChannelWriter {
    tx: SyncSender<Vec<u8>>,
}

impl ChannelWriter {
    fn new(tx: SyncSender<Vec<u8>>) -> Self {
        Self { tx }
    }
}

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        self.tx
            .send(buf.to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // No-op — bytes are pushed eagerly via the channel.
        Ok(())
    }
}

/// Handles incremental parquet encoding → XET upload.
///
/// Owns a [`BatchedWriter<ChannelWriter>`] for encoding and an async upload
/// task that streams bytes to an [`XetWriter`].
///
/// # Usage
///
/// ```ignore
/// let mut uploader = StreamingBucketUploader::new(&config, &schema, &opts).await?;
/// for morsel in morsels {
///     uploader.write_batch(&morsel_df)?;
/// }
/// let info = uploader.finish().await?;
/// ```
pub struct StreamingBucketUploader {
    batched_writer: BatchedWriter<ChannelWriter>,
    upload_handle: AbortOnDropHandle<PolarsResult<UploadedFileInfo>>,
}

impl StreamingBucketUploader {
    /// Create a new uploader: connects to XET, starts the async upload task,
    /// and prepares the parquet [`BatchedWriter`].
    ///
    /// Takes owned values so the returned future is `'static` (required by
    /// `tokio::spawn` / `pl_async::get_runtime().spawn()`).
    pub async fn new(
        config: HfBucketConfig,
        schema: Schema,
        parquet_options: ParquetWriteOptions,
    ) -> PolarsResult<Self> {
        // Bounded channel for backpressure (16 chunks in flight).
        let (tx, rx) = sync_channel::<Vec<u8>>(16);

        // Connect to XET and open a writer.
        let http = reqwest::Client::new();
        let bucket_writer = BucketWriter::new(&http, &config).await?;
        let xet_writer = bucket_writer.new_writer().await?;

        // Spawn the async upload task that drains the channel into XET.
        //
        // A bridge pattern is used: a `spawn_blocking` task drains the
        // std::sync channel (blocking recv) into a tokio mpsc channel,
        // which the main async loop consumes to feed XET.
        let upload_handle: AbortOnDropHandle<PolarsResult<UploadedFileInfo>> =
            AbortOnDropHandle::new(tokio::spawn(async move {
                let mut xet_writer = xet_writer;

                let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);

                // Drain std::sync::mpsc → tokio::sync::mpsc in a blocking thread.
                tokio::task::spawn_blocking(move || {
                    while let Ok(chunk) = rx.recv() {
                        if bridge_tx.blocking_send(chunk).is_err() {
                            break; // upload task dropped bridge_rx (error or done)
                        }
                    }
                });

                // Forward chunks to XET.
                while let Some(chunk) = bridge_rx.recv().await {
                    xet_writer
                        .write(bytes::Bytes::from(chunk))
                        .await
                        .map_err(to_compute_err)?;
                }

                // Finalize the XET upload.
                let file_info = xet_writer.close().await.map_err(to_compute_err)?;
                Ok(UploadedFileInfo {
                    xet_hash: file_info.hash().to_string(),
                    file_size: file_info.file_size(),
                })
            }));

        // Build the parquet BatchedWriter with our ChannelWriter.
        let channel_writer = ChannelWriter::new(tx);
        let batched_writer = parquet_options.to_writer(channel_writer).batched(&schema)?;

        Ok(Self {
            batched_writer,
            upload_handle,
        })
    }

    /// Encode a [`DataFrame`] as parquet row group(s) and stream the bytes
    /// to XET.  Called once per morsel from the sink node.
    pub fn write_batch(&mut self, df: &DataFrame) -> PolarsResult<()> {
        self.batched_writer.write_batch(df)
    }

    /// Write the parquet footer, close the XET writer, and return file info.
    ///
    /// This consumes the uploader.  The returned [`UploadedFileInfo`] contains
    /// the XET hash needed for the bucket batch API registration.
    pub async fn finish(self) -> PolarsResult<UploadedFileInfo> {
        // Write parquet footer — this flushes remaining bytes through the
        // ChannelWriter and into the channel.
        self.batched_writer.finish()?;
        // Drop the BatchedWriter (and its ChannelWriter / SyncSender) so the
        // upload task sees the channel close and can finalize.
        drop(self.batched_writer);
        // Await the upload task.
        self.upload_handle.join().await.map_err(to_compute_err)?
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::mpsc::sync_channel;

    use super::*;

    #[tokio::test]
    async fn abort_on_drop_cancels_task() {
        let handle = tokio::spawn(async {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            42
        });
        let raw = handle.abort_handle();
        let wrapper = AbortOnDropHandle::new(handle);
        assert!(!raw.is_finished());
        drop(wrapper);
        // Give the runtime a moment to process the abort.
        tokio::task::yield_now().await;
        assert!(raw.is_finished());
    }

    #[tokio::test]
    async fn abort_on_drop_join_returns_value() {
        let handle = tokio::spawn(async { 99u64 });
        let wrapper = AbortOnDropHandle::new(handle);
        let val = wrapper.join().await.unwrap();
        assert_eq!(val, 99);
    }

    #[test]
    fn channel_writer_sends_bytes() {
        let (tx, rx) = sync_channel::<Vec<u8>>(4);
        let mut w = ChannelWriter::new(tx);
        let n = w.write(b"hello").unwrap();
        assert_eq!(n, 5);
        assert_eq!(rx.recv().unwrap(), b"hello");
    }

    #[test]
    fn channel_writer_empty_write_is_noop() {
        let (tx, rx) = sync_channel::<Vec<u8>>(4);
        let mut w = ChannelWriter::new(tx);
        let n = w.write(b"").unwrap();
        assert_eq!(n, 0);
        // Nothing should have been sent.
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn channel_writer_broken_pipe_on_closed_channel() {
        let (tx, rx) = sync_channel::<Vec<u8>>(4);
        drop(rx);
        let mut w = ChannelWriter::new(tx);
        let err = w.write(b"data").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }
}
