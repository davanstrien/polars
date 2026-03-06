//! Streaming parquet encode → XET upload pipeline.
//!
//! [`StreamingBucketUploader`] owns a [`BatchedWriter<ChannelWriter>`] for
//! incremental parquet encoding and an async task that streams the encoded
//! bytes to a [`SingleFileCleaner`] via the `xet-session` API.  Memory usage
//! stays at O(row_group_size) instead of O(total_dataset).

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::mpsc::{SyncSender, sync_channel};

use polars_core::frame::DataFrame;
use polars_core::schema::Schema;
use polars_error::{PolarsResult, to_compute_err};
use tokio::task::JoinHandle;
use xet_utils::auth::TokenRefresher;

use super::HfBucketConfig;
use super::xet_upload::{HfTokenRefresher, create_xet_session, fetch_xet_write_token};
use crate::parquet::write::{BatchedWriter, ParquetWriteOptions};

/// Information about a completed XET upload (hash + size).
pub struct UploadedFileInfo {
    pub xet_hash: String,
    pub file_size: u64,
}

/// Sync [`Write`] adapter that sends byte chunks over a bounded channel.
///
/// The receiving end is an async task that forwards bytes to a
/// [`SingleFileCleaner`].  The bounded channel (capacity 16) provides
/// backpressure: when the XET upload falls behind, `write()` blocks the
/// encoding thread.
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
/// task that streams bytes to a [`SingleFileCleaner`] via `xet-session`.
///
/// # Usage
///
/// ```ignore
/// let mut uploader = StreamingBucketUploader::new(config, schema, opts).await?;
/// for morsel in morsels {
///     uploader.write_batch(&morsel_df)?;
/// }
/// let info = uploader.finish().await?;
/// ```
pub struct StreamingBucketUploader {
    batched_writer: BatchedWriter<ChannelWriter>,
    upload_handle: JoinHandle<PolarsResult<UploadedFileInfo>>,
}

impl StreamingBucketUploader {
    /// Create a new uploader: connects to XET via `xet-session`, starts the
    /// async upload task, and prepares the parquet [`BatchedWriter`].
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

        // Create XetSession with token refresher for long-running uploads.
        //
        // XetSession internally creates its own tokio runtime, so we must
        // build it outside the current async context to avoid a nested
        // runtime panic.
        let http = reqwest::Client::new();
        let token = fetch_xet_write_token(&http, &config).await?;
        let refresher: Arc<dyn TokenRefresher> = Arc::new(HfTokenRefresher {
            http: http.clone(),
            config: config.clone(),
        });
        let (commit, cleaner, _task_handle) = tokio::task::spawn_blocking(move || {
            let session = create_xet_session(&token, Some(refresher))?;
            let commit = session.new_upload_commit().map_err(to_compute_err)?;
            let (task_handle, cleaner) = commit
                // file_size 0 = unknown (streaming). xet-core uses this for
                // progress tracking only; debug builds may hit a benign
                // assertion — release builds are unaffected.
                .upload_file(Some("upload.parquet".to_string()), 0)
                .map_err(to_compute_err)?;
            Ok::<_, polars_error::PolarsError>((commit, cleaner, task_handle))
        })
        .await
        .map_err(to_compute_err)??;

        // Spawn the async upload task that drains the channel into the cleaner.
        //
        // A bridge pattern is used: a `spawn_blocking` task drains the
        // std::sync channel (blocking recv) into a tokio mpsc channel,
        // which the main async loop consumes to feed the SingleFileCleaner.
        let upload_handle: JoinHandle<PolarsResult<UploadedFileInfo>> =
            tokio::spawn(async move {
                let mut cleaner = cleaner;

                let (bridge_tx, mut bridge_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(4);

                // Drain std::sync::mpsc → tokio::sync::mpsc in a blocking thread.
                tokio::task::spawn_blocking(move || {
                    while let Ok(chunk) = rx.recv() {
                        if bridge_tx.blocking_send(chunk).is_err() {
                            break; // upload task dropped bridge_rx (error or done)
                        }
                    }
                });

                // Forward chunks to SingleFileCleaner.
                while let Some(chunk) = bridge_rx.recv().await {
                    cleaner
                        .add_data(&chunk)
                        .await
                        .map_err(to_compute_err)?;
                }

                // Finalize the XET upload.
                let (file_info, _metrics) = cleaner.finish().await.map_err(to_compute_err)?;

                // Commit the upload — this finalizes the data in XET storage.
                // Must run outside async context since it calls block_on internally.
                tokio::task::spawn_blocking(move || {
                    commit.commit().map_err(to_compute_err)
                })
                .await
                .map_err(to_compute_err)??;

                Ok(UploadedFileInfo {
                    xet_hash: file_info.hash().to_string(),
                    file_size: file_info.file_size(),
                })
            });

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
        self.upload_handle.await.map_err(to_compute_err)?
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::mpsc::sync_channel;

    use super::*;

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
