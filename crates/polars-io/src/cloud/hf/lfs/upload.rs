//! Upload executor for HuggingFace Hub LFS uploads.
//!
//! Handles both basic (single PUT) and multipart uploads to presigned S3 URLs.
//! Files uploaded via LFS are automatically migrated to Xet storage by HF Hub.

use std::collections::HashMap;

use bytes::Bytes;
use polars_core::config;
use polars_error::{PolarsResult, polars_bail, to_compute_err};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG};

use super::types::{LfsPartCompletion, LfsPartInfo, LfsTransfer};
use crate::cloud::hf::mmap_buffer::MmapReadHandle;
use crate::cloud::options::USER_AGENT;
use crate::pl_async::with_concurrency_budget;

/// Maximum retries for upload operations.
const MAX_UPLOAD_RETRIES: usize = 3;

/// Base delay for exponential backoff (milliseconds).
const RETRY_BASE_DELAY_MS: u64 = 500;

/// Executor for uploading data to HF Hub via LFS presigned URLs.
///
/// Supports both basic (single PUT) and multipart uploads.
///
/// # Example
///
/// ```ignore
/// use polars_io::cloud::hf::lfs::{UploadExecutor, LfsTransfer};
///
/// let executor = UploadExecutor::new()?;
/// executor.upload(data, transfer, "sha256hash").await?;
/// ```
pub struct UploadExecutor {
    client: reqwest::Client,
}

impl Default for UploadExecutor {
    fn default() -> Self {
        Self::new().expect("failed to create UploadExecutor")
    }
}

impl UploadExecutor {
    /// Create a new upload executor.
    pub fn new() -> PolarsResult<Self> {
        let client = reqwest::ClientBuilder::new()
            .user_agent(USER_AGENT)
            .https_only(true)
            .build()
            .map_err(to_compute_err)?;

        Ok(Self { client })
    }

    /// Upload data using the transfer method specified by LFS.
    ///
    /// # Arguments
    /// * `data` - The file data as a read handle (zero-copy mmap)
    /// * `transfer` - The transfer method from LFS batch response
    /// * `sha256` - SHA256 hash of the data (for logging)
    ///
    /// # Returns
    /// * `Ok(None)` - File already exists, skipped upload
    /// * `Ok(Some(completions))` - Multipart upload completed, returns part ETags
    /// * `Err(...)` - Upload failed after retries
    pub async fn upload(
        &self,
        data: MmapReadHandle,
        transfer: LfsTransfer,
        sha256: &str,
    ) -> PolarsResult<Option<Vec<LfsPartCompletion>>> {
        match transfer {
            LfsTransfer::AlreadyExists => {
                if config::verbose() {
                    eprintln!("LFS: file {} already exists, skipping upload", sha256);
                }
                Ok(None)
            },
            LfsTransfer::Basic { url, headers } => {
                self.upload_basic(data.as_slice(), &url, &headers).await?;
                Ok(None)
            },
            LfsTransfer::Multipart { parts } => {
                let completions = self.upload_multipart(data.as_slice(), &parts).await?;
                Ok(Some(completions))
            },
        }
    }

    /// Upload data with a single PUT request (basic transfer).
    async fn upload_basic(
        &self,
        data: &[u8],
        url: &str,
        headers: &HashMap<String, String>,
    ) -> PolarsResult<()> {
        let mut retries = 0;

        loop {
            let result = self.send_put_request(url, data, headers).await;

            match result {
                Ok(()) => return Ok(()),
                Err(e) => {
                    retries += 1;
                    if retries > MAX_UPLOAD_RETRIES {
                        polars_bail!(
                            ComputeError: "upload failed after {} retries: {}",
                            MAX_UPLOAD_RETRIES,
                            e
                        );
                    }

                    // Exponential backoff: 500ms, 1s, 2s
                    let delay = RETRY_BASE_DELAY_MS * (1 << (retries - 1));
                    if config::verbose() {
                        eprintln!(
                            "Upload failed, retrying in {}ms (attempt {}/{}): {}",
                            delay, retries, MAX_UPLOAD_RETRIES, e
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                },
            }
        }
    }

    /// Send a single PUT request.
    async fn send_put_request(
        &self,
        url: &str,
        data: &[u8],
        extra_headers: &HashMap<String, String>,
    ) -> PolarsResult<()> {
        with_concurrency_budget(1, || async {
            let mut req = self
                .client
                .put(url)
                .header(CONTENT_LENGTH, data.len())
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Bytes::copy_from_slice(data));

            // Add extra headers from LFS response
            for (key, value) in extra_headers {
                req = req.header(key.as_str(), value.as_str());
            }

            let resp = req.send().await.map_err(to_compute_err)?;
            let status = resp.status();

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                polars_bail!(
                    ComputeError: "S3 upload failed (HTTP {}): {}",
                    status.as_u16(),
                    body
                );
            }

            Ok(())
        })
        .await
    }

    /// Upload data using multipart upload.
    ///
    /// This uploads parts in parallel and returns completion info with ETags.
    /// The caller should then call `LfsClient::complete_multipart()` with these.
    async fn upload_multipart(
        &self,
        data: &[u8],
        parts: &[LfsPartInfo],
    ) -> PolarsResult<Vec<LfsPartCompletion>> {
        if parts.is_empty() {
            polars_bail!(ComputeError: "multipart upload requires at least one part");
        }

        // Calculate byte ranges for each part
        let mut offset = 0usize;
        let part_ranges: Vec<_> = parts
            .iter()
            .map(|part| {
                let start = offset;
                let end = start + part.size as usize;
                offset = end;
                (part, start..end)
            })
            .collect();

        // Verify total size matches
        if offset != data.len() {
            polars_bail!(
                ComputeError: "multipart parts total size ({}) doesn't match data size ({})",
                offset,
                data.len()
            );
        }

        // Upload parts sequentially for now
        // TODO: Consider parallel upload with futures::stream::buffered()
        let mut completions = Vec::with_capacity(parts.len());

        for (part, range) in part_ranges {
            let data_slice = &data[range];
            let completion = self.upload_single_part_with_retry(part, data_slice).await?;
            completions.push(completion);
        }

        if config::verbose() {
            eprintln!(
                "Multipart upload complete: {} parts uploaded",
                completions.len()
            );
        }

        Ok(completions)
    }

    /// Upload a single part with retry, returning completion info.
    async fn upload_single_part_with_retry(
        &self,
        part: &LfsPartInfo,
        data: &[u8],
    ) -> PolarsResult<LfsPartCompletion> {
        let mut retries = 0;

        loop {
            let result = self.upload_single_part(part, data).await;

            match result {
                Ok(etag) => {
                    return Ok(LfsPartCompletion {
                        part_number: part.part_number,
                        etag,
                    });
                },
                Err(e) => {
                    retries += 1;
                    if retries > MAX_UPLOAD_RETRIES {
                        polars_bail!(
                            ComputeError: "part {} upload failed after {} retries: {}",
                            part.part_number,
                            MAX_UPLOAD_RETRIES,
                            e
                        );
                    }

                    let delay = RETRY_BASE_DELAY_MS * (1 << (retries - 1));
                    if config::verbose() {
                        eprintln!(
                            "Part {} upload failed, retrying in {}ms: {}",
                            part.part_number, delay, e
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                },
            }
        }
    }

    /// Upload a single part and return the ETag.
    async fn upload_single_part(&self, part: &LfsPartInfo, data: &[u8]) -> PolarsResult<String> {
        with_concurrency_budget(1, || async {
            let resp = self
                .client
                .put(&part.href)
                .header(CONTENT_LENGTH, data.len())
                .header(CONTENT_TYPE, "application/octet-stream")
                .body(Bytes::copy_from_slice(data))
                .send()
                .await
                .map_err(to_compute_err)?;

            let status = resp.status();

            // Extract ETag before consuming response
            let etag = resp
                .headers()
                .get(ETAG)
                .and_then(|h| h.to_str().ok())
                .map(|s| s.to_string());

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                polars_bail!(
                    ComputeError: "S3 multipart part {} upload failed (HTTP {}): {}",
                    part.part_number,
                    status.as_u16(),
                    body
                );
            }

            // ETag is required for multipart completion
            etag.ok_or_else(|| {
                polars_error::polars_err!(
                    ComputeError: "S3 response missing ETag for part {}",
                    part.part_number
                )
            })
        })
        .await
    }
}

// ============================================================================
// Progress Tracking (Stub for Future Implementation)
// ============================================================================

/// Progress callback for tracking upload progress.
///
/// This trait allows callers to receive progress updates during uploads.
/// Implement this trait to add progress bars, logging, or metrics.
pub trait UploadProgress: Send + Sync {
    /// Called when upload of a shard starts.
    fn on_upload_start(&self, sha256: &str, total_bytes: u64);

    /// Called periodically during upload with bytes transferred.
    fn on_progress(&self, sha256: &str, bytes_uploaded: u64, total_bytes: u64);

    /// Called when upload completes successfully.
    fn on_upload_complete(&self, sha256: &str);

    /// Called when upload fails.
    fn on_upload_error(&self, sha256: &str, error: &str);
}

/// No-op progress tracker (default).
pub struct NoOpProgress;

impl UploadProgress for NoOpProgress {
    fn on_upload_start(&self, _sha256: &str, _total_bytes: u64) {}
    fn on_progress(&self, _sha256: &str, _bytes_uploaded: u64, _total_bytes: u64) {}
    fn on_upload_complete(&self, _sha256: &str) {}
    fn on_upload_error(&self, _sha256: &str, _error: &str) {}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_executor_new() {
        let executor = UploadExecutor::new();
        assert!(executor.is_ok());
    }

    #[test]
    fn test_upload_executor_default() {
        let _executor = UploadExecutor::default();
        // Should not panic
    }

    #[test]
    fn test_part_range_calculation() {
        // Simulate what upload_multipart does for range calculation
        let parts = vec![
            LfsPartInfo {
                part_number: 1,
                size: 100,
                href: "http://example.com/part1".to_string(),
            },
            LfsPartInfo {
                part_number: 2,
                size: 150,
                href: "http://example.com/part2".to_string(),
            },
            LfsPartInfo {
                part_number: 3,
                size: 50,
                href: "http://example.com/part3".to_string(),
            },
        ];

        let mut offset = 0usize;
        let ranges: Vec<_> = parts
            .iter()
            .map(|part| {
                let start = offset;
                let end = start + part.size as usize;
                offset = end;
                (part.part_number, start..end)
            })
            .collect();

        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (1, 0..100));
        assert_eq!(ranges[1], (2, 100..250));
        assert_eq!(ranges[2], (3, 250..300));
        assert_eq!(offset, 300); // Total size
    }

    #[test]
    fn test_noop_progress() {
        let progress = NoOpProgress;

        // These should not panic
        progress.on_upload_start("abc123", 1000);
        progress.on_progress("abc123", 500, 1000);
        progress.on_upload_complete("abc123");
        progress.on_upload_error("abc123", "test error");
    }
}
