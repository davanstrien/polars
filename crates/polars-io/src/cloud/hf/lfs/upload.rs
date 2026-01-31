//! Upload executor for HuggingFace Hub LFS uploads.
//!
//! Handles both basic (single PUT) and multipart uploads to presigned S3 URLs.
//! Files uploaded via LFS are automatically migrated to Xet storage by HF Hub.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use polars_core::config;
use polars_error::{PolarsResult, polars_bail, to_compute_err};
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE, ETAG};

use super::types::{LfsPartCompletion, LfsTransfer};
use crate::cloud::hf::mmap_buffer::MmapReadHandle;
use crate::cloud::hf::HfSinkProgress;
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

    /// Create upload executor with custom base URL (for testing).
    ///
    /// When `base_url` is provided and starts with "http://", https_only
    /// is disabled to allow testing with mock servers.
    ///
    /// # Arguments
    /// * `base_url` - Optional base URL for determining https_only setting
    pub fn new_with_base_url(base_url: Option<&str>) -> PolarsResult<Self> {
        let https_only = base_url
            .map(|url| url.starts_with("https://"))
            .unwrap_or(true);

        let client = reqwest::ClientBuilder::new()
            .user_agent(USER_AGENT)
            .https_only(https_only)
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
    /// * `shard_index` - Zero-based index of the shard being uploaded
    /// * `progress` - Optional progress callback for upload tracking
    ///
    /// # Returns
    /// * `Ok(None)` - File already exists or basic upload (no multipart completion needed)
    /// * `Ok(Some((completion_url, completions)))` - Multipart upload completed,
    ///   returns the completion URL and part ETags. Caller should POST to completion_url
    ///   with the part completions.
    /// * `Err(...)` - Upload failed after retries
    pub async fn upload(
        &self,
        data: MmapReadHandle,
        transfer: LfsTransfer,
        sha256: &str,
        shard_index: usize,
        progress: Option<Arc<dyn HfSinkProgress>>,
    ) -> PolarsResult<Option<(String, Vec<LfsPartCompletion>)>> {
        match transfer {
            LfsTransfer::AlreadyExists => {
                if config::verbose() {
                    eprintln!("LFS: file {} already exists, skipping upload", sha256);
                }
                Ok(None)
            },
            LfsTransfer::Basic { url, headers } => {
                self.upload_basic(data.as_slice(), &url, &headers, shard_index, progress)
                    .await?;
                Ok(None)
            },
            LfsTransfer::Multipart {
                completion_url,
                chunk_size,
                part_urls,
            } => {
                let completions = self
                    .upload_multipart(data.as_slice(), chunk_size, &part_urls, shard_index, progress)
                    .await?;
                // Return the completion URL along with part completions
                // The caller needs the completion_url to finalize the upload
                Ok(Some((completion_url, completions)))
            },
        }
    }

    /// Upload data with a single PUT request (basic transfer).
    async fn upload_basic(
        &self,
        data: &[u8],
        url: &str,
        headers: &HashMap<String, String>,
        shard_index: usize,
        progress: Option<Arc<dyn HfSinkProgress>>,
    ) -> PolarsResult<()> {
        let total = data.len() as u64;

        // Report upload start (0 bytes)
        if let Some(ref p) = progress {
            p.on_shard_upload_progress(shard_index, 0, total);
        }

        let mut retries = 0;

        loop {
            let result = self.send_put_request(url, data, headers).await;

            match result {
                Ok(()) => {
                    // Report upload complete (all bytes)
                    if let Some(ref p) = progress {
                        p.on_shard_upload_progress(shard_index, total, total);
                    }
                    return Ok(());
                },
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
    /// This uploads parts sequentially and returns completion info with ETags.
    /// The caller should then call `LfsClient::complete_multipart()` with these.
    ///
    /// # Arguments
    /// * `data` - The complete file data
    /// * `chunk_size` - Size of each chunk in bytes (from LFS batch response)
    /// * `part_urls` - Presigned S3 URLs for each part, as (part_number, url) tuples
    /// * `shard_index` - Zero-based index of the shard being uploaded
    /// * `progress` - Optional progress callback
    async fn upload_multipart(
        &self,
        data: &[u8],
        chunk_size: u64,
        part_urls: &[(u32, String)],
        shard_index: usize,
        progress: Option<Arc<dyn HfSinkProgress>>,
    ) -> PolarsResult<Vec<LfsPartCompletion>> {
        if part_urls.is_empty() {
            polars_bail!(ComputeError: "multipart upload requires at least one part");
        }

        let total_size = data.len() as u64;
        let chunk_size = chunk_size as usize;

        // Report upload start (0 bytes)
        if let Some(ref p) = progress {
            p.on_shard_upload_progress(shard_index, 0, total_size);
        }

        // Upload parts sequentially
        // TODO: Consider parallel upload with futures::stream::buffered()
        let mut completions = Vec::with_capacity(part_urls.len());
        let mut bytes_uploaded: u64 = 0;

        for (i, (part_number, url)) in part_urls.iter().enumerate() {
            // Calculate byte range for this part
            let start = i * chunk_size;
            let end = std::cmp::min(start + chunk_size, data.len());
            let data_slice = &data[start..end];

            let completion = self
                .upload_single_part_with_retry(*part_number, url, data_slice)
                .await?;
            completions.push(completion);

            // Report progress after each part
            bytes_uploaded += data_slice.len() as u64;
            if let Some(ref p) = progress {
                p.on_shard_upload_progress(shard_index, bytes_uploaded, total_size);
            }
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
        part_number: u32,
        url: &str,
        data: &[u8],
    ) -> PolarsResult<LfsPartCompletion> {
        let mut retries = 0;

        loop {
            let result = self.upload_single_part(part_number, url, data).await;

            match result {
                Ok(etag) => {
                    return Ok(LfsPartCompletion {
                        part_number,
                        etag,
                    });
                },
                Err(e) => {
                    retries += 1;
                    if retries > MAX_UPLOAD_RETRIES {
                        polars_bail!(
                            ComputeError: "part {} upload failed after {} retries: {}",
                            part_number,
                            MAX_UPLOAD_RETRIES,
                            e
                        );
                    }

                    let delay = RETRY_BASE_DELAY_MS * (1 << (retries - 1));
                    if config::verbose() {
                        eprintln!(
                            "Part {} upload failed, retrying in {}ms: {}",
                            part_number, delay, e
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                },
            }
        }
    }

    /// Upload a single part and return the ETag.
    async fn upload_single_part(
        &self,
        part_number: u32,
        url: &str,
        data: &[u8],
    ) -> PolarsResult<String> {
        with_concurrency_budget(1, || async {
            let resp = self
                .client
                .put(url)
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
                    part_number,
                    status.as_u16(),
                    body
                );
            }

            // ETag is required for multipart completion
            etag.ok_or_else(|| {
                polars_error::polars_err!(
                    ComputeError: "S3 response missing ETag for part {}",
                    part_number
                )
            })
        })
        .await
    }
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
        // With the new format, we use chunk_size to calculate ranges
        let chunk_size: usize = 100;
        let part_urls = vec![
            (1u32, "http://example.com/part1".to_string()),
            (2u32, "http://example.com/part2".to_string()),
            (3u32, "http://example.com/part3".to_string()),
        ];
        let data_len = 280; // 3 parts: 100 + 100 + 80 bytes

        let ranges: Vec<_> = part_urls
            .iter()
            .enumerate()
            .map(|(i, (part_number, _url))| {
                let start = i * chunk_size;
                let end = std::cmp::min(start + chunk_size, data_len);
                (*part_number, start..end)
            })
            .collect();

        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0], (1, 0..100));
        assert_eq!(ranges[1], (2, 100..200));
        assert_eq!(ranges[2], (3, 200..280)); // Last part is smaller
    }
}
