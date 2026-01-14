//! LFS client for HuggingFace Hub.
//!
//! Handles the LFS Batch API for upload coordination.

use polars_core::config;
use polars_error::{PolarsResult, polars_bail, to_compute_err};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};

use super::types::{
    LfsBatchRequest, LfsBatchResponse, LfsMultipartCompleteRequest, LfsObjectRequest,
    LfsPartCompletion, LfsTransfer,
};
use crate::cloud::hf::url::HFRepoLocation;
use crate::cloud::options::USER_AGENT;
use crate::pl_async::with_concurrency_budget;
use crate::utils::decode_json_response;

/// Maximum number of retries on rate limit (429).
const MAX_RATE_LIMIT_RETRIES: usize = 3;

/// Client for interacting with HF Hub's LFS (Large File Storage) API.
///
/// Used to request upload URLs and verify uploads.
pub struct LfsClient {
    client: reqwest::Client,
    repo_location: HFRepoLocation,
    token: String,
}

impl LfsClient {
    /// Create a new LFS client.
    ///
    /// # Arguments
    /// * `bucket` - Repository type: "datasets", "spaces", or "models"
    /// * `repo_id` - Repository ID (e.g., "user/repo")
    /// * `revision` - Git revision (e.g., "main")
    /// * `token` - HF Hub authentication token
    pub fn new(
        bucket: &str,
        repo_id: &str,
        revision: &str,
        token: impl Into<String>,
    ) -> PolarsResult<Self> {
        let client = reqwest::ClientBuilder::new()
            .user_agent(USER_AGENT)
            .http1_only()
            .https_only(true)
            .build()
            .map_err(to_compute_err)?;

        let repo_location = HFRepoLocation::new(bucket, repo_id, revision);

        Ok(Self {
            client,
            repo_location,
            token: token.into(),
        })
    }

    /// Request upload URL(s) for a file.
    ///
    /// # Arguments
    /// * `sha256` - SHA256 hash of the file (lowercase hex, 64 chars)
    /// * `size` - File size in bytes
    ///
    /// # Returns
    /// * `LfsTransfer::AlreadyExists` - File already exists, skip upload
    /// * `LfsTransfer::Basic { url, headers }` - Single PUT request upload
    /// * `LfsTransfer::Multipart { parts }` - Multipart upload with part URLs
    pub async fn request_upload(&self, sha256: &str, size: u64) -> PolarsResult<LfsTransfer> {
        let request =
            LfsBatchRequest::upload(vec![LfsObjectRequest::new(sha256.to_string(), size)]);

        let response = self.send_batch_request(&request).await?;

        // Extract transfer info for our single object
        if response.objects.is_empty() {
            polars_bail!(ComputeError: "LFS batch API returned empty objects list");
        }

        let obj = response.objects.into_iter().next().unwrap();
        obj.into_transfer().map_err(
            |e| polars_error::polars_err!(ComputeError: "LFS error for object {}: {}", sha256, e),
        )
    }

    /// Request upload URLs for multiple files at once.
    ///
    /// # Arguments
    /// * `files` - Iterator of (sha256, size) tuples
    ///
    /// # Returns
    /// Vector of (sha256, LfsTransfer) tuples in same order as input
    pub async fn request_uploads<I>(&self, files: I) -> PolarsResult<Vec<(String, LfsTransfer)>>
    where
        I: IntoIterator<Item = (String, u64)>,
    {
        let objects: Vec<_> = files
            .into_iter()
            .map(|(oid, size)| LfsObjectRequest::new(oid, size))
            .collect();

        if objects.is_empty() {
            return Ok(Vec::new());
        }

        let request = LfsBatchRequest::upload(objects);
        let response = self.send_batch_request(&request).await?;

        // Convert each object response to transfer info
        response
            .objects
            .into_iter()
            .map(|obj| {
                let oid = obj.oid.clone();
                obj.into_transfer()
                    .map(|transfer| (oid.clone(), transfer))
                    .map_err(|e| {
                        polars_error::polars_err!(ComputeError: "LFS error for object {}: {}", oid, e)
                    })
            })
            .collect()
    }

    /// Verify that an upload completed successfully.
    ///
    /// Called after uploading to the presigned URL if the LFS response included a verify action.
    ///
    /// # Arguments
    /// * `verify_url` - The verification URL from LfsAction::verify
    /// * `sha256` - SHA256 hash of the uploaded file
    /// * `size` - Size of the uploaded file
    pub async fn verify_upload(
        &self,
        verify_url: &str,
        sha256: &str,
        size: u64,
    ) -> PolarsResult<()> {
        let body = serde_json::json!({
            "oid": sha256,
            "size": size
        });

        self.send_request_with_retry(verify_url, Some(&body))
            .await?;
        Ok(())
    }

    /// Complete a multipart upload after all parts are uploaded to S3.
    ///
    /// After `UploadExecutor::upload()` returns part completions, call this method
    /// to finalize the multipart upload on HF Hub.
    ///
    /// # Arguments
    /// * `sha256` - SHA256 hash of the complete file
    /// * `parts` - Part completions with ETags from S3 responses
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Upload returns part completions for multipart transfers
    /// if let Some(completions) = executor.upload(data, transfer, sha256).await? {
    ///     lfs_client.complete_multipart(sha256, completions).await?;
    /// }
    /// ```
    pub async fn complete_multipart(
        &self,
        sha256: &str,
        parts: Vec<LfsPartCompletion>,
    ) -> PolarsResult<()> {
        if parts.is_empty() {
            polars_bail!(ComputeError: "cannot complete multipart upload with no parts");
        }

        let complete_url = self.repo_location.get_lfs_multipart_complete_uri(sha256);

        let request = LfsMultipartCompleteRequest {
            oid: sha256.to_string(),
            parts,
        };

        let body = serde_json::to_vec(&request).map_err(to_compute_err)?;

        if config::verbose() {
            eprintln!(
                "Completing multipart upload for {} ({} parts)",
                sha256,
                request.parts.len()
            );
        }

        self.send_bytes_request_with_retry(&complete_url, body)
            .await?;

        Ok(())
    }

    /// Send a batch request to the LFS API with retry on rate limit.
    async fn send_batch_request(
        &self,
        request: &LfsBatchRequest,
    ) -> PolarsResult<LfsBatchResponse> {
        let batch_url = self.repo_location.get_lfs_batch_uri();
        let body = serde_json::to_vec(request).map_err(to_compute_err)?;

        let response_bytes = self.send_bytes_request_with_retry(&batch_url, body).await?;
        decode_json_response(&response_bytes)
    }

    /// Send a JSON request with retry on rate limit.
    async fn send_request_with_retry(
        &self,
        url: &str,
        body: Option<&serde_json::Value>,
    ) -> PolarsResult<bytes::Bytes> {
        let body_bytes = match body {
            Some(v) => serde_json::to_vec(v).map_err(to_compute_err)?,
            None => Vec::new(),
        };
        self.send_bytes_request_with_retry(url, body_bytes).await
    }

    /// Send a request with byte body, with retry on rate limit.
    async fn send_bytes_request_with_retry(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> PolarsResult<bytes::Bytes> {
        let mut retries = 0;

        loop {
            let result = self.send_single_request(url, body.clone()).await;

            match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    // Check if this is a rate limit error with retry info
                    let err_str = e.to_string();
                    if err_str.contains("HTTP 429") && retries < MAX_RATE_LIMIT_RETRIES {
                        // Try to extract wait time from error message
                        if let Some(wait_secs) = extract_rate_limit_wait(&err_str) {
                            retries += 1;
                            if config::verbose() {
                                eprintln!(
                                    "Rate limited, waiting {} seconds (retry {}/{})",
                                    wait_secs, retries, MAX_RATE_LIMIT_RETRIES
                                );
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                            continue;
                        }
                    }
                    return Err(e);
                },
            }
        }
    }

    /// Send a single HTTP request (no retry).
    async fn send_single_request(&self, url: &str, body: Vec<u8>) -> PolarsResult<bytes::Bytes> {
        with_concurrency_budget(1, || async {
            let resp = self
                .client
                .post(url)
                .header(AUTHORIZATION, format!("Bearer {}", self.token))
                .header(CONTENT_TYPE, "application/vnd.git-lfs+json")
                .header("Accept", "application/vnd.git-lfs+json")
                .body(body)
                .send()
                .await
                .map_err(to_compute_err)?;

            let status = resp.status();

            // Handle rate limiting specially to allow retry
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let wait_secs = resp
                    .headers()
                    .get("RateLimit")
                    .and_then(|h| h.to_str().ok())
                    .and_then(parse_rate_limit_wait);

                let body = resp.text().await.unwrap_or_default();
                polars_bail!(
                    ComputeError: "HTTP 429 rate limited (wait={}): {}",
                    wait_secs.unwrap_or(0),
                    body
                );
            }

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                polars_bail!(
                    ComputeError: "LFS API request failed (HTTP {}): {}",
                    status.as_u16(),
                    body
                );
            }

            resp.bytes().await.map_err(to_compute_err)
        })
        .await
    }
}

/// Parse HF Hub RateLimit header to extract wait time in seconds.
///
/// Header format: "api";r=0;t=42 means wait 42 seconds
fn parse_rate_limit_wait(header: &str) -> Option<u64> {
    for part in header.split(';') {
        let part = part.trim();
        if part.starts_with("t=") {
            return part[2..].parse().ok();
        }
    }
    None
}

/// Extract wait time from rate limit error message.
fn extract_rate_limit_wait(err_msg: &str) -> Option<u64> {
    // Look for "wait=N" pattern in error message
    if let Some(start) = err_msg.find("wait=") {
        let after_wait = &err_msg[start + 5..];
        // Parse until non-digit
        let digits: String = after_wait
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if !digits.is_empty() {
            return digits.parse().ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client_datasets() {
        let result = LfsClient::new("datasets", "user/repo", "main", "hf_token");
        assert!(result.is_ok());
    }

    #[test]
    fn test_new_client_spaces() {
        let result = LfsClient::new("spaces", "org/my-space", "main", "token");
        assert!(result.is_ok());
    }

    #[test]
    fn test_lfs_batch_url_via_repo_location() {
        let client = LfsClient::new("datasets", "user/repo", "main", "token").unwrap();
        let batch_url = client.repo_location.get_lfs_batch_uri();
        assert_eq!(
            batch_url,
            "https://huggingface.co/datasets/user/repo.git/info/lfs/objects/batch"
        );
    }

    #[test]
    fn test_parse_rate_limit_wait() {
        // Standard format from HF Hub
        assert_eq!(parse_rate_limit_wait("api;r=0;t=42"), Some(42));
        assert_eq!(parse_rate_limit_wait("api;r=100;t=300"), Some(300));

        // With quotes (as shown in docs)
        assert_eq!(parse_rate_limit_wait("\"api\";r=0;t=42"), Some(42));

        // Missing t= parameter
        assert_eq!(parse_rate_limit_wait("api;r=0"), None);

        // Empty/invalid
        assert_eq!(parse_rate_limit_wait(""), None);
    }

    #[test]
    fn test_extract_rate_limit_wait() {
        assert_eq!(
            extract_rate_limit_wait("HTTP 429 rate limited (wait=42): Too many requests"),
            Some(42)
        );
        assert_eq!(
            extract_rate_limit_wait("HTTP 429 rate limited (wait=300): Slow down"),
            Some(300)
        );
        assert_eq!(
            extract_rate_limit_wait("HTTP 429 rate limited (wait=0): Try again"),
            Some(0)
        );
        assert_eq!(extract_rate_limit_wait("Some other error"), None);
    }
}
