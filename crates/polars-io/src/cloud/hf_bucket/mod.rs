//! HF Bucket sink — XET upload and bucket batch API wrappers.
//!
//! Gated behind `#[cfg(feature = "hf_bucket_sink")]`.
//! These are the building blocks the streaming sink node (Phase 2.5) will call.

use polars_error::{PolarsResult, polars_bail};

use crate::cloud::CloudOptions;
#[cfg(feature = "http")]
use crate::cloud::options::CloudConfig;

mod batch;
mod streaming_upload;
mod xet_upload;

pub use batch::*;
pub use streaming_upload::*;
pub use xet_upload::*;

/// Configuration for connecting to an HF bucket.
#[derive(Clone, Debug)]
pub struct HfBucketConfig {
    /// Bucket namespace (user or org), e.g. "davanstrien".
    pub namespace: String,
    /// Bucket name, e.g. "my-bucket".
    pub bucket_name: String,
    /// HuggingFace API token (Bearer token).
    pub hf_token: String,
    /// HF API endpoint, defaults to "https://huggingface.co".
    pub endpoint: String,
}

impl HfBucketConfig {
    pub fn new(
        namespace: impl Into<String>,
        bucket_name: impl Into<String>,
        hf_token: impl Into<String>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            bucket_name: bucket_name.into(),
            hf_token: hf_token.into(),
            endpoint: "https://huggingface.co".to_string(),
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }
}

/// Parse an `hf://buckets/namespace/name/path/file.parquet` URL into its components.
///
/// Returns `(namespace, bucket_name, file_path)`.
pub fn parse_hf_bucket_url(url: &str) -> PolarsResult<(String, String, String)> {
    let rest = url.strip_prefix("hf://buckets/").unwrap_or_else(|| {
        // Also handle the case where just the path portion is passed
        url.strip_prefix("buckets/").unwrap_or(url)
    });

    let parts: Vec<&str> = rest.splitn(3, '/').collect();
    if parts.len() < 3 || parts.iter().any(|p| p.is_empty()) {
        polars_bail!(
            ComputeError:
            "invalid HF bucket URL '{}': expected format hf://buckets/namespace/name/path",
            url
        );
    }

    Ok((
        parts[0].to_string(),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

/// Extract the HF Bearer token from `CloudOptions`, falling back to env var and cached file.
pub fn extract_hf_token(cloud_options: Option<&CloudOptions>) -> PolarsResult<String> {
    // 1. Try to extract from CloudOptions HTTP headers
    #[cfg(feature = "http")]
    if let Some(opts) = cloud_options {
        if let Some(CloudConfig::Http { headers }) = &opts.config {
            for (key, value) in headers {
                if key.eq_ignore_ascii_case("authorization") {
                    if let Some(token) = value.strip_prefix("Bearer ") {
                        return Ok(token.to_string());
                    }
                }
            }
        }
    }

    #[cfg(not(feature = "http"))]
    let _ = cloud_options;

    // 2. Fall back to HF_TOKEN env var
    if let Ok(token) = std::env::var("HF_TOKEN") {
        if !token.is_empty() {
            return Ok(token);
        }
    }

    // 3. Fall back to cached token file
    let hf_home = std::env::var("HF_HOME");
    let hf_home = hf_home.as_deref().unwrap_or("~/.cache/huggingface");
    let hf_home = crate::path_utils::resolve_homedir(hf_home);
    let cached_token_path = hf_home.join("token");

    if let Ok(bytes) = std::fs::read(&cached_token_path) {
        if let Ok(token) = String::from_utf8(bytes) {
            let token = token.trim().to_string();
            if !token.is_empty() {
                return Ok(token);
            }
        }
    }

    polars_bail!(
        ComputeError: "no HF token found: set HF_TOKEN env var, pass via cloud_options, or login with `huggingface-cli login`"
    );
}

/// Upload a file to an HF bucket via XET and register it with the batch API.
///
/// This is a high-level helper that encapsulates the entire upload flow:
/// 1. Fetch XET write token
/// 2. Upload data via XET protocol
/// 3. Register file via batch API
pub async fn upload_and_register_file(
    config: &HfBucketConfig,
    file_path: String,
    data: Vec<u8>,
) -> PolarsResult<()> {
    let client = reqwest::Client::new();
    let bucket_writer = BucketWriter::new(&client, config).await?;
    let file_info = bucket_writer.upload_bytes(bytes::Bytes::from(data)).await?;
    let xet_hash = file_info.hash().to_string();
    bucket_batch(
        &client,
        config,
        &[BucketOperation::AddFile {
            path: file_path,
            xet_hash,
        }],
    )
    .await
}

/// Register an already-uploaded file in an HF bucket via the batch API.
///
/// This is the second half of the upload flow — call it after
/// [`StreamingBucketUploader::finish`] returns the XET hash.
pub async fn register_file(
    config: &HfBucketConfig,
    file_path: String,
    xet_hash: String,
) -> PolarsResult<()> {
    let client = reqwest::Client::new();
    bucket_batch(
        &client,
        config,
        &[BucketOperation::AddFile {
            path: file_path,
            xet_hash,
        }],
    )
    .await
}
