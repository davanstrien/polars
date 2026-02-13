//! HF Bucket sink — XET upload and bucket batch API wrappers.
//!
//! Gated behind `#[cfg(feature = "hf_bucket_sink")]`.
//! These are the building blocks the streaming sink node (Phase 2.5) will call.

mod batch;
mod xet_upload;

pub use batch::*;
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
