//! XET upload path — token fetch, client creation, and `BucketWriter`.
//!
//! Ports the validated patterns from `scratch/xet_upload_test/src/main.rs`.

use std::sync::Arc;

use bytes::Bytes;
use polars_error::{PolarsResult, polars_bail, to_compute_err};
use reqwest::Client;
use serde::Deserialize;
use subxet::utils::auth::TokenRefresher;
use subxet::utils::errors::AuthError;

use super::HfBucketConfig;

/// XET write token returned by the HF bucket API.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XetToken {
    pub access_token: String,
    pub cas_url: String,
    pub exp: u64,
}

/// Fetch a XET write token from the HF bucket API.
///
/// `GET /api/buckets/{namespace}/{name}/xet-write-token`
pub async fn fetch_xet_write_token(
    http: &Client,
    config: &HfBucketConfig,
) -> PolarsResult<XetToken> {
    let url = format!(
        "{}/api/buckets/{}/{}/xet-write-token",
        config.endpoint, config.namespace, config.bucket_name
    );

    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {}", config.hf_token))
        .send()
        .await
        .map_err(to_compute_err)?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        polars_bail!(
            ComputeError:
            "HF bucket XET write token request failed (HTTP {}): {}",
            status,
            body
        );
    }

    resp.json::<XetToken>().await.map_err(to_compute_err)
}

/// Refreshes XET write tokens for long-running uploads.
///
/// HF XET tokens typically expire after ~1 hour. For large streaming uploads
/// that exceed this window, the refresher re-fetches a token from the HF API.
struct HfTokenRefresher {
    http: Client,
    config: HfBucketConfig,
}

#[async_trait::async_trait]
impl TokenRefresher for HfTokenRefresher {
    async fn refresh(&self) -> Result<(String, u64), AuthError> {
        let token = fetch_xet_write_token(&self.http, &self.config)
            .await
            .map_err(AuthError::token_refresh_failure)?;
        Ok((token.access_token, token.exp))
    }
}

/// Create an `XetClient` from a write token, with an optional token refresher
/// for long-running uploads.
pub fn create_xet_client(
    token: &XetToken,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> PolarsResult<subxet::data::streaming::XetClient> {
    subxet::data::streaming::XetClient::new(
        Some(token.cas_url.clone()),
        Some((token.access_token.clone(), token.exp)),
        token_refresher,
        "polars-hf-bucket/0.1".to_string(),
    )
    .map_err(to_compute_err)
}

/// Wraps an `XetClient` to manage the upload lifecycle.
///
/// Provides helpers to create writers, upload bytes, and close files.
pub struct BucketWriter {
    client: subxet::data::streaming::XetClient,
}

impl BucketWriter {
    /// Create a new `BucketWriter` by fetching a token and constructing the client.
    ///
    /// The client is configured with a token refresher so that long-running
    /// uploads automatically re-fetch XET tokens before they expire.
    pub async fn new(http: &Client, config: &HfBucketConfig) -> PolarsResult<Self> {
        let token = fetch_xet_write_token(http, config).await?;
        let refresher: Arc<dyn TokenRefresher> = Arc::new(HfTokenRefresher {
            http: http.clone(),
            config: config.clone(),
        });
        let client = create_xet_client(&token, Some(refresher))?;
        Ok(Self { client })
    }

    /// Open a new XET writer for a single file upload.
    ///
    /// Write bytes with `writer.write(bytes).await?`, then call
    /// `writer.close().await?` to get the `XetFileInfo` (hash + size).
    pub async fn new_writer(&self) -> PolarsResult<subxet::data::streaming::XetWriter> {
        self.client.write(None).await.map_err(to_compute_err)
    }

    /// Convenience: upload a complete byte buffer and return file info.
    ///
    /// For streaming use, prefer `new_writer()` and write incrementally.
    pub async fn upload_bytes(&self, data: Bytes) -> PolarsResult<subxet::data::XetFileInfo> {
        let mut writer = self.new_writer().await?;
        writer.write(data).await.map_err(to_compute_err)?;
        writer.close().await.map_err(to_compute_err)
    }
}
