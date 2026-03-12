//! XET upload path — token fetch, session creation, and token refresh.
//!
//! Uses the `xet-session` crate for the high-level upload API.

use std::sync::Arc;

use polars_error::{PolarsResult, polars_bail, to_compute_err};
use reqwest::Client;
use serde::Deserialize;
use xet_client::cas_client::auth::TokenRefresher;
use xet_client::cas_client::auth::AuthError;

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
            "HF bucket XET write token request failed for '{}/{}' (HTTP {}): {}",
            config.namespace,
            config.bucket_name,
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
pub(crate) struct HfTokenRefresher {
    pub(crate) http: Client,
    pub(crate) config: HfBucketConfig,
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

/// Create an [`XetSession`] from a write token, with an optional token refresher
/// for long-running uploads.
pub fn create_xet_session(
    token: &XetToken,
    token_refresher: Option<Arc<dyn TokenRefresher>>,
) -> PolarsResult<xet::xet_session::XetSession> {
    let mut builder = xet::xet_session::XetSessionBuilder::new()
        .with_endpoint(token.cas_url.clone())
        .with_token_info(token.access_token.clone(), token.exp);
    if let Some(refresher) = token_refresher {
        builder = builder.with_token_refresher(refresher);
    }
    builder.build().map_err(to_compute_err)
}
