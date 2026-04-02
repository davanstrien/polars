//! Hugging Face cloud storage support via OpenDAL.
//!
//! Provides an [`ObjectStore`] implementation for `hf://` URLs by bridging
//! OpenDAL's HF backend through `object_store_opendal`.
//!
//! Gated behind `#[cfg(feature = "hf")]`.

use std::sync::Arc;

use object_store::ObjectStore;
use polars_error::{PolarsResult, polars_bail, polars_err, to_compute_err};
use polars_utils::pl_path::PlRefPath;

use super::options::CloudOptions;

/// Parse an `hf://` URL and build an [`ObjectStore`] backed by OpenDAL.
///
/// Supported URL formats:
/// - `hf://buckets/<namespace>/<name>[/<path>]`
/// - `hf://datasets/<namespace>/<name>[/<path>]`
/// - `hf://models/<namespace>/<name>[/<path>]`
pub fn build_hf(
    url: PlRefPath,
    options: Option<&CloudOptions>,
) -> PolarsResult<Arc<dyn ObjectStore>> {
    let after_scheme = url.strip_scheme();
    let (repo_type_plural, rest) = after_scheme
        .split_once('/')
        .ok_or_else(|| polars_err!(ComputeError: "invalid hf:// URL: {}", url.as_str()))?;

    // hf:// URLs use plural form ("buckets", "datasets", "models")
    // but OpenDAL expects singular ("bucket", "dataset", "model")
    let repo_type: &str = repo_type_plural
        .strip_suffix('s')
        .unwrap_or(repo_type_plural);

    // Extract repo_id (namespace/name) from the remaining path
    let parts = rest.splitn(3, '/').collect::<Vec<&str>>();
    if parts.len() < 2 || parts[0].is_empty() || parts[1].is_empty() {
        polars_bail!(
            ComputeError:
            "invalid hf:// URL: expected hf://<type>/<namespace>/<name>[/path], got: {}",
            url.as_str()
        );
    }
    let repo_id = format!("{}/{}", parts[0], parts[1]);

    let token = extract_hf_token(options)?;

    let builder = opendal::services::Hf::default()
        .repo_type(repo_type)
        .repo_id(&repo_id)
        .token(&token);

    let op = opendal::Operator::new(builder)
        .map_err(to_compute_err)?
        .finish();

    Ok(Arc::new(object_store_opendal::OpendalStore::new(op)) as Arc<dyn ObjectStore>)
}

/// Extract an HF token from cloud options, environment, or cached file.
///
/// Resolution order:
/// 1. `storage_options` / CloudOptions HTTP Authorization header
/// 2. `HF_TOKEN` environment variable
/// 3. Cached token at `$HF_HOME/token` (default: `~/.cache/huggingface/token`)
fn extract_hf_token(cloud_options: Option<&CloudOptions>) -> PolarsResult<String> {
    #[cfg(feature = "http")]
    if let Some(opts) = cloud_options {
        if let Some(super::options::CloudConfig::Http { headers }) = &opts.config {
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

    if let Ok(token) = std::env::var("HF_TOKEN") {
        if !token.is_empty() {
            return Ok(token);
        }
    }

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
        ComputeError:
        "no HF token found: set HF_TOKEN env var, pass via storage_options, \
         or login with `huggingface-cli login`"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_from_env() {
        let original = std::env::var("HF_TOKEN").ok();
        std::env::set_var("HF_TOKEN", "hf_test_token_123");

        let result = extract_hf_token(None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hf_test_token_123");

        match original {
            Some(v) => std::env::set_var("HF_TOKEN", v),
            None => std::env::remove_var("HF_TOKEN"),
        }
    }

    #[test]
    fn test_empty_token_skipped() {
        let original = std::env::var("HF_TOKEN").ok();
        std::env::set_var("HF_TOKEN", "");

        let result = extract_hf_token(None);
        if let Ok(token) = &result {
            assert!(!token.is_empty());
        }

        match original {
            Some(v) => std::env::set_var("HF_TOKEN", v),
            None => std::env::remove_var("HF_TOKEN"),
        }
    }

    #[test]
    fn test_build_hf_valid_bucket_url() {
        std::env::set_var("HF_TOKEN", "hf_test");
        let url = PlRefPath::new("hf://buckets/myorg/mybucket/path/file.parquet");
        let result = build_hf(url, None);
        // Builder succeeds (actual I/O would fail without a real token,
        // but the ObjectStore is constructed)
        assert!(result.is_ok());
        std::env::remove_var("HF_TOKEN");
    }

    #[test]
    fn test_build_hf_valid_dataset_url() {
        std::env::set_var("HF_TOKEN", "hf_test");
        let url = PlRefPath::new("hf://datasets/user/dataset-name/train.parquet");
        let result = build_hf(url, None);
        assert!(result.is_ok());
        std::env::remove_var("HF_TOKEN");
    }

    #[test]
    fn test_build_hf_invalid_url_no_repo() {
        std::env::set_var("HF_TOKEN", "hf_test");
        let url = PlRefPath::new("hf://buckets/only-namespace");
        let result = build_hf(url, None);
        assert!(result.is_err());
        std::env::remove_var("HF_TOKEN");
    }
}
