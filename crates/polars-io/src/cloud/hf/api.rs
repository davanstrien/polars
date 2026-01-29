//! Shared HF Hub API types and utilities.
//!
//! Provides common types for interacting with the HF Hub Tree API,
//! used by both glob expansion (read) and mode handling (write).

use polars_error::{PolarsResult, polars_bail, to_compute_err};

use super::url::HFRepoLocation;
use crate::pl_async::with_concurrency_budget;
use crate::utils::decode_json_response;

/// Response from HF Hub Tree API.
///
/// Represents a single entry (file or directory) from the API response.
#[derive(Debug, serde::Deserialize)]
pub struct HFAPIResponse {
    #[serde(rename = "type")]
    pub type_: String,
    pub path: String,
    pub size: u64,
}

impl HFAPIResponse {
    /// Returns true if this entry is a file (not a directory).
    pub fn is_file(&self) -> bool {
        self.type_ == "file"
    }
}

/// Paginated API response iterator using Link headers.
///
/// HF Hub API uses GitHub-style pagination with `Link` headers:
/// * https://huggingface.co/docs/hub/en/api#get-apidatasets
/// * https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2022-11-28#using-link-headers
pub struct GetPages<'a> {
    pub client: &'a reqwest::Client,
    pub uri: Option<String>,
}

impl GetPages<'_> {
    /// Fetch the next page of results, if any.
    ///
    /// Returns `None` when all pages have been consumed.
    /// Returns `Some(Ok(None))` for 404 responses (path doesn't exist).
    /// Returns `Some(Ok(Some(bytes)))` for successful responses.
    /// Returns `Some(Err(...))` for other errors.
    pub async fn next(&mut self) -> Option<PolarsResult<Option<bytes::Bytes>>> {
        let uri = self.uri.take()?;

        Some(
            async {
                let resp = with_concurrency_budget(1, || async {
                    self.client.get(uri).send().await.map_err(to_compute_err)
                })
                .await?;

                // Handle 404 - path doesn't exist, return empty
                if resp.status() == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }

                // Check for other error status codes
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    polars_bail!(ComputeError: "HF API error: {} - {}", status, body);
                }

                self.uri = resp
                    .headers()
                    .get("link")
                    .and_then(|x| Self::find_link(x.as_bytes(), "next".as_bytes()))
                    .transpose()?;

                let resp_bytes = resp.bytes().await.map_err(to_compute_err)?;

                Ok(Some(resp_bytes))
            }
            .await,
        )
    }

    /// Parse a Link header to find a specific rel type.
    ///
    /// Link header format: `<https://...>; rel="next", <https://...>; rel="last"`
    pub fn find_link(mut link: &[u8], rel: &[u8]) -> Option<PolarsResult<String>> {
        // "<https://...>; rel=\"next\", <https://...>; rel=\"last\""
        while !link.is_empty() {
            let i = memchr::memchr(b'<', link)?;
            link = link.get(1 + i..)?;
            let i = memchr::memchr(b'>', link)?;
            let uri = &link[..i];
            link = link.get(1 + i..)?;

            while !link.starts_with("rel=\"".as_bytes()) {
                link = link.get(1..)?
            }

            // rel="next"
            link = link.get(5..)?;
            let i = memchr::memchr(b'"', link)?;

            if &link[..i] == rel {
                return Some(
                    std::str::from_utf8(uri)
                        .map_err(to_compute_err)
                        .map(ToString::to_string),
                );
            }
        }

        None
    }
}

/// Information about an existing file in an HF Hub repository.
///
/// Used for mode handling (ErrorIfExists, Overwrite, Append) to
/// check what files already exist at the target path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExistingFile {
    /// Path of the file within the repository (e.g., "data/train-00000.parquet")
    pub path: String,
    /// Size of the file in bytes
    pub size: u64,
}

/// List existing files at a given path in the repository.
///
/// Used for mode handling (ErrorIfExists, Overwrite, Append) to check
/// what files already exist before committing new shards.
///
/// # Arguments
/// * `client` - HTTP client with appropriate headers (auth, user-agent)
/// * `repo_location` - Repository location (type, id, revision)
/// * `path_prefix` - Path prefix to list (e.g., "data" or "data/train")
///
/// # Returns
/// A vector of `ExistingFile` entries for all files under the path.
pub async fn list_existing_files(
    client: &reqwest::Client,
    repo_location: &HFRepoLocation,
    path_prefix: &str,
) -> PolarsResult<Vec<ExistingFile>> {
    let uri = format!("{}?recursive=true", repo_location.get_api_uri(path_prefix));
    let mut files = Vec::new();
    let mut gp = GetPages {
        uri: Some(uri),
        client,
    };

    while let Some(result) = gp.next().await {
        let Some(bytes) = result? else {
            // 404 - path doesn't exist, return empty
            return Ok(Vec::new());
        };
        let response: Vec<HFAPIResponse> = decode_json_response(bytes.as_ref())?;

        for entry in response {
            if entry.is_file() && entry.size > 0 {
                files.push(ExistingFile {
                    path: entry.path,
                    size: entry.size,
                });
            }
        }
    }

    Ok(files)
}

/// High-level helper to check existing files without requiring external HTTP client.
///
/// Creates its own HTTP client with appropriate auth headers and calls
/// `list_existing_files`. This is the preferred entry point from polars-stream
/// which doesn't have direct reqwest access.
///
/// # Arguments
/// * `repo_type` - Repository bucket type ("datasets", "models", "spaces")
/// * `repo_id` - Repository ID ("user/repo" or "org/repo")
/// * `revision` - Git revision ("main", "refs/convert/parquet", etc.)
/// * `path_prefix` - Path prefix to check (e.g., "data/train")
/// * `token` - Optional HF API token for private repos
/// * `api_base_url` - Optional custom API base URL (for testing)
pub async fn check_existing_files(
    repo_type: &str,
    repo_id: &str,
    revision: &str,
    path_prefix: &str,
    token: Option<&str>,
    api_base_url: Option<&str>,
) -> PolarsResult<Vec<ExistingFile>> {
    use crate::cloud::options::USER_AGENT;

    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = token {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
                .map_err(to_compute_err)?,
        );
    }

    // Allow http for testing with mock servers
    let https_only = api_base_url
        .map(|url| url.starts_with("https://"))
        .unwrap_or(true);

    let client = reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .http1_only()
        .https_only(https_only)
        .default_headers(headers)
        .build()
        .map_err(to_compute_err)?;

    let repo_location = HFRepoLocation::new(repo_type, repo_id, revision, api_base_url);

    list_existing_files(&client, &repo_location, path_prefix).await
}

/// Fetch the README.md file from an HF Hub repository.
///
/// Used to retrieve the existing dataset card content before updating it
/// with new split metadata during writes.
///
/// # Arguments
/// * `repo_type` - Repository bucket type ("datasets", "spaces")
/// * `repo_id` - Repository ID ("user/repo" or "org/repo")
/// * `revision` - Git revision ("main", etc.)
/// * `token` - Optional HF API token for private repos
/// * `api_base_url` - Optional custom API base URL (for testing)
///
/// # Returns
/// * `Ok(Some(content))` - README content as a string
/// * `Ok(None)` - README does not exist (404)
/// * `Err(...)` - Other errors (network, auth, etc.)
pub async fn fetch_readme(
    repo_type: &str,
    repo_id: &str,
    revision: &str,
    token: Option<&str>,
    api_base_url: Option<&str>,
) -> PolarsResult<Option<String>> {
    use crate::cloud::options::USER_AGENT;

    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(token) = token {
        headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
                .map_err(to_compute_err)?,
        );
    }

    // Allow http for testing with mock servers
    let https_only = api_base_url
        .map(|url| url.starts_with("https://"))
        .unwrap_or(true);

    let client = reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .http1_only()
        .https_only(https_only)
        .default_headers(headers)
        .build()
        .map_err(to_compute_err)?;

    let repo_location = HFRepoLocation::new(repo_type, repo_id, revision, api_base_url);
    let uri = repo_location.get_file_uri("README.md");

    let resp = with_concurrency_budget(1, || async { client.get(&uri).send().await })
        .await
        .map_err(to_compute_err)?;

    let status = resp.status();

    // Handle 404 - README doesn't exist
    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    // Handle other errors
    if !status.is_success() {
        polars_bail!(ComputeError: "Failed to fetch README.md: HTTP {}", status);
    }

    // Read content
    let content = resp.text().await.map_err(to_compute_err)?;
    Ok(Some(content))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hf_api_response_is_file() {
        let file_entry = HFAPIResponse {
            type_: "file".to_string(),
            path: "data/train.parquet".to_string(),
            size: 1024,
        };
        assert!(file_entry.is_file());

        let dir_entry = HFAPIResponse {
            type_: "directory".to_string(),
            path: "data".to_string(),
            size: 0,
        };
        assert!(!dir_entry.is_file());
    }

    #[test]
    fn test_existing_file_struct() {
        let file = ExistingFile {
            path: "data/train-00000.parquet".to_string(),
            size: 1024,
        };
        assert_eq!(file.path, "data/train-00000.parquet");
        assert_eq!(file.size, 1024);
    }

    #[test]
    fn test_existing_file_equality() {
        let file1 = ExistingFile {
            path: "data/train-00000.parquet".to_string(),
            size: 1024,
        };
        let file2 = ExistingFile {
            path: "data/train-00000.parquet".to_string(),
            size: 1024,
        };
        let file3 = ExistingFile {
            path: "data/train-00001.parquet".to_string(),
            size: 1024,
        };

        assert_eq!(file1, file2);
        assert_ne!(file1, file3);
    }

    #[test]
    fn test_get_pages_find_next_link() {
        let link = r#"<https://api.github.com/repositories/263727855/issues?page=3>; rel="next", <https://api.github.com/repositories/263727855/issues?page=7>; rel="last""#.as_bytes();

        assert_eq!(
            GetPages::find_link(link, "next".as_bytes()).map(Result::unwrap),
            Some("https://api.github.com/repositories/263727855/issues?page=3".into()),
        );

        assert_eq!(
            GetPages::find_link(link, "last".as_bytes()).map(Result::unwrap),
            Some("https://api.github.com/repositories/263727855/issues?page=7".into()),
        );

        assert_eq!(
            GetPages::find_link(link, "non-existent".as_bytes()).map(Result::unwrap),
            None,
        );
    }
}
