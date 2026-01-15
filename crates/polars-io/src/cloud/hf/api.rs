//! Shared HF Hub API types and utilities.
//!
//! Provides common types for interacting with the HF Hub Tree API,
//! used by both glob expansion (read) and mode handling (write).

use polars_error::{PolarsResult, to_compute_err};

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
    pub async fn next(&mut self) -> Option<PolarsResult<bytes::Bytes>> {
        let uri = self.uri.take()?;

        Some(
            async {
                let resp = with_concurrency_budget(1, || async {
                    self.client.get(uri).send().await.map_err(to_compute_err)
                })
                .await?;

                self.uri = resp
                    .headers()
                    .get("link")
                    .and_then(|x| Self::find_link(x.as_bytes(), "next".as_bytes()))
                    .transpose()?;

                let resp_bytes = resp.bytes().await.map_err(to_compute_err)?;

                Ok(resp_bytes)
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
pub(crate) async fn list_existing_files(
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

    while let Some(bytes) = gp.next().await {
        let bytes = bytes?;
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
