//! HF Hub glob expansion for reading files.

use std::borrow::Cow;

use polars_error::{PolarsResult, to_compute_err};
use polars_utils::pl_path::PlRefPath;

use super::url::{HFPathParts, HFRepoLocation};
use crate::cloud::{
    CloudConfig, CloudOptions, Matcher, USER_AGENT, extract_prefix_expansion,
    try_build_http_header_map_from_items_slice,
};
use crate::path_utils::HiveIdxTracker;
use crate::pl_async::with_concurrency_budget;
use crate::utils::decode_json_response;

#[derive(Debug, serde::Deserialize)]
struct HFAPIResponse {
    #[serde(rename = "type")]
    type_: String,
    path: String,
    size: u64,
}

impl HFAPIResponse {
    fn is_file(&self) -> bool {
        self.type_ == "file"
    }
}

/// API response is paginated with a `link` header.
/// * https://huggingface.co/docs/hub/en/api#get-apidatasets
/// * https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2022-11-28#using-link-headers
struct GetPages<'a> {
    client: &'a reqwest::Client,
    uri: Option<String>,
}

impl GetPages<'_> {
    async fn next(&mut self) -> Option<PolarsResult<bytes::Bytes>> {
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

    fn find_link(mut link: &[u8], rel: &[u8]) -> Option<PolarsResult<String>> {
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

pub async fn expand_paths_hf(
    paths: &[PlRefPath],
    check_directory_level: bool,
    cloud_options: &Option<CloudOptions>,
    glob: bool,
) -> PolarsResult<(usize, Vec<PlRefPath>)> {
    assert!(!paths.is_empty());

    let client = reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .http1_only()
        .https_only(true);

    let client = if let Some(CloudOptions {
        config: Some(CloudConfig::Http { headers }),
        ..
    }) = cloud_options
    {
        client.default_headers(try_build_http_header_map_from_items_slice(
            headers.as_slice(),
        )?)
    } else {
        client
    };

    let client = &client.build().unwrap();

    let mut out_paths = vec![];
    let mut hive_idx_tracker = HiveIdxTracker {
        idx: usize::MAX,
        paths,
        check_directory_level,
    };

    for (path_idx, path) in paths.iter().enumerate() {
        let path_parts = &HFPathParts::try_from_uri(path.as_str())?;
        let repo_location = &HFRepoLocation::new(
            &path_parts.bucket,
            &path_parts.repository,
            &path_parts.revision,
        );
        let rel_path = path_parts.path.as_str();

        let (prefix, expansion) = if glob {
            extract_prefix_expansion(rel_path)?
        } else {
            (Cow::Owned(path_parts.path.clone()), None)
        };
        let expansion_matcher = &if expansion.is_some() {
            Some(Matcher::new(prefix.to_string(), expansion.as_deref())?)
        } else {
            None
        };

        let file_uri = repo_location.get_file_uri(rel_path);

        if !path_parts.path.ends_with("/") && expansion.is_none() {
            // Confirm that this is a file using a HEAD request.
            if with_concurrency_budget(1, || async {
                client.head(&file_uri).send().await.map_err(to_compute_err)
            })
            .await?
            .status()
                == 200
            {
                hive_idx_tracker.update(0, path_idx)?;
                out_paths.push(PlRefPath::new(file_uri));
                continue;
            }
        }

        hive_idx_tracker.update(file_uri.len(), path_idx)?;

        let uri = format!("{}?recursive=true", repo_location.get_api_uri(&prefix));
        let mut gp = GetPages {
            uri: Some(uri),
            client,
        };

        while let Some(bytes) = gp.next().await {
            let bytes = bytes?;
            let response: Vec<HFAPIResponse> = decode_json_response(bytes.as_ref())?;

            for entry in response {
                // Only include files with size > 0
                if entry.is_file() && entry.size > 0 {
                    // If we have a glob pattern, filter by it; otherwise include all files
                    let matches = if let Some(matcher) = expansion_matcher {
                        matcher.is_matching(entry.path.as_str())
                    } else {
                        true
                    };

                    if matches {
                        out_paths.push(PlRefPath::new(repo_location.get_file_uri(&entry.path)));
                    }
                }
            }
        }
    }

    Ok((hive_idx_tracker.idx, out_paths))
}

#[cfg(test)]
mod tests {
    use super::*;

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
