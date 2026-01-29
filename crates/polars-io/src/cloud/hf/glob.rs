//! HF Hub glob expansion for reading files.

use std::borrow::Cow;

use polars_error::{PolarsResult, to_compute_err};
use polars_utils::pl_path::PlRefPath;

use super::api::{GetPages, HFAPIResponse};
use super::url::{HFPathParts, HFRepoLocation};
use crate::cloud::{
    CloudConfig, CloudOptions, Matcher, USER_AGENT, extract_prefix_expansion,
    try_build_http_header_map_from_items_slice,
};
use crate::path_utils::HiveIdxTracker;
use crate::pl_async::with_concurrency_budget;
use crate::utils::decode_json_response;

pub async fn expand_paths_hf(
    paths: &[PlRefPath],
    check_directory_level: bool,
    cloud_options: &Option<CloudOptions>,
    glob: bool,
    api_base_url: Option<&str>,
) -> PolarsResult<(usize, Vec<PlRefPath>)> {
    assert!(!paths.is_empty());

    // Allow http for testing with mock servers
    let https_only = api_base_url
        .map(|url| url.starts_with("https://"))
        .unwrap_or(true);

    let client = reqwest::ClientBuilder::new()
        .user_agent(USER_AGENT)
        .http1_only()
        .https_only(https_only);

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
            api_base_url,
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

        while let Some(result) = gp.next().await {
            let Some(bytes) = result? else {
                // 404 - path doesn't exist, break out of the loop
                break;
            };
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

// Tests for GetPages::find_link moved to api.rs
