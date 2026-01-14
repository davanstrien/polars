//! HF Hub URL parsing and construction.

use polars_error::{PolarsResult, polars_bail};

use crate::utils::URL_ENCODE_CHARSET;

/// Percent-encoding character set for HF Hub paths.
///
/// This is URL_ENCODE_CHARSET with slashes preserved - by not encoding slashes,
/// the API request will be counted under a higher "resolvers" ratelimit of (3000/5min)
/// compared to the default "pages" limit of (100/5min limit).
///
/// ref <https://github.com/pola-rs/polars/issues/25389>
pub(crate) const HF_PATH_ENCODE_CHARSET: &percent_encoding::AsciiSet =
    &URL_ENCODE_CHARSET.remove(b'/');

#[derive(Debug, PartialEq)]
pub(crate) struct HFPathParts {
    pub bucket: String,
    pub repository: String,
    pub revision: String,
    /// Path relative to the repository root.
    pub path: String,
}

pub(crate) struct HFRepoLocation {
    /// Repository bucket type: "datasets", "spaces", or "models"
    bucket: String,
    /// Repository ID: "user/repo" or "org/repo"
    repository: String,
    /// Git revision (unencoded)
    revision: String,
    pub api_base_path: String,
    pub download_base_path: String,
}

impl HFRepoLocation {
    pub fn new(bucket: &str, repository: &str, revision: &str) -> Self {
        // * Don't percent-encode bucket/repository - they are path segments where
        //   slashes are separators. E.g. "HuggingFaceFW/fineweb-2" must stay as-is.
        // * DO encode revision - slashes in revisions like "refs/convert/parquet"
        //   are part of the revision name, not path separators.
        //   See: https://github.com/pola-rs/polars/issues/25389
        let encoded_revision =
            percent_encoding::percent_encode(revision.as_bytes(), URL_ENCODE_CHARSET);
        let api_base_path = format!(
            "https://huggingface.co/api/{}/{}/tree/{}/",
            bucket, repository, encoded_revision
        );
        let download_base_path = format!(
            "https://huggingface.co/{}/{}/resolve/{}/",
            bucket, repository, encoded_revision
        );

        Self {
            bucket: bucket.to_string(),
            repository: repository.to_string(),
            revision: revision.to_string(),
            api_base_path,
            download_base_path,
        }
    }

    pub fn get_file_uri(&self, rel_path: &str) -> String {
        format!(
            "{}{}",
            self.download_base_path,
            percent_encoding::percent_encode(rel_path.as_bytes(), HF_PATH_ENCODE_CHARSET)
        )
    }

    pub fn get_api_uri(&self, rel_path: &str) -> String {
        format!(
            "{}{}",
            self.api_base_path,
            percent_encoding::percent_encode(rel_path.as_bytes(), HF_PATH_ENCODE_CHARSET)
        )
    }

    /// Returns URL for LFS batch API (upload coordination).
    ///
    /// Used to request presigned upload URLs before uploading files to HF Hub.
    /// POST https://huggingface.co/{bucket}/{repo}.git/info/lfs/objects/batch
    #[cfg(feature = "hf_sink")]
    pub fn get_lfs_batch_uri(&self) -> String {
        format!(
            "https://huggingface.co/{}/{}.git/info/lfs/objects/batch",
            self.bucket, self.repository
        )
    }

    /// Returns URL for Commit API (atomic commit).
    ///
    /// Used to atomically commit uploaded files to the repository.
    /// POST https://huggingface.co/api/{bucket}/{repo}/commit/{revision}
    #[cfg(feature = "hf_sink")]
    pub fn get_commit_uri(&self) -> String {
        let encoded_revision =
            percent_encoding::percent_encode(self.revision.as_bytes(), URL_ENCODE_CHARSET);
        format!(
            "https://huggingface.co/api/{}/{}/commit/{}",
            self.bucket, self.repository, encoded_revision
        )
    }

    /// Returns URL for completing a multipart LFS upload.
    ///
    /// After uploading all parts to S3, this endpoint finalizes the multipart upload.
    /// POST https://huggingface.co/{bucket}/{repo}.git/info/lfs/objects/{sha256}/finalize
    #[cfg(feature = "hf_sink")]
    pub fn get_lfs_multipart_complete_uri(&self, sha256: &str) -> String {
        format!(
            "https://huggingface.co/{}/{}.git/info/lfs/objects/{}/finalize",
            self.bucket, self.repository, sha256
        )
    }
}

impl HFPathParts {
    /// Extracts path components from a hugging face path:
    /// `hf:// [datasets | spaces] / {username} / {reponame} @ {revision} / {path from root}`
    pub fn try_from_uri(uri: &str) -> PolarsResult<Self> {
        let Some(this) = (|| {
            // hf:// [datasets | spaces] / {username} / {reponame} @ {revision} / {path from root}
            //       !>
            if !uri.starts_with("hf://") {
                return None;
            }
            let uri = &uri[5..];

            // [datasets | spaces] / {username} / {reponame} @ {revision} / {path from root}
            // ^-----------------^   !>
            let i = memchr::memchr(b'/', uri.as_bytes())?;
            let bucket = uri.get(..i)?.to_string();
            let uri = uri.get(1 + i..)?;

            // {username} / {reponame} @ {revision} / {path from root}
            // ^----------------------------------^   !>
            let i = memchr::memchr(b'/', uri.as_bytes())?;
            let i = {
                // Also handle if they just give the repository, i.e.:
                // hf:// [datasets | spaces] / {username} / {reponame} @ {revision}
                let uri = uri.get(1 + i..)?;
                if uri.is_empty() {
                    return None;
                }
                1 + i + memchr::memchr(b'/', uri.as_bytes()).unwrap_or(uri.len())
            };
            let repository = uri.get(..i)?;
            let uri = uri.get(1 + i..).unwrap_or("");

            let (repository, revision) =
                if let Some(i) = memchr::memchr(b'@', repository.as_bytes()) {
                    (repository[..i].to_string(), repository[1 + i..].to_string())
                } else {
                    // No @revision in uri, default to `main`
                    (repository.to_string(), "main".to_string())
                };

            // {path from root}
            // ^--------------^
            let path = uri.to_string();

            Some(HFPathParts {
                bucket,
                repository,
                revision,
                path,
            })
        })() else {
            polars_bail!(ComputeError: "invalid Hugging Face path: {}", uri);
        };

        const BUCKETS: [&str; 2] = ["datasets", "spaces"];
        if !BUCKETS.contains(&this.bucket.as_str()) {
            polars_bail!(ComputeError: "hugging face uri bucket must be one of {:?}, got {} instead.", BUCKETS, this.bucket);
        }

        Ok(this)
    }

    /// Returns the RepoType for this path.
    ///
    /// Converts the bucket string ("datasets", "spaces") to the corresponding RepoType enum.
    #[cfg(feature = "hf_sink")]
    pub fn repo_type(&self) -> super::options::RepoType {
        super::options::RepoType::from_bucket_str(&self.bucket)
            .expect("bucket validated in try_from_uri")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hf_path_from_uri() {
        let uri = "hf://datasets/pola-rs/polars/README.md";
        let expect = HFPathParts {
            bucket: "datasets".into(),
            repository: "pola-rs/polars".into(),
            revision: "main".into(),
            path: "README.md".into(),
        };

        assert_eq!(HFPathParts::try_from_uri(uri).unwrap(), expect);

        let uri = "hf://spaces/pola-rs/polars@~parquet/";
        let expect = HFPathParts {
            bucket: "spaces".into(),
            repository: "pola-rs/polars".into(),
            revision: "~parquet".into(),
            path: "".into(),
        };

        assert_eq!(HFPathParts::try_from_uri(uri).unwrap(), expect);

        let uri = "hf://spaces/pola-rs/polars@~parquet";
        let expect = HFPathParts {
            bucket: "spaces".into(),
            repository: "pola-rs/polars".into(),
            revision: "~parquet".into(),
            path: "".into(),
        };

        assert_eq!(HFPathParts::try_from_uri(uri).unwrap(), expect);

        for uri in [
            "://",
            "s3://",
            "https://",
            "hf://",
            "hf:///",
            "hf:////",
            "hf://datasets/a",
            "hf://datasets/a/",
            "hf://bucket/a/b/c", // Invalid bucket name
        ] {
            let out = HFPathParts::try_from_uri(uri);
            if out.is_err() {
                continue;
            }
            panic!("expected err result for uri {uri} instead of {out:?}");
        }
    }

    #[test]
    fn test_hf_url_encoding() {
        // Verify URLs preserve slashes (don't encode as %2F) but encode special chars.
        // Slashes must remain for correct rate limit classification by HF Hub.
        // Special chars (spaces, colons) must be encoded for file downloads to work.
        // See: https://github.com/pola-rs/polars/issues/25389

        let loc = HFRepoLocation::new("datasets", "HuggingFaceFW/fineweb-2", "main");

        // Check base paths don't encode slashes
        assert_eq!(
            loc.api_base_path,
            "https://huggingface.co/api/datasets/HuggingFaceFW/fineweb-2/tree/main/"
        );
        assert_eq!(
            loc.download_base_path,
            "https://huggingface.co/datasets/HuggingFaceFW/fineweb-2/resolve/main/"
        );

        // Check file URIs preserve slashes in paths
        let file_uri = loc.get_file_uri("data/aai_Latn/train/000_00000.parquet");
        assert_eq!(
            file_uri,
            "https://huggingface.co/datasets/HuggingFaceFW/fineweb-2/resolve/main/data/aai_Latn/train/000_00000.parquet"
        );

        // Check that special characters ARE encoded (spaces -> %20, colons -> %3A)
        // This is needed for hive-partitioned paths like "date2=2023-01-01 00:00:00.000000"
        let file_uri = loc.get_file_uri(
            "hive_dates/date1=2024-01-01/date2=2023-01-01 00:00:00.000000/00000000.parquet",
        );
        assert_eq!(
            file_uri,
            "https://huggingface.co/datasets/HuggingFaceFW/fineweb-2/resolve/main/hive_dates/date1%3D2024-01-01/date2%3D2023-01-01%2000%3A00%3A00.000000/00000000.parquet"
        );

        // Check that brackets are encoded ([ -> %5B, ] -> %5D)
        let file_uri = loc.get_file_uri("special-chars/[*.parquet");
        assert_eq!(
            file_uri,
            "https://huggingface.co/datasets/HuggingFaceFW/fineweb-2/resolve/main/special-chars/%5B%2A.parquet"
        );

        // Check that revision slashes ARE encoded (they're part of the revision name)
        // e.g. "refs/convert/parquet" -> "refs%2Fconvert%2Fparquet"
        let loc = HFRepoLocation::new("datasets", "user/repo", "refs/convert/parquet");
        assert_eq!(
            loc.api_base_path,
            "https://huggingface.co/api/datasets/user/repo/tree/refs%2Fconvert%2Fparquet/"
        );
        assert_eq!(
            loc.download_base_path,
            "https://huggingface.co/datasets/user/repo/resolve/refs%2Fconvert%2Fparquet/"
        );
    }

    #[cfg(feature = "hf_sink")]
    #[test]
    fn test_get_lfs_batch_uri() {
        let loc = HFRepoLocation::new("datasets", "user/repo", "main");
        assert_eq!(
            loc.get_lfs_batch_uri(),
            "https://huggingface.co/datasets/user/repo.git/info/lfs/objects/batch"
        );

        // Spaces should also work
        let loc = HFRepoLocation::new("spaces", "org/my-space", "main");
        assert_eq!(
            loc.get_lfs_batch_uri(),
            "https://huggingface.co/spaces/org/my-space.git/info/lfs/objects/batch"
        );
    }

    #[cfg(feature = "hf_sink")]
    #[test]
    fn test_get_commit_uri() {
        let loc = HFRepoLocation::new("datasets", "user/repo", "main");
        assert_eq!(
            loc.get_commit_uri(),
            "https://huggingface.co/api/datasets/user/repo/commit/main"
        );
    }

    #[cfg(feature = "hf_sink")]
    #[test]
    fn test_get_commit_uri_encodes_revision() {
        // Revision with slashes should be percent-encoded
        let loc = HFRepoLocation::new("datasets", "user/repo", "refs/convert/parquet");
        assert_eq!(
            loc.get_commit_uri(),
            "https://huggingface.co/api/datasets/user/repo/commit/refs%2Fconvert%2Fparquet"
        );
    }

    #[cfg(feature = "hf_sink")]
    #[test]
    fn test_hfpathparts_repo_type() {
        use super::super::options::RepoType;

        let parts = HFPathParts::try_from_uri("hf://datasets/user/repo/file.parquet").unwrap();
        assert_eq!(parts.repo_type(), RepoType::Dataset);

        let parts = HFPathParts::try_from_uri("hf://spaces/org/my-space/app.py").unwrap();
        assert_eq!(parts.repo_type(), RepoType::Space);
    }

    #[cfg(feature = "hf_sink")]
    #[test]
    fn test_get_lfs_multipart_complete_uri() {
        let loc = HFRepoLocation::new("datasets", "user/repo", "main");
        assert_eq!(
            loc.get_lfs_multipart_complete_uri("abc123def456"),
            "https://huggingface.co/datasets/user/repo.git/info/lfs/objects/abc123def456/finalize"
        );
    }
}
