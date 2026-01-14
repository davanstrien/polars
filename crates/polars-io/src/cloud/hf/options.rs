//! HF Hub sink options and configuration.
//!
//! Provides configuration types for writing datasets to Hugging Face Hub.

use std::path::PathBuf;

use polars_error::{PolarsResult, polars_bail};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Default shard size: 500MB
const DEFAULT_MAX_SHARD_SIZE: usize = 500 * 1024 * 1024;
/// Default upload concurrency
const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;

/// Type of Hugging Face repository.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum RepoType {
    /// Dataset repository (default)
    #[default]
    Dataset,
    /// Model repository
    Model,
    /// Space repository
    Space,
}

impl RepoType {
    /// Returns the URL path segment for this repo type.
    ///
    /// # Examples
    /// ```ignore
    /// assert_eq!(RepoType::Dataset.as_str(), "datasets");
    /// assert_eq!(RepoType::Model.as_str(), "models");
    /// assert_eq!(RepoType::Space.as_str(), "spaces");
    /// ```
    pub fn as_str(&self) -> &'static str {
        match self {
            RepoType::Dataset => "datasets",
            RepoType::Model => "models",
            RepoType::Space => "spaces",
        }
    }

    /// Parse from bucket string (as used in hf:// URLs).
    ///
    /// # Examples
    /// ```ignore
    /// assert_eq!(RepoType::from_bucket_str("datasets"), Some(RepoType::Dataset));
    /// assert_eq!(RepoType::from_bucket_str("invalid"), None);
    /// ```
    pub fn from_bucket_str(s: &str) -> Option<Self> {
        match s {
            "datasets" => Some(Self::Dataset),
            "models" => Some(Self::Model),
            "spaces" => Some(Self::Space),
            _ => None,
        }
    }
}

impl std::fmt::Display for RepoType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Write mode for HF Hub uploads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum HfWriteMode {
    /// Error if files already exist at the target path (default)
    #[default]
    ErrorIfExists,
    /// Delete existing files at the target path before writing
    Overwrite,
    /// Append new shards to existing files, renumbering as needed
    Append,
}

/// Configuration options for writing to Hugging Face Hub.
///
/// Use [`HfSinkOptions::builder`] for ergonomic construction.
///
/// # Example
/// ```ignore
/// let options = HfSinkOptions::builder("username/my-dataset")
///     .with_path_in_repo("data/train")
///     .with_split("train")
///     .with_max_shard_size(100 * 1024 * 1024) // 100MB shards
///     .with_mode(HfWriteMode::Overwrite)
///     .build()?;
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HfSinkOptions {
    /// Repository ID in format "user/repo" or "org/repo"
    pub repo_id: String,
    /// Type of repository (Dataset, Model, Space)
    pub repo_type: RepoType,
    /// Git revision/branch (defaults to "main" if None)
    pub revision: Option<String>,
    /// Path within the repository (e.g., "data/train")
    pub path_in_repo: String,
    /// Dataset split name for file naming (e.g., "train", "test")
    pub split: String,
    /// Maximum size in bytes per shard file (default: 500MB)
    pub max_shard_size: usize,
    /// Maximum number of rows per shard (optional)
    pub max_shard_rows: Option<usize>,
    /// Fixed number of shards (optional, overrides size-based sharding)
    pub num_shards: Option<usize>,
    /// Write mode (ErrorIfExists, Overwrite, Append)
    pub mode: HfWriteMode,
    /// Explicit HF token (falls back to env vars / token file if None)
    pub token: Option<String>,
    /// Custom commit message
    pub commit_message: Option<String>,
    /// Create a pull request instead of committing directly
    pub create_pr: bool,
    /// Path to checkpoint file for resumable uploads
    pub checkpoint_path: Option<PathBuf>,
    /// Number of concurrent uploads (default: 4)
    pub upload_concurrency: usize,
}

impl Default for HfSinkOptions {
    fn default() -> Self {
        Self {
            repo_id: String::new(),
            repo_type: RepoType::default(),
            revision: None,
            path_in_repo: String::new(),
            split: "train".to_string(),
            max_shard_size: DEFAULT_MAX_SHARD_SIZE,
            max_shard_rows: None,
            num_shards: None,
            mode: HfWriteMode::default(),
            token: None,
            commit_message: None,
            create_pr: false,
            checkpoint_path: None,
            upload_concurrency: DEFAULT_UPLOAD_CONCURRENCY,
        }
    }
}

impl HfSinkOptions {
    /// Create a new builder for HfSinkOptions.
    ///
    /// # Arguments
    /// * `repo_id` - Repository ID in format "user/repo" or "org/repo"
    pub fn builder(repo_id: impl Into<String>) -> HfSinkOptionsBuilder {
        HfSinkOptionsBuilder::new(repo_id)
    }

    /// Validate the options configuration.
    ///
    /// # Errors
    /// Returns an error if:
    /// - `repo_id` is empty or not in "user/repo" format
    /// - `path_in_repo` starts with "/"
    /// - `max_shard_size` is 0
    /// - `upload_concurrency` is 0
    pub fn validate(&self) -> PolarsResult<()> {
        // Validate repo_id format
        if self.repo_id.is_empty() {
            polars_bail!(InvalidOperation: "repo_id cannot be empty");
        }

        let slash_count = self.repo_id.chars().filter(|&c| c == '/').count();
        if slash_count != 1 {
            polars_bail!(
                InvalidOperation: "repo_id must be in format 'user/repo' or 'org/repo', got '{}'",
                self.repo_id
            );
        }

        // Validate path_in_repo
        if self.path_in_repo.starts_with('/') {
            polars_bail!(
                InvalidOperation: "path_in_repo must not start with '/', got '{}'",
                self.path_in_repo
            );
        }

        // Validate sizes
        if self.max_shard_size == 0 {
            polars_bail!(InvalidOperation: "max_shard_size must be greater than 0");
        }

        if self.upload_concurrency == 0 {
            polars_bail!(InvalidOperation: "upload_concurrency must be greater than 0");
        }

        Ok(())
    }

    /// Get the effective revision (defaults to "main").
    pub fn effective_revision(&self) -> &str {
        self.revision.as_deref().unwrap_or("main")
    }
}

/// Builder for [`HfSinkOptions`].
#[derive(Debug, Clone)]
pub struct HfSinkOptionsBuilder {
    options: HfSinkOptions,
}

impl HfSinkOptionsBuilder {
    /// Create a new builder with the given repository ID.
    pub fn new(repo_id: impl Into<String>) -> Self {
        Self {
            options: HfSinkOptions {
                repo_id: repo_id.into(),
                ..Default::default()
            },
        }
    }

    /// Set the repository type.
    pub fn with_repo_type(mut self, repo_type: RepoType) -> Self {
        self.options.repo_type = repo_type;
        self
    }

    /// Set the git revision/branch.
    pub fn with_revision(mut self, revision: impl Into<String>) -> Self {
        self.options.revision = Some(revision.into());
        self
    }

    /// Set the path within the repository.
    pub fn with_path_in_repo(mut self, path: impl Into<String>) -> Self {
        self.options.path_in_repo = path.into();
        self
    }

    /// Set the dataset split name.
    pub fn with_split(mut self, split: impl Into<String>) -> Self {
        self.options.split = split.into();
        self
    }

    /// Set the maximum shard size in bytes.
    pub fn with_max_shard_size(mut self, bytes: usize) -> Self {
        self.options.max_shard_size = bytes;
        self
    }

    /// Set the maximum number of rows per shard.
    pub fn with_max_shard_rows(mut self, rows: usize) -> Self {
        self.options.max_shard_rows = Some(rows);
        self
    }

    /// Set a fixed number of shards.
    pub fn with_num_shards(mut self, num: usize) -> Self {
        self.options.num_shards = Some(num);
        self
    }

    /// Set the write mode.
    pub fn with_mode(mut self, mode: HfWriteMode) -> Self {
        self.options.mode = mode;
        self
    }

    /// Set an explicit HF token.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.options.token = Some(token.into());
        self
    }

    /// Set a custom commit message.
    pub fn with_commit_message(mut self, msg: impl Into<String>) -> Self {
        self.options.commit_message = Some(msg.into());
        self
    }

    /// Set whether to create a pull request.
    pub fn with_create_pr(mut self, create_pr: bool) -> Self {
        self.options.create_pr = create_pr;
        self
    }

    /// Set the checkpoint path for resumable uploads.
    pub fn with_checkpoint_path(mut self, path: PathBuf) -> Self {
        self.options.checkpoint_path = Some(path);
        self
    }

    /// Set the upload concurrency.
    pub fn with_upload_concurrency(mut self, concurrency: usize) -> Self {
        self.options.upload_concurrency = concurrency;
        self
    }

    /// Build the options, validating the configuration.
    ///
    /// # Errors
    /// Returns an error if validation fails.
    pub fn build(self) -> PolarsResult<HfSinkOptions> {
        self.options.validate()?;
        Ok(self.options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repo_type_as_str() {
        assert_eq!(RepoType::Dataset.as_str(), "datasets");
        assert_eq!(RepoType::Model.as_str(), "models");
        assert_eq!(RepoType::Space.as_str(), "spaces");
    }

    #[test]
    fn test_repo_type_from_bucket_str() {
        assert_eq!(
            RepoType::from_bucket_str("datasets"),
            Some(RepoType::Dataset)
        );
        assert_eq!(RepoType::from_bucket_str("models"), Some(RepoType::Model));
        assert_eq!(RepoType::from_bucket_str("spaces"), Some(RepoType::Space));
        assert_eq!(RepoType::from_bucket_str("invalid"), None);
        assert_eq!(RepoType::from_bucket_str(""), None);
    }

    #[test]
    fn test_repo_type_default() {
        assert_eq!(RepoType::default(), RepoType::Dataset);
    }

    #[test]
    fn test_write_mode_default() {
        assert_eq!(HfWriteMode::default(), HfWriteMode::ErrorIfExists);
    }

    #[test]
    fn test_default_options() {
        let opts = HfSinkOptions::default();
        assert_eq!(opts.split, "train");
        assert_eq!(opts.max_shard_size, 500 * 1024 * 1024);
        assert_eq!(opts.upload_concurrency, 4);
        assert!(!opts.create_pr);
    }

    #[test]
    fn test_builder_minimal() {
        let opts = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .build()
            .unwrap();

        assert_eq!(opts.repo_id, "user/repo");
        assert_eq!(opts.path_in_repo, "data");
        assert_eq!(opts.repo_type, RepoType::Dataset);
        assert_eq!(opts.split, "train");
    }

    #[test]
    fn test_builder_full() {
        let opts = HfSinkOptions::builder("org/my-dataset")
            .with_repo_type(RepoType::Dataset)
            .with_revision("dev")
            .with_path_in_repo("data/processed")
            .with_split("validation")
            .with_max_shard_size(100 * 1024 * 1024)
            .with_max_shard_rows(10000)
            .with_num_shards(5)
            .with_mode(HfWriteMode::Overwrite)
            .with_token("hf_xxxx")
            .with_commit_message("Add validation split")
            .with_create_pr(true)
            .with_checkpoint_path(PathBuf::from("/tmp/checkpoint.json"))
            .with_upload_concurrency(8)
            .build()
            .unwrap();

        assert_eq!(opts.repo_id, "org/my-dataset");
        assert_eq!(opts.repo_type, RepoType::Dataset);
        assert_eq!(opts.revision, Some("dev".to_string()));
        assert_eq!(opts.path_in_repo, "data/processed");
        assert_eq!(opts.split, "validation");
        assert_eq!(opts.max_shard_size, 100 * 1024 * 1024);
        assert_eq!(opts.max_shard_rows, Some(10000));
        assert_eq!(opts.num_shards, Some(5));
        assert_eq!(opts.mode, HfWriteMode::Overwrite);
        assert_eq!(opts.token, Some("hf_xxxx".to_string()));
        assert_eq!(
            opts.commit_message,
            Some("Add validation split".to_string())
        );
        assert!(opts.create_pr);
        assert_eq!(
            opts.checkpoint_path,
            Some(PathBuf::from("/tmp/checkpoint.json"))
        );
        assert_eq!(opts.upload_concurrency, 8);
    }

    #[test]
    fn test_validation_empty_repo_id() {
        let result = HfSinkOptions::builder("").with_path_in_repo("data").build();

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("repo_id cannot be empty"));
    }

    #[test]
    fn test_validation_invalid_repo_id_no_slash() {
        let result = HfSinkOptions::builder("myrepo")
            .with_path_in_repo("data")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("user/repo"));
    }

    #[test]
    fn test_validation_invalid_repo_id_multiple_slashes() {
        let result = HfSinkOptions::builder("user/org/repo")
            .with_path_in_repo("data")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("user/repo"));
    }

    #[test]
    fn test_validation_path_starts_with_slash() {
        let result = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("/data")
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("must not start with '/'"));
    }

    #[test]
    fn test_validation_zero_shard_size() {
        let result = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .with_max_shard_size(0)
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_shard_size must be greater than 0"));
    }

    #[test]
    fn test_validation_zero_concurrency() {
        let result = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .with_upload_concurrency(0)
            .build();

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("upload_concurrency must be greater than 0"));
    }

    #[test]
    fn test_effective_revision() {
        let opts1 = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .build()
            .unwrap();
        assert_eq!(opts1.effective_revision(), "main");

        let opts2 = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .with_revision("dev")
            .build()
            .unwrap();
        assert_eq!(opts2.effective_revision(), "dev");
    }

    #[cfg(feature = "serde")]
    #[test]
    fn test_serde_roundtrip() {
        let original = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .with_split("test")
            .with_mode(HfWriteMode::Append)
            .build()
            .unwrap();

        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HfSinkOptions = serde_json::from_str(&json).unwrap();

        assert_eq!(original, deserialized);
    }
}
