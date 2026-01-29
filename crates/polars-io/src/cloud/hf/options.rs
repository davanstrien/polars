//! HF Hub sink options and configuration.
//!
//! Provides configuration types for writing datasets to Hugging Face Hub.

use std::path::PathBuf;

use polars_error::{PolarsResult, polars_bail, polars_err};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::progress::SinkProgressRef;

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

impl std::str::FromStr for HfWriteMode {
    type Err = polars_error::PolarsError;

    fn from_str(s: &str) -> PolarsResult<Self> {
        match s.to_lowercase().as_str() {
            "error_if_exists" | "errorifexists" => Ok(Self::ErrorIfExists),
            "overwrite" => Ok(Self::Overwrite),
            "append" => Ok(Self::Append),
            _ => polars_bail!(InvalidOperation: "Invalid HfWriteMode: '{}'. Valid options: error_if_exists, overwrite, append", s),
        }
    }
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
#[derive(Clone)]
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
    /// Whether to update README.md with split metadata (default: true)
    pub update_card: bool,
    /// Optional progress callback for tracking upload progress.
    ///
    /// When set, callbacks will be invoked at shard and commit lifecycle events.
    /// This field is excluded from serialization and equality comparisons.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub progress: Option<SinkProgressRef>,
    /// Optional column name for Hive-style partitioning.
    ///
    /// When set, data is written to paths like `data/{partition_col}={value}/train-00000.parquet`.
    /// Only single-column partitioning is supported.
    pub partition_col: Option<String>,
    /// Override API base URL (for testing). Default: None (uses https://huggingface.co)
    ///
    /// This field is excluded from serialization as it's primarily for testing purposes.
    #[cfg_attr(feature = "serde", serde(skip))]
    pub api_base_url: Option<String>,
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
            update_card: true,
            progress: None,
            partition_col: None,
            api_base_url: None,
        }
    }
}

// Manual implementations of PartialEq, Eq, Hash that exclude the progress field
// (progress is a runtime callback, not meaningful for equality/hashing)

impl PartialEq for HfSinkOptions {
    fn eq(&self, other: &Self) -> bool {
        self.repo_id == other.repo_id
            && self.repo_type == other.repo_type
            && self.revision == other.revision
            && self.path_in_repo == other.path_in_repo
            && self.split == other.split
            && self.max_shard_size == other.max_shard_size
            && self.max_shard_rows == other.max_shard_rows
            && self.num_shards == other.num_shards
            && self.mode == other.mode
            && self.token == other.token
            && self.commit_message == other.commit_message
            && self.create_pr == other.create_pr
            && self.checkpoint_path == other.checkpoint_path
            && self.upload_concurrency == other.upload_concurrency
            && self.update_card == other.update_card
            && self.partition_col == other.partition_col
            && self.api_base_url == other.api_base_url
        // Note: progress is intentionally excluded from equality comparison
    }
}

impl Eq for HfSinkOptions {}

impl std::hash::Hash for HfSinkOptions {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.repo_id.hash(state);
        self.repo_type.hash(state);
        self.revision.hash(state);
        self.path_in_repo.hash(state);
        self.split.hash(state);
        self.max_shard_size.hash(state);
        self.max_shard_rows.hash(state);
        self.num_shards.hash(state);
        self.mode.hash(state);
        self.token.hash(state);
        self.commit_message.hash(state);
        self.create_pr.hash(state);
        self.checkpoint_path.hash(state);
        self.upload_concurrency.hash(state);
        self.update_card.hash(state);
        self.partition_col.hash(state);
        self.api_base_url.hash(state);
        // Note: progress is intentionally excluded from hash computation
    }
}

impl std::fmt::Debug for HfSinkOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HfSinkOptions")
            .field("repo_id", &self.repo_id)
            .field("repo_type", &self.repo_type)
            .field("revision", &self.revision)
            .field("path_in_repo", &self.path_in_repo)
            .field("split", &self.split)
            .field("max_shard_size", &self.max_shard_size)
            .field("max_shard_rows", &self.max_shard_rows)
            .field("num_shards", &self.num_shards)
            .field("mode", &self.mode)
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("commit_message", &self.commit_message)
            .field("create_pr", &self.create_pr)
            .field("checkpoint_path", &self.checkpoint_path)
            .field("upload_concurrency", &self.upload_concurrency)
            .field("update_card", &self.update_card)
            .field("progress", &self.progress.as_ref().map(|_| "<callback>"))
            .field("partition_col", &self.partition_col)
            .field("api_base_url", &self.api_base_url)
            .finish()
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

        // Validate partition_col if provided
        if let Some(ref col) = self.partition_col {
            if col.is_empty() {
                polars_bail!(InvalidOperation: "partition_col cannot be empty");
            }
        }

        Ok(())
    }

    /// Get the effective revision (defaults to "main").
    pub fn effective_revision(&self) -> &str {
        self.revision.as_deref().unwrap_or("main")
    }

    /// Parse an hf:// URL and create HfSinkOptions.
    ///
    /// URL format: `hf://datasets/user/repo@revision/path/to/data`
    ///
    /// Token resolution is deferred to initialization time via `auth::get_hf_token()`.
    ///
    /// # Errors
    /// Returns an error if the URL is invalid or not in HF format.
    pub fn from_url(url: &str) -> PolarsResult<Self> {
        let parts = super::url::HFPathParts::try_from_uri(url)?;

        HfSinkOptions::builder(&parts.repository)
            .with_repo_type(parts.repo_type())
            .with_revision(parts.revision)
            .with_path_in_repo(parts.path)
            .build()
    }

    /// Apply options from key-value string pairs (from Python hf_options parameter).
    ///
    /// Known keys: split, mode, max_shard_size, max_shard_rows, num_shards,
    /// commit_message, create_pr, checkpoint_path, upload_concurrency, update_card, partition_col
    ///
    /// Unknown keys are silently ignored.
    pub fn apply_key_value_options(&mut self, opts: &[(String, String)]) -> PolarsResult<()> {
        for (key, value) in opts {
            match key.to_lowercase().as_str() {
                "split" => self.split = value.clone(),
                "mode" => self.mode = value.parse()?,
                "max_shard_size" => {
                    self.max_shard_size = value.parse().map_err(|_| {
                        polars_err!(InvalidOperation: "Invalid max_shard_size: '{}'. Expected a number in bytes.", value)
                    })?;
                },
                "max_shard_rows" => {
                    self.max_shard_rows = Some(value.parse().map_err(|_| {
                        polars_err!(InvalidOperation: "Invalid max_shard_rows: '{}'. Expected a number.", value)
                    })?);
                },
                "num_shards" => {
                    self.num_shards = Some(value.parse().map_err(|_| {
                        polars_err!(InvalidOperation: "Invalid num_shards: '{}'. Expected a number.", value)
                    })?);
                },
                "commit_message" => self.commit_message = Some(value.clone()),
                "create_pr" => self.create_pr = value.to_lowercase() == "true",
                "checkpoint_path" => {
                    self.checkpoint_path = Some(std::path::PathBuf::from(value))
                },
                "upload_concurrency" => {
                    self.upload_concurrency = value.parse().map_err(|_| {
                        polars_err!(InvalidOperation: "Invalid upload_concurrency: '{}'. Expected a number.", value)
                    })?;
                },
                "update_card" => self.update_card = value.to_lowercase() == "true",
                "partition_col" => self.partition_col = Some(value.clone()),
                _ => { /* Silently ignore unknown keys */ },
            }
        }
        Ok(())
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

    /// Set whether to update README.md with split metadata.
    ///
    /// When enabled (default), the sink will update the repository's README.md
    /// with `dataset_info.splits` metadata including row counts and byte sizes.
    pub fn with_update_card(mut self, update: bool) -> Self {
        self.options.update_card = update;
        self
    }

    /// Set a progress callback for tracking upload progress.
    ///
    /// The callback will receive notifications for:
    /// - Shard start/complete events
    /// - Upload byte progress
    /// - Commit start/complete events
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Arc;
    /// use polars_io::cloud::hf::{HfSinkOptions, NoOpSinkProgress};
    ///
    /// let options = HfSinkOptions::builder("user/repo")
    ///     .with_path_in_repo("data")
    ///     .with_progress(Arc::new(NoOpSinkProgress))
    ///     .build()?;
    /// ```
    pub fn with_progress(mut self, progress: SinkProgressRef) -> Self {
        self.options.progress = Some(progress);
        self
    }

    /// Set the partition column for Hive-style partitioned writes.
    ///
    /// When set, data will be written to paths like:
    /// `{path_in_repo}/{partition_col}={value}/{split}-00000.parquet`
    ///
    /// Only single-column partitioning is supported.
    ///
    /// # Example
    /// ```ignore
    /// let options = HfSinkOptions::builder("user/repo")
    ///     .with_path_in_repo("data")
    ///     .with_partition_col("split")
    ///     .build()?;
    /// // Writes to: data/split=train/train-00000.parquet
    /// ```
    pub fn with_partition_col(mut self, col: impl Into<String>) -> Self {
        self.options.partition_col = Some(col.into());
        self
    }

    /// Set a custom API base URL (primarily for testing).
    ///
    /// When set, all HF Hub API calls will use this URL instead of the default
    /// `https://huggingface.co`. This is useful for integration testing with mock servers.
    ///
    /// # Example
    /// ```ignore
    /// let options = HfSinkOptions::builder("user/repo")
    ///     .with_path_in_repo("data")
    ///     .with_api_base_url("http://localhost:8080")
    ///     .build()?;
    /// ```
    pub fn with_api_base_url(mut self, url: impl Into<String>) -> Self {
        self.options.api_base_url = Some(url.into());
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
        assert!(opts.update_card); // Default is true
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
            .with_update_card(false)
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
        assert!(!opts.update_card); // Explicitly disabled
    }

    #[test]
    fn test_update_card_default_enabled() {
        let opts = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .build()
            .unwrap();

        assert!(opts.update_card); // Enabled by default
    }

    #[test]
    fn test_update_card_can_be_disabled() {
        let opts = HfSinkOptions::builder("user/repo")
            .with_path_in_repo("data")
            .with_update_card(false)
            .build()
            .unwrap();

        assert!(!opts.update_card);
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

    // --- Tests for apply_key_value_options ---

    #[test]
    fn test_apply_key_value_options_empty() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[]).unwrap();
        assert_eq!(opts.split, "train"); // unchanged
    }

    #[test]
    fn test_apply_key_value_options_split() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[("split".to_string(), "validation".to_string())])
            .unwrap();
        assert_eq!(opts.split, "validation");
    }

    #[test]
    fn test_apply_key_value_options_mode_variants() {
        for (mode_str, expected) in [
            ("error_if_exists", HfWriteMode::ErrorIfExists),
            ("errorifexists", HfWriteMode::ErrorIfExists),
            ("overwrite", HfWriteMode::Overwrite),
            ("append", HfWriteMode::Append),
            ("OVERWRITE", HfWriteMode::Overwrite), // case insensitive
        ] {
            let mut opts = HfSinkOptions::default();
            opts.apply_key_value_options(&[("mode".to_string(), mode_str.to_string())])
                .unwrap();
            assert_eq!(opts.mode, expected, "mode string: {}", mode_str);
        }
    }

    #[test]
    fn test_apply_key_value_options_mode_invalid() {
        let mut opts = HfSinkOptions::default();
        let result =
            opts.apply_key_value_options(&[("mode".to_string(), "invalid_mode".to_string())]);
        assert!(result.is_err());
    }

    #[test]
    fn test_apply_key_value_options_numeric_fields() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[
            ("max_shard_size".to_string(), "1048576".to_string()),
            ("max_shard_rows".to_string(), "50000".to_string()),
            ("num_shards".to_string(), "10".to_string()),
            ("upload_concurrency".to_string(), "8".to_string()),
        ])
        .unwrap();

        assert_eq!(opts.max_shard_size, 1048576);
        assert_eq!(opts.max_shard_rows, Some(50000));
        assert_eq!(opts.num_shards, Some(10));
        assert_eq!(opts.upload_concurrency, 8);
    }

    #[test]
    fn test_apply_key_value_options_invalid_numeric() {
        let mut opts = HfSinkOptions::default();
        assert!(opts
            .apply_key_value_options(&[(
                "max_shard_size".to_string(),
                "not_a_number".to_string()
            ),])
            .is_err());

        let mut opts = HfSinkOptions::default();
        assert!(opts
            .apply_key_value_options(&[("max_shard_rows".to_string(), "abc".to_string()),])
            .is_err());
    }

    #[test]
    fn test_apply_key_value_options_boolean_fields() {
        // Test true variations
        for true_str in ["true", "True", "TRUE"] {
            let mut opts = HfSinkOptions::default();
            opts.apply_key_value_options(&[("create_pr".to_string(), true_str.to_string())])
                .unwrap();
            assert!(
                opts.create_pr,
                "create_pr should be true for '{}'",
                true_str
            );
        }

        // Test false variations
        for false_str in ["false", "False", "FALSE", "anything_else"] {
            let mut opts = HfSinkOptions::default();
            opts.create_pr = true; // start with true
            opts.apply_key_value_options(&[("create_pr".to_string(), false_str.to_string())])
                .unwrap();
            assert!(
                !opts.create_pr,
                "create_pr should be false for '{}'",
                false_str
            );
        }
    }

    #[test]
    fn test_apply_key_value_options_string_fields() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[
            ("commit_message".to_string(), "My commit".to_string()),
            ("partition_col".to_string(), "language".to_string()),
        ])
        .unwrap();

        assert_eq!(opts.commit_message, Some("My commit".to_string()));
        assert_eq!(opts.partition_col, Some("language".to_string()));
    }

    #[test]
    fn test_apply_key_value_options_checkpoint_path() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[(
            "checkpoint_path".to_string(),
            "/tmp/my_checkpoint.json".to_string(),
        )])
        .unwrap();

        assert_eq!(
            opts.checkpoint_path,
            Some(PathBuf::from("/tmp/my_checkpoint.json"))
        );
    }

    #[test]
    fn test_apply_key_value_options_unknown_keys_ignored() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[
            ("unknown_key".to_string(), "some_value".to_string()),
            ("another_unknown".to_string(), "another_value".to_string()),
        ])
        .unwrap();
        // No error, options unchanged from default
        assert_eq!(opts.split, "train");
    }

    #[test]
    fn test_apply_key_value_options_case_insensitive_keys() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[
            ("SPLIT".to_string(), "test".to_string()),
            ("Max_Shard_Size".to_string(), "1000000".to_string()),
        ])
        .unwrap();

        assert_eq!(opts.split, "test");
        assert_eq!(opts.max_shard_size, 1000000);
    }

    #[test]
    fn test_apply_key_value_options_multiple_combined() {
        let mut opts = HfSinkOptions::default();
        opts.apply_key_value_options(&[
            ("split".to_string(), "validation".to_string()),
            ("mode".to_string(), "overwrite".to_string()),
            ("max_shard_size".to_string(), "100000000".to_string()),
            ("create_pr".to_string(), "true".to_string()),
            ("commit_message".to_string(), "Test upload".to_string()),
        ])
        .unwrap();

        assert_eq!(opts.split, "validation");
        assert_eq!(opts.mode, HfWriteMode::Overwrite);
        assert_eq!(opts.max_shard_size, 100000000);
        assert!(opts.create_pr);
        assert_eq!(opts.commit_message, Some("Test upload".to_string()));
    }
}
