//! Commit API client for Hugging Face Hub.
//!
//! This module provides functionality to create atomic commits to HF Hub
//! repositories, supporting LFS file additions and deletions.

use polars_error::PolarsResult;
use serde::{Deserialize, Serialize};

/// Convert any error to a PolarsError (ComputeError variant).
#[allow(dead_code)]
fn to_compute_err<E: std::error::Error + Send + Sync + 'static>(e: E) -> polars_error::PolarsError {
    polars_error::polars_err!(ComputeError: "{}", e)
}

/// An LFS file to add in a commit.
///
/// The file must have been uploaded via LFS batch API before committing.
/// The SHA256 hash (`oid`) must match the hash computed during upload.
#[derive(Debug, Clone)]
pub struct CommitOperationAdd {
    /// Path in the repository (e.g., "data/train-00000.parquet").
    /// Should not start with a leading slash.
    pub path_in_repo: String,
    /// SHA256 hash of the file in hexadecimal format.
    pub oid: String,
    /// File size in bytes.
    pub size: u64,
}

/// A file or folder to delete in a commit.
#[derive(Debug, Clone)]
pub struct CommitOperationDelete {
    /// Path in the repository.
    /// If the path ends with '/', it's treated as a folder deletion.
    pub path_in_repo: String,
}

impl CommitOperationDelete {
    /// Returns true if this is a folder deletion (path ends with '/').
    pub fn is_folder(&self) -> bool {
        self.path_in_repo.ends_with('/')
    }
}

/// A commit operation (add or delete).
#[derive(Debug, Clone)]
pub enum CommitOperation {
    /// Add an LFS file to the repository.
    Add(CommitOperationAdd),
    /// Delete a file or folder from the repository.
    Delete(CommitOperationDelete),
}

impl From<CommitOperationAdd> for CommitOperation {
    fn from(add: CommitOperationAdd) -> Self {
        CommitOperation::Add(add)
    }
}

impl From<CommitOperationDelete> for CommitOperation {
    fn from(delete: CommitOperationDelete) -> Self {
        CommitOperation::Delete(delete)
    }
}

// ============================================================================
// Response Types
// ============================================================================

/// Response from the HF Hub commit API.
///
/// Contains information about the created commit and optionally the
/// associated pull request if `create_pr=true` was used.
#[derive(Debug, Clone, Deserialize)]
pub struct CommitInfo {
    /// URL to view the commit (e.g., "https://huggingface.co/datasets/user/repo/commit/abc123")
    #[serde(rename = "commitUrl")]
    pub commit_url: String,

    /// Git commit SHA (40-character hex string)
    pub oid: String,

    /// PR URL if `create_pr=true` was used
    #[serde(rename = "prUrl")]
    pub pr_url: Option<String>,

    /// PR number if `create_pr=true` was used
    #[serde(rename = "prNum")]
    pub pr_num: Option<u64>,

    /// PR revision/branch name if `create_pr=true` was used (e.g., "refs/pr/42")
    #[serde(rename = "prRevision")]
    pub pr_revision: Option<String>,
}

// ============================================================================
// NDJSON Serialization Types
// ============================================================================

/// NDJSON header line for commit.
///
/// Format: `{"key":"header","value":{"summary":"...","description":"..."}}`
#[derive(Debug, Serialize)]
pub(crate) struct NdjsonHeader {
    key: &'static str,
    value: HeaderValue,
}

/// Value object for the header NDJSON line.
#[derive(Debug, Serialize)]
pub(crate) struct HeaderValue {
    summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl NdjsonHeader {
    /// Create a new header line with summary and optional description.
    pub fn new(summary: impl Into<String>, description: Option<String>) -> Self {
        Self {
            key: "header",
            value: HeaderValue {
                summary: summary.into(),
                description,
            },
        }
    }
}

/// NDJSON line for LFS file addition.
///
/// Format: `{"key":"lfsFile","value":{"path":"...","algo":"sha256","oid":"...","size":...}}`
#[derive(Debug, Serialize)]
pub(crate) struct NdjsonLfsFile {
    key: &'static str,
    value: LfsFileValue,
}

/// Value object for the lfsFile NDJSON line.
#[derive(Debug, Serialize)]
pub(crate) struct LfsFileValue {
    path: String,
    algo: &'static str,
    oid: String,
    size: u64,
}

impl NdjsonLfsFile {
    /// Create an NDJSON line from a CommitOperationAdd.
    pub fn from_add(add: &CommitOperationAdd) -> Self {
        Self {
            key: "lfsFile",
            value: LfsFileValue {
                path: add.path_in_repo.clone(),
                algo: "sha256",
                oid: add.oid.clone(),
                size: add.size,
            },
        }
    }
}

/// NDJSON line for file deletion.
///
/// Format: `{"key":"deletedFile","value":{"path":"..."}}`
#[derive(Debug, Serialize)]
pub(crate) struct NdjsonDeletedFile {
    key: &'static str,
    value: DeletedValue,
}

impl NdjsonDeletedFile {
    /// Create a new deleted file line.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            key: "deletedFile",
            value: DeletedValue { path: path.into() },
        }
    }
}

/// NDJSON line for folder deletion.
///
/// Format: `{"key":"deletedFolder","value":{"path":".../"}}` (trailing slash required)
#[derive(Debug, Serialize)]
pub(crate) struct NdjsonDeletedFolder {
    key: &'static str,
    value: DeletedValue,
}

impl NdjsonDeletedFolder {
    /// Create a new deleted folder line.
    /// Note: The path should include a trailing slash.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            key: "deletedFolder",
            value: DeletedValue { path: path.into() },
        }
    }
}

/// Shared value object for deleted file/folder NDJSON lines.
#[derive(Debug, Serialize)]
pub(crate) struct DeletedValue {
    path: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commit_operation_add() {
        let add = CommitOperationAdd {
            path_in_repo: "data/train-00000.parquet".into(),
            oid: "abc123def456".into(),
            size: 1000,
        };
        assert_eq!(add.path_in_repo, "data/train-00000.parquet");
        assert_eq!(add.oid, "abc123def456");
        assert_eq!(add.size, 1000);
    }

    #[test]
    fn test_commit_operation_delete_file() {
        let del = CommitOperationDelete {
            path_in_repo: "data/old.parquet".into(),
        };
        assert!(!del.is_folder());
        assert_eq!(del.path_in_repo, "data/old.parquet");
    }

    #[test]
    fn test_commit_operation_delete_folder() {
        let del = CommitOperationDelete {
            path_in_repo: "data/old_folder/".into(),
        };
        assert!(del.is_folder());
    }

    #[test]
    fn test_commit_operation_from_add() {
        let add = CommitOperationAdd {
            path_in_repo: "test.parquet".into(),
            oid: "abc".into(),
            size: 100,
        };
        let op: CommitOperation = add.into();
        assert!(matches!(op, CommitOperation::Add(_)));
    }

    #[test]
    fn test_commit_operation_from_delete() {
        let del = CommitOperationDelete {
            path_in_repo: "test.parquet".into(),
        };
        let op: CommitOperation = del.into();
        assert!(matches!(op, CommitOperation::Delete(_)));
    }

    #[test]
    fn test_commit_info_deserialize_basic() {
        let json = r#"{
            "commitUrl": "https://huggingface.co/datasets/user/repo/commit/abc123",
            "oid": "abc123def456789012345678901234567890abcd"
        }"#;
        let info: CommitInfo = serde_json::from_str(json).unwrap();
        assert_eq!(
            info.commit_url,
            "https://huggingface.co/datasets/user/repo/commit/abc123"
        );
        assert_eq!(info.oid, "abc123def456789012345678901234567890abcd");
        assert!(info.pr_url.is_none());
        assert!(info.pr_num.is_none());
        assert!(info.pr_revision.is_none());
    }

    #[test]
    fn test_commit_info_deserialize_with_pr() {
        let json = r#"{
            "commitUrl": "https://huggingface.co/datasets/user/repo/commit/abc123",
            "oid": "abc123def456789012345678901234567890abcd",
            "prUrl": "https://huggingface.co/datasets/user/repo/pull/42",
            "prNum": 42,
            "prRevision": "refs/pr/42"
        }"#;
        let info: CommitInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.pr_num, Some(42));
        assert_eq!(
            info.pr_url,
            Some("https://huggingface.co/datasets/user/repo/pull/42".to_string())
        );
        assert_eq!(info.pr_revision, Some("refs/pr/42".to_string()));
    }

    #[test]
    fn test_commit_info_partial_pr_fields() {
        // Test that we can deserialize even if only some optional fields are present
        let json = r#"{
            "commitUrl": "https://example.com/commit",
            "oid": "abc123",
            "prNum": 5
        }"#;
        let info: CommitInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.pr_num, Some(5));
        assert!(info.pr_url.is_none());
        assert!(info.pr_revision.is_none());
    }

    // ========================================================================
    // NDJSON Serialization Tests
    // ========================================================================

    #[test]
    fn test_ndjson_header_serialize() {
        let header =
            NdjsonHeader::new("Upload via Polars", Some("Batch upload of 2 shards".into()));
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(
            json,
            r#"{"key":"header","value":{"summary":"Upload via Polars","description":"Batch upload of 2 shards"}}"#
        );
    }

    #[test]
    fn test_ndjson_header_no_description() {
        let header = NdjsonHeader::new("Simple commit", None);
        let json = serde_json::to_string(&header).unwrap();
        // description should be omitted, not null
        assert_eq!(
            json,
            r#"{"key":"header","value":{"summary":"Simple commit"}}"#
        );
        assert!(!json.contains("description"));
    }

    #[test]
    fn test_ndjson_lfs_file_serialize() {
        let add = CommitOperationAdd {
            path_in_repo: "data/train-00000.parquet".into(),
            oid: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".into(),
            size: 5242880,
        };
        let lfs_file = NdjsonLfsFile::from_add(&add);
        let json = serde_json::to_string(&lfs_file).unwrap();
        assert_eq!(
            json,
            r#"{"key":"lfsFile","value":{"path":"data/train-00000.parquet","algo":"sha256","oid":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":5242880}}"#
        );
    }

    #[test]
    fn test_ndjson_deleted_file_serialize() {
        let deleted = NdjsonDeletedFile::new("data/old_train.parquet");
        let json = serde_json::to_string(&deleted).unwrap();
        assert_eq!(
            json,
            r#"{"key":"deletedFile","value":{"path":"data/old_train.parquet"}}"#
        );
    }

    #[test]
    fn test_ndjson_deleted_folder_serialize() {
        let deleted = NdjsonDeletedFolder::new("data/previous_version/");
        let json = serde_json::to_string(&deleted).unwrap();
        assert_eq!(
            json,
            r#"{"key":"deletedFolder","value":{"path":"data/previous_version/"}}"#
        );
    }
}
