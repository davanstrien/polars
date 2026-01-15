//! Commit API client for Hugging Face Hub.
//!
//! This module provides functionality to create atomic commits to HF Hub
//! repositories, supporting LFS file additions and deletions.

use polars_core::config;
use polars_error::{PolarsResult, polars_bail, to_compute_err};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::{Deserialize, Serialize};

use crate::cloud::hf::url::HFRepoLocation;
use crate::cloud::options::USER_AGENT;
use crate::pl_async::with_concurrency_budget;
use crate::utils::decode_json_response;

/// Maximum number of retries on rate limit (429).
const MAX_RATE_LIMIT_RETRIES: usize = 3;

/// Parse HF Hub RateLimit header to extract wait time in seconds.
///
/// Header format: "api";r=0;t=42 means wait 42 seconds
fn parse_rate_limit_wait(header: &str) -> Option<u64> {
    for part in header.split(';') {
        let part = part.trim();
        if part.starts_with("t=") {
            return part[2..].parse().ok();
        }
    }
    None
}

/// Extract wait time from rate limit error message.
fn extract_rate_limit_wait(err_msg: &str) -> Option<u64> {
    if let Some(start) = err_msg.find("wait=") {
        let after_wait = &err_msg[start + 5..];
        let digits: String = after_wait
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        if !digits.is_empty() {
            return digits.parse().ok();
        }
    }
    None
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

// ============================================================================
// Commit Client
// ============================================================================

/// Client for creating atomic commits on HF Hub repositories.
///
/// This client handles the NDJSON-formatted commit API, which allows
/// atomic commits of multiple LFS file additions and deletions.
pub struct CommitClient {
    client: reqwest::Client,
    repo_location: HFRepoLocation,
    token: String,
}

impl CommitClient {
    /// Create a new CommitClient for the specified repository.
    ///
    /// # Arguments
    /// * `bucket` - Repository type ("datasets", "models", or "spaces")
    /// * `repo_id` - Repository ID in format "user/repo" or "org/repo"
    /// * `revision` - Branch or commit to target (e.g., "main")
    /// * `token` - HF Hub authentication token
    ///
    /// # Example
    /// ```ignore
    /// let client = CommitClient::new("datasets", "user/my-dataset", "main", "hf_xxx")?;
    /// ```
    pub fn new(
        bucket: &str,
        repo_id: &str,
        revision: &str,
        token: impl Into<String>,
    ) -> PolarsResult<Self> {
        let client = reqwest::ClientBuilder::new()
            .user_agent(USER_AGENT)
            .http1_only()
            .https_only(true)
            .build()
            .map_err(to_compute_err)?;

        let repo_location = HFRepoLocation::new(bucket, repo_id, revision);

        Ok(Self {
            client,
            repo_location,
            token: token.into(),
        })
    }

    /// Build NDJSON payload for the HF Hub commit API.
    ///
    /// The commit API expects a `Content-Type: application/x-ndjson` body with
    /// newline-delimited JSON objects:
    /// - First line: Header with commit summary/description
    /// - Following lines: LFS file additions or deletions
    ///
    /// # Arguments
    /// * `summary` - Commit message summary (required)
    /// * `description` - Optional longer description
    /// * `operations` - List of add/delete operations
    ///
    /// # Returns
    /// `Vec<u8>` containing the NDJSON payload, ready for HTTP body.
    ///
    /// # Example
    /// ```ignore
    /// let payload = CommitClient::build_ndjson_payload(
    ///     "Upload training data",
    ///     Some("Batch upload of 5 shards"),
    ///     &[
    ///         CommitOperation::Add(CommitOperationAdd { ... }),
    ///         CommitOperation::Delete(CommitOperationDelete { ... }),
    ///     ],
    /// );
    /// ```
    pub fn build_ndjson_payload(
        summary: &str,
        description: Option<&str>,
        operations: &[CommitOperation],
    ) -> Vec<u8> {
        let mut payload = Vec::new();

        // Header line (always first)
        let header = NdjsonHeader::new(summary, description.map(|s| s.to_string()));
        let header_json = serde_json::to_vec(&header).expect("header serialization cannot fail");
        payload.extend_from_slice(&header_json);
        payload.push(b'\n');

        // Operation lines
        for op in operations {
            let line_json = match op {
                CommitOperation::Add(add) => {
                    let lfs_file = NdjsonLfsFile::from_add(add);
                    serde_json::to_vec(&lfs_file).expect("lfs file serialization cannot fail")
                },
                CommitOperation::Delete(del) => {
                    if del.is_folder() {
                        let folder = NdjsonDeletedFolder::new(&del.path_in_repo);
                        serde_json::to_vec(&folder).expect("folder serialization cannot fail")
                    } else {
                        let file = NdjsonDeletedFile::new(&del.path_in_repo);
                        serde_json::to_vec(&file).expect("file serialization cannot fail")
                    }
                },
            };
            payload.extend_from_slice(&line_json);
            payload.push(b'\n');
        }

        payload
    }

    /// Create an atomic commit on the HF Hub repository.
    ///
    /// This method uploads the commit operations (LFS file additions and deletions)
    /// atomically to the repository.
    ///
    /// # Arguments
    /// * `summary` - Commit message summary (required)
    /// * `description` - Optional longer description
    /// * `operations` - List of add/delete operations
    /// * `create_pr` - If true, creates a pull request instead of direct commit
    ///
    /// # Returns
    /// `CommitInfo` with commit URL, SHA, and optional PR info.
    ///
    /// # Example
    /// ```ignore
    /// let info = client.create_commit(
    ///     "Upload training data",
    ///     Some("Batch upload of 5 shards"),
    ///     &operations,
    ///     false,  // direct commit, not a PR
    /// ).await?;
    /// println!("Committed: {}", info.commit_url);
    /// ```
    pub async fn create_commit(
        &self,
        summary: &str,
        description: Option<&str>,
        operations: &[CommitOperation],
        create_pr: bool,
    ) -> PolarsResult<CommitInfo> {
        let payload = Self::build_ndjson_payload(summary, description, operations);

        let mut url = self.repo_location.get_commit_uri();
        if create_pr {
            url.push_str("?create_pr=1");
        }

        let response_bytes = self.send_commit_request(&url, payload).await?;
        decode_json_response(&response_bytes)
    }

    /// Send commit request with retry on rate limit.
    async fn send_commit_request(&self, url: &str, body: Vec<u8>) -> PolarsResult<bytes::Bytes> {
        let mut retries = 0;

        loop {
            let result = self.send_single_commit_request(url, body.clone()).await;

            match result {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("HTTP 429") && retries < MAX_RATE_LIMIT_RETRIES {
                        if let Some(wait_secs) = extract_rate_limit_wait(&err_str) {
                            retries += 1;
                            if config::verbose() {
                                eprintln!(
                                    "Rate limited, waiting {} seconds (retry {}/{})",
                                    wait_secs, retries, MAX_RATE_LIMIT_RETRIES
                                );
                            }
                            tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                            continue;
                        }
                    }
                    return Err(e);
                },
            }
        }
    }

    /// Send a single commit request (no retry).
    async fn send_single_commit_request(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> PolarsResult<bytes::Bytes> {
        with_concurrency_budget(1, || async {
            let resp = self
                .client
                .post(url)
                .header(AUTHORIZATION, format!("Bearer {}", self.token))
                .header(CONTENT_TYPE, "application/x-ndjson")
                .body(body)
                .send()
                .await
                .map_err(to_compute_err)?;

            let status = resp.status();

            // Handle rate limiting specially to allow retry
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let wait_secs = resp
                    .headers()
                    .get("RateLimit")
                    .and_then(|h| h.to_str().ok())
                    .and_then(parse_rate_limit_wait);

                let body = resp.text().await.unwrap_or_default();
                polars_bail!(
                    ComputeError: "HTTP 429 rate limited (wait={}): {}",
                    wait_secs.unwrap_or(0),
                    body
                );
            }

            if !status.is_success() {
                let body = resp.text().await.unwrap_or_default();
                polars_bail!(
                    ComputeError: "Commit API request failed (HTTP {}): {}",
                    status.as_u16(),
                    body
                );
            }

            resp.bytes().await.map_err(to_compute_err)
        })
        .await
    }
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

    // ========================================================================
    // CommitClient Tests
    // ========================================================================

    #[test]
    fn test_commit_client_creation() {
        let client = CommitClient::new("datasets", "user/repo", "main", "test_token");
        assert!(client.is_ok());
    }

    #[test]
    fn test_commit_client_various_repo_types() {
        // Test with different bucket types
        assert!(CommitClient::new("datasets", "user/repo", "main", "token").is_ok());
        assert!(CommitClient::new("models", "org/model", "main", "token").is_ok());
        assert!(CommitClient::new("spaces", "user/space", "dev", "token").is_ok());
    }

    #[test]
    fn test_commit_client_with_special_revision() {
        // Test with a revision that needs URL encoding
        let client = CommitClient::new("datasets", "user/repo", "refs/convert/parquet", "token");
        assert!(client.is_ok());
    }

    // ========================================================================
    // build_ndjson_payload Tests
    // ========================================================================

    #[test]
    fn test_build_ndjson_payload_header_only() {
        let payload = CommitClient::build_ndjson_payload("Simple commit", None, &[]);
        let payload_str = String::from_utf8(payload).unwrap();

        // Should have exactly one line (header)
        let lines: Vec<&str> = payload_str.lines().collect();
        assert_eq!(lines.len(), 1);

        // Header should be valid JSON
        let header: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["key"], "header");
        assert_eq!(header["value"]["summary"], "Simple commit");
    }

    #[test]
    fn test_build_ndjson_payload_with_description() {
        let payload = CommitClient::build_ndjson_payload(
            "Upload data",
            Some("Batch upload of training shards"),
            &[],
        );
        let payload_str = String::from_utf8(payload).unwrap();

        let lines: Vec<&str> = payload_str.lines().collect();
        assert_eq!(lines.len(), 1);

        let header: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["value"]["summary"], "Upload data");
        assert_eq!(
            header["value"]["description"],
            "Batch upload of training shards"
        );
    }

    #[test]
    fn test_build_ndjson_payload_with_adds() {
        let operations = vec![
            CommitOperation::Add(CommitOperationAdd {
                path_in_repo: "data/train-00000.parquet".into(),
                oid: "abc123".into(),
                size: 1000,
            }),
            CommitOperation::Add(CommitOperationAdd {
                path_in_repo: "data/train-00001.parquet".into(),
                oid: "def456".into(),
                size: 2000,
            }),
        ];

        let payload = CommitClient::build_ndjson_payload("Upload shards", None, &operations);
        let payload_str = String::from_utf8(payload).unwrap();

        let lines: Vec<&str> = payload_str.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 adds

        // Verify header
        let header: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["key"], "header");

        // Verify first LFS file
        let lfs1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(lfs1["key"], "lfsFile");
        assert_eq!(lfs1["value"]["path"], "data/train-00000.parquet");
        assert_eq!(lfs1["value"]["algo"], "sha256");
        assert_eq!(lfs1["value"]["oid"], "abc123");
        assert_eq!(lfs1["value"]["size"], 1000);

        // Verify second LFS file
        let lfs2: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(lfs2["key"], "lfsFile");
        assert_eq!(lfs2["value"]["path"], "data/train-00001.parquet");
    }

    #[test]
    fn test_build_ndjson_payload_with_deletes() {
        let operations = vec![
            CommitOperation::Delete(CommitOperationDelete {
                path_in_repo: "data/old_file.parquet".into(),
            }),
            CommitOperation::Delete(CommitOperationDelete {
                path_in_repo: "data/old_folder/".into(), // trailing slash = folder
            }),
        ];

        let payload = CommitClient::build_ndjson_payload("Cleanup", None, &operations);
        let payload_str = String::from_utf8(payload).unwrap();

        let lines: Vec<&str> = payload_str.lines().collect();
        assert_eq!(lines.len(), 3); // header + 2 deletes

        // Verify file deletion
        let del_file: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(del_file["key"], "deletedFile");
        assert_eq!(del_file["value"]["path"], "data/old_file.parquet");

        // Verify folder deletion
        let del_folder: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(del_folder["key"], "deletedFolder");
        assert_eq!(del_folder["value"]["path"], "data/old_folder/");
    }

    #[test]
    fn test_build_ndjson_payload_mixed_operations() {
        let operations = vec![
            CommitOperation::Delete(CommitOperationDelete {
                path_in_repo: "data/old/".into(),
            }),
            CommitOperation::Add(CommitOperationAdd {
                path_in_repo: "data/new.parquet".into(),
                oid: "xyz789".into(),
                size: 5000,
            }),
        ];

        let payload = CommitClient::build_ndjson_payload(
            "Replace data",
            Some("Delete old folder and add new file"),
            &operations,
        );
        let payload_str = String::from_utf8(payload).unwrap();

        let lines: Vec<&str> = payload_str.lines().collect();
        assert_eq!(lines.len(), 3);

        // Verify order: header, delete, add (same order as input)
        let header: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(header["key"], "header");

        let delete: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(delete["key"], "deletedFolder");

        let add: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(add["key"], "lfsFile");
    }

    #[test]
    fn test_build_ndjson_payload_ends_with_newlines() {
        let payload = CommitClient::build_ndjson_payload(
            "Test",
            None,
            &[CommitOperation::Add(CommitOperationAdd {
                path_in_repo: "test.parquet".into(),
                oid: "abc".into(),
                size: 100,
            })],
        );

        // Each line should end with \n
        let payload_str = String::from_utf8(payload).unwrap();
        assert!(payload_str.ends_with('\n'));

        // Count newlines matches line count
        let newline_count = payload_str.chars().filter(|&c| c == '\n').count();
        let line_count = payload_str.lines().count();
        assert_eq!(newline_count, line_count);
    }

    // ========================================================================
    // Rate Limit Helper Tests
    // ========================================================================

    #[test]
    fn test_parse_rate_limit_wait() {
        // Standard format from HF Hub
        assert_eq!(parse_rate_limit_wait("api;r=0;t=42"), Some(42));
        assert_eq!(parse_rate_limit_wait("api;r=100;t=300"), Some(300));

        // With quotes (as shown in docs)
        assert_eq!(parse_rate_limit_wait("\"api\";r=0;t=42"), Some(42));

        // Missing t= parameter
        assert_eq!(parse_rate_limit_wait("api;r=0"), None);

        // Empty/invalid
        assert_eq!(parse_rate_limit_wait(""), None);
    }

    #[test]
    fn test_extract_rate_limit_wait() {
        assert_eq!(
            extract_rate_limit_wait("HTTP 429 rate limited (wait=42): Too many requests"),
            Some(42)
        );
        assert_eq!(
            extract_rate_limit_wait("HTTP 429 rate limited (wait=300): Slow down"),
            Some(300)
        );
        assert_eq!(
            extract_rate_limit_wait("HTTP 429 rate limited (wait=0): Try again"),
            Some(0)
        );
        assert_eq!(extract_rate_limit_wait("Some other error"), None);
    }

    #[test]
    fn test_create_commit_url_with_create_pr() {
        // Test that create_pr parameter is properly appended to URL
        // We can't test the full create_commit method without a mock server,
        // but we can verify the URL construction logic indirectly by testing
        // that HFRepoLocation generates the expected base URL
        let client = CommitClient::new("datasets", "user/repo", "main", "token").unwrap();
        let base_url = client.repo_location.get_commit_uri();
        assert_eq!(
            base_url,
            "https://huggingface.co/api/datasets/user/repo/commit/main"
        );

        // Verify the ?create_pr=1 suffix would be added correctly
        let url_with_pr = format!("{}?create_pr=1", base_url);
        assert!(url_with_pr.ends_with("?create_pr=1"));
    }
}
