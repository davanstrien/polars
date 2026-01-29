//! Mock HTTP integration tests for HfSinkNode.
//!
//! Uses wiremock to mock HF Hub API responses for testing the full
//! upload pipeline without making real network calls.
//!
//! Key infrastructure:
//! - `MockHfHub` struct wraps MockServer with builder methods for API mocks
//! - `HfSinkOptions::with_api_base_url()` to redirect API calls to mock server
//! - `HFRepoLocation::new(..., Some(base_url))` for custom base URL
//!
//! Example usage pattern:
//! ```ignore
//! let mock = MockHfHub::start().await;
//! mock.mock_lfs_batch(vec![
//!     MockLfsObject::new_upload("sha256...", 1024),
//! ]).await;
//!
//! let options = HfSinkOptions::builder("user/repo")
//!     .with_api_base_url(mock.uri())
//!     .with_token("test_token")
//!     .build()?;
//! ```

use wiremock::matchers::{body_string_contains, header, method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use polars_io::cloud::hf::{HFRepoLocation, HfSinkOptions};

// ============================================================================
// MockHfHub - Reusable mock fixture for HF Hub APIs
// ============================================================================

/// Mock HF Hub server for integration tests.
///
/// Wraps a wiremock `MockServer` with convenient methods to mock
/// the various HF Hub APIs (LFS batch, presigned upload, commit, tree).
pub struct MockHfHub {
    mock_server: MockServer,
}

impl MockHfHub {
    /// Start a new mock HF Hub server.
    pub async fn start() -> Self {
        Self {
            mock_server: MockServer::start().await,
        }
    }

    /// Get the base URI of the mock server (e.g., "http://127.0.0.1:12345").
    pub fn uri(&self) -> String {
        self.mock_server.uri()
    }

    /// Mount a mock for the LFS batch API endpoint.
    ///
    /// The LFS batch API is the first call in an upload flow - it takes SHA256
    /// hashes and sizes, and returns presigned upload URLs.
    ///
    /// URL pattern: `POST /datasets/{repo}.git/info/lfs/objects/batch`
    ///
    /// # Arguments
    /// * `objects` - Mock objects to return. Each specifies an OID, size, and
    ///   whether to return an upload URL or indicate the file already exists.
    ///
    /// # Example
    /// ```ignore
    /// mock.mock_lfs_batch(vec![
    ///     MockLfsObject::new_upload("abc123...", 1024),  // Needs upload
    ///     MockLfsObject::new_exists("def456...", 2048), // Already exists
    /// ]).await;
    /// ```
    pub async fn mock_lfs_batch(&self, objects: Vec<MockLfsObject>) -> &Self {
        let response_json = build_lfs_batch_response(&self.mock_server.uri(), objects);

        Mock::given(method("POST"))
            .and(path_regex(r".*\.git/info/lfs/objects/batch$"))
            .and(header("content-type", "application/vnd.git-lfs+json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(response_json)
                    .insert_header("content-type", "application/vnd.git-lfs+json"),
            )
            .mount(&self.mock_server)
            .await;

        self
    }

    /// Mount a mock for presigned S3 upload (PUT request).
    ///
    /// After getting upload URLs from LFS batch, the client PUTs file content
    /// to the presigned URL. This mocks that endpoint.
    ///
    /// Returns 200 OK with an ETag header (required for multipart completion).
    pub async fn mock_presigned_upload(&self) -> &Self {
        Mock::given(method("PUT"))
            .and(path_regex(r"/upload/.*"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"mock-etag-12345\""),
            )
            .mount(&self.mock_server)
            .await;

        self
    }

    /// Mount a mock for the commit API endpoint.
    ///
    /// The commit API is called after all LFS uploads complete to atomically
    /// commit the files to the repository.
    ///
    /// URL pattern: `POST /api/datasets/{repo}/commit/{revision}`
    ///
    /// # Arguments
    /// * `commit_oid` - The commit OID to return in the response (40-char hex)
    ///
    /// # Example
    /// ```ignore
    /// mock.mock_lfs_batch(vec![...])
    ///     .await
    ///     .mock_presigned_upload()
    ///     .await
    ///     .mock_commit("abc123def456789012345678901234567890abcd")
    ///     .await;
    /// ```
    pub async fn mock_commit(&self, commit_oid: impl Into<String>) -> &Self {
        let oid = commit_oid.into();
        let response_json = format!(
            r#"{{
                "commitUrl": "{}/datasets/user/repo/commit/{}",
                "commitOid": "{}"
            }}"#,
            self.uri(),
            oid,
            oid
        );

        Mock::given(method("POST"))
            .and(path_regex(r".*/api/datasets/.*/commit/.*"))
            .and(header("content-type", "application/x-ndjson"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string(response_json)
                    .insert_header("content-type", "application/json"),
            )
            .mount(&self.mock_server)
            .await;

        self
    }

    /// Get reference to underlying MockServer for custom mocks.
    pub fn server(&self) -> &MockServer {
        &self.mock_server
    }
}

// ============================================================================
// MockLfsObject - Helper for building LFS batch responses
// ============================================================================

/// Specification for a mock LFS object in a batch response.
#[derive(Debug, Clone)]
pub struct MockLfsObject {
    /// SHA256 hash (lowercase hex, 64 chars)
    pub oid: String,
    /// File size in bytes
    pub size: u64,
    /// Whether this file already exists (skip upload)
    pub already_exists: bool,
}

impl MockLfsObject {
    /// Create a mock object that needs to be uploaded.
    pub fn new_upload(oid: impl Into<String>, size: u64) -> Self {
        Self {
            oid: oid.into(),
            size,
            already_exists: false,
        }
    }

    /// Create a mock object that already exists (no upload needed).
    pub fn new_exists(oid: impl Into<String>, size: u64) -> Self {
        Self {
            oid: oid.into(),
            size,
            already_exists: true,
        }
    }
}

/// Build the JSON response body for an LFS batch request.
///
/// For objects needing upload, provides a presigned URL on the mock server.
/// For objects already existing, omits the actions field.
fn build_lfs_batch_response(mock_uri: &str, objects: Vec<MockLfsObject>) -> String {
    let objects_json: Vec<String> = objects
        .into_iter()
        .map(|obj| {
            if obj.already_exists {
                // No actions = file already exists
                format!(
                    r#"{{
                        "oid": "{}",
                        "size": {},
                        "authenticated": true
                    }}"#,
                    obj.oid, obj.size
                )
            } else {
                // Provide upload URL pointing back to mock server
                format!(
                    r#"{{
                        "oid": "{}",
                        "size": {},
                        "authenticated": true,
                        "actions": {{
                            "upload": {{
                                "href": "{}/upload/{}",
                                "header": {{
                                    "Content-Type": "application/octet-stream"
                                }}
                            }}
                        }}
                    }}"#,
                    obj.oid, obj.size, mock_uri, obj.oid
                )
            }
        })
        .collect();

    format!(
        r#"{{
            "transfer": "basic",
            "objects": [{}]
        }}"#,
        objects_json.join(",")
    )
}

/// Verify wiremock infrastructure works.
#[tokio::test]
async fn test_mock_server_setup() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path_regex("/api/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}"))
        .mount(&mock_server)
        .await;

    let uri = mock_server.uri();
    assert!(uri.starts_with("http://127.0.0.1:"));
}

/// Verify HfSinkOptions can be configured with custom base URL.
#[test]
fn test_options_with_custom_base_url() {
    let options = HfSinkOptions::builder("user/test-repo")
        .with_path_in_repo("data")
        .with_api_base_url("http://localhost:8080")
        .with_token("test_token")
        .build()
        .unwrap();

    assert_eq!(options.api_base_url, Some("http://localhost:8080".to_string()));
}

/// Verify HFRepoLocation uses custom base URL for all endpoint URLs.
#[test]
fn test_repo_location_custom_base_url() {
    let base_url = "http://127.0.0.1:12345";
    let loc = HFRepoLocation::new("datasets", "user/repo", "main", Some(base_url));

    // All URLs should use the custom base
    assert!(loc.api_base_path.starts_with(base_url));
    assert!(loc.download_base_path.starts_with(base_url));
    assert!(loc.get_lfs_batch_uri().starts_with(base_url));
    assert!(loc.get_commit_uri().starts_with(base_url));
}

/// Integration test showing mock server with HfSinkOptions pattern.
///
/// This demonstrates how future integration tests will use wiremock:
/// 1. Start mock server
/// 2. Configure HfSinkOptions with mock server URL
/// 3. Mount expected request/response mocks
/// 4. Run sink operations
/// 5. Verify mock received expected requests
#[tokio::test]
async fn test_mock_server_with_options_pattern() {
    let mock_server = MockServer::start().await;

    // Example: mock the Tree API endpoint for checking existing files
    Mock::given(method("GET"))
        .and(path_regex("/api/datasets/user/repo/tree/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
        .mount(&mock_server)
        .await;

    // Create options configured to use mock server
    let options = HfSinkOptions::builder("user/repo")
        .with_path_in_repo("data")
        .with_api_base_url(&mock_server.uri())
        .with_token("test_token")
        .build()
        .unwrap();

    // Verify the URL would go to mock server
    let loc = HFRepoLocation::new(
        "datasets",
        &options.repo_id,
        options.effective_revision(),
        options.api_base_url.as_deref(),
    );

    assert!(loc.api_base_path.contains("127.0.0.1"));
    assert!(loc.get_commit_uri().contains("127.0.0.1"));
}

// ============================================================================
// MockHfHub Tests
// ============================================================================

/// Verify MockHfHub can be started and provides a valid URI.
#[tokio::test]
async fn test_mock_hf_hub_start() {
    let mock = MockHfHub::start().await;

    let uri = mock.uri();
    assert!(uri.starts_with("http://127.0.0.1:"));
}

/// Test that mock_lfs_batch mounts correctly and returns expected response.
#[tokio::test]
async fn test_mock_lfs_batch_single_upload() {
    let mock = MockHfHub::start().await;
    let test_oid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    // Mount mock for LFS batch
    mock.mock_lfs_batch(vec![MockLfsObject::new_upload(test_oid, 1024)])
        .await;

    // Make actual HTTP request to mock server
    let client = reqwest::Client::new();
    let lfs_url = format!("{}/datasets/user/repo.git/info/lfs/objects/batch", mock.uri());

    let response = client
        .post(&lfs_url)
        .header("content-type", "application/vnd.git-lfs+json")
        .body(r#"{"operation":"upload","transfers":["basic"],"objects":[{"oid":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","size":1024}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();

    // Verify response contains expected fields
    assert!(body.contains("\"transfer\""));
    assert!(body.contains("basic"));
    assert!(body.contains(test_oid));
    assert!(body.contains("upload"));
    assert!(body.contains("/upload/")); // Presigned URL path
}

/// Test mock_lfs_batch with file that already exists (no upload needed).
#[tokio::test]
async fn test_mock_lfs_batch_already_exists() {
    let mock = MockHfHub::start().await;
    let test_oid = "abc123def456";

    mock.mock_lfs_batch(vec![MockLfsObject::new_exists(test_oid, 2048)])
        .await;

    let client = reqwest::Client::new();
    let lfs_url = format!("{}/datasets/user/repo.git/info/lfs/objects/batch", mock.uri());

    let response = client
        .post(&lfs_url)
        .header("content-type", "application/vnd.git-lfs+json")
        .body(r#"{"operation":"upload","transfers":["basic"],"objects":[{"oid":"abc123def456","size":2048}]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();

    // For existing files, no "actions" field should be present
    assert!(body.contains(test_oid));
    assert!(!body.contains("\"actions\"")); // No upload needed
}

/// Test mock_lfs_batch with multiple objects (mix of upload and exists).
#[tokio::test]
async fn test_mock_lfs_batch_multiple_objects() {
    let mock = MockHfHub::start().await;

    mock.mock_lfs_batch(vec![
        MockLfsObject::new_upload("upload_oid_1", 1024),
        MockLfsObject::new_exists("exists_oid_2", 2048),
        MockLfsObject::new_upload("upload_oid_3", 4096),
    ])
    .await;

    let client = reqwest::Client::new();
    let lfs_url = format!("{}/datasets/user/repo.git/info/lfs/objects/batch", mock.uri());

    let response = client
        .post(&lfs_url)
        .header("content-type", "application/vnd.git-lfs+json")
        .body(r#"{"operation":"upload","transfers":["basic"],"objects":[]}"#)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();

    // All OIDs should be in the response
    assert!(body.contains("upload_oid_1"));
    assert!(body.contains("exists_oid_2"));
    assert!(body.contains("upload_oid_3"));
}

/// Test mock_presigned_upload returns 200 with ETag.
#[tokio::test]
async fn test_mock_presigned_upload() {
    let mock = MockHfHub::start().await;

    mock.mock_presigned_upload().await;

    let client = reqwest::Client::new();
    let upload_url = format!("{}/upload/abc123", mock.uri());

    let response = client
        .put(&upload_url)
        .body(b"file content".to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let etag = response.headers().get("etag").unwrap();
    assert!(etag.to_str().unwrap().contains("mock-etag"));
}

/// Test full LFS flow: batch → upload → verify.
#[tokio::test]
async fn test_mock_lfs_batch_and_upload_flow() {
    let mock = MockHfHub::start().await;
    let test_oid = "sha256_test_hash";

    // Mount both LFS batch and presigned upload mocks
    mock.mock_lfs_batch(vec![MockLfsObject::new_upload(test_oid, 1024)])
        .await
        .mock_presigned_upload()
        .await;

    let client = reqwest::Client::new();

    // Step 1: Call LFS batch API
    let lfs_url = format!("{}/datasets/user/repo.git/info/lfs/objects/batch", mock.uri());
    let batch_response = client
        .post(&lfs_url)
        .header("content-type", "application/vnd.git-lfs+json")
        .body(format!(r#"{{"operation":"upload","transfers":["basic"],"objects":[{{"oid":"{}","size":1024}}]}}"#, test_oid))
        .send()
        .await
        .unwrap();

    assert_eq!(batch_response.status(), 200);
    let batch_body = batch_response.text().await.unwrap();
    assert!(batch_body.contains("/upload/"));

    // Step 2: PUT to presigned URL
    let upload_url = format!("{}/upload/{}", mock.uri(), test_oid);
    let upload_response = client
        .put(&upload_url)
        .body(b"test file content".to_vec())
        .send()
        .await
        .unwrap();

    assert_eq!(upload_response.status(), 200);
}

/// Test mock_commit returns success response.
#[tokio::test]
async fn test_mock_commit_success() {
    let mock = MockHfHub::start().await;
    let commit_oid = "abc123def456789012345678901234567890abcd";

    mock.mock_commit(commit_oid).await;

    let client = reqwest::Client::new();
    let commit_url = format!("{}/api/datasets/user/repo/commit/main", mock.uri());

    let response = client
        .post(&commit_url)
        .header("content-type", "application/x-ndjson")
        .body(
            r#"{"key":"header","value":{"summary":"Test commit"}}
{"key":"lfsFile","value":{"path":"data/test.parquet","algo":"sha256","oid":"sha256...","size":1024}}
"#,
        )
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    let body = response.text().await.unwrap();
    assert!(body.contains("commitUrl"));
    assert!(body.contains("commitOid"));
    assert!(body.contains(commit_oid));
}

/// Test full upload flow: LFS batch → presigned upload → commit.
#[tokio::test]
async fn test_mock_full_upload_flow() {
    let mock = MockHfHub::start().await;
    let test_oid = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    let commit_oid = "abc123def456789012345678901234567890abcd";

    // Mount all mocks (chaining)
    mock.mock_lfs_batch(vec![MockLfsObject::new_upload(test_oid, 1024)])
        .await
        .mock_presigned_upload()
        .await
        .mock_commit(commit_oid)
        .await;

    let client = reqwest::Client::new();

    // Step 1: LFS batch
    let lfs_url = format!(
        "{}/datasets/user/repo.git/info/lfs/objects/batch",
        mock.uri()
    );
    let batch_response = client
        .post(&lfs_url)
        .header("content-type", "application/vnd.git-lfs+json")
        .body(format!(
            r#"{{"operation":"upload","objects":[{{"oid":"{}","size":1024}}]}}"#,
            test_oid
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(batch_response.status(), 200);

    // Step 2: Upload
    let upload_url = format!("{}/upload/{}", mock.uri(), test_oid);
    let upload_response = client
        .put(&upload_url)
        .body(b"test content".to_vec())
        .send()
        .await
        .unwrap();
    assert_eq!(upload_response.status(), 200);

    // Step 3: Commit
    let commit_url = format!("{}/api/datasets/user/repo/commit/main", mock.uri());
    let commit_response = client
        .post(&commit_url)
        .header("content-type", "application/x-ndjson")
        .body(format!(
            r#"{{"key":"header","value":{{"summary":"Upload"}}}}
{{"key":"lfsFile","value":{{"path":"test.parquet","algo":"sha256","oid":"{}","size":1024}}}}
"#,
            test_oid
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(commit_response.status(), 200);

    let body = commit_response.text().await.unwrap();
    assert!(body.contains(commit_oid));
}
