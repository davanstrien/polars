//! Mock HTTP integration tests for HfSinkNode.
//!
//! Uses wiremock to mock HF Hub API responses for testing the full
//! upload pipeline without making real network calls.
//!
//! Key infrastructure:
//! - `HfSinkOptions::with_api_base_url()` to redirect API calls to mock server
//! - `HFRepoLocation::new(..., Some(base_url))` for custom base URL
//!
//! Example usage pattern:
//! ```ignore
//! let mock_server = MockServer::start().await;
//! let options = HfSinkOptions::builder("user/repo")
//!     .with_api_base_url(mock_server.uri())  // Redirect to mock
//!     .with_token("test_token")
//!     .build()?;
//! ```

use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

use polars_io::cloud::hf::{HFRepoLocation, HfSinkOptions};

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
