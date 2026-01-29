//! Mock HTTP integration tests for HfSinkNode.
//!
//! Uses wiremock to mock HF Hub API responses for testing the full
//! upload pipeline without making real network calls.

use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
