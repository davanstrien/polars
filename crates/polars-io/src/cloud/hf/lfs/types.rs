//! LFS protocol types for HuggingFace Hub.
//!
//! Implements the Git LFS Batch API types used for file uploads.
//! See: <https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md>

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ============================================================================
// Request Types
// ============================================================================

/// Request body for the LFS batch API.
///
/// Sent to `POST /{repo_type}s/{repo_id}.git/info/lfs/objects/batch`
#[derive(Debug, Clone, Serialize)]
pub struct LfsBatchRequest {
    /// Operation type: "upload" or "download"
    pub operation: LfsOperation,
    /// Supported transfer types (e.g., ["basic", "multipart"])
    pub transfers: Vec<String>,
    /// Objects to upload/download
    pub objects: Vec<LfsObjectRequest>,
}

impl LfsBatchRequest {
    /// Create a new upload request for the given objects.
    pub fn upload(objects: Vec<LfsObjectRequest>) -> Self {
        Self {
            operation: LfsOperation::Upload,
            transfers: vec!["basic".to_string(), "multipart".to_string()],
            objects,
        }
    }
}

/// LFS operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LfsOperation {
    /// Upload files to LFS
    Upload,
    /// Download files from LFS
    Download,
}

/// Object specification in an LFS batch request.
#[derive(Debug, Clone, Serialize)]
pub struct LfsObjectRequest {
    /// SHA256 hash of the file content as lowercase hex (64 characters)
    pub oid: String,
    /// File size in bytes
    pub size: u64,
}

impl LfsObjectRequest {
    /// Create a new object request with the given SHA256 hash and size.
    pub fn new(sha256_hex: impl Into<String>, size: u64) -> Self {
        Self {
            oid: sha256_hex.into(),
            size,
        }
    }
}

// ============================================================================
// Response Types
// ============================================================================

/// Response from the LFS batch API.
#[derive(Debug, Clone, Deserialize)]
pub struct LfsBatchResponse {
    /// Transfer adapter to use ("basic" or "multipart")
    pub transfer: String,
    /// Per-object responses
    pub objects: Vec<LfsObject>,
    /// Hash algorithm (usually "sha256")
    #[serde(default)]
    pub hash_algo: Option<String>,
}

/// Per-object response from LFS batch API.
///
/// Contains either upload/download actions or an error.
#[derive(Debug, Clone, Deserialize)]
pub struct LfsObject {
    /// SHA256 hash of the object
    pub oid: String,
    /// Object size in bytes
    pub size: u64,
    /// Whether the request was authenticated
    #[serde(default)]
    pub authenticated: Option<bool>,
    /// Upload/download actions (None if object already exists)
    #[serde(default)]
    pub actions: Option<LfsActions>,
    /// Error for this object (if operation failed)
    #[serde(default)]
    pub error: Option<LfsObjectError>,
}

impl LfsObject {
    /// Convert this LFS response to an upload action.
    ///
    /// Returns `Err` if the object has an error, otherwise returns the
    /// appropriate transfer action (AlreadyExists, Basic, or Multipart).
    ///
    /// For multipart uploads, HF Hub returns the format:
    /// ```json
    /// {
    ///   "actions": {
    ///     "upload": {
    ///       "href": "https://huggingface.co/api/complete_multipart?...",
    ///       "header": {
    ///         "chunk_size": "16000000",
    ///         "00001": "https://s3.../part1?...",
    ///         "00002": "https://s3.../part2?..."
    ///       }
    ///     }
    ///   }
    /// }
    /// ```
    pub fn into_transfer(self) -> Result<LfsTransfer, LfsObjectError> {
        // Check for per-object error
        if let Some(error) = self.error {
            return Err(error);
        }

        // No actions means object already exists
        match self.actions {
            None => Ok(LfsTransfer::AlreadyExists),
            Some(actions) => {
                if let Some(upload) = actions.upload {
                    // Check for multipart by looking for chunk_size in header
                    if let Some(chunk_size_str) = upload.header.get("chunk_size") {
                        // Parse chunk_size
                        let chunk_size: u64 = chunk_size_str.parse().map_err(|_| LfsObjectError {
                            code: 422,
                            message: format!(
                                "Invalid chunk_size in multipart response: {}",
                                chunk_size_str
                            ),
                        })?;

                        // Extract part URLs from numeric header keys (e.g., "00001", "00002", "1", "2")
                        let mut part_urls: Vec<(u32, String)> = upload
                            .header
                            .iter()
                            .filter_map(|(key, url)| {
                                // Try to parse the key as a number (handles "1", "00001", etc.)
                                key.parse::<u32>().ok().map(|n| (n, url.clone()))
                            })
                            .collect();

                        // Sort by part number to ensure correct upload order
                        part_urls.sort_by_key(|(n, _)| *n);

                        if part_urls.is_empty() {
                            return Err(LfsObjectError {
                                code: 422,
                                message: "No part URLs found in multipart response".to_string(),
                            });
                        }

                        Ok(LfsTransfer::Multipart {
                            completion_url: upload.href,
                            chunk_size,
                            part_urls,
                        })
                    } else {
                        // Basic transfer - no chunk_size means single upload
                        Ok(LfsTransfer::Basic {
                            url: upload.href,
                            headers: upload.header,
                        })
                    }
                } else {
                    // No upload action = already exists
                    Ok(LfsTransfer::AlreadyExists)
                }
            },
        }
    }
}

/// Actions available for an LFS object.
#[derive(Debug, Clone, Deserialize)]
pub struct LfsActions {
    /// Upload action (for basic transfer)
    #[serde(default)]
    pub upload: Option<LfsAction>,
    /// Verify action (optional, for post-upload verification)
    #[serde(default)]
    pub verify: Option<LfsAction>,
    /// Parts for multipart upload
    #[serde(default)]
    pub parts: Option<Vec<LfsPartInfo>>,
}

/// A single LFS action (upload, download, or verify).
#[derive(Debug, Clone, Deserialize)]
pub struct LfsAction {
    /// URL to upload/download to/from
    pub href: String,
    /// Additional headers to include in the request
    #[serde(default)]
    pub header: HashMap<String, String>,
    /// Expiration time (RFC3339 format)
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// Information about a single part in a multipart upload.
#[derive(Debug, Clone, Deserialize)]
pub struct LfsPartInfo {
    /// Part number (1-indexed)
    pub part_number: u32,
    /// Size of this part in bytes
    pub size: u64,
    /// Presigned URL to upload this part
    pub href: String,
}

// ============================================================================
// Error Types
// ============================================================================

/// Per-object error from LFS API.
#[derive(Debug, Clone, Deserialize)]
pub struct LfsObjectError {
    /// HTTP status code
    pub code: u32,
    /// Error message
    pub message: String,
}

impl std::fmt::Display for LfsObjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LFS error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for LfsObjectError {}

// ============================================================================
// Transfer Action Enum
// ============================================================================

/// Represents the upload action to take for a file.
///
/// This enum abstracts the three possible outcomes from the LFS batch API:
/// - File already exists (skip upload)
/// - Basic upload (single PUT request)
/// - Multipart upload (multiple PUT requests to S3, then POST to completion URL)
#[derive(Debug, Clone)]
pub enum LfsTransfer {
    /// File already exists on HF Hub - skip upload.
    AlreadyExists,
    /// Basic upload - single PUT request to the URL with given headers.
    Basic {
        /// Presigned upload URL
        url: String,
        /// Headers to include in the PUT request
        headers: HashMap<String, String>,
    },
    /// Multipart upload - upload chunks to S3, then complete via HF Hub API.
    ///
    /// HF Hub returns multipart info in `actions.upload` with:
    /// - `href`: The completion URL to POST to after all parts uploaded
    /// - `header.chunk_size`: Size of each part in bytes
    /// - `header["00001"]`, `header["00002"]`, etc.: Presigned S3 URLs for each part
    Multipart {
        /// URL to POST completion request to (from `actions.upload.href`)
        completion_url: String,
        /// Size of each chunk in bytes (from `header.chunk_size`)
        chunk_size: u64,
        /// Presigned S3 URLs for each part, sorted by part number.
        /// Each tuple is (part_number, presigned_url).
        part_urls: Vec<(u32, String)>,
    },
}

impl LfsTransfer {
    /// Returns `true` if this is an `AlreadyExists` variant.
    pub fn is_already_exists(&self) -> bool {
        matches!(self, Self::AlreadyExists)
    }

    /// Returns `true` if this requires an upload.
    pub fn requires_upload(&self) -> bool {
        !self.is_already_exists()
    }
}

// ============================================================================
// Multipart Completion Types
// ============================================================================

/// Part completion info with ETag from S3 response.
///
/// After uploading a part to the presigned S3 URL, the response includes
/// an ETag header. This struct captures that for the completion request.
///
/// Note: HF Hub expects `partNumber` (camelCase) in the JSON payload.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LfsPartCompletion {
    /// Part number (1-indexed)
    pub part_number: u32,
    /// ETag from S3 response header (includes surrounding quotes)
    pub etag: String,
}

/// Request to complete a multipart upload.
///
/// Sent after all parts have been uploaded to S3.
#[derive(Debug, Clone, Serialize)]
pub struct LfsMultipartCompleteRequest {
    /// SHA256 hash of the complete file
    pub oid: String,
    /// Completion info for each uploaded part
    pub parts: Vec<LfsPartCompletion>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_batch_request() {
        let request = LfsBatchRequest::upload(vec![
            LfsObjectRequest::new("abc123def456", 1024),
            LfsObjectRequest::new("789xyz", 2048),
        ]);

        let json = serde_json::to_string(&request).unwrap();

        // Verify operation is lowercase
        assert!(json.contains(r#""operation":"upload""#));
        // Verify transfers
        assert!(json.contains(r#""transfers":["basic","multipart"]"#));
        // Verify objects
        assert!(json.contains(r#""oid":"abc123def456""#));
        assert!(json.contains(r#""size":1024"#));
    }

    #[test]
    fn test_deserialize_basic_upload() {
        let json = r#"{
            "transfer": "basic",
            "objects": [{
                "oid": "abc123",
                "size": 1024,
                "authenticated": true,
                "actions": {
                    "upload": {
                        "href": "https://example.com/upload",
                        "header": {
                            "Content-Type": "application/octet-stream"
                        },
                        "expires_at": "2026-01-15T12:00:00Z"
                    }
                }
            }]
        }"#;

        let response: LfsBatchResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.transfer, "basic");
        assert_eq!(response.objects.len(), 1);

        let obj = &response.objects[0];
        assert_eq!(obj.oid, "abc123");
        assert_eq!(obj.size, 1024);
        assert_eq!(obj.authenticated, Some(true));
        assert!(obj.error.is_none());

        let actions = obj.actions.as_ref().unwrap();
        let upload = actions.upload.as_ref().unwrap();
        assert_eq!(upload.href, "https://example.com/upload");
        assert_eq!(
            upload.header.get("Content-Type"),
            Some(&"application/octet-stream".to_string())
        );
    }

    #[test]
    fn test_deserialize_multipart_upload() {
        // Test the actual HF Hub multipart format with numeric header keys
        let json = r#"{
            "transfer": "multipart",
            "objects": [{
                "oid": "large_file_hash",
                "size": 104857600,
                "authenticated": true,
                "actions": {
                    "upload": {
                        "href": "https://huggingface.co/api/complete_multipart?uploadId=xyz",
                        "header": {
                            "chunk_size": "16000000",
                            "00001": "https://s3.example.com/part1?partNumber=1",
                            "00002": "https://s3.example.com/part2?partNumber=2"
                        }
                    }
                }
            }]
        }"#;

        let response: LfsBatchResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.transfer, "multipart");

        let obj = response.objects.into_iter().next().unwrap();
        let transfer = obj.into_transfer().unwrap();

        match transfer {
            LfsTransfer::Multipart {
                completion_url,
                chunk_size,
                part_urls,
            } => {
                assert_eq!(
                    completion_url,
                    "https://huggingface.co/api/complete_multipart?uploadId=xyz"
                );
                assert_eq!(chunk_size, 16000000);
                assert_eq!(part_urls.len(), 2);
                assert_eq!(part_urls[0].0, 1); // part number
                assert!(part_urls[0].1.contains("part1"));
                assert_eq!(part_urls[1].0, 2);
                assert!(part_urls[1].1.contains("part2"));
            },
            _ => panic!("Expected Multipart transfer"),
        }
    }

    #[test]
    fn test_deserialize_already_exists() {
        // When a file already exists, there are no actions
        let json = r#"{
            "transfer": "basic",
            "objects": [{
                "oid": "existing_hash",
                "size": 1024,
                "authenticated": true
            }]
        }"#;

        let response: LfsBatchResponse = serde_json::from_str(json).unwrap();

        let obj = &response.objects[0];
        assert!(obj.actions.is_none());
        assert!(obj.error.is_none());
    }

    #[test]
    fn test_deserialize_with_error() {
        let json = r#"{
            "transfer": "basic",
            "objects": [{
                "oid": "bad_hash",
                "size": 1024,
                "error": {
                    "code": 422,
                    "message": "Invalid object size"
                }
            }]
        }"#;

        let response: LfsBatchResponse = serde_json::from_str(json).unwrap();

        let obj = &response.objects[0];
        let error = obj.error.as_ref().unwrap();
        assert_eq!(error.code, 422);
        assert_eq!(error.message, "Invalid object size");
    }

    #[test]
    fn test_into_transfer_basic() {
        let obj = LfsObject {
            oid: "abc123".to_string(),
            size: 1024,
            authenticated: Some(true),
            actions: Some(LfsActions {
                upload: Some(LfsAction {
                    href: "https://upload.example.com".to_string(),
                    header: [("X-Custom".to_string(), "value".to_string())]
                        .into_iter()
                        .collect(),
                    expires_at: None,
                }),
                verify: None,
                parts: None,
            }),
            error: None,
        };

        let transfer = obj.into_transfer().unwrap();

        match transfer {
            LfsTransfer::Basic { url, headers } => {
                assert_eq!(url, "https://upload.example.com");
                assert_eq!(headers.get("X-Custom"), Some(&"value".to_string()));
            },
            _ => panic!("Expected Basic transfer"),
        }
    }

    #[test]
    fn test_into_transfer_multipart() {
        // Multipart is detected by presence of chunk_size in header
        let obj = LfsObject {
            oid: "large_hash".to_string(),
            size: 100_000_000,
            authenticated: Some(true),
            actions: Some(LfsActions {
                upload: Some(LfsAction {
                    href: "https://huggingface.co/api/complete_multipart?uploadId=abc".to_string(),
                    header: [
                        ("chunk_size".to_string(), "50000000".to_string()),
                        ("1".to_string(), "https://s3.example.com/part1".to_string()),
                        ("2".to_string(), "https://s3.example.com/part2".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    expires_at: None,
                }),
                verify: None,
                parts: None,
            }),
            error: None,
        };

        let transfer = obj.into_transfer().unwrap();

        match transfer {
            LfsTransfer::Multipart {
                completion_url,
                chunk_size,
                part_urls,
            } => {
                assert_eq!(
                    completion_url,
                    "https://huggingface.co/api/complete_multipart?uploadId=abc"
                );
                assert_eq!(chunk_size, 50_000_000);
                assert_eq!(part_urls.len(), 2);
                assert_eq!(part_urls[0].0, 1);
                assert_eq!(part_urls[1].0, 2);
            },
            _ => panic!("Expected Multipart transfer"),
        }
    }

    #[test]
    fn test_into_transfer_already_exists() {
        let obj = LfsObject {
            oid: "existing_hash".to_string(),
            size: 1024,
            authenticated: Some(true),
            actions: None,
            error: None,
        };

        let transfer = obj.into_transfer().unwrap();

        assert!(matches!(transfer, LfsTransfer::AlreadyExists));
        assert!(transfer.is_already_exists());
        assert!(!transfer.requires_upload());
    }

    #[test]
    fn test_into_transfer_error() {
        let obj = LfsObject {
            oid: "bad_hash".to_string(),
            size: 1024,
            authenticated: None,
            actions: None,
            error: Some(LfsObjectError {
                code: 403,
                message: "Permission denied".to_string(),
            }),
        };

        let result = obj.into_transfer();
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.code, 403);
        assert_eq!(error.message, "Permission denied");
    }

    #[test]
    fn test_lfs_object_error_display() {
        let error = LfsObjectError {
            code: 422,
            message: "Invalid size".to_string(),
        };

        assert_eq!(error.to_string(), "LFS error 422: Invalid size");
    }

    #[test]
    fn test_request_roundtrip() {
        let request = LfsBatchRequest::upload(vec![LfsObjectRequest::new(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            0,
        )]);

        // Serialize to JSON and back (as a Value for verification)
        let json = serde_json::to_value(&request).unwrap();

        assert_eq!(json["operation"], "upload");
        assert!(
            json["transfers"]
                .as_array()
                .unwrap()
                .contains(&"basic".into())
        );
        assert!(
            json["transfers"]
                .as_array()
                .unwrap()
                .contains(&"multipart".into())
        );
        assert_eq!(
            json["objects"][0]["oid"],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(json["objects"][0]["size"], 0);
    }

    #[test]
    fn test_part_completion_serialization() {
        let completion = LfsPartCompletion {
            part_number: 1,
            etag: "\"abc123def456\"".to_string(),
        };

        let json = serde_json::to_string(&completion).unwrap();

        // HF Hub expects camelCase: "partNumber" not "part_number"
        assert!(json.contains("\"partNumber\":1"));
        assert!(json.contains("\"etag\":"));
        // ETag value should be preserved with quotes
        assert!(json.contains("abc123def456"));
    }

    #[test]
    fn test_multipart_complete_request_serialization() {
        let request = LfsMultipartCompleteRequest {
            oid: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855".to_string(),
            parts: vec![
                LfsPartCompletion {
                    part_number: 1,
                    etag: "\"etag1\"".to_string(),
                },
                LfsPartCompletion {
                    part_number: 2,
                    etag: "\"etag2\"".to_string(),
                },
            ],
        };

        let json = serde_json::to_value(&request).unwrap();

        assert_eq!(
            json["oid"],
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(json["parts"].as_array().unwrap().len(), 2);
        // HF Hub expects camelCase: "partNumber"
        assert_eq!(json["parts"][0]["partNumber"], 1);
        assert_eq!(json["parts"][1]["partNumber"], 2);
    }
}
