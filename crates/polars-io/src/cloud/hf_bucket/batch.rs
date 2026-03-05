//! Bucket batch API — register uploaded files in a bucket.
//!
//! Ports step 4 from `scratch/xet_upload_test/src/main.rs`.

use polars_error::{PolarsResult, polars_bail, to_compute_err};
use reqwest::Client;
use serde::Serialize;

use super::HfBucketConfig;

/// A single operation in a bucket batch request.
///
/// Serializes as NDJSON with `{"type":"addFile","path":"...","xetHash":"..."}`.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BucketOperation {
    #[serde(rename_all = "camelCase")]
    AddFile { path: String, xet_hash: String },
    #[serde(rename_all = "camelCase")]
    DeleteFile { path: String },
}

/// Submit a batch of operations to the bucket API.
///
/// `POST /api/buckets/{namespace}/{name}/batch` with NDJSON body.
pub async fn bucket_batch(
    http: &Client,
    config: &HfBucketConfig,
    operations: &[BucketOperation],
) -> PolarsResult<()> {
    if operations.is_empty() {
        return Ok(());
    }

    let url = format!(
        "{}/api/buckets/{}/{}/batch",
        config.endpoint, config.namespace, config.bucket_name
    );

    let mut body = String::new();
    for op in operations {
        let line = serde_json::to_string(op).map_err(to_compute_err)?;
        body.push_str(&line);
        body.push('\n');
    }

    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {}", config.hf_token))
        .header("Content-Type", "application/x-ndjson")
        .body(body)
        .send()
        .await
        .map_err(to_compute_err)?;

    let status = resp.status();
    if !status.is_success() {
        let resp_body = resp.text().await.unwrap_or_default();

        // Build a bounded summary of operations for the error message.
        let op_summary: String = {
            let max_show = 3;
            let mut parts: Vec<String> = operations
                .iter()
                .take(max_show)
                .map(|op| match op {
                    BucketOperation::AddFile { path, .. } => format!("add:{path}"),
                    BucketOperation::DeleteFile { path } => format!("delete:{path}"),
                })
                .collect();
            if operations.len() > max_show {
                parts.push(format!("(+{} more)", operations.len() - max_show));
            }
            parts.join(", ")
        };

        polars_bail!(
            ComputeError:
            "HF bucket batch API request failed for '{}/{}' (HTTP {}): {}; operations: [{}]",
            config.namespace,
            config.bucket_name,
            status,
            resp_body,
            op_summary
        );
    }

    Ok(())
}
