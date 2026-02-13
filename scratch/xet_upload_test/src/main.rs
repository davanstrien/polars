use bytes::Bytes;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::env;

// --- API types ---

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct XetToken {
    access_token: String,
    cas_url: String,
    exp: u64,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum BucketOperation {
    #[serde(rename_all = "camelCase")]
    AddFile { path: String, xet_hash: String },
}

// --- Config ---

struct Config {
    hf_token: String,
    namespace: String,
    bucket_name: String,
    endpoint: String,
}

impl Config {
    fn from_env() -> Result<Self, String> {
        let hf_token =
            env::var("HF_TOKEN").map_err(|_| "HF_TOKEN env var is required".to_string())?;
        let namespace = env::var("HF_BUCKET_NAMESPACE")
            .map_err(|_| "HF_BUCKET_NAMESPACE env var is required".to_string())?;
        let bucket_name = env::var("HF_BUCKET_NAME")
            .map_err(|_| "HF_BUCKET_NAME env var is required".to_string())?;
        let endpoint = env::var("HF_ENDPOINT")
            .unwrap_or_else(|_| "https://huggingface.co".to_string());
        Ok(Config {
            hf_token,
            namespace,
            bucket_name,
            endpoint,
        })
    }
}

// --- Step functions ---

async fn step1_fetch_xet_token(
    http: &Client,
    config: &Config,
) -> Result<XetToken, Box<dyn std::error::Error>> {
    println!("[1/5] Fetching XET write token...");

    let url = format!(
        "{}/api/buckets/{}/{}/xet-write-token",
        config.endpoint, config.namespace, config.bucket_name
    );

    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {}", config.hf_token))
        .send()
        .await?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        return Err(format!(
            "XET write token request failed (HTTP {}): {}",
            status, body
        )
        .into());
    }

    let token: XetToken = resp.json().await?;

    let token_prefix = if token.access_token.len() > 10 {
        &token.access_token[..10]
    } else {
        &token.access_token
    };
    println!(
        "      OK (token={}..., cas_url={}, exp={})",
        token_prefix, token.cas_url, token.exp
    );
    Ok(token)
}

fn step2_create_client(
    token: &XetToken,
) -> Result<xet_data::streaming::XetClient, Box<dyn std::error::Error>> {
    println!("[2/5] Creating XetClient...");

    let client = xet_data::streaming::XetClient::new(
        Some(token.cas_url.clone()),
        Some((token.access_token.clone(), token.exp)),
        None, // no token refresher for this quick test
        "polars-xet-test/0.1".to_string(),
    )?;

    println!("      OK");
    Ok(client)
}

async fn step3_upload_data(
    client: &xet_data::streaming::XetClient,
) -> Result<xet_data::XetFileInfo, Box<dyn std::error::Error>> {
    println!("[3/5] Uploading test data via XetWriter...");

    let mut writer = client.write(None).await?;
    let test_data = b"Hello from Polars XET upload test!\n".repeat(100);
    writer.write(Bytes::from(test_data)).await?;
    let file_info = writer.close().await?;

    println!(
        "      OK (hash={}, size={})",
        file_info.hash(),
        file_info.file_size()
    );
    Ok(file_info)
}

async fn step4_register_file(
    http: &Client,
    config: &Config,
    file_info: &xet_data::XetFileInfo,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[4/5] Registering file via bucket batch API...");

    let url = format!(
        "{}/api/buckets/{}/{}/batch",
        config.endpoint, config.namespace, config.bucket_name
    );

    let op = BucketOperation::AddFile {
        path: "test_upload.txt".to_string(),
        xet_hash: file_info.hash().to_string(),
    };
    let body = serde_json::to_string(&op)? + "\n";

    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {}", config.hf_token))
        .header("Content-Type", "application/x-ndjson")
        .body(body)
        .send()
        .await?;

    let status = resp.status();
    let resp_body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(format!(
            "Batch API request failed (HTTP {}): {}",
            status, resp_body
        )
        .into());
    }

    println!("      OK (status={}, response={})", status, resp_body);
    Ok(())
}

async fn step5_verify_file(
    http: &Client,
    config: &Config,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("[5/5] Verifying file exists...");

    let url = format!(
        "{}/api/buckets/{}/{}/paths-info",
        config.endpoint, config.namespace, config.bucket_name
    );

    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {}", config.hf_token))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({ "paths": ["test_upload.txt"] }))
        .send()
        .await?;

    let status = resp.status();
    let resp_body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        return Err(format!(
            "Paths-info request failed (HTTP {}): {}",
            status, resp_body
        )
        .into());
    }

    println!("      OK (status={}, response={})", status, resp_body);
    Ok(())
}

// --- Main ---

#[tokio::main]
async fn main() {
    println!("=== XET Upload Test ===\n");

    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            eprintln!("\nRequired env vars:");
            eprintln!("  HF_TOKEN             - HuggingFace access token");
            eprintln!("  HF_BUCKET_NAMESPACE  - e.g. 'davanstrien'");
            eprintln!("  HF_BUCKET_NAME       - bucket name");
            eprintln!("\nOptional:");
            eprintln!("  HF_ENDPOINT          - defaults to https://huggingface.co");
            std::process::exit(1);
        },
    };

    println!(
        "Bucket: {}/{} @ {}\n",
        config.namespace, config.bucket_name, config.endpoint
    );

    let http = Client::new();

    // Step 1: Fetch XET write token
    let token = match step1_fetch_xet_token(&http, &config).await {
        Ok(t) => t,
        Err(e) => {
            eprintln!("\n[FAIL] Step 1 failed: {}", e);
            std::process::exit(1);
        },
    };

    // Step 2: Create XetClient
    let client = match step2_create_client(&token) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("\n[FAIL] Step 2 failed: {}", e);
            std::process::exit(1);
        },
    };

    // Step 3: Upload data
    let file_info = match step3_upload_data(&client).await {
        Ok(fi) => fi,
        Err(e) => {
            eprintln!("\n[FAIL] Step 3 failed: {}", e);
            std::process::exit(1);
        },
    };

    // Step 4: Register file
    if let Err(e) = step4_register_file(&http, &config, &file_info).await {
        eprintln!("\n[FAIL] Step 4 failed: {}", e);
        std::process::exit(1);
    }

    // Step 5: Verify
    if let Err(e) = step5_verify_file(&http, &config).await {
        eprintln!("\n[FAIL] Step 5 failed: {}", e);
        std::process::exit(1);
    }

    println!("\n=== All steps passed! ===");
}
