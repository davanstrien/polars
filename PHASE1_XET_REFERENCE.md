# Phase 1: XET Upload & Bucket Batch API Reference

> Rust reference for XET streaming uploads and HF Bucket batch operations.
> Source: OpenDAL service at `opendal/core/services/huggingface/src/`.
> Fork: `kszucs/xet-core` branch `download_bytes`.

---

## A) XetClient Creation

**File**: `opendal/core/services/huggingface/src/core.rs` (lines 384–395)

```rust
#[cfg(feature = "xet")]
pub(super) async fn xet_client(&self, token_type: &'static str) -> Result<XetClient> {
    let token = self.xet_token(token_type).await?;
    let refresher = Arc::new(XetTokenRefresher::new(self, token_type));
    XetClient::new(
        Some(token.cas_url),                       // CAS endpoint URL
        Some((token.access_token, token.exp)),     // (token, expiry_timestamp)
        Some(refresher),                           // auto-refresh callback
        "opendal/1.0".to_string(),                 // user agent
    )
    .map_err(map_xet_error)
}
```

**Key**: `XetClient::new()` takes:
1. `Option<String>` — CAS URL (from token response)
2. `Option<(String, u64)>` — (access_token, expiry_epoch)
3. `Option<Arc<dyn TokenRefresher>>` — auto-refresh on expiry
4. `String` — user agent string

For Polars, user agent should be `"polars/<version>"`.

---

## B) XetWriter Flow

**File**: `opendal/core/services/huggingface/src/writer.rs` (lines 51–68, 108–187)

### Create writer

```rust
// Buckets always use XET — no preupload endpoint needed
let client = core.xet_client("write").await?;
let writer = client.write(None).await.map_err(map_xet_error)?;
// writer is xet_data::streaming::XetWriter
```

### Write bytes (streaming)

```rust
// writer.rs line 108–123
impl oio::Write for HfWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        match self {
            HfWriter::Xet { writer, .. } => writer
                .get_mut()
                .unwrap()
                .write(bs.to_bytes())       // accepts bytes::Bytes
                .await
                .map_err(map_xet_error),
            ...
        }
    }
}
```

### Close and get file info

```rust
// writer.rs line 146–163
HfWriter::Xet { core, path, writer } => {
    let file_info = writer
        .get_mut()
        .unwrap()
        .close()
        .await
        .map_err(map_xet_error)?;

    // For bucket repos: register via batch API
    if core.repo.repo_type == RepoType::Bucket {
        let xet_hash = file_info.hash().to_string();
        let operation = BucketOperation::AddFile {
            path: path.clone(),
            xet_hash,
        };
        core.bucket_batch(vec![operation]).await?;
    }
}
```

### XetFileInfo properties

```rust
file_info.hash()       // → xet hash string (used for bucket AddFile)
file_info.file_size()  // → u64 (total bytes uploaded)
file_info.sha256()     // → Option<...> (for LFS compat — not needed for buckets)
```

### Writer lifecycle for bucket sink

```
XetClient::new(cas_url, token, refresher, user_agent)
    → client.write(None)
        → XetWriter
            → .write(bytes) [called per chunk/morsel — streaming!]
            → .write(bytes)
            → ...
            → .close()
                → XetFileInfo { hash, file_size }
                    → BucketOperation::AddFile { path, xet_hash }
```

### Abort

```rust
// writer.rs line 189–200
HfWriter::Xet { writer, .. } => {
    let _ = writer.get_mut().unwrap().abort().await;
}
```

---

## C) BucketOperation Type

**File**: `opendal/core/services/huggingface/src/core.rs` (lines 89–99)

```rust
#[cfg(feature = "xet")]
#[derive(Debug, serde::Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(super) enum BucketOperation {
    #[serde(rename_all = "camelCase")]
    AddFile { path: String, xet_hash: String },
    #[serde(rename_all = "camelCase")]
    DeleteFile { path: String },
}
```

**Serialized JSON examples**:

```json
{"type":"addFile","path":"data/shard_00001.parquet","xetHash":"abc123..."}
{"type":"deleteFile","path":"data/old_file.parquet"}
```

Note: `#[serde(tag = "type")]` produces a tagged union. `rename_all = "camelCase"` converts field names.

---

## D) bucket_batch() Function

**File**: `opendal/core/services/huggingface/src/core.rs` (lines 532–566)

```rust
/// Upload files to a bucket using the batch API.
///
/// Sends operations as JSON lines (one operation per line).
#[cfg(feature = "xet")]
pub(super) async fn bucket_batch(&self, operations: Vec<BucketOperation>) -> Result<()> {
    let _token = self.token.as_deref().ok_or_else(|| {
        Error::new(ErrorKind::PermissionDenied, "token is required for bucket operations")
    })?;

    if operations.is_empty() {
        return Err(Error::new(ErrorKind::Unexpected, "no operations to perform"));
    }

    let url = self.repo.bucket_batch_url(&self.endpoint);

    // Serialize as NDJSON (one JSON object per line)
    let mut body = String::new();
    for op in operations {
        let json = serde_json::to_string(&op).map_err(new_json_serialize_error)?;
        body.push_str(&json);
        body.push('\n');
    }

    let req = self
        .request(http::Method::POST, &url, Operation::Write)
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .header(header::CONTENT_LENGTH, body.len())
        .body(Buffer::from(Bytes::from(body)))
        .map_err(new_request_build_error)?;

    self.send(req).await?;
    Ok(())
}
```

**Key details**:
- Format: NDJSON (newline-delimited JSON) — one JSON object per line
- Content-Type: `application/x-ndjson`
- Endpoint: `POST /api/buckets/{namespace}/{name}/batch`
- Auth: Bearer token in request header
- Can batch multiple operations (add + delete) in a single request

---

## E) Token Management

**File**: `opendal/core/services/huggingface/src/core.rs` (lines 179–215)

### XetToken struct

```rust
#[cfg(feature = "xet")]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct XetToken {
    pub access_token: String,  // JWT for CAS
    pub cas_url: String,       // CAS endpoint (e.g., "https://cas.huggingface.co")
    pub exp: u64,              // Expiry timestamp (Unix epoch)
}
```

### Token fetching

```rust
#[cfg(feature = "xet")]
pub(super) async fn xet_token(&self, token_type: &str) -> Result<XetToken> {
    let url = self.repo.xet_token_url(&self.endpoint, token_type);
    let req = self
        .request(http::Method::GET, &url, Operation::Read)
        .body(Buffer::new())
        .map_err(new_request_build_error)?;
    let (_, token) = self.send_parse(req).await?;
    Ok(token)
}
```

### Token refresher

```rust
#[cfg(feature = "xet")]
pub(super) struct XetTokenRefresher {
    core: HfCore,
    token_type: &'static str,
}

#[cfg(feature = "xet")]
#[async_trait::async_trait]
impl TokenRefresher for XetTokenRefresher {
    async fn refresh(&self) -> std::result::Result<(String, u64), xet_utils::errors::AuthError> {
        let token = self.core.xet_token(self.token_type).await
            .map_err(xet_utils::errors::AuthError::token_refresh_failure)?;
        Ok((token.access_token, token.exp))
    }
}
```

**Key**: `TokenRefresher` is a trait from `xet_utils::auth`. The `XetClient` calls `refresh()` automatically when the token expires. This means long-running uploads won't fail due to token expiry.

---

## F) API Endpoints

**File**: `opendal/core/services/huggingface/src/uri.rs` (lines 104–148)

### XET write token

```
GET {endpoint}/api/buckets/{namespace}/{name}/xet-write-token
Authorization: Bearer {hf_token}
→ Response: { "accessToken": "...", "casUrl": "https://...", "exp": 1234567890 }
```

```rust
// uri.rs line 122-141
pub fn xet_token_url(&self, endpoint: &str, token_type: &str) -> String {
    match self.repo_type {
        RepoType::Bucket => format!(
            "{}/api/buckets/{}/xet-{}-token",
            endpoint, &self.repo_id, token_type
        ),
        _ => format!(
            "{}/api/{}/{}/xet-{}-token/{}",
            endpoint, self.repo_type.as_plural_str(),
            &self.repo_id, token_type, self.revision(),
        ),
    }
}
```

### Bucket batch

```
POST {endpoint}/api/buckets/{namespace}/{name}/batch
Authorization: Bearer {hf_token}
Content-Type: application/x-ndjson
Body:
  {"type":"addFile","path":"shard_001.parquet","xetHash":"..."}
  {"type":"addFile","path":"shard_002.parquet","xetHash":"..."}
```

```rust
// uri.rs line 144-148
pub fn bucket_batch_url(&self, endpoint: &str) -> String {
    format!("{}/api/buckets/{}/batch", endpoint, &self.repo_id)
}
```

### Paths info

```
GET {endpoint}/api/buckets/{namespace}/{name}/paths-info
```

```rust
// uri.rs line 103-118
pub fn paths_info_url(&self, endpoint: &str) -> String {
    match self.repo_type {
        RepoType::Bucket => format!(
            "{}/api/buckets/{}/paths-info", endpoint, &self.repo_id
        ),
        _ => ...
    }
}
```

### Resolve (download)

```
GET {endpoint}/buckets/{namespace}/{name}/resolve/{path}
```

(Note: resolve URL does NOT have `/api/` prefix.)

### Summary table

| Operation | Method | URL Pattern |
|-----------|--------|-------------|
| XET write token | `GET` | `/api/buckets/{ns}/{name}/xet-write-token` |
| XET read token | `GET` | `/api/buckets/{ns}/{name}/xet-read-token` |
| Bucket batch | `POST` | `/api/buckets/{ns}/{name}/batch` |
| Paths info | `GET` | `/api/buckets/{ns}/{name}/paths-info` |
| Resolve/download | `GET` | `/buckets/{ns}/{name}/resolve/{path}` |

---

## G) kszucs/xet-core Fork

### Why the fork?

The `streaming` module (`XetClient`, `XetWriter`) is **NOT in the main `huggingface/xet-core` repo yet**. It exists only in `kszucs/xet-core` on the `download_bytes` branch.

### Crates needed

| Cargo name | Package name | What it provides |
|-----------|--------------|-----------------|
| `xet-data` | `data` | `streaming::XetClient`, `streaming::XetWriter`, `XetFileInfo` |
| `xet-utils` | `utils` | `auth::TokenRefresher` trait |
| `cas_types` | `cas_types` | `FileRange` and CAS protocol types |

### Key imports (from OpenDAL writer.rs)

```rust
use xet_data::streaming::XetClient;
use xet_data::streaming::XetWriter;
use xet_data::XetFileInfo;
use xet_utils::auth::TokenRefresher;
use cas_types::FileRange;
```

### Dependency declarations

```toml
[dependencies]
xet-data = { package = "data", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
xet-utils = { package = "utils", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
cas_types = { git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
async-trait = { version = "0.1", optional = true }
```

### Tracking

When `streaming` module merges to main `huggingface/xet-core`, update the git URL from `kszucs/xet-core` to `huggingface/xet-core` and remove the branch specifier.

---

## H) Data Flow Diagram for Bucket Sink

```
┌─────────────────────────────────────────────────────────────────┐
│ Polars Streaming Engine                                         │
│                                                                 │
│  morsel (DataFrame)                                             │
│    ↓                                                            │
│  Parquet encoder (row group)                                    │
│    ↓                                                            │
│  bytes::Bytes                                                   │
│    ↓                                                            │
│  ┌──────────────────────────────────────────────────────┐       │
│  │ HfBucketSinkNode (implements SinkNode)               │       │
│  │                                                      │       │
│  │  Per shard:                                          │       │
│  │  ┌─────────────────────────────────────────────┐     │       │
│  │  │ XetWriter.write(bytes)  ──→  CAS storage    │     │       │
│  │  │ XetWriter.write(bytes)  ──→  (streaming)    │     │       │
│  │  │ ...                                         │     │       │
│  │  │ XetWriter.close()       ──→  XetFileInfo    │     │       │
│  │  │   └─ { hash, file_size }                    │     │       │
│  │  └─────────────────────────────────────────────┘     │       │
│  │                                                      │       │
│  │  Accumulate: Vec<(path, xet_hash)>                   │       │
│  │                                                      │       │
│  │  On finalize():                                      │       │
│  │  ┌─────────────────────────────────────────────┐     │       │
│  │  │ POST /api/buckets/{id}/batch                │     │       │
│  │  │ Content-Type: application/x-ndjson          │     │       │
│  │  │                                             │     │       │
│  │  │ {"type":"addFile","path":"shard_001.parquet",│     │       │
│  │  │  "xetHash":"abc..."}                        │     │       │
│  │  │ {"type":"addFile","path":"shard_002.parquet",│     │       │
│  │  │  "xetHash":"def..."}                        │     │       │
│  │  └─────────────────────────────────────────────┘     │       │
│  └──────────────────────────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

### Memory model

- **Parquet row group buffer**: ~64–256 MB (configurable via `row_group_size`)
- **XetWriter internal buffer**: managed by xet-core, streams to CAS
- **No shard-level buffering**: bytes flow through XetWriter as produced
- **Peak memory**: O(row_group_size), NOT O(shard_size)

### Error recovery

- Each shard uploads independently
- If a shard fails mid-upload: `XetWriter::abort()`
- On finalize failure: bucket already has data from successful shards
- Re-run: check existing files via `paths-info` API, skip already-uploaded shards

### Parallelism options

Two strategies for multi-shard uploads:

1. **Sequential** (simpler): One `XetWriter` at a time. New writer per shard. Works with `is_sink_input_parallel() → false`.
2. **Parallel** (faster): Multiple `XetWriter` instances, one per concurrent shard. Works with `is_sink_input_parallel() → true`. Each parallel pipeline gets its own writer.

Recommendation: Start with sequential (option 1) for correctness, optimize later.

---

## Implementation Checklist for Bucket Sink XET Integration

| Step | What | Reference |
|------|------|-----------|
| 1 | Add xet-core deps to `polars-io/Cargo.toml` | Section G |
| 2 | Implement `XetTokenFetcher` (HTTP GET for token) | Section E |
| 3 | Implement `TokenRefresher` trait for auto-refresh | Section E |
| 4 | Create `XetClient` with token + refresher | Section A |
| 5 | Per shard: `client.write(None)` → `XetWriter` | Section B |
| 6 | Stream bytes: `writer.write(bytes)` per morsel | Section B |
| 7 | Close shard: `writer.close()` → `XetFileInfo` | Section B |
| 8 | Accumulate `(path, xet_hash)` pairs | Section C |
| 9 | On finalize: build `Vec<BucketOperation::AddFile>` | Section C |
| 10 | Call `bucket_batch()` with NDJSON payload | Section D |
