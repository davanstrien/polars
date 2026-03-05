# XetSession Refactor Tracking

## Goal

Refactor the HF bucket sink from `subxet` (low-level `XetClient`/`XetWriter`) to the official `xet-session` API (`XetSession`/`UploadCommit`/`SingleFileCleaner`). This reduces the code diff for an upstream PR and uses the official HF XET APIs.

## Branch

`feature/hf-bucket-xet-session` (off `feature/hf-bucket-sink`)

## Status

- [ ] Swap deps in Cargo.toml (subxet -> xet-session + xet-data + xet-utils)
- [ ] Rewrite `xet_upload.rs` — remove BucketWriter, add create_xet_session()
- [ ] Rewrite `streaming_upload.rs` — use SingleFileCleaner instead of XetWriter
- [ ] Update `mod.rs` — upload_and_register_file()
- [ ] cargo check/build/test passes
- [ ] CI wheel build succeeds
- [ ] E2E smoke test passes

## Key Files

| File | Change |
|------|--------|
| `crates/polars-io/Cargo.toml` | Dep swap |
| `crates/polars-io/src/cloud/hf_bucket/xet_upload.rs` | Major rewrite (133 -> ~50 lines) |
| `crates/polars-io/src/cloud/hf_bucket/streaming_upload.rs` | Rewrite (252 -> ~150 lines) |
| `crates/polars-io/src/cloud/hf_bucket/mod.rs` | Update upload_and_register_file() |
| `crates/polars-io/src/cloud/hf_bucket/batch.rs` | No changes |
| `crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink.rs` | No changes |

## XetSession API Reference (from xet-core)

```rust
// Session creation
XetSessionBuilder::new()
    .with_endpoint(cas_url)
    .with_token_info(token, expiry)
    .with_token_refresher(Arc<dyn TokenRefresher>)
    .build() -> Result<XetSession>

// Upload commit
session.new_upload_commit() -> Result<UploadCommit>
commit.upload_file(name, size) -> Result<(TaskHandle, SingleFileCleaner)>

// Streaming (async)
cleaner.add_data(&[u8]).await -> Result<()>
cleaner.finish().await -> Result<(XetFileInfo, DeduplicationMetrics)>

// TokenRefresher trait (utils::auth)
async fn refresh(&self) -> Result<(String, u64), AuthError>
```

## Risks

1. **Multiple git deps from same repo** — Cargo handles natively
2. **XetSession owns its own tokio runtime** — coexists with pl_async
3. **SingleFileCleaner not re-exported from xet-session** — need direct dep on `data` crate
