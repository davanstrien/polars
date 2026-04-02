# Implementation Review: HF Bucket Sink for Polars

You are reviewing a proof-of-concept feature that adds a streaming parquet sink to HuggingFace Buckets in Polars. The feature writes parquet data to HF Buckets via the XET protocol, streaming encoded bytes through a bounded channel to avoid buffering the full dataset in memory.

The implementation is on the `feature/hf-bucket-sink` branch of a Polars fork. All changes are gated behind `#[cfg(feature = "hf_bucket_sink")]`. The goal is to eventually upstream this to the main Polars repo.

## Your Task

Review the implementation for correctness, safety, Polars idioms, and upstreamability. Assume you have no prior context — read every file listed below and form your own assessment.

**Do not assume the code is correct.** Look for real issues. The author wants to know what would need to change before an upstream PR.

## Files to Review

### Core streaming engine integration (polars-stream)

These files contain small additions to the Polars streaming engine. The changes should be minimal, idiomatic, and cleanly gated.

1. **`crates/polars-stream/src/physical_plan/lower_ir.rs`** — Search for `hf://buckets` and `hf_bucket_sink`. This is where the `hf://buckets/` URL is intercepted and routed to `HfBucketSink` instead of `FileSink`. Review: Is the URL detection correct? Is the feature gating clean? Does the fallback error for missing feature make sense?

2. **`crates/polars-stream/src/physical_plan/mod.rs`** — Search for `HfBucketSink`. This adds the `HfBucketSink` variant to `PhysNodeKind` and a visit arm. Review: Does it follow the same pattern as other sink variants?

3. **`crates/polars-stream/src/physical_plan/to_graph.rs`** — Search for `HfBucketSink`. This wires the physical plan node into the execution graph. Review: Does it match the pattern of `FileSink`/`IOSinkNode`?

4. **`crates/polars-stream/src/physical_plan/fmt.rs`** — Search for `HfBucketSink`. Display arm. Should be trivial.

5. **`crates/polars-stream/src/nodes/io_sinks/mod.rs`** — Module declaration for `hf_bucket_sink`. Check feature gating.

6. **`crates/polars-stream/src/nodes/io_sinks/hf_bucket_sink.rs`** — **This is the main sink node.** ~250 lines. Implements `ComputeNode` with a state machine: `Uninitialized → Initialized → Finished`. Review thoroughly:
   - Does the `ComputeNode` implementation follow the same pattern as `IOSinkNode` in `crates/polars-stream/src/nodes/io_sinks/node.rs`?
   - Is the phase-channel bridging correct? (It re-sequences morsels from per-phase `PortReceiver`s into a single stream.)
   - Is the `update_state` implementation correct? (It should block on the upload task when `recv[0] == Done`.)
   - Is the `spawn` implementation correct? (It sends the per-phase `PortReceiver` through the phase channel.)
   - Are there resource leaks? What happens if the upload task panics or errors?
   - Is the `AbortOnDropHandle` used correctly for cleanup?

### HF/XET upload logic (polars-io)

These files are entirely new and self-contained. They implement the HF Bucket API and XET upload protocol.

7. **`crates/polars-io/src/cloud/hf_bucket/mod.rs`** — ~170 lines. Config struct, URL parsing, token extraction (from CloudOptions headers, env var, cached file). Review:
   - Is `parse_hf_bucket_url` robust? Edge cases?
   - Is `extract_hf_token` following the right precedence? Is the `~/.cache/huggingface/token` fallback correct?
   - Are errors descriptive enough?

8. **`crates/polars-io/src/cloud/hf_bucket/xet_upload.rs`** — ~100 lines. Fetches XET write token from HF API, creates `XetClient`, wraps it in `BucketWriter`. Review:
   - Error handling on HTTP responses
   - Is the `XetClient` construction correct?
   - Is `upload_bytes` (the non-streaming convenience method) safe/correct?

9. **`crates/polars-io/src/cloud/hf_bucket/batch.rs`** — ~70 lines. Bucket batch API (NDJSON body with `AddFile`/`DeleteFile` operations). Review:
   - Is the NDJSON serialization correct?
   - Error handling on HTTP responses
   - Is the serde tagging correct?

10. **`crates/polars-io/src/cloud/hf_bucket/streaming_upload.rs`** — ~160 lines. **The streaming pipeline core.** Review thoroughly:
    - `ChannelWriter`: Sync `Write` impl that sends byte chunks over a bounded `sync_channel(16)`. Is the backpressure model correct?
    - `StreamingBucketUploader::new`: Sets up the sync→async bridge (`spawn_blocking` draining sync channel into tokio mpsc). Is this bridge pattern sound? Any deadlock risks?
    - `StreamingBucketUploader::finish`: Writes parquet footer, drops the `BatchedWriter` (closing the channel sender), then awaits the upload task. Is the ordering correct? Can the upload task miss data?
    - What happens if the XET upload fails mid-stream? Does the error propagate cleanly back to the caller?

### Other touched files

11. **`crates/polars-io/src/cloud/mod.rs`** — Check that `pub mod hf_bucket` is properly feature-gated.

12. **`crates/polars-io/src/path_utils/hugging_face.rs`** — Search for `buckets`. Check that `"buckets"` is added to the right constant/list.

### Feature flag chain (Cargo.toml files)

13. Check that the `hf_bucket_sink` feature is threaded correctly through:
    - `crates/polars-io/Cargo.toml`
    - `crates/polars-stream/Cargo.toml`
    - `crates/polars-lazy/Cargo.toml`
    - `crates/polars/Cargo.toml`
    - `py-polars/Cargo.toml`
    - `crates/polars-python/Cargo.toml`

    Each should forward the feature to the next crate in the chain. The root feature should depend on `cloud` and `dep:subxet`.

## Review Structure

Please organize your review into these sections:

### 1. Correctness
- Does the streaming pipeline work correctly end-to-end?
- Are there race conditions, deadlocks, or data loss scenarios?
- Is the `ComputeNode` state machine correct?

### 2. Safety & Resource Management
- Are all spawned tasks properly joined/aborted on error?
- Are channels properly closed on all paths (success, error, panic)?
- Are there memory leaks or unbounded allocations?

### 3. Error Handling
- Do errors propagate correctly from async upload tasks back to the streaming engine?
- Are error messages descriptive and actionable?
- Are there silent failures (errors swallowed or ignored)?

### 4. Polars Idioms & Style
- Does the code follow Polars conventions? (Compare with `IOSinkNode`, existing sink implementations.)
- Is the `polars_error!`/`polars_bail!` usage correct?
- Are there unnecessary `unwrap()`s or `expect()`s that should be proper error handling?

### 5. Feature Gating
- Is everything properly behind `#[cfg(feature = "hf_bucket_sink")]`?
- Does the feature leak into any non-feature-gated code paths?
- Is the feature flag chain in Cargo.toml files correct and complete?

### 6. Upstreamability
- What would Polars maintainers likely object to?
- What changes would be required before submitting an upstream PR?
- Are there any unnecessary dependencies or complexity?
- Is the code well-documented enough for upstream reviewers?

### 7. Summary
- Overall assessment (ready to share / needs work / major issues)
- Prioritized list of issues (blocking vs. nice-to-have)
- Suggested improvements
