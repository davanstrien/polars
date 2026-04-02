# ISSUE-005: Critical Code Paths

Quick reference for where memory accumulates in the streaming pipeline.

## 1. HTTP Buffering (ROOT CAUSE)

**File:** `crates/polars-io/src/cloud/polars_object_store.rs`

```rust
// Lines 196-210 - THE PROBLEM
.try_collect::<Vec<Bytes>>()  // Collects ALL concurrent chunks into memory
let mut combined = Vec::with_capacity(range.len());  // Allocates full size
combined.extend_from_slice(&part)  // Copies everything
PolarsResult::Ok(Bytes::from(combined))  // Another copy
```

**Why it matters:** For a 200MB file split into 3 chunks, this holds 200MB+ in memory per file being read.

---

## 2. Concurrent File Readers

**File:** `crates/polars-stream/src/nodes/io_sources/multi_scan/functions/mod.rs`

```rust
// Lines 36-46
pub fn calc_max_concurrent_scans(num_pipelines: usize, num_sources: usize) -> usize {
    if let Ok(v) = std::env::var("POLARS_MAX_CONCURRENT_SCANS") {
        return v.parse().unwrap();
    }
    num_pipelines.min(num_sources).clamp(1, 128)  // DEFAULT: up to 128 files!
}
```

---

## 3. Row Group Prefetch

**File:** `crates/polars-stream/src/nodes/io_sources/parquet/builder.rs`

```rust
// Lines 58-82
let prefetch_limit = std::env::var("POLARS_ROW_GROUP_PREFETCH_SIZE")
    .map(|x| x.parse::<NonZeroUsize>().unwrap().get())
    .unwrap_or(execution_state.num_pipelines.saturating_mul(2))  // DEFAULT: num_pipelines * 2
```

**File:** `crates/polars-stream/src/nodes/io_sources/parquet/init.rs`

```rust
// Lines 58-60 - Per-file prefetch channel
let (prefetch_send, mut prefetch_recv) =
    tokio::sync::mpsc::channel(row_group_prefetch_size);  // Creates buffer PER FILE
```

---

## 4. HfSinkNode Backpressure (OUR CODE)

**File:** `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs`

```rust
// Lines 854-868 - DataFrame accumulation
let mut buffer = DataFrame::empty_with_schema(schema.as_ref());
while let Ok(morsel) = rx.recv().await {
    let (df, _, _, consume_token) = morsel.into_inner();
    buffer.vstack_mut_owned(df)?;  // ACCUMULATES unbounded
}

// Lines 930-931 - Token dropped TOO EARLY
drop(consume_token);  // Should be AFTER shard_tx.send()

// Line 1532 - Shard channel
let (shard_tx, shard_rx) = connector::<ShardToUpload>();  // capacity-1, but no wait for upload
```

---

## 5. MmapBuffer Growth (OUR CODE)

**File:** `crates/polars-io/src/cloud/hf/mmap_buffer.rs`

```rust
// Lines 111-132
fn grow(&mut self, min_capacity: usize) -> io::Result<()> {
    let new_capacity = self
        .capacity
        .saturating_mul(2)  // DOUBLES each time: 1MB → 2MB → 4MB → ... → 500MB
        .max(min_capacity)
        .max(MIN_CAPACITY);
}
```

---

## Memory Math

For 266 files × 200MB with 8 pipelines:

| Component | Calculation | Memory |
|-----------|-------------|--------|
| Concurrent readers | min(8, 266) = 8 files | - |
| Prefetch per file | 8 × 2 = 16 row groups | - |
| Row group size | ~25MB average | - |
| **Prefetch buffers** | 8 files × 16 RGs × 25MB | **3.2 GB** |
| HTTP buffers | 8 files × 200MB (worst case) | **1.6 GB** |
| Decode buffers | ~2x prefetch | **6.4 GB** |
| HfSink shards | 3 × 500MB in-flight | **1.5 GB** |
| **Total estimate** | | **~13 GB minimum** |

With overhead, contention, and Arc clones: **34GB observed**

---

## Quick Grep Commands

```bash
# Find all buffering points
rg "try_collect" crates/polars-io/src/cloud/
rg "Vec::with_capacity" crates/polars-io/src/cloud/
rg "vstack_mut" crates/polars-stream/src/nodes/io_sinks/

# Find channel configurations
rg "mpsc::channel" crates/polars-stream/src/nodes/
rg "connector::<" crates/polars-stream/src/nodes/

# Find env var controls
rg "POLARS_MAX_CONCURRENT" crates/
rg "POLARS_ROW_GROUP_PREFETCH" crates/
```
