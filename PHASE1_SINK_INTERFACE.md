# Phase 1: Polars Streaming Sink Interface — Integration Map

> How to wire a new streaming sink into Polars.
> Generated from source analysis of `main` branch (commit `31b62e796`).
> Reference: existing `feature/hf-hub-sink` branch (read-only).

---

## A) SinkNode Trait

**File**: `crates/polars-stream/src/nodes/io_sinks/mod.rs` (lines 201–242)

The `SinkNode` trait is the core interface every streaming sink must implement:

```rust
pub trait SinkNode {
    /// Human-readable name for debugging / explain output.
    fn name(&self) -> &str;

    /// Whether the sink can accept input from multiple parallel pipelines.
    fn is_sink_input_parallel(&self) -> bool;

    /// Whether the sink requires ordered input (default: true).
    fn do_maintain_order(&self) -> bool { true }

    /// Main entry: spawn the async task(s) that consume incoming data.
    fn spawn_sink(
        &mut self,
        recv_ports_recv: Receiver<(PhaseOutcome, SinkInputPort)>,
        state: &StreamingExecutionState,
        join_handles: &mut Vec<JoinHandle<PolarsResult<()>>>,
    );

    /// Called once before spawn_sink.
    fn initialize(&mut self, state: &StreamingExecutionState) -> PolarsResult<()> {
        _ = state; Ok(())
    }

    /// Called after all data written and join handles awaited.
    fn finalize(
        &mut self,
        state: &StreamingExecutionState,
    ) -> Option<Pin<Box<dyn Future<Output = PolarsResult<()>> + Send>>> {
        _ = state; None
    }

    /// Optional write metrics (bytes written, rows written, etc.).
    fn get_metrics(&self) -> PolarsResult<Option<WriteMetrics>> { Ok(None) }
}
```

### SinkComputeNode wrapper (lines 250–288)

`SinkComputeNode` wraps any `SinkNode` into a `ComputeNode` for the execution graph:

```rust
pub struct SinkComputeNode {
    sink: Box<dyn SinkNode + Send>,
    started: Option<StartedSinkComputeNode>,
    state: SinkState, // Uninitialized → Initialized → Finished
}

impl SinkComputeNode {
    pub fn new(sink: Box<dyn SinkNode + Send>) -> Self { ... }
}

// Convenience: any SinkNode auto-converts to SinkComputeNode
impl<T: SinkNode + Send + 'static> From<T> for SinkComputeNode {
    fn from(value: T) -> Self { Self::new(Box::new(value)) }
}
```

---

## B) Type Chain: Python → Logical Plan → Physical Plan → Graph Node

### 1. Python entry point

**File**: `crates/polars-python/src/lazyframe/general.rs` (line 685)

```python
# Python
lf.sink_parquet("hf://buckets/namespace/name/path.parquet", ...)
```

Calls `PyLazyFrame::sink_parquet()` which extracts options and calls:

```rust
self.ldf.read().clone().sink(
    target,                              // SinkDestination::File { target }
    FileWriteFormat::Parquet(Arc::new(options)),
    unified_sink_args,
)
```

### 2. LazyFrame → DslPlan::Sink

**File**: `crates/polars-lazy/src/frame/mod.rs` (line 991)

```rust
pub fn sink(
    mut self,
    sink_type: SinkDestination,
    file_format: FileWriteFormat,
    unified_sink_args: UnifiedSinkArgs,
) -> PolarsResult<Self> {
    self.logical_plan = DslPlan::Sink {
        input: Arc::new(self.logical_plan),
        payload: match sink_type {
            SinkDestination::File { target } => SinkType::File(FileSinkOptions {
                target,
                file_format,
                unified_sink_args,
            }),
            ...
        },
    };
    Ok(self)
}
```

### 3. IR lowering: SinkTypeIR → PhysNodeKind

**File**: `crates/polars-stream/src/physical_plan/lower_ir.rs` (line 249)

```rust
IR::Sink { input, payload } => match payload {
    SinkTypeIR::Memory => { ... PhysNodeKind::InMemorySink { ... } },
    SinkTypeIR::Callback(...) => { ... PhysNodeKind::CallbackSink { ... } },
    SinkTypeIR::File(options) => {
        let input = lower_ir!(*input)?;
        PhysNodeKind::FileSink { input, options: options.clone() }
    },
    SinkTypeIR::Partitioned(options) => { ... },
}
```

### 4. Physical node enum

**File**: `crates/polars-stream/src/physical_plan/mod.rs` (line 199)

```rust
pub enum PhysNodeKind {
    ...
    FileSink { input: PhysStream, options: FileSinkOptions },
    PartitionedSink { ... },
    PartitionedSink2 { ... },
    // NEW: HfBucketSink { input: PhysStream, options: HfBucketSinkOptions },
    ...
}
```

### 5. Physical → graph wiring

**File**: `crates/polars-stream/src/physical_plan/to_graph.rs` (line 317)

```rust
FileSink { input, options: FileSinkOptions { target, file_format, unified_sink_args } } => {
    use crate::nodes::io_sinks2::IOSinkNode;
    use crate::nodes::io_sinks2::config::{IOSinkNodeConfig, IOSinkTarget};

    let input_schema = ctx.phys_sm[input.node].output_schema.clone();
    let input_key = to_graph_rec(input.node, ctx)?;
    let target = IOSinkTarget::File(target.clone());
    let config = IOSinkNodeConfig { file_format, target, unified_sink_args, input_schema };

    ctx.graph.add_node(IOSinkNode::new(config), [(input_key, input.port)])
},
```

---

## C) Minimal Diff to Add HfBucketSink (6 files)

### 1. `crates/polars-stream/src/physical_plan/mod.rs`

Add variant to `PhysNodeKind`:

```rust
#[cfg(feature = "hf_bucket_sink")]
HfBucketSink {
    input: PhysStream,
    options: HfBucketSinkOptions,  // defined in polars-io or polars-plan
},
```

### 2. `crates/polars-stream/src/physical_plan/lower_ir.rs`

In the `SinkTypeIR::File(options)` match arm, check if the target URL is `hf://buckets/...` and route to `HfBucketSink` instead of `FileSink`:

```rust
SinkTypeIR::File(options) => {
    let input = lower_ir!(*input)?;

    #[cfg(feature = "hf_bucket_sink")]
    if let Some(CloudScheme::Hf) = options.target.cloud_scheme() {
        // Check if target path contains "buckets" prefix
        // → PhysNodeKind::HfBucketSink { input, options: ... }
    }

    PhysNodeKind::FileSink { input, options: options.clone() }
},
```

### 3. `crates/polars-stream/src/physical_plan/to_graph.rs`

Add match arm creating `HfBucketSinkNode` wrapped in `SinkComputeNode`:

```rust
#[cfg(feature = "hf_bucket_sink")]
HfBucketSink { input, options } => {
    let input_schema = ctx.phys_sm[input.node].output_schema.clone();
    let input_key = to_graph_rec(input.node, ctx)?;
    let sink_node = HfBucketSinkNode::new(options.clone(), input_schema);
    ctx.graph.add_node(
        SinkComputeNode::from(sink_node),
        [(input_key, input.port)],
    )
},
```

### 4. `crates/polars-stream/src/nodes/io_sinks/mod.rs`

Add module declaration:

```rust
#[cfg(feature = "hf_bucket_sink")]
pub mod hf_bucket_sink;
```

### 5. `crates/polars-stream/Cargo.toml`

Add feature:

```toml
[features]
hf_bucket_sink = ["cloud", "polars-io/hf_bucket_sink"]
```

### 6. `crates/polars-io/Cargo.toml`

Add feature with xet-core dependencies:

```toml
[features]
hf_bucket_sink = [
    "cloud",
    "dep:xet-data",
    "dep:cas_types",
    "dep:xet-utils",
    "dep:async-trait",
]

[dependencies]
xet-data = { package = "data", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
xet-utils = { package = "utils", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
cas_types = { git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
async-trait = { version = "0.1", optional = true }
```

---

## D) UnifiedSinkArgs Flow

**File**: `crates/polars-plan/src/dsl/options/sink2.rs` (lines 47–52)

```rust
#[derive(Clone, Debug, Hash, PartialEq)]
pub struct UnifiedSinkArgs {
    pub mkdir: bool,
    pub maintain_order: bool,
    pub sync_on_close: SyncOnCloseType,
    pub cloud_options: Option<Arc<CloudOptions>>,
}
```

This flows from Python → `LazyFrame::sink()` → `DslPlan::Sink` → `FileSinkOptions` → `PhysNodeKind::FileSink`.

**For bucket sink**: `cloud_options` carries the HF token and endpoint config. The `maintain_order` flag controls whether sink receives ordered data. The `sync_on_close` is not relevant for bucket sink (no fsync).

**HF options passing**: The HF hub sink branch added `hf_options: Option<Vec<(String, String)>>` to pass HF-specific config from Python (repo_id, token, etc.). For bucket sink, we may need similar extension or embed the options in `CloudOptions::config`.

---

## E) URL Parsing

**File**: `crates/polars-io/src/path_utils/hugging_face.rs`

### HFPathParts (lines 26–32)

```rust
struct HFPathParts {
    bucket: String,      // "datasets", "spaces", or future "buckets"
    repository: String,  // "namespace/name"
    revision: String,    // "main" or explicit revision
    path: String,        // path relative to repo root
}
```

### BUCKETS validation (line 135)

```rust
const BUCKETS: [&str; 2] = ["datasets", "spaces"];
if !BUCKETS.contains(&this.bucket.as_str()) {
    polars_bail!(ComputeError: "hugging face uri bucket must be one of {:?}, got {} instead.", BUCKETS, this.bucket);
}
```

**Required change**: Add `"buckets"` to the `BUCKETS` constant:

```rust
const BUCKETS: [&str; 3] = ["datasets", "spaces", "buckets"];
```

This allows URLs like `hf://buckets/namespace/name/path/file.parquet`.

---

## F) Recommended Dependencies

From OpenDAL's `core/services/huggingface/Cargo.toml`:

```toml
[features]
hf_bucket_sink = [
    "cloud",
    "dep:xet-data",
    "dep:cas_types",
    "dep:xet-utils",
    "dep:async-trait",
]

[dependencies]
xet-data = { package = "data", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
xet-utils = { package = "utils", git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
cas_types = { git = "https://github.com/kszucs/xet-core.git", branch = "download_bytes", optional = true }
async-trait = { version = "0.1", optional = true }
```

**Key types from these crates**:
- `xet_data::streaming::XetClient` — creates streaming write sessions
- `xet_data::streaming::XetWriter` — streaming byte writer
- `xet_data::XetFileInfo` — returned on close, contains hash + size
- `xet_utils::auth::TokenRefresher` — trait for auto-refreshing auth tokens
- `cas_types::FileRange` — range type (may not be needed for write path)

**Note**: The `streaming` module only exists in the `kszucs/xet-core` fork (`download_bytes` branch), not in main `huggingface/xet-core`. Track when this merges upstream.

---

## G) Design Decision: Old SinkNode vs. New IOSinkNode

Polars has two sink architectures:

1. **Old**: `SinkNode` trait in `io_sinks/mod.rs` — used by csv, ipc, json, parquet (pre-refactor), and partition sinks. Wrapped by `SinkComputeNode`.

2. **New**: `IOSinkNode` in `io_sinks2/` — uses `IOSinkNodeConfig` with `IOSinkTarget` and `FileWriteFormat`. More abstracted, assumes standard file I/O patterns.

**Decision**: Use **old `SinkNode` trait** for bucket sink because:
- Bucket sink needs custom protocol (XetWriter → bucket_batch API), not standard file I/O
- `IOSinkNode` (new system) assumes `IOSinkTarget::File` → object-store or local file writes
- `SinkNode` gives full control over `spawn_sink`, `initialize`, and `finalize` lifecycle
- The HF hub sink branch also used the old `SinkNode` pattern for the same reason

The `to_graph.rs` match arm should create `SinkComputeNode::from(HfBucketSinkNode::new(...))`, not `IOSinkNode::new(config)`.

---

## Summary: Integration Checklist

| Step | File | Change | Status |
|------|------|--------|--------|
| 1 | `polars-io/Cargo.toml` | Add `hf_bucket_sink` feature + xet-core deps | **DONE** |
| 2 | `polars-stream/Cargo.toml` | Add `hf_bucket_sink` feature forwarding to polars-io | **DONE** |
| 3 | `polars-io/src/path_utils/hugging_face.rs:135` | Add `"buckets"` to BUCKETS const | **DONE** |
| 4 | `polars-stream/src/nodes/io_sinks/mod.rs` | Add `#[cfg(feature = "hf_bucket_sink")] pub mod hf_bucket_sink;` | TODO |
| 5 | `polars-stream/src/nodes/io_sinks/hf_bucket_sink/mod.rs` | Implement `SinkNode` trait | TODO |
| 6 | `polars-stream/src/physical_plan/mod.rs` | Add `HfBucketSink` variant to `PhysNodeKind` | TODO |
| 7 | `polars-stream/src/physical_plan/lower_ir.rs` | Route `hf://buckets/` URLs to `HfBucketSink` | TODO |
| 8 | `polars-stream/src/physical_plan/to_graph.rs` | Match arm creating `SinkComputeNode::from(HfBucketSinkNode)` | TODO |

All changes are feature-gated behind `hf_bucket_sink` — zero impact on normal Polars builds.

### Revised execution order

Before proceeding to Steps 4-8, a **standalone XET upload test** (Phase 2.1a in `BUCKET_SINK_PLAN.md`) will validate the xet-core fork's upload path end-to-end. This de-risks the biggest unknown (third-party fork API stability) before committing to the full Polars integration.

1. **Standalone XET upload test** — `scratch/xet_upload_test/` (next)
2. **Rebase onto latest `main`** — fix pre-existing `polars-core` build issue
3. **Steps 4-8 above** — polars-io modules, sink node, pipeline wiring
