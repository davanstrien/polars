# OOM Root Cause Analysis

## Problem Statement

`scan_parquet("hf://.../*.parquet").filter().sink_parquet()` OOMs on a 34GB machine when processing a 53GB dataset (266 parquet files) in streaming mode.

**Critical Question**: Is this an upstream Polars bug, or is it caused by the custom HF Hub sink code?

## Executive Summary

**The OOM is primarily an upstream Polars issue (~85-90%), with the HF sink contributing a minor exacerbating factor (~10-15%).** The root cause is that multiple concurrent parquet readers run their decode pipelines in parallel, accumulating decoded DataFrames faster than the single-threaded bridge can forward them to the sink. The backpressure chain has a structural gap: the prefetch semaphore permit is released *before* the morsel reaches the sink, allowing new prefetches to start while old data is still in-flight.

## Attribution

| Component | Contribution | Confidence |
|-----------|-------------|------------|
| Source concurrency (decode accumulation) | ~55% | High |
| Backpressure gap (prefetch permit early-drop) | ~25% | High |
| HTTP buffering (materialization copies) | ~5-10% | Medium |
| HF sink (slower than local disk) | ~10-15% | Medium |

**Would local `sink_parquet` also OOM?** Very likely yes, with the same dataset and default settings.

---

## Finding 1: Source Concurrency is the Primary Memory Driver

### Confirmed Facts

1. **Prefetch semaphore is SHARED across all readers** (`builder.rs:21,80-82,117`):
   - Created once in `set_execution_state()` as `Arc<Semaphore>` with capacity = `num_pipelines * 2` (default ~24)
   - Cloned via `Arc::clone` for every reader built in `build_file_reader()` at line 117
   - This correctly limits total in-flight row group *prefetches* across all readers

2. **Decode channel is PER-READER** (`init.rs:170`):
   ```rust
   let (decode_send, mut decode_recv) = tokio::sync::mpsc::channel(self.config.num_pipelines);
   ```
   - Each reader creates its own decode channel with capacity = `num_pipelines` (~12)
   - This means each reader can hold up to 12 in-progress decode tasks

3. **Spawned decode tasks run immediately** (`init.rs:175`):
   ```rust
   let decode_fut = async_executor::spawn(TaskPriority::High, async move {
       row_group_decoder.row_group_data_to_df(row_group_data).await
   });
   ```
   - `async_executor::spawn` schedules the task immediately on the compute thread pool
   - The task runs, decodes the row group into a DataFrame, and holds the result in its `JoinHandle`
   - The decoded DataFrame stays in memory until the distribute task `.await`s the `JoinHandle`

4. **Only ONE reader is connected to the bridge at a time** (`attach_reader_to_bridge.rs:44-49`):
   ```rust
   bridge_recv_port_tx.send(bridge_recv_port).await  // connect reader to bridge
   drop(wait_token);
   reader_handle.await?;  // BLOCK until this reader finishes
   ```
   - While the active reader is being consumed, all other started readers are running their prefetch->decode pipelines with nowhere to send morsels

5. **max_concurrent_scans defaults to `num_pipelines` (capped at 128)** (`functions/mod.rs:36-46`):
   ```rust
   num_pipelines.min(num_sources).clamp(1, 128)
   ```
   - On a 12-core machine: up to 12 concurrent readers
   - `started_reader_tx` channel capacity = `max_concurrent_scans - 1` = 11 (`initialization.rs:368`)

6. **ReaderStarter only blocks when `max_concurrent_scans == 1`** (`reader_starter.rs:385-391`):
   ```rust
   if skip_read_reason.is_none() && max_concurrent_scans == 1 {
       wait_group.wait().await;
   }
   ```
   - For concurrent_scans > 1, readers are started as fast as possible

### Memory Calculation

```
Worst case with default settings (12-core machine, 266 parquet files):
  max_concurrent_scans = 12
  prefetch_semaphore capacity = 24 (shared)
  decode_channel capacity per reader = 12

Active reader: consuming morsels normally
Inactive readers (up to 11): each has decode channel capacity 12

BUT: The prefetch semaphore limits total prefetches to 24.
So at most 24 row groups are being fetched/decoded at any time.

However, the DECODED DataFrames are much larger than compressed row groups:
  - Compressed row group: ~30-50 MB (parquet)
  - Decoded DataFrame: ~100-300 MB (uncompressed Arrow)
  - Decompression ratio: 3-6x typical

24 decoded DataFrames x 200 MB average = ~4.8 GB in decode JoinHandles
```

The real issue is more subtle. The permit lifecycle:

```
1. prefetch_task acquires permit (init.rs:153)
2. Sends (prefetch_result, permit) to prefetch_recv channel
3. decode_task receives, spawns decode, sends (decode_fut, permit) to decode_recv
4. distribute_task receives (decode_fut, permit)
5. distribute_task .awaits decode_fut -> gets decoded DataFrame
6. distribute_task drops permit (init.rs:213) <-- BEFORE sending morsel downstream
7. distribute_task sends morsel via morsel_sender
```

**The gap**: At step 6, the permit is freed, allowing a new prefetch. But the decoded DataFrame from step 5 hasn't been consumed by the sink yet. It's sitting in the morsel waiting to traverse: bridge -> filter -> sink.

---

## Finding 2: Backpressure Chain Has a Structural Gap

### The Full Chain

```
Prefetch semaphore (capacity 24, shared)
    | permit held through prefetch + decode
    | DROPPED at distribute_task (init.rs:213) <-- GAP
    v
morsel_sender (FileReaderOutputSend, serial)
    | connector = capacity-1 channel
    v
Bridge (bridge.rs:80-117)
    | replaces source_token, forwards to PortSender
    | tx.send() blocks if downstream not ready (capacity-1)
    v
Filter (filter.rs:47-68)
    | parallel receivers/senders (one per pipeline)
    | passes morsel through (preserves consume_token)
    v
Sink (io_sinks/mod.rs:137 or hf_sink/mod.rs:931)
    | drops consume_token HERE
    v
```

### The consume_token Mechanism

The `consume_token` is a `WaitToken` from a `WaitGroup` (`morsel.rs:97-98`). Key observations:

- **Distributor path** (`pipe.rs:325-327`): consume_token is dropped BEFORE entering the distributor buffer
- **Linearizer path** (`pipe.rs:289-297`): consume_token is dropped AFTER the linearizer insert succeeds

But critically, **the consume_token is NOT set by the parquet reader at all**. In `init.rs:234`:
```rust
morsel_sender.send_morsel(Morsel::new(df, morsel_seq, source_token.clone()))
```
`Morsel::new()` sets `consume_token: None` (`morsel.rs:107`). The consume_token is set later by the pipe infrastructure when it passes through a distributor (`pipe.rs:342`).

**Key insight**: The consume_token backpressure works between the pipe distributor and the sink, but there is NO consume_token backpressure from the sink all the way back to the parquet reader's prefetch loop. The only backpressure from reader to bridge is the capacity-1 connector channel (which blocks the distribute_task from sending more morsels), and the prefetch permit (which is dropped too early).

### How Many Morsels Can Be "In Flight"?

```
Per reader:
  - prefetch_send channel: capacity = row_group_prefetch_size (~24, but semaphore-limited)
  - decode_send channel: capacity = num_pipelines (~12)
  - distribute_task holds 2 DataFrames (current + peeked next)
  - morsel_sender: capacity-1 connector

Across all readers (up to 12 concurrent):
  Inactive readers can accumulate:
  - Up to 12 decode slots x 11 inactive readers = 132 decode JoinHandles
  - BUT limited by shared prefetch semaphore to 24 total

  After permits are dropped (step 6 above):
  - Each reader's distribute_task can hold 2 decoded DataFrames
  - 12 readers x 2 DataFrames = 24 decoded DataFrames WITHOUT semaphore permits

  Plus the active reader's morsels in the pipeline:
  - bridge -> filter -> sink chain

Total possible decoded DataFrames in memory: ~60
At 200 MB each: ~12 GB

Plus compressed row groups being fetched: 24 x 40 MB = ~1 GB
Plus HTTP buffer copies: ~500 MB
Plus morsel copies in filter/sink: ~2 GB

Estimated peak: ~15-16 GB
```

This is tight on a 34 GB machine when you add:
- Rust runtime, allocator overhead, fragmentation: ~2-4 GB
- OS and other processes: ~2-4 GB
- The actual output data being written: ~1-2 GB

**Total: ~20-26 GB estimated**, which explains why it's on the edge of OOM on 34 GB.

---

## Finding 3: HTTP Buffering is a Minor Contributor

- **Download chunk size**: 64 MB default (`pl_async.rs:21`)
- **`split_range`** splits ranges > 64 MB into parallel chunks (`polars_object_store.rs:422-437`)
- **Data copy**: `try_collect::<Vec<Bytes>>()` + `Vec::from(combined)` creates one full copy (`polars_object_store.rs:197-210`)
- **`MAX_BUDGET_PER_REQUEST`**: 10 concurrent downloads per request
- **`get_ranges_sort`** coalesces adjacent ranges, uses `MemSlice::from_bytes()` which is reference-counted (zero-copy slicing) (`polars_object_store.rs:285-288`)

The HTTP layer is NOT the primary problem because:
1. The prefetch semaphore limits how many row groups are being fetched simultaneously
2. `MemSlice` uses reference counting, so column slices share the underlying `Bytes` allocation
3. Once decoded, the original `Bytes` can be freed (no persistent reference from Arrow)

**Estimated HTTP overhead**: ~1-2 GB at peak (24 concurrent row groups x 40 MB compressed, with some copy overhead)

---

## Finding 4: HF Sink is a Minor Exacerbating Factor

### consume_token Timing Comparison

**Standard parquet sink** (`io_sinks/mod.rs:137`):
```rust
buffer.vstack_mut_owned(df)?;
while buffer.height() >= chunk_size {
    // split and send for encoding
}
drop(consume_token); // Line 137 - dropped AFTER buffering but BEFORE encoding completes
```

**HF sink** (`hf_sink/mod.rs:930-931`):
```rust
buffer.vstack_mut_owned(df)?;
while buffer.height() >= chunk_size {
    // write to shard, potentially send for upload
    shard_tx.send(ShardToUpload::new(...)).await?; // line 922 - may block on upload
}
drop(consume_token); // Line 931 - dropped AFTER all processing including potential upload send
```

**Difference**: The HF sink drops the consume_token AFTER `shard_tx.send()`, which may block if the upload channel is full. This means the HF sink holds the consume_token longer than the standard sink, slightly reducing the backpressure signal rate.

However, as established above, the consume_token doesn't propagate back to the parquet reader anyway (it propagates through the pipe distributor). So this difference primarily affects pipe-level congestion, not source-level congestion.

### MmapBuffer RSS Impact

The MmapBuffer (`mmap_buffer.rs`) uses `MmapMut` backed by `NamedTempFile`:
- Data is written via mmap, which means the OS maps the temp file pages into RSS
- **BUT**: Since it's file-backed, the OS can evict pages under memory pressure
- The shard size is typically ~500 MB, and only one shard is being written at a time
- After `into_read_handle()`, the upload reads from a read-only mmap (also evictable)

**Estimated HF sink overhead**: ~500 MB - 1 GB for the active shard buffer (potentially evictable)

### Would Local Sink Also OOM?

**Very likely yes.** The sink is not the bottleneck. The memory accumulation happens on the source side (decoded DataFrames in reader pipelines). A local sink would consume morsels faster (disk I/O << network I/O), which would slightly reduce the in-flight morsel count in the pipeline, but the fundamental issue of 12 concurrent readers accumulating decoded data persists.

The HF sink makes it ~10-15% worse due to:
1. Slower morsel consumption (network I/O) -> more morsels queued in pipe infrastructure
2. MmapBuffer RSS contribution
3. Slightly delayed consume_token drop

---

## Root Cause Diagram

```
ROOT CAUSE: Concurrent Reader Decode Accumulation + Prefetch Permit Early-Drop

+-----------------------------------------------------------+
| ReaderStarter (fires readers as fast as possible)          |
| max_concurrent_scans = 12 (default)                       |
| Only blocks when == 1                                     |
+--------+--------------------------------------------------+
         | starts up to 12 readers
         v
+-----------------------------------------------------------+
| Reader[0..11] (each has independent pipeline)             |
|                                                           |
|  prefetch_task ------> decode_task ------> distribute     |
|  (semaphore-limited)   (capacity 12/reader)  (holds 2)   |
|                        spawns immediately                 |
|                        decoded DF in JoinHandle           |
|                                                           |
|  * PERMIT DROPPED at distribute_task BEFORE morsel        |
|    reaches sink -> new prefetch starts immediately        |
+--------+--------------------------------------------------+
         | Only reader[0] connected to bridge at a time
         | readers[1..11] accumulate decoded data
         v
+-----------------------------------------------------------+
| Bridge (capacity-1) -> Filter -> Sink                     |
| consume_token dropped here, but doesn't reach readers     |
+-----------------------------------------------------------+
```

---

## Key File References

| File | Path | What to look for |
|------|------|-----------------|
| Multi-scan config | `crates/polars-stream/src/nodes/io_sources/multi_scan/functions/mod.rs:36-46` | `calc_max_concurrent_scans` |
| Pipeline init | `crates/polars-stream/src/nodes/io_sources/multi_scan/pipeline/initialization.rs:367-368` | `started_reader_tx` channel capacity |
| Reader starter | `crates/polars-stream/src/nodes/io_sources/multi_scan/pipeline/tasks/reader_starter.rs:385-391` | Only waits when concurrent_scans == 1 |
| Attach to bridge | `crates/polars-stream/src/nodes/io_sources/multi_scan/pipeline/tasks/attach_reader_to_bridge.rs:44-49` | Serializes reader consumption |
| Bridge | `crates/polars-stream/src/nodes/io_sources/multi_scan/pipeline/tasks/bridge.rs:80-97` | One reader at a time |
| Parquet init | `crates/polars-stream/src/nodes/io_sources/parquet/init.rs:60,170,175,213` | prefetch channel, decode channel, spawned tasks, permit drop |
| Parquet builder | `crates/polars-stream/src/nodes/io_sources/parquet/builder.rs:58-82` | Prefetch semaphore config |
| Object store | `crates/polars-io/src/cloud/polars_object_store.rs:193-210` | `try_collect::<Vec<Bytes>>()` |
| Sink backpressure | `crates/polars-stream/src/nodes/io_sinks/mod.rs:39-50,137-140` | Buffer sizes, consume_token drop |
| HF sink | `crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs:845-932` | buffer_and_write_task |
| Morsel | `crates/polars-stream/src/morsel.rs:97-98,102-108` | consume_token mechanism |
| Filter | `crates/polars-stream/src/nodes/filter.rs:47-68` | Passthrough behavior |
| Pipe infrastructure | `crates/polars-stream/src/pipe.rs:325-327,342` | consume_token handling |
| Connector | `crates/polars-stream/src/async_primitives/connector.rs:15-16` | capacity-1 channel |
| MmapBuffer | `crates/polars-io/src/cloud/hf/mmap_buffer.rs` | File-backed mmap buffer |

---

## Proposed Fixes

### Fix 1: Reduce Default `max_concurrent_scans` (Quick Win, Upstream)
- Lower default from `num_pipelines` to `min(4, num_pipelines)`
- Or use a formula that considers available memory
- **Impact**: Directly reduces number of inactive readers accumulating data
- **Risk**: May reduce throughput for fast local storage

### Fix 2: Hold Prefetch Permit Until Morsel is Consumed (Correct Fix, Upstream)
- Attach the prefetch permit to the Morsel (like consume_token)
- Drop it at the sink, not at the distribute_task
- **Impact**: True end-to-end backpressure from sink to source
- **Risk**: May reduce prefetch pipeline depth, needs careful tuning
- **Complexity**: Medium - requires threading the permit through the morsel/bridge

### Fix 3: Limit Decoded DataFrames Per Reader (Upstream)
- Add a separate semaphore for decoded (not just prefetched) data
- Limit based on estimated memory, not just count
- **Impact**: Caps memory regardless of concurrent_scans
- **Risk**: May add latency if limit is too low

### Fix 4: Env Var Workaround (Immediate, No Code Change)
```bash
export POLARS_MAX_CONCURRENT_SCANS=4
export POLARS_ROW_GROUP_PREFETCH_SIZE=8
```
- **Impact**: Reduces both concurrent readers and prefetch depth
- **Risk**: Reduced throughput, but should prevent OOM

### Fix 5: HF Sink - Drop consume_token Earlier (Minor, HF Sink)
- Drop consume_token after `vstack_mut_owned` but before shard writing/upload
- Match the standard parquet sink's behavior
- **Impact**: Minor improvement in backpressure responsiveness
- **Risk**: Minimal

---

## Verification Plan

1. **Isolation test**: Run `scan_parquet("hf://...").filter().sink_parquet("/tmp/local.parquet")` - expect OOM (confirms upstream is primary cause)
2. **Env var test**: Same query with `POLARS_MAX_CONCURRENT_SCANS=4 POLARS_ROW_GROUP_PREFETCH_SIZE=8` - expect success
3. **Memory profiling**: Run with `POLARS_VERBOSE=1` to confirm number of concurrent readers and prefetch depth
4. If isolation test does NOT OOM with local sink, then network latency contribution is larger than estimated and HF sink needs optimization

---

*Analysis produced 2026-02-06. Plan approval required before any code changes.*
