# ISSUE-005: Streaming Memory OOM - TL;DR

## The Bug
`scan_parquet("hf://.../*.parquet").filter().sink_parquet(engine="streaming")` uses 34GB RAM on 53GB dataset despite "streaming" mode.

## FIRST: Isolate the Problem

Before assuming it's our HfSinkNode, run this diagnostic:

```python
import os
os.environ["POLARS_MAX_CONCURRENT_SCANS"] = "4"
os.environ["POLARS_ROW_GROUP_PREFETCH_SIZE"] = "2"
import polars as pl

# Test: HF source → LOCAL sink (removes HfSinkNode from equation)
(
    pl.scan_parquet("hf://datasets/nvidia/OpenMathReasoning/data/*.parquet")
    .filter(pl.col("problem_source") == "MATH_training_set")
    .sink_parquet("/tmp/test_local_sink.parquet")  # LOCAL, not hf://
)
```

**Reasoning:** This test writes to a local file instead of HF Hub. If it still OOMs, the problem is in Polars' cloud parquet READING (HTTP buffering, concurrent scans) - not our HfSinkNode. If it works, our sink is the bottleneck.

| Result | Conclusion | Action |
|--------|------------|--------|
| Local sink OOMs | Upstream Polars issue | Document as limitation, recommend env vars |
| Local sink works | HfSinkNode is the problem | Fix our backpressure code |
| Works with env vars, OOMs without | Env vars are the fix | Document the workaround |

**Note on referenced GitHub issues:** Issue #23173 (most similar to ours) was closed as environment-specific (HPC). The other open issues (#24206, #22635) are about multi-joins and nested columns respectively - not directly our case. We should verify this is actually a Polars bug before assuming so.

## Likely Contributors (NOT YET PROVEN)

**Aggregate buffering from multiple sources that multiply under multi-file scans:**

| Factor | Location | Why it matters |
|--------|----------|----------------|
| Multi-scan concurrency | `multi_scan/functions/mod.rs:36-46` | Up to 128 concurrent file readers |
| Per-reader row-group prefetch | `parquet/init.rs:58-60` | `num_pipelines * 2` per file |
| Object-store range buffering | `polars_object_store.rs:196-210` | Full range collected before decode |

**Previous assessment overstatements (corrected):**
- ~~"ROOT CAUSE" for HTTP buffering~~ → Unproven without isolation experiments
- ~~"No upload backpressure"~~ → Wrong: shard channel IS capacity-1 with `await`
- ~~"consume_token dropped too early"~~ → Not supported by code review
- ~~"MmapBuffer 500MB is a bug"~~ → Expected behavior for 500MB shard size

## Immediate Workaround (No Code)

```bash
export POLARS_MAX_CONCURRENT_SCANS=4
export POLARS_ROW_GROUP_PREFETCH_SIZE=2
```

Set BEFORE `import polars`. Reduces memory ~4-5x.

## Root Cause Chain

```
1. SOURCE: 12-16 files read simultaneously (default)
   Each prefetches 16+ row groups = 5-10GB in buffers

2. TRANSPORT: HTTP responses fully collected into Vec
   .try_collect::<Vec<Bytes>>() before decode = no streaming

3. SINK: HfSinkNode creates shards faster than uploads
   5s upload latency vs 2s rotation = shards pile up
```

## Related GitHub Issues (Verified 2026-02-02)

### Polars
| Issue | Status | Relevance to Our Case |
|-------|--------|----------------------|
| [#23173](https://github.com/pola-rs/polars/issues/23173) | **CLOSED** | Was HPC environment-specific, not a Polars bug |
| [#24206](https://github.com/pola-rs/polars/issues/24206) | OPEN | Multi-join pipelines only (not filter) |
| [#20218](https://github.com/pola-rs/polars/issues/20218) | CLOSED | Filter + hive partitions, root cause in PR #19850 |
| [#22635](https://github.com/pola-rs/polars/issues/22635) | OPEN | Nested columns only (structs/lists) |
| [#15771](https://github.com/pola-rs/polars/issues/15771) | ? | General streaming OOM |

**Caveat:** The most similar issue (#23173) turned out to be environment-specific. We should verify our issue is reproducible before blaming Polars.

### Apache Arrow (Underlying Issues)
- [#45287](https://github.com/apache/arrow/issues/45287) - Metadata memory leak
- [#38552](https://github.com/apache/arrow/issues/38552) - High memory reading from disk
- [#37630](https://github.com/apache/arrow/issues/37630) - Dataset reading memory leak

## Bottom Line
- **HF sink isn't the main problem** - upstream Polars buffers too aggressively
- **We CAN improve HfSinkNode** backpressure to not make it worse
- **Users CAN work around** with env vars
- **Long-term**: Polars needs streaming HTTP/parquet (issues already open)

## References
- [Streaming in Polars - Rho Signal](https://www.rhosignal.com/posts/streaming-in-polars/)
- [DuckDB Memory Management](https://duckdb.org/2024/07/09/memory-management) (how they avoid this)
