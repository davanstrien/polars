Agent: concurrency-audit
Focus: multi-scan concurrency defaults and reader pre-init

Observations:
- Multi-scan concurrency defaults are high relative to pipeline count. calc_max_concurrent_scans defaults to min(num_pipelines, num_sources) clamped to [1,128]. On large multi-file scans this can spawn many concurrent readers by default.
- The pre-init reader count is also sized from num_pipelines (+3) and clamped to [1,128]. This can front-load readers even before steady-state backpressure is known.
- These defaults combine with per-reader prefetching (see other notes) to amplify memory use when scanning many parquet files.

Potential failure mode:
- Large hf:// parquet scans with streaming engine open many files at once, each with its own prefetch buffer and row group queue. If scan concurrency is near num_pipelines and row-group prefetch is set to num_pipelines*2 per file, memory scales with num_pipelines^2 and file count.

References:
- calc_n_readers_pre_init uses num_pipelines + 3 and clamps to 128.
- calc_max_concurrent_scans defaults to min(num_pipelines, num_sources) clamped to 128.

TL;DR
High default concurrency can multiply per-file prefetch and buffer memory. Likely contributor to streaming OOM on multi-file hf:// scans. Pointers: crates/polars-stream/src/nodes/io_sources/multi_scan/functions/mod.rs:8-47.
