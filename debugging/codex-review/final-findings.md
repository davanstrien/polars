Final Findings (codex-review)

Scope
- Looked for likely causes of the reported HF sink streaming OOM / memory growth behavior in code + HF Hub docs.
- Focused on multi-file scans + HF sink buffering. No fixes proposed.

Likely Causes (ranked)
1) High default concurrent scans
- calc_max_concurrent_scans defaults to min(num_pipelines, num_sources) clamped to 128. With many files, this allows many concurrent readers. Combined with row-group prefetch per reader, memory scales fast.
- File: crates/polars-stream/src/nodes/io_sources/multi_scan/functions/mod.rs:8-47

2) Per-reader row group prefetch default is num_pipelines*2 (no global coordination)
- ParquetReaderBuilder sets prefetch_limit from POLARS_ROW_GROUP_PREFETCH_SIZE or defaults to num_pipelines*2; semaphore is per reader.
- Reader interface explicitly notes that row-group prefetch is not synchronized across readers/files.
- Files: crates/polars-stream/src/nodes/io_sources/parquet/builder.rs:58-118; crates/polars-stream/src/nodes/io_sources/multi_scan/reader_interface/mod.rs:151-156

3) Object store range reads buffer whole ranges in memory
- get_range() concatenates parts into a Vec<u8> before converting to Bytes (duplicate buffering for large ranges).
- get_ranges_sort() aggregates bytes into a Vec and may concatenate into a new Vec for merged ranges.
- File: crates/polars-io/src/cloud/polars_object_store.rs:149-210, 243-280

4) HfSinkNode buffering is row-count based (256k rows) with unbounded growth in partitioned mode
- buffer_and_write_task buffers until DEFAULT_CHUNK_SIZE (256k rows) with vstack_mut_owned; no adaptive memory cap.
- partitioned_buffer_and_write_task holds per-partition buffers; high-cardinality partitioning multiplies memory.
- File: crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs:64, 823-931, 1002-1154

External/Operational factors to consider (from HF Hub docs)
- Hugging Face Hub enforces rate limits; 429s can occur and clients should use RateLimit headers to back off. This can affect retries/timeouts during long uploads and may keep buffers alive longer than expected.
- The official upload guides emphasize LFS/xet usage and multi-commit strategies for large uploads, which may influence expected behavior when integrating with custom upload pipelines.
- Docs: Hugging Face Hub rate limits (HF docs) and upload guides (huggingface_hub docs).

TL;DR
Primary suspects are internal concurrency + prefetch defaults (multi_scan + parquet prefetch) and buffering behavior (object store range reads + HfSinkNode buffers). These combine multiplicatively under multi-file streaming scans, producing OOM even in “streaming” mode.
