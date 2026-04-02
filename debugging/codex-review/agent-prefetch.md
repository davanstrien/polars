Agent: prefetch-audit
Focus: parquet row group prefetch sizing and lack of global coordination

Observations:
- ParquetReaderBuilder sets row group prefetch limit from POLARS_ROW_GROUP_PREFETCH_SIZE or defaults to num_pipelines*2. This is per reader and per file.
- The reader interface explicitly notes lack of synchronization for row group prefetch across multiple files/readers.
- This implies row group prefetch occurs independently per file reader; with many concurrent readers, total in-flight row group buffers can explode.

Potential failure mode:
- When scanning many HF Hub parquet files, each reader prefetches up to row_group_prefetch_size row groups concurrently. Without a global semaphore across readers, total buffered row group data can scale with (concurrent_readers * prefetch_limit * row_group_size), potentially exceeding memory.

References:
- Parquet prefetch size default and semaphore setup in builder.
- Comment about lack of synchronized prefetch across multiple files in reader interface.

TL;DR
Row group prefetch is sized to num_pipelines*2 per file and is not globally coordinated across readers. This can cause large aggregate in-memory row-group buffers during multi-file scans. Pointers: crates/polars-stream/src/nodes/io_sources/parquet/builder.rs:58-118; crates/polars-stream/src/nodes/io_sources/multi_scan/reader_interface/mod.rs:151-156.
