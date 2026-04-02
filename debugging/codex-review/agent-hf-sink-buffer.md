Agent: hf-sink-buffer
Focus: HfSinkNode buffering and partitioned buffering

Observations:
- buffer_and_write_task accumulates incoming DataFrames into a single buffer via vstack_mut_owned() until buffer.height() >= DEFAULT_CHUNK_SIZE (256k rows) before flushing.
- DEFAULT_CHUNK_SIZE is fixed at 256k rows, not sized by memory, column width, or target shard size. This can yield very large in-memory buffers for wide schemas or large row sizes.
- partitioned_buffer_and_write_task maintains a HashMap of buffers per partition value. With high-cardinality partitioning, each partition can accumulate its own buffer, multiplying memory usage.

Potential failure modes:
- For wide schemas or large row sizes, the 256k row buffer can be very large, and if upstream produces large morsels, buffer may grow further until split_at() cycles catch up.
- With partitioned writes, unbounded per-partition buffers can grow in aggregate, especially when input rows are spread thinly across many partition values.

References:
- buffer_and_write_task and DEFAULT_CHUNK_SIZE.
- partitioned_buffer_and_write_task per-partition buffers and chunk flush logic.

TL;DR
HfSinkNode buffers up to 256k rows per shard, and partitioned writes hold per-partition buffers with no global cap. This can create large in-memory buffers and amplify OOM risk when input is wide or partition cardinality is high. Pointers: crates/polars-stream/src/nodes/io_sinks/hf_sink/mod.rs:64, 823-931, 1002-1154.
