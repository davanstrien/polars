Agent: object-store-buffering
Focus: object store range fetch buffering behavior

Observations:
- get_range() splits large ranges into parts, then collects all Bytes into a Vec and concatenates into a single Vec<u8> before converting to Bytes. This temporarily doubles memory for the range.
- get_ranges_sort() uses get_buffered_ranges_stream() and aggregates into a Vec of Bytes, then may concatenate multiple parts into a Vec<u8> when merged ranges cross boundaries.
- Both paths buffer full byte ranges in memory (not streaming). This can be costly when row groups or column chunks are large, or when many ranges are requested concurrently.

Potential failure mode:
- During parquet scanning over hf:// object store, range requests for row groups/columns can be large. The concatenation pattern can cause transient memory spikes, especially when combined with high concurrency and prefetching.

References:
- get_range combines parts into a single Vec (combines all parts before Bytes::from).
- get_ranges_sort collects buffered stream into Vec and conditionally concatenates parts into a new Vec.

TL;DR
Object store range reads materialize full byte ranges in memory and sometimes duplicate buffers while concatenating parts. Under high concurrency, this can create large memory spikes. Pointers: crates/polars-io/src/cloud/polars_object_store.rs:149-210 and 243-280.
