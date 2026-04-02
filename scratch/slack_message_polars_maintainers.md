## Streaming Parquet Sink to HF Buckets — PoC ready for review

Hey! I've been working on a PoC for `sink_parquet("hf://buckets/...")` — streaming parquet writes from Polars directly to HuggingFace Buckets via the XET protocol.

### What it does

```python
# Scan a dataset on the Hub, filter, sink to a bucket — fully streaming
pl.scan_parquet("hf://datasets/HuggingFaceFW/fineweb-edu/sample/10BT/*.parquet")
  .filter(pl.col("score").ge(4.0))
  .filter(pl.col("token_count").le(800))
  .sink_parquet("hf://buckets/my-org/my-bucket/output.parquet")
```

Data flows Hub → Polars → Bucket with bounded memory. No intermediate files, no `collect()`.

### Benchmarks

Tested on HF Jobs (2 vCPU, 17 GB RAM) filtering FineWeb-Edu (`score >= 4.0`, `token_count <= 800`):

| Source | Shards | Wall Time | Output | Peak RSS |
|--------|--------|-----------|--------|----------|
| 10BT sample | 88 | 16 min | 310 MB | ~1 GB |
| 100BT sample | ~880 | 2.6 hrs | 3.0 GB | ~7.2 GB |

Memory plateaus early and stays flat — streaming works as expected.

### Implementation

- ~52 lines across 7 files in `polars-io` / `polars-stream`, all behind `#[cfg(feature = "hf_bucket_sink")]`
- Parquet row groups are encoded incrementally and uploaded via a bounded async channel to the XET backend
- XET upload tokens auto-refresh for long-running jobs
- E2E pytest suite included (smoke, 10K, 10M row tests)

### Try it

- **Branch**: https://github.com/davanstrien/polars/tree/feature/hf-bucket-sink
- **Pre-built wheels** (x86_64 Linux): https://huggingface.co/datasets/davanstrien/polars-hf-bucket-sink-wheels
- **Colab notebook**: `scratch/demo_hf_bucket_sink.ipynb` in the branch — installs wheels from HF Hub, runs 3 examples including the FineWeb-Edu ETL

Install in any environment:
```
pip install --no-deps --force-reinstall \
  "https://huggingface.co/datasets/davanstrien/polars-hf-bucket-sink-wheels/resolve/main/polars-1.38.1-py3-none-any.whl" \
  "https://huggingface.co/datasets/davanstrien/polars-hf-bucket-sink-wheels/resolve/main/polars_runtime_32-1.38.1-cp310-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
```

### What's next

This is a PoC to validate the approach. Would love feedback on:
- Whether this integration pattern makes sense for Polars
- The right way to land this upstream (feature-gated, separate crate, etc.)
- Read support (`scan_parquet("hf://buckets/...")`) as a follow-up

Happy to walk through the code or hop on a call!
