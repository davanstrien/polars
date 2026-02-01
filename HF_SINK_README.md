# Polars HF Hub Sink (Experimental)

Native streaming writes to Hugging Face Hub from Polars. Write DataFrames directly to HF datasets with `sink_parquet("hf://datasets/user/repo/...")` - no intermediate files, true streaming with O(shard_size) memory usage.

> **Status:** Experimental pre-release on the `feature/hf-hub-sink` branch.

## About This Project

This is an experiment in seeing how far a developer (Daniel van Strien) can get building a Rust feature for a large codebase using Claude Code as a development partner. The entire implementation - ~5000 lines of Rust across 15+ files - was developed with heavy AI assistance.

The feature itself is functional and tested, but this is primarily a learning exercise and proof-of-concept rather than a polished production feature.

## Quick Start

```python
import polars as pl

# Write a DataFrame to HF Hub
df = pl.DataFrame({"text": ["hello", "world"], "label": [0, 1]})
df.write_parquet(
    "hf://datasets/username/my-dataset/data/train.parquet",
    storage_options={"token": "hf_xxx"}
)
```

## Installation

### Option 1: Pre-built Wheel (Linux x64)

<!-- TODO: Update wheel URLs when release is created -->
```bash
# Install base package + runtime wheel
pip install <wheel-url-tbd>
```

### Option 2: Build from Source

```bash
git clone https://github.com/davanstrien/polars.git
cd polars
git checkout feature/hf-hub-sink

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Build (takes ~15-20 minutes)
cd py-polars
maturin develop --release -m runtime/polars-runtime-32/Cargo.toml
```

## Features

- **Streaming writes** - `sink_parquet("hf://...")` streams data with constant memory
- **Eager writes** - `write_parquet("hf://...")` for smaller DataFrames
- **Large file support** - Multipart uploads for files >16MB (tested up to 576MB)
- **Write modes** - `ErrorIfExists`, `Overwrite`, `Append`
- **Partitioned writes** - Hive-style `col=value/` directory partitioning
- **Checkpoint/resume** - Resume interrupted uploads from local checkpoint
- **Auto README** - Generates dataset card (README.md) automatically

## Known Limitations

1. **Nested/List columns not supported** - Columns with List, Array, or Struct types will cause a panic in streaming mode. Use primitive types (String, Int, Float, Boolean) only. This is an upstream Polars parquet streaming limitation, not specific to HF sink.

2. **Datasets only** - Currently supports `hf://datasets/...` URLs. Models and Spaces are not yet supported.

3. **Platform availability** - Pre-built wheels for Linux x64. macOS and Windows users need to build from source.

4. **Memory usage** - Uses O(shard_size) memory per worker. Default shard size is ~500MB.

5. **Experimental** - This is a proof-of-concept. The API and behavior may change.

## Usage Examples

### Basic Upload

```python
import polars as pl

df = pl.DataFrame({
    "id": range(1000),
    "text": [f"sample text {i}" for i in range(1000)],
    "score": [i * 0.1 for i in range(1000)]
})

df.write_parquet(
    "hf://datasets/username/my-dataset/data/train.parquet",
    storage_options={"token": "hf_xxx"}
)
```

### "Hub is your disk" - Streaming Pattern

Process large datasets with minimal RAM by streaming from one HF dataset to another:

```python
import polars as pl

# Lazy scan - no data loaded yet
lf = pl.scan_parquet(
    "hf://datasets/source-org/large-dataset/data/*.parquet",
    storage_options={"token": "hf_xxx"}
)

# Filter/transform (still lazy)
filtered = lf.filter(pl.col("quality_score") > 0.8).select(["text", "label"])

# Stream directly to HF Hub - constant memory!
filtered.sink_parquet(
    "hf://datasets/username/filtered-dataset/data/train.parquet",
    storage_options={"token": "hf_xxx"}
)
```

This pattern enables processing TB-scale datasets with only ~500MB RAM.

### Write Modes

```python
# Overwrite existing files
df.write_parquet(
    "hf://datasets/user/repo/data/train.parquet",
    storage_options={"token": "hf_xxx"},
    hf_options={"write_mode": "overwrite"}
)

# Append to existing dataset
df.write_parquet(
    "hf://datasets/user/repo/data/train.parquet",
    storage_options={"token": "hf_xxx"},
    hf_options={"write_mode": "append"}
)
```

### Partitioned Writes

```python
df.write_parquet(
    "hf://datasets/user/repo/data/",
    storage_options={"token": "hf_xxx"},
    partition_by=["language", "split"]
)
# Creates: data/language=en/split=train/xxx.parquet
```

## Authentication

The HF token is resolved in this order:
1. Explicit `storage_options={"token": "hf_xxx"}`
2. `HF_TOKEN` environment variable
3. `~/.cache/huggingface/token` file (from `huggingface-cli login`)

## Troubleshooting

### "upload channel closed unexpectedly"
Usually indicates a network issue or rate limiting. The sink retries automatically with exponential backoff.

### Panic with List/Array columns
Use primitive types only. Filter out or flatten nested columns before writing.

## Links

- **Branch:** [feature/hf-hub-sink](https://github.com/davanstrien/polars/tree/feature/hf-hub-sink)
- **Test repository:** [davanstrien/test-polars-streaming](https://huggingface.co/datasets/davanstrien/test-polars-streaming)
