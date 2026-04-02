# Tweet Draft: Polars HF Bucket Sink PoC

## Benchmarks (Dutch run)
- 74 GB scanned (25 parquet shards)
- 650 MB output
- 18 minutes on a 2-vCPU HF Jobs instance
- Constant memory throughout

---

## Draft (Twitter/Bluesky)

I just scanned 74GB of Dutch PDFs from @HuggingFaceFW's FinePDFs dataset, filtered to high-quality educational content, and streamed the result directly to an HF Bucket.

18 minutes. No local disk. No collect(). One script:

```python
pl.scan_parquet(
    "hf://datasets/HuggingFaceFW/finepdfs/data/nld_Latn/train/*.parquet",
    storage_options={"token": token},
)
.filter(...)  # edu quality, dedup, token count
.sink_parquet(
    "hf://buckets/davanstrien/finepdfs-edu-gold/nld-edu-gold.parquet",
    storage_options={"token": token},
)
```

Hub is your disk — on both ends. Data streams from HF dataset → Polars → HF Bucket with constant memory.

This is a PoC branch adding native `sink_parquet("hf://buckets/...")` support to Polars via the XET protocol. Full 3.65TB multilingual run is going too.

Full example + branch: https://github.com/davanstrien/polars/tree/feature/hf-bucket-sink

---

## Draft (LinkedIn — slightly longer)

I just scanned 74GB of Dutch PDFs from HuggingFace's FinePDFs dataset (3.65TB, 475M documents, 1,733 languages), filtered to high-quality educational content, and streamed the result directly to an HF Storage Bucket.

18 minutes on a cheap 2-vCPU instance. No local disk. No intermediate files. One Python script.

The pattern:

```python
pl.scan_parquet("hf://datasets/.../nld_Latn/train/*.parquet")
  .filter(...)  # quality scores, dedup, token count
  .sink_parquet("hf://buckets/.../output.parquet")
```

This is a proof-of-concept I've been working on: native streaming Polars writes to HuggingFace's new Storage Buckets via the XET protocol. Data flows Hub → Polars → Hub with constant memory — O(row_group_size), not O(dataset_size).

Now that HF Buckets are publicly available (https://huggingface.co/blog/storage-buckets), this "hub is your disk" pattern works end-to-end. Scan terabytes of public data, transform, and write back — without ever touching local storage.

PoC branch: https://github.com/davanstrien/polars/tree/feature/hf-bucket-sink
Output: https://huggingface.co/buckets/davanstrien/finepdfs-edu-gold

---

## Code screenshot content (for image)

```python
import polars as pl

token = os.environ["HF_TOKEN"]
so = {"token": token}

# Scan 74GB of Dutch PDFs from FinePDFs
# Filter for high educational quality
# Sink directly to an HF Bucket — no local disk
(
    pl.scan_parquet(
        "hf://datasets/HuggingFaceFW/finepdfs/data/nld_Latn/train/*.parquet",
        storage_options=so,
    )
    .filter(pl.col("fw_edu_scores").list.mean() > 3.0)
    .filter(pl.col("duplicate_count") == 0)
    .filter(~pl.col("is_truncated"))
    .filter(pl.col("token_count") > 500)
    .select("id", "url", "text", "token_count", "language")
    .sink_parquet(
        "hf://buckets/davanstrien/finepdfs-edu-gold/nld-edu-gold.parquet",
        storage_options=so,
    )
)
# 74 GB → 650 MB in 18 min. Constant memory. No intermediate files.
```
