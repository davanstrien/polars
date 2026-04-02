# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "huggingface_hub",
#     "polars",
#     "hf-xet",
# ]
# ///
"""Download and validate a parquet file from an HF Bucket."""

import os
import tempfile

import polars as pl
from huggingface_hub import download_bucket_files

NAMESPACE = "davanstrien"
BUCKET = "polars-ci-test"
FILE = "stress-fineweb-edu-10bt-filtered.parquet"
TOKEN = os.environ["HF_TOKEN"]

with tempfile.TemporaryDirectory() as tmpdir:
    local_path = f"{tmpdir}/{FILE}"
    print(f"Downloading {FILE}...")
    download_bucket_files(
        f"{NAMESPACE}/{BUCKET}",
        files=[(FILE, local_path)],
        token=TOKEN,
    )

    print("Reading parquet...")
    df = pl.read_parquet(local_path)

print(f"Shape: {df.shape}")
print(f"Schema: {df.schema}")
print(f"\nFirst 5 rows:")
print(df.head(5))
print(f"\nLast 5 rows:")
print(df.tail(5))
print(f"\nNull counts:")
print(df.null_count())
