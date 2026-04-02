"""
Post-merge validation script for Colab (2026-02-20)

Instructions:
1. Download both wheel artifacts from CI run:
   gh run download <RUN_ID> -R davanstrien/polars -D wheels/
2. Upload the .whl files to Colab (or use gdown/wget from a GH release)
3. Run this script in a Colab cell

IMPORTANT: Use --no-deps to prevent pip from replacing the custom
polars-runtime-32 wheel with the upstream PyPI version.

Expected: Both smoke test and larger scan→filter→sink complete without error.
"""

# -- Cell 1: Install wheels --
# !pip uninstall polars polars-runtime-32 -y
# !pip install --no-deps --force-reinstall polars-*.whl polars_runtime_32-*.whl

# -- Cell 2: Setup --
import os
from google.colab import userdata

os.environ["HF_TOKEN"] = userdata.get("HF_TOKEN")
TOKEN = os.environ["HF_TOKEN"]

import polars as pl

print(f"Polars version: {pl.__version__}")
pl.show_versions()

# -- Cell 3: Smoke test (3 rows) --
print("\n=== Smoke test: 3 rows ===")
pl.DataFrame({"a": [1, 2, 3]}).lazy().sink_parquet(
    "hf://buckets/davanstrien/test-polars-bucket/post-merge-smoke.parquet",
    storage_options={"token": TOKEN},
)
print("Smoke test PASSED")

# -- Cell 4: Larger scan → filter → sink (10K rows) --
print("\n=== Scan → filter → sink: 10K rows ===")
lf = pl.scan_parquet(
    "hf://datasets/nvidia/OpenMathReasoning/data/cot-*.parquet",
    storage_options={"token": TOKEN},
)
lf.filter(pl.col("generated_solution").str.len_chars() > 500).head(10_000).sink_parquet(
    "hf://buckets/davanstrien/test-polars-bucket/post-merge-10k.parquet",
    storage_options={"token": TOKEN},
)
print("Scan→filter→sink test PASSED")

print("\n=== Post-merge validation complete! ===")
