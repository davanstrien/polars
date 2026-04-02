#!/usr/bin/env python3
"""
Benchmark Polars vs PyArrow parquet writing on real text data.
Tests whether Polars' dictionary encoding advantage holds for high-cardinality text.
"""

import tempfile
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from datasets import load_dataset


def get_file_size(path: Path) -> int:
    return path.stat().st_size


def format_size(size: int) -> str:
    if size >= 1024 * 1024:
        return f"{size / (1024 * 1024):.2f} MB"
    return f"{size / 1024:.2f} KB"


def benchmark_dataset(name: str, config: str | None, split: str, text_column: str, n_rows: int = 10000):
    """Benchmark a single dataset."""
    print(f"\n{'='*60}")
    print(f"Dataset: {name} ({n_rows} rows)")
    print(f"Text column: {text_column}")
    print("=" * 60)

    # Load dataset
    if config:
        ds = load_dataset(name, config, split=split, streaming=True)
    else:
        ds = load_dataset(name, split=split, streaming=True)

    rows = list(ds.take(n_rows))

    # Convert to both formats
    df_polars = pl.DataFrame(rows)
    table_arrow = pa.Table.from_pylist(rows)

    # Check cardinality
    unique_texts = df_polars[text_column].n_unique()
    print(f"Unique values in '{text_column}': {unique_texts}/{n_rows} ({100*unique_texts/n_rows:.1f}%)")

    # Get sample text lengths
    text_lens = df_polars[text_column].str.len_bytes()
    print(f"Text length: min={text_lens.min()}, median={text_lens.median():.0f}, max={text_lens.max()}")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Write with Polars
        polars_path = tmpdir / "polars.parquet"
        df_polars.write_parquet(polars_path)
        polars_size = get_file_size(polars_path)

        # Write with PyArrow (default)
        pyarrow_path = tmpdir / "pyarrow.parquet"
        pq.write_table(table_arrow, pyarrow_path)
        pyarrow_size = get_file_size(pyarrow_path)

        # Write with PyArrow + compression matching Polars (zstd)
        pyarrow_zstd_path = tmpdir / "pyarrow_zstd.parquet"
        pq.write_table(table_arrow, pyarrow_zstd_path, compression="zstd")
        pyarrow_zstd_size = get_file_size(pyarrow_zstd_path)

        print(f"\nFile sizes:")
        print(f"  Polars (zstd default):  {format_size(polars_size)}")
        print(f"  PyArrow (snappy):       {format_size(pyarrow_size)}")
        print(f"  PyArrow (zstd):         {format_size(pyarrow_zstd_size)}")
        print(f"\nPolars vs PyArrow (zstd): {polars_size/pyarrow_zstd_size:.2f}x")

        # Check what encoding Polars actually used
        polars_meta = pq.read_metadata(polars_path)
        pyarrow_meta = pq.read_metadata(pyarrow_path)

        print(f"\nRow groups: Polars={polars_meta.num_row_groups}, PyArrow={pyarrow_meta.num_row_groups}")

        # Find the text column and check encoding
        for i in range(polars_meta.num_row_groups):
            rg = polars_meta.row_group(i)
            for j in range(rg.num_columns):
                col = rg.column(j)
                if text_column in col.path_in_schema:
                    print(f"\nPolars '{text_column}' encoding: {col.encodings}")
                    print(f"  Compressed size: {format_size(col.total_compressed_size)}")
                    break

        for i in range(pyarrow_meta.num_row_groups):
            rg = pyarrow_meta.row_group(i)
            for j in range(rg.num_columns):
                col = rg.column(j)
                if text_column in col.path_in_schema:
                    print(f"PyArrow '{text_column}' encoding: {col.encodings}")
                    print(f"  Compressed size: {format_size(col.total_compressed_size)}")
                    break

        return {
            "dataset": name,
            "polars_size": polars_size,
            "pyarrow_size": pyarrow_zstd_size,
            "ratio": polars_size / pyarrow_zstd_size,
            "cardinality": unique_texts / n_rows,
        }


def main():
    results = []

    # Test 1: High-cardinality text (Wikipedia articles)
    results.append(benchmark_dataset(
        name="wikimedia/wikipedia",
        config="20231101.en",
        split="train",
        text_column="text",
        n_rows=5000,  # Wikipedia articles are long
    ))

    # Test 2: Medium-cardinality (news headlines - some duplicates possible)
    results.append(benchmark_dataset(
        name="SetFit/ag_news",
        config=None,
        split="train",
        text_column="text",
        n_rows=10000,
    ))

    # Test 3: Low-cardinality (categorical labels for comparison)
    results.append(benchmark_dataset(
        name="SetFit/ag_news",
        config=None,
        split="train",
        text_column="label_text",
        n_rows=10000,
    ))

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"{'Dataset':<30} {'Cardinality':<12} {'Polars/PyArrow':<15}")
    print("-" * 60)
    for r in results:
        print(f"{r['dataset']:<30} {r['cardinality']*100:>6.1f}%      {r['ratio']:.2f}x")

    print("\n< 1.0 = Polars smaller, > 1.0 = PyArrow smaller")


if __name__ == "__main__":
    main()
