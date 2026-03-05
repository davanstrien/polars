"""End-to-end tests for the HF bucket sink.

These tests upload real data to a Hugging Face bucket and are gated behind:
  - pytest.mark.slow  (excluded from normal pytest runs)
  - HF_TOKEN env var  (auto-skipped if absent via the hf_token fixture)
  - huggingface_hub   (auto-skipped if not installed)
"""

from __future__ import annotations

import uuid

import pytest

import polars as pl
from polars.testing import assert_frame_equal

pytest.importorskip("huggingface_hub")
pytestmark = [pytest.mark.slow]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_target(namespace: str, bucket: str, prefix: str = "test") -> tuple[str, str]:
    """Return (hf_bucket_url, file_name) with a unique uuid suffix."""
    file_name = f"{prefix}_{uuid.uuid4().hex[:12]}.parquet"
    url = f"hf://buckets/{namespace}/{bucket}/{file_name}"
    return url, file_name


def _read_back(
    namespace: str, bucket: str, file_name: str, token: str
) -> pl.DataFrame:
    """Download a file from the bucket and return as a DataFrame."""
    import os
    import tempfile

    from huggingface_hub import download_bucket_files

    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, file_name)
        download_bucket_files(
            f"{namespace}/{bucket}",
            files=[(file_name, local_path)],
            token=token,
        )
        return pl.read_parquet(local_path)


# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


class TestHfBucketSinkSmoke:
    def test_3_rows(self, hf_bucket_config: dict) -> None:
        """Minimal write succeeds without error."""
        cfg = hf_bucket_config
        url, _file_name = _make_target(cfg["namespace"], cfg["bucket_name"], "smoke3")

        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df.lazy().sink_parquet(url, storage_options=cfg["storage_options"])

    def test_write_read_back(self, hf_bucket_config: dict) -> None:
        """Write + download + assert_frame_equal."""
        cfg = hf_bucket_config
        url, file_name = _make_target(
            cfg["namespace"], cfg["bucket_name"], "roundtrip"
        )

        df = pl.DataFrame(
            {
                "id": list(range(50)),
                "value": [float(x) * 1.1 for x in range(50)],
                "label": [f"row_{x}" for x in range(50)],
            }
        )
        df.lazy().sink_parquet(url, storage_options=cfg["storage_options"])

        result = _read_back(
            cfg["namespace"],
            cfg["bucket_name"],
            file_name,
            cfg["storage_options"]["token"],
        )
        assert_frame_equal(result, df)


# ---------------------------------------------------------------------------
# Medium-sized tests
# ---------------------------------------------------------------------------


class TestHfBucketSinkMedium:
    def test_10k_synthetic_rows(self, hf_bucket_config: dict) -> None:
        """10K rows with 4 columns, exercises the streaming path."""
        cfg = hf_bucket_config
        url, file_name = _make_target(cfg["namespace"], cfg["bucket_name"], "med10k")

        n = 10_000
        df = pl.DataFrame(
            {
                "id": list(range(n)),
                "value": [float(x) * 0.01 for x in range(n)],
                "category": [f"cat_{x % 20}" for x in range(n)],
                "flag": [x % 2 == 0 for x in range(n)],
            }
        )

        # Use LazyFrame.sink_parquet for the streaming path.
        df.lazy().sink_parquet(url, storage_options=cfg["storage_options"])

        result = _read_back(
            cfg["namespace"],
            cfg["bucket_name"],
            file_name,
            cfg["storage_options"]["token"],
        )
        assert_frame_equal(result, df)


# ---------------------------------------------------------------------------
# Large-scale tests
# ---------------------------------------------------------------------------


class TestHfBucketSinkLarge:
    def test_10m_synthetic_rows(self, hf_bucket_config: dict) -> None:
        """10M rows with 6 columns (~500 MB+), heavy streaming stress test.

        Validates that the streaming upload pipeline handles large data
        without OOM or timeout. Uses diverse column types to exercise
        parquet encoding paths.
        """
        cfg = hf_bucket_config
        url, file_name = _make_target(cfg["namespace"], cfg["bucket_name"], "large10m")

        n = 10_000_000
        df = pl.DataFrame(
            {
                "id": pl.arange(0, n, eager=True),
                "value_f64": pl.arange(0, n, eager=True).cast(pl.Float64) * 0.001,
                "category": (pl.arange(0, n, eager=True) % 100).cast(pl.Utf8),
                "flag": (pl.arange(0, n, eager=True) % 2).cast(pl.Boolean),
                "small_int": (pl.arange(0, n, eager=True) % 256).cast(pl.UInt8),
                "text": pl.Series([f"row_{i:08d}_payload" for i in range(n)]),
            }
        )

        df.lazy().sink_parquet(url, storage_options=cfg["storage_options"])

        result = _read_back(
            cfg["namespace"],
            cfg["bucket_name"],
            file_name,
            cfg["storage_options"]["token"],
        )
        assert result.shape == df.shape
        assert result.schema == df.schema
        # Spot-check first and last rows instead of full frame equality
        # (full comparison on 10M rows is slow).
        assert_frame_equal(result.head(100), df.head(100))
        assert_frame_equal(result.tail(100), df.tail(100))
