"""E2E tests for HF Hub sink (write_parquet to hf:// URLs)."""
from __future__ import annotations

import uuid

import pytest

import polars as pl
from polars.testing import assert_frame_equal


@pytest.mark.hf_hub
@pytest.mark.write_disk
def test_basic_upload(hf_token: str, hf_test_repo: str) -> None:
    """Test basic DataFrame upload to HF Hub."""
    # Create test DataFrame
    df = pl.DataFrame({
        "id": [1, 2, 3],
        "text": ["hello", "world", "test"],
    })

    # Generate unique path to avoid conflicts
    test_id = uuid.uuid4().hex[:8]
    path = f"hf://datasets/{hf_test_repo}/data/e2e-basic-{test_id}.parquet"

    # Write to HF Hub
    df.write_parquet(path, storage_options={"token": hf_token})

    # Verify by reading back
    result = pl.read_parquet(path, storage_options={"token": hf_token})

    # Assert data matches
    assert result.shape == df.shape
    assert_frame_equal(result.sort("id"), df.sort("id"))
