from __future__ import annotations

import os

import pytest


@pytest.fixture
def hf_token() -> str:
    """Get HF token from environment, skip if not available."""
    token = os.environ.get("HF_TOKEN")
    if not token:
        pytest.skip("HF_TOKEN environment variable not set")
    return token


@pytest.fixture
def hf_test_repo() -> str:
    """Test repository for HF Hub E2E tests."""
    return os.environ.get("HF_TEST_REPO", "davanstrien/test-polars-streaming")
