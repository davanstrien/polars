from __future__ import annotations

import os

import pytest


@pytest.fixture
def hf_token() -> str:
    """Return HF_TOKEN from environment, skip test if absent."""
    token = os.environ.get("HF_TOKEN")
    if not token:
        pytest.skip("HF_TOKEN not set")
    return token


@pytest.fixture
def hf_bucket_config(hf_token: str) -> dict:
    """Return standard HF bucket config for tests.

    Keys: namespace, bucket_name, storage_options.
    """
    namespace = os.environ.get("HF_BUCKET_NAMESPACE", "davanstrien")
    bucket_name = os.environ.get("HF_BUCKET_NAME", "polars-ci-test")
    return {
        "namespace": namespace,
        "bucket_name": bucket_name,
        "storage_options": {"token": hf_token},
    }
