#!/usr/bin/env python3
"""
Smoke test for HF Hub sink functionality.

This script tests the HF sink feature as it's being developed.
Run with: .venv/bin/python test_hf_sink.py
"""
import polars as pl


def test_version():
    """Verify polars is importable and print version."""
    print(f"Polars version: {pl.__version__}")
    return True


def test_hf_sink_basic():
    """Test basic HF sink functionality (placeholder until implemented)."""
    df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

    # Target API (will work once HF sink is implemented):
    # df.write_parquet("hf://datasets/test-user/test-repo/data/test.parquet")
    #
    # Or with options:
    # df.lazy().sink_parquet(
    #     "hf://datasets/test-user/test-repo/data/train.parquet",
    #     hf_options={
    #         "split": "train",
    #         "max_shard_size": "500MB",
    #         "mode": "overwrite",
    #         "commit_message": "Upload via Polars",
    #     },
    # )

    print("HF sink: Not yet implemented")
    print(f"Test DataFrame shape: {df.shape}")
    return True


def test_hf_url_parsing():
    """Test that hf:// URLs can be parsed (placeholder)."""
    test_urls = [
        "hf://datasets/user/repo/data/train.parquet",
        "hf://datasets/user/repo@main/data/train.parquet",
        "hf://datasets/org/repo/data/train-*.parquet",
    ]

    for url in test_urls:
        print(f"  URL: {url}")

    # Once implemented, these should parse without error
    print("HF URL parsing: Not yet tested (read path exists, write path TBD)")
    return True


def main():
    print("=" * 60)
    print("HF Hub Sink Smoke Test")
    print("=" * 60)
    print()

    tests = [
        ("Version check", test_version),
        ("HF sink basic", test_hf_sink_basic),
        ("HF URL parsing", test_hf_url_parsing),
    ]

    results = []
    for name, test_fn in tests:
        print(f"[TEST] {name}")
        try:
            passed = test_fn()
            results.append((name, "PASS" if passed else "FAIL"))
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append((name, "ERROR"))
        print()

    print("=" * 60)
    print("Results:")
    for name, status in results:
        print(f"  {status}: {name}")
    print("=" * 60)


if __name__ == "__main__":
    main()
