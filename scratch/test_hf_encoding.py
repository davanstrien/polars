"""
Test HF Hub URL encoding fix.

This tests the fix for https://github.com/pola-rs/polars/pull/25521
to address review comments from @nameexhaustion.

The fix ensures:
1. Slashes are NOT encoded (to avoid rate limit misclassification)
2. Special characters (spaces, colons, etc.) ARE encoded (for file downloads to work)
"""

import polars as pl


def test_hive_dates_with_special_chars():
    """
    Test scanning hive-partitioned data with special characters in paths.

    The hive_dates dataset has paths like:
    hive_dates/date1=2024-01-01/date2=2023-01-01 00:00:00.000000/00000000.parquet

    The spaces and colons need to be percent-encoded for downloads to work.
    """
    print("Testing hf://datasets/nameexhaustion/polars-docs/hive_dates ...")
    try:
        q = pl.scan_parquet("hf://datasets/nameexhaustion/polars-docs/hive_dates")
        df = q.collect()
        print(f"  Success! Got {len(df)} rows")
        print(f"  Columns: {df.columns}")
        print(df.head(3))
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def test_special_chars_glob_false():
    """
    Test scanning files with special characters using glob=False.

    The special-chars directory has files like:
    - sp ace.parquet (space)
    - %.parquet (percent sign)
    - [*.parquet (bracket and asterisk)
    """
    print("\nTesting special-chars files with glob=False ...")

    test_files = [
        "hf://datasets/nameexhaustion/polars-docs/special-chars/sp ace.parquet",
        "hf://datasets/nameexhaustion/polars-docs/special-chars/%.parquet",
        "hf://datasets/nameexhaustion/polars-docs/special-chars/[*.parquet",
    ]

    all_passed = True
    for file_path in test_files:
        print(f"  Testing: {file_path}")
        try:
            q = pl.scan_parquet(file_path, glob=False)
            df = q.collect()
            print(f"    Success! Got {len(df)} rows")
        except Exception as e:
            print(f"    FAILED: {e}")
            all_passed = False

    return all_passed


def test_fineweb2_glob_pattern():
    """
    Test that glob patterns with many subdirectories don't hit rate limits.

    This tests the recursive tree API fix (not just encoding).
    Uses a smaller subset to avoid long download times.
    """
    print("\nTesting fineweb-2 glob pattern (should use recursive tree API) ...")
    try:
        # Just list files, don't actually download data
        q = pl.scan_parquet(
            "hf://datasets/HuggingFaceFW/fineweb-2/data/aai_Latn/train/*.parquet"
        )
        # Get schema to verify it can find files
        schema = q.collect_schema()
        print(f"  Success! Schema: {schema}")
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def test_parquet_revision_shortcut():
    """
    Test the @~parquet revision shortcut.

    The ~parquet shortcut points to the refs/convert/parquet branch which
    contains auto-converted parquet versions of datasets.
    """
    print("\nTesting @~parquet revision shortcut ...")
    try:
        q = pl.scan_parquet(
            "hf://datasets/openai/gsm8k@~parquet/main/train/0000.parquet"
        )
        schema = q.collect_schema()
        print(f"  Success! Schema: {schema}")
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def test_example_from_review():
    """
    Test the example from the review comment:"""
    print("\nTesting example from review comment ...")
    try:
        df = pl.scan_parquet(
            "hf://datasets/nameexhaustion/polars-docs/special-chars/[*.parquet",
            glob=False,
        )
        print(df.collect().head())
        print(f"  Success! Got {len(df.collect())} rows")
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Testing HF Hub URL encoding fix")
    print("=" * 60)

    results = [("hive_dates", test_hive_dates_with_special_chars())]

    # Test 2: Special characters with glob=False (reviewer's test case)
    results.append(("special-chars glob=False", test_special_chars_glob_false()))

    # Test 3: Glob pattern with recursive tree API
    results.append(("fineweb-2 glob", test_fineweb2_glob_pattern()))

    # Test 4: @~parquet revision shortcut
    results.append(("@~parquet revision", test_parquet_revision_shortcut()))

    # Test 5: Example from review comment
    results.append(("example from review", test_example_from_review()))

    print("\n" + "=" * 60)
    print("RESULTS:")
    print("=" * 60)
    all_passed = True
    for name, passed in results:
        status = "PASS" if passed else "FAIL"
        print(f"  {name}: {status}")
        if not passed:
            all_passed = False

    print("\n" + ("All tests passed!" if all_passed else "Some tests FAILED!"))
    exit(0 if all_passed else 1)
