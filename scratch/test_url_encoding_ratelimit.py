"""
Test script to verify if URL encoding differences affect rate limit classification.

Hypothesis: Polars encodes slashes in URLs which may cause HF Hub to classify
requests as "pages" instead of "resolvers", resulting in lower rate limits.

This script tests both URL formats to see which rate limit bucket they hit.
"""

import httpx
import time
import os

# Get HF token if available
HF_TOKEN = os.environ.get("HF_TOKEN")
headers = {"User-Agent": "polars/0.52.0"}
if HF_TOKEN:
    headers["Authorization"] = f"Bearer {HF_TOKEN}"
    print(f"✓ Using HF_TOKEN")
else:
    print("⚠ No HF_TOKEN set - using anonymous requests (lower limits)")

print()

# Test file from fineweb-2
repo = "HuggingFaceFW/fineweb-2"
filepath = "data/aai_Latn/train/000_00000.parquet"

# Two URL formats
url_correct = f"https://huggingface.co/datasets/{repo}/resolve/main/{filepath}"
url_encoded = f"https://huggingface.co/datasets/HuggingFaceFW%2Ffineweb-2/resolve/main/data%2Faai_Latn%2Ftrain%2F000_00000.parquet"

print("=" * 70)
print("URL COMPARISON")
print("=" * 70)
print(f"Correct (huggingface_hub style):\n  {url_correct}\n")
print(f"Encoded (Polars style):\n  {url_encoded}\n")

def test_url_no_redirect(name: str, url: str, num_requests: int = 3):
    """Make HEAD requests WITHOUT following redirects to see HF Hub's rate limit headers."""
    print(f"\n{'=' * 70}")
    print(f"Testing: {name} (no redirect follow)")
    print(f"{'=' * 70}")

    # Don't follow redirects - we want to see HF Hub's response, not CDN's
    with httpx.Client(follow_redirects=False) as client:
        for i in range(num_requests):
            try:
                resp = client.head(url, headers=headers, timeout=30)

                # Check for rate limit headers
                ratelimit = resp.headers.get("ratelimit", "N/A")
                ratelimit_policy = resp.headers.get("ratelimit-policy", "N/A")

                print(f"\nRequest {i+1}:")
                print(f"  Status: {resp.status_code}")
                print(f"  ratelimit: {ratelimit}")
                print(f"  ratelimit-policy: {ratelimit_policy}")

                # Show all headers for debugging
                if i == 0:
                    print(f"  All headers: {dict(resp.headers)}")

                if resp.status_code == 429:
                    print(f"  ⚠️  RATE LIMITED!")
                    break

                # Small delay between requests
                time.sleep(0.1)

            except Exception as e:
                print(f"  Error: {e}")
                break

print("\n" + "=" * 70)
print("TEST 1: Correct URL format (slashes preserved)")
print("=" * 70)
test_url_no_redirect("Correct URL", url_correct, num_requests=3)

print("\n" + "=" * 70)
print("TEST 2: Encoded URL format (slashes as %2F)")
print("=" * 70)
test_url_no_redirect("Encoded URL", url_encoded, num_requests=3)

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
print("""
Look at the 'ratelimit' header values above.
- Format: "resource_type";r=remaining;t=reset_seconds
- 'resolver' = high limits (12k/5min for PRO)
- 'pages' = low limits (400/5min for PRO)

If both show 'resolver', encoding is not the issue.
If encoded shows 'pages', that's the bug!
""")
