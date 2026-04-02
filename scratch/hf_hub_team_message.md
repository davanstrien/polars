# Message for HF Hub Team

## Subject: URL encoding affects rate limit classification (resolvers vs pages)

Hi!

While working on fixing rate limiting issues in Polars' HF Hub integration (https://github.com/pola-rs/polars/issues/25389), I discovered something interesting about how URL encoding affects rate limit classification.

### The Issue

When accessing files via `/resolve/`, URLs with percent-encoded slashes (`%2F`) are classified as **"pages"** requests instead of **"resolvers"** requests, resulting in much lower rate limits.

### Test Results

I tested two URL formats for the same file:

**Correct URL (slashes preserved):**
```
https://huggingface.co/datasets/HuggingFaceFW/fineweb-2/resolve/main/data/aai_Latn/train/000_00000.parquet
```
Response header: `ratelimit: "resolvers";r=2999;t=289`

**Encoded URL (slashes as %2F):**
```
https://huggingface.co/datasets/HuggingFaceFW%2Ffineweb-2/resolve/main/data%2Faai_Latn%2Ftrain%2F000_00000.parquet
```
Response header: `ratelimit: "pages";r=99;t=288`

Both URLs resolve to the same file, but the encoded version gets classified as "pages" with ~30x lower limits (100 vs 3000 per 5 minutes for anonymous users).

### Question

Is this behavior intentional? I can see arguments either way:
- **Intentional**: The URL pattern matching expects proper path structure with real slashes
- **Unintentional**: Semantically equivalent URLs should get the same rate limit treatment

We've fixed this in Polars by not encoding slashes (matching `huggingface_hub`'s behavior), but wanted to flag this in case:
1. Other libraries might hit the same issue
2. You might want to normalize URLs before classification
3. This should be documented somewhere

Happy to provide more details or the test script if helpful!

---

*Test script available at: test_url_encoding_ratelimit.py in the Polars repo*
