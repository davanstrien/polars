# Pre-share TODOs for HF Bucket Sink PoC

## Before sharing with Polars team / publicly

### Notebook
- [ ] Add FineWeb-Edu code cell — Example 3 is markdown-only, needs actual ETL code
- [ ] Verify notebook runs end-to-end on Colab

### Messaging
- [ ] Note that Buckets requires access (not yet public) — or wait for GA launch
- [ ] Send URL encoding rate limit finding to Hub team (hf_hub_team_message.md)

### Code
- [ ] Rebase onto latest main (17 ahead, 78 behind; Cargo.lock + streaming engine conflicts)
- [ ] Re-test after rebase — streaming engine files touched by both sides
- [ ] Wheels are x86_64 Linux only — note clearly or build additional platforms

### Distribution strategy
- [ ] Internal Slack posted (done)
- [ ] If no traction: GitHub Discussion on pola-rs/polars after Buckets GA
- [ ] Frame as RFC / PoC, not a finished PR
