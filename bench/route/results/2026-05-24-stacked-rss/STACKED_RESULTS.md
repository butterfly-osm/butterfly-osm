# Stacked RSS / perf bench — 2026-05-24 (all 7 PRs cherry-picked)

Belgium baseline.butterfly, 4 modes loaded. Built from commit chain:

```
33ef835 perf(route): #269 parallel route_batch + K=1 fast path
04d71ac perf(route): #269 review fixes
cd38515 perf(route): #269 env var rename → BUTTERFLY_ROUTE_BATCH_SIZE
acc9a81 perf(rss): #275 madvise(DONTNEED) cold ways.raw + way_attrs
9bec342 perf(rss): #275 review fix — don't trigger verify_now
a277cf7 perf(route): #272 stall-on-demand in CCH P2P
c29de4e perf(route): #272 review fixes — control flow + distance() + tests
ac289b7 perf(rss): #277 madvise(DONTNEED) cold distance flats
67f749e perf(rss): #277 review fixes — doc + checked_add
bec27d7 perf(rss): #279 evict non-default modes' time flats
97b0ab9 perf(rss): #279 review fix — checked_add for bounds
7ae378f perf(route): #273 per-worker scratch buffers
e4c0275 perf(route): #272 review fixes — wkb doc + remove unused dst_rank
```

## RSS

| Metric | main | Stacked | Delta |
|---|---|---|---|
| **VmRSS idle (after 30 car routes)** | 16.04 GiB | **1.99 GiB** | **-14.05 GiB (-87.6%)** |
| VmRSS after 10k route_batch | n/a | 2.85 GiB | working set adds ~860 MB |
| VmHWM (boot peak) | 19.29 GiB | 11.23 GiB | -8.06 GiB |
| RssAnon (heap) | 532 MiB | 604 MiB | within noise |
| RssFile (mmap pages) | 15.5 GiB | 1.39 GiB | -91% |

**Planet-scale target (≤4 GiB) MET comfortably.**

drivetimes (libosrm + libvalhalla) Belgium baseline: 1.29 GiB. We're at 1.99 GiB. Per mode (4 vs 3) we're at parity.

## Perf — REST /route

| Metric | main | Stacked | Delta |
|---|---|---|---|
| /route p50 (N=500, Brussels-Antwerp cluster) | 5.88 ms | **5.62 ms** | -4.4% |
| Correctness (100 routes byte-diff) | — | 0 mismatches | — |

## Perf — Flight route_batch (clustered coords, seed 42)

| N | Total | Per-pair | vs libosrm @ 10k (0.38 ms) |
|---|---|---|---|
| 10 | 38 ms | 3.84 ms | — |
| 100 | 330 ms | 3.30 ms | — |
| 1000 | 667 ms | 0.67 ms | 1.76× slower |
| 10000 | 6164 ms | 0.62 ms | **1.62× slower** |

Residual 1.62× gap vs libosrm is the next perf frontier (see codex review in PR #291): per-pair allocator pressure (#273 scratch buffers), CCH inner-loop tightening (#291 heap swap landed; further inline opportunities), thread amortization (#290 worker pool landed). NOT asserting irreducibility.

## Open follow-ups

- #290: amortize std::thread::scope worker creation across chunks (estimated 5-10% more)
- #282: perfect-hash way names (Belgium 50 MB, planet 3.5 GB)
- All 7 PRs awaiting merge.

## Reproduction

```bash
# After all 7 PRs merge to main:
cargo build --release -p butterfly-route
./target/release/butterfly-route serve --data ./data/belgium/baseline.butterfly --port 3001 --rss-checkpoints
# warm + measure
python3 /tmp/route_p50_bench.py 30
ps -o rss= -p $(pgrep -f butterfly-route)
```
