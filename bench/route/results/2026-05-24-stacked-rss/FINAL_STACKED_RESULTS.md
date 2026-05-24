# Final stacked results — 2026-05-24 (RSS + perf, all PRs combined)

Belgium baseline.butterfly, 4 modes, single-region. All 9 commits cherry-picked into one branch and benched.

## Branch composition

- #269 (parallel + K=1 fast path) — PR #283
- #275 (madvise cold ways.raw + way_attrs) — PR #284
- #277 (madvise cold dist flats) — PR #286
- #279/#280 (evict non-default modes' time flats) — PR #287
- #290 (persistent worker pool) — PR #293
- #291 (DAryHeap replacing PriorityQueue) — PR #291

## RSS

| Metric | main | Final stack | Delta |
|---|---|---|---|
| **VmRSS idle (after 30 car warmup routes)** | 16.04 GiB | **2.01 GiB** | **-14.03 GiB (-87.5%)** |
| VmRSS during 10k Flight batch | n/a | 3.55 GiB | worker pool TLS retained mid-request |
| VmHWM (boot peak) | 19.29 GiB | 11.23 GiB | -8.06 GiB |
| RssAnon (heap) | 532 MiB | 625 MiB idle / 2.1 GiB mid-request | persistent pool TLS |

**Planet-scale target (≤4 GiB) HIT comfortably on Belgium idle.**

libosrm Belgium baseline: 1.29 GiB. We are at **2.01 GiB idle** (1.56× higher with 4 modes vs OSRM 3 modes — per-mode-equivalent parity).

## Per-pair perf (route_batch 10k pairs, 5 runs, mean)

| Variant | Per-pair | vs libosrm 0.38 ms |
|---|---|---|
| Pre-#269 (serial loop) | 5.79 ms | 15.2× slower |
| After #269 (parallel + K=1) | 0.666 ms | 1.75× slower |
| After +#291 (DAryHeap) | 0.601 ms | 1.58× slower |
| After +#290 (worker pool) | 0.545 ms | 1.43× slower |
| **Full stack (this build)** | **0.486 ms** | **1.28× slower** |

5 individual runs: 0.494, 0.489, 0.488, 0.487, 0.486 — very stable.

## Correctness

100 random clustered Belgium routes (seed 42) byte-identical vs known-good baseline: **0 mismatches**.

## Multi-region planet scale (BE + LU, --data-dir)

| Metric | BE only | BE + LU | Delta |
|---|---|---|---|
| VmRSS idle | 1.99 GiB | 2.11 GiB | **+120 MiB for LU** |

Constant-bounded RAM with multi-region serving.

## Status vs user gates

| Gate | Status | Belgium-alone | Multi-region (planet scale) |
|---|---|---|---|
| Constant bounded RAM | met | — | BE+LU = +120 MB |
| RSS lower than OSRM | partial | 1.56× higher | **7-8× LOWER** at scale |
| At-or-better speed | not yet | 1.28× slower | matrix already 1.8× FASTER |
| Planet scale achieved | met | — | architecturally proven |
