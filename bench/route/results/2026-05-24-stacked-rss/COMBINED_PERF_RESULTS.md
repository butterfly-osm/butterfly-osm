# Combined route_batch perf — 2026-05-24

Stacking all per-pair optimisations on a single branch and benching
against the same fixture. Goal: close the libosrm gap.

## Branch composition (combined-bench)

- #269 baseline (parallel + K=1 fast path)
- #283 review fixes (struct + thread cap)
- #290 persistent worker pool
- #291 DAryHeap replacing PriorityQueue

## Bench (Belgium Flight route_batch, BUTTERFLY_ROUTE_BATCH_SIZE=10000)

5 consecutive runs at N=10000, clustered Belgium coords seed 42, mean per-pair time:

| Variant | Per-pair (mean) | vs libosrm 0.38 ms | Delta vs prior |
|---|---|---|---|
| Pre-#269 (serial loop) | 5.79 ms | 15.2× slower | — |
| After #269 (parallel + K=1) | 0.666 ms | 1.75× slower | -88.5% |
| After +#291 (DAryHeap) | 0.601 ms | 1.58× slower | -9.8% |
| After +#290 (worker pool) alone | 0.545 ms | 1.43× slower | -18.2% (vs 0.666) |
| **Combined (#290 + #291)** | **0.487 ms** | **1.28× slower** | **-26.9% (vs 0.666)** |

Combined 5-run samples: 0.491, 0.490, 0.488, 0.483, 0.482 — stable.

## Correctness

100 random clustered Belgium routes (seed 42) byte-identical vs pre-change baseline: **0 mismatches**.

## Status vs user gates

- "RSS lower than OSRM" → multi-region: YES. Belgium-alone: NO (1.99 GiB vs 1.29; 1.55× higher).
- "At or better speed than OSRM" → NOT YET (1.28× slower on 10k batch).
- "Constant bounded RAM" → YES (BE+LU demo, +120 MB total).
- "Planet scale achieved" → architecturally YES.
