# Post codec-sprint baseline (Belgium, 2026-05-26)

Locked-in performance baseline after the nine-PR disk/RAM codec sprint
that landed today. Numbers are the reference for any future
optimization to compare against — "no regression" means staying within
this band.

## Host

- **CPU**: 20-core, 30 MiB L3, single NUMA node (dev box per CLAUDE.md)
- **Data**: Belgium full pipeline (~5 M EBG nodes, ~14.6 M edges, 3 modes)
- **Container**: 12.87 GiB packed (post #345/#347/#352/#359)

## bucket-M2M (parallel, post-codec)

5-run noise check at 1000×1000 (parallel):

| run | wall (ms) |
|----|----------|
| 1 | 238.8 |
| 2 | 243.0 |
| 3 | 241.4 |
| 4 | 239.1 |
| 5 | 252.0 |
| **mean** | **242.9** |
| **range** | 13.2 |

Representative timing split (1000×1000):
- Forward (10×Dijkstra parallel): **63 ms** (26%)
- Sort (bucket SoA build): **35 ms** (14%)
- Backward (1000×Dijkstra+join parallel): **140 ms** (57%)

Other sizes:

| Size | Mean (ms) | vs OSRM CH | Winner |
|------|-----------|------------|--------|
| 10×10 | 18.5 | 6 ms | OSRM 3.1× (small-N gap, not a goal per Pierre) |
| 25×25 | 10.5 | 10 ms | parity |
| 50×50 | 9.9 | 17 ms | Butterfly 1.7× |
| 100×100 | 18.1 | 35 ms | Butterfly 1.9× |
| 500×500 | 105 | ~250 ms | Butterfly 2.4× |
| 1000×1000 | 242.9 | 684 ms | **Butterfly 2.8×** |

## Isochrone

- Single via `e2e-isochrone` (no HTTP): **4.11 ms mean / 11.5 ms p99**, 243 iso/sec single-threaded.
- `POST /isochrone/bulk` 1000 origins: **2.29 ms/iso, 436 iso/sec** (rayon-parallel internally — 18× per-iso vs sequential HTTP).

## E2E /route

- HTTP `/route Brussels→Antwerp` (Belgium full container): **12 ms p50** (5-sample).
- Output byte-identical across the 9-PR codec chain.

## Disk

- Belgium packed container: 16.06 GiB → **12.87 GiB** (−20%).
- All step1..step8 intermediates auto-pruned (#344) after CRC-verified pack.

## RAM

- `cch.topo` middles in their own `SectionKind::CchMiddles` (#359), `madvise(DONTNEED)` after CRC walk. Matrix-only workloads keep the middle range out of RSS.
- Hot path 100% mmap-backed via `ArcCow::Mmap`. Heap costs O(enum wrappers).

## Variance discipline

Treat ±5 ms / ±2% as the noise floor on 1000×1000. Any future change
showing a 1000×1000 delta inside that band is **noise**, not a real
move. Real wins are ≥10 ms (or ≥4%) sustained across 5-run mean.

## Identified next levers (analysis, not committed)

**Candidate: K-target backward batching for bucket-M2M.**

The backward sweep dominates (140 ms / 57%). Currently it runs 1000
independent Dijkstras-from-target. The existing `batched_phast.rs` infra
already implements K=8 lane batching for the **forward** PHAST
(isochrones use it via `BatchedPhastEngine`). The same pattern would
apply to bucket-M2M backward:

- Today: 1000 sequential target Dijkstras, each visiting ~2257 nodes.
- Batched: 125 K=8-sweeps over the same topology. Per-node read of
  `down_rev_flat.{sources, weights}` amortises across K lanes; only
  the per-lane `dist[u]` update and bucket join is per-target.

Expected: backward 140 ms → ~50-70 ms (memory-bandwidth limited but
fewer cache lines fetched per useful work unit). Total 1000×1000:
245 ms → ~155-175 ms. **~30-40% speedup**.

Risks: per-lane bound divergence (some lanes finish early); careful
bucket join under K-lane.

**Status**: not committed. Pierre to confirm prioritization. Codex
consult on broader question still pending (28+ min, hung).
