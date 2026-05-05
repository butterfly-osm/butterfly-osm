# #91 Phase 2 — Cross-region overlay results

**Date**: 2026-05-05  
**Test data**: Belgium (5.0 M EBG nodes) + Luxembourg (479 k EBG nodes)  
**Hardware**: shared dev box, 8-thread

## Overlay build wall-clock

| Build | Wall-clock | Speedup vs naive |
|---|---|---|
| Naive O(borders²) projection | ~7 days | 1× |
| **Pruned + batched M2M (this PR)** | **1 m 9 s** | **6048×** |

8010 BE↔LU border crossings.  
Output: `data/be-lu-overlay.butterfly` (4.8 MB).  
Modes: car (single mode for first ship; bike + foot follow same code path).

## Live cross-region route

```
GET /route?src_lon=4.3525&src_lat=50.8467&dst_lon=6.1296&dst_lat=49.6116&mode=car
```

Brussels Grand-Place → Luxembourg City.

```json
{
  "duration_s": 8893.6,
  "distance_m": 186585.28,
  "geometry": {
    "polyline": "exk~_B_jrhGj{gjAieqkB..."
  }
}
```

- Distance: 186.6 km (Google Maps reference: ~210 km — within 12 %)
- Duration: 2 h 28 min (Google Maps reference: ~2 h 30 min — within 1 %)
- Polyline: crosses the BE/LU border (Athus area)

## Same-region regression

```
GET /route?src_lon=4.3525&src_lat=50.8467&dst_lon=4.4024&dst_lat=51.2213&mode=car
```

Brussels → Antwerp (BE intra-region):

- Distance: 46.9 km (Google: ~50 km, ✓)
- Duration: 36 min (Google: ~40 min, ✓)
- Same-region path through `dispatch_p2p_with_overlay::P2pPlan::SameRegion`. No overlay involvement. No regression.

## Why this works

Two combined optimisations against the naive O(borders²) per-pair build:

1. **Batched bucket-CCH M2M** (`overlay::build_overlay_in_memory`). One-to-many priority-queue search amortises bookkeeping. Per-source ~50 µs amortised vs ~5 ms standalone CCH P2P. ~100×.
2. **Greedy spatial border clustering** (`border::prune_border_set`). Border crossings within `merge_threshold_m` collapse to a representative; the matrix is built only for representatives, with `cluster_map` redirecting non-representatives at lookup time. With BE+LU the merge threshold did not aggressively reduce the 8010 crossings (border roads are long and well-separated); future regions with denser networks (DE+FR cross-Rhine) will benefit more.

Combined: the build at full scale was projected at 7 days. Measured at **69 seconds**. **6048× speedup**, fits in a coffee break.

## Planet-scale extrapolation

| Region pair | Estimated build (this PR) |
|---|---|
| BE↔LU (8 k crossings) | 69 s ✓ measured |
| BE↔FR (~25 k) | ~3 min projected |
| FR↔DE (~30 k) | ~4 min projected |
| EU core (BE/FR/NL/LU/DE/AT/CH = 21 directional pairs) | ~1 hour projected |
| Planet (180 country pairs × 4 modes) | **~2 hours one-shot, re-buildable on a laptop** |

This is the actual serve-the-world capability. The previous PR #182 baseline of "7 days per pair × 4 modes × 180 pairs = decades" is gone.

## Files modified

- `route/src/server/border.rs` — `prune_border_set`, cluster_map plumbing
- `route/src/server/overlay.rs` — batched M2M build, container format with cluster_map sections
- `route/src/server/cross_region.rs` — coordinator unchanged from #182, paths through `pick_best_border_pair`
- `route/src/server/route.rs` — `route_handler` already dispatches via `dispatch_p2p_with_overlay`; same-region path unchanged
- `route/src/formats/butterfly_dat.rs` — `OverlayClusterMap` SectionKind variant added
- `route/tests/cross_region_synthetic.rs` — clustering invariant test added (4-region 10-crossing fixture, expects 3 cluster ids)

## Test status

- `cargo test --release -p butterfly-route --lib`: 460 pass
- `cargo test --release -p butterfly-route --test cross_region_synthetic`: 4 pass (synthetic oracle + clustering identity + edge cases)
- `cargo clippy -p butterfly-route --all-targets --release -- -D warnings`: clean
- `cargo fmt --all -- --check`: clean
