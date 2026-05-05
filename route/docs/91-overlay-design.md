# #91 Phase 2 — Cross-region overlay (design)

## Problem

Phase 1 (#177) shipped multi-region container loading and same-region
dispatch. Cross-region requests (source in BE, target in LU) returned
501 because the engine had no way to express a route that crossed an
operational region boundary.

Phase 2 is the overlay graph that fills that gap: precomputed
border-node tables and border-to-border distance matrices that let the
runtime stitch a single route from two per-region CCHs.

## Architecture

```
src ──► CCH(src_region) ──► border_i ──► matrix[i,j] ──► border_j ──► CCH(dst_region) ──► tgt
```

The runtime cost of a cross-region query decomposes into three legs:

1. **Access leg** (CCH 1-to-N in `src_state`): `src_rank → every src
   border rank`. Cost ~5 ms × `n_src_borders` for typical Belgium-size
   CCHs.
2. **Overlay middle**: a single `O(n_src × n_dst)` table lookup. The
   table is precomputed offline.
3. **Egress leg** (CCH 1-to-N in `dst_state`): `every dst border rank →
   dst_rank`. Same shape as access. We currently issue this as N
   separate bidirectional searches because we don't have a reverse-CCH
   wrapper; for sparse overlays this is acceptable, see Future work.

The combinator is a pure function:

```rust
pub fn pick_best_border_pair(
    dist_src: &[u32],          // n_src
    matrix_row_major: &[u32],  // n_src * n_dst
    n_dst: usize,
    dist_tgt: &[u32],          // n_dst
) -> Option<(u32, u32, u32)>;  // (best_total, best_i, best_j)
```

Exposed standalone so the synthetic-fixture test can verify it
against a brute-force Dijkstra on the union graph without spinning up
two real `ServerState` instances.

## Border-node extraction

Two thresholds:

| Constant         | Value | Purpose                                                      |
| ---------------- | ----- | ------------------------------------------------------------ |
| `BORDER_PROX_M`  | 200 m | Bbox slack each region's bbox is expanded by before intersecting |
| `MAX_PAIR_DIST_M`| 75 m  | Maximum haversine distance between paired EBG samples       |

The extractor:

1. For each ordered region pair `(A, B)` with `id(A) < id(B)`,
2. expand each region's snap-index bbox by 200 m and intersect,
3. collect candidate samples (one per EBG node id, first-seen) inside
   the intersection,
4. for each A candidate find its nearest B candidate; if `≤ 75 m`,
   emit a `BorderCrossing`.

Both thresholds were chosen from typical OSM road density:

- **200 m bbox slack** covers the discrepancy between a region's
  declared bbox and the actual coverage of road samples near the
  border. Tunnels, bridges, and slightly different administrative
  boundaries all live inside this band.
- **75 m pair distance** is well above the snap dedup epsilon (~5 m)
  and well below typical inter-segment spacing (~50 – 100 m). It
  catches genuine cross-border road continuations without spuriously
  pairing two unrelated road segments that happen to run parallel
  either side of the border.

On real BE+LU containers, this extractor produced **8010 unique border
crossings** in 67 seconds (single-threaded, on a 2024-vintage laptop).
The ~14 k figure cited in the prior agent's report came from a more
generous threshold pair (likely 100 m); we deliberately tightened to
75 m so the matrix-build cost is bounded.

A representative sample is at `(49.638 N, 5.906 E)` — the BE/LU border
at Athus, with both `node_a` and `node_b` snapping to the same
geocoded point because the OSM road geometry there is shared between
the two regions.

## On-disk container format

The overlay reuses [`butterfly_dat::Container`] so it gets the same
CRC, alignment, and mmap guarantees as the per-region road container.
Four new section kinds:

| Kind                  | Discriminant | Body                                       |
| --------------------- | ------------ | ------------------------------------------ |
| `OverlayManifest`     | `0x000D_0001`| JSON: region order, modes, border counts, provenance |
| `OverlayBorderNodes`  | `0x000D_0002`| Flat `[BorderNodeRecord; total]` (24 B/record) |
| `OverlayMatrix`       | `0x000D_0003`| Row-major `[u32; n_src × n_dst]` per (src, dst, mode) |
| `OverlayCrossings`    | `0x000D_0004`| Flat `[CrossingRecord; n_crossings]` (24 B/record) |

The manifest's `border_counts` slices the flat `OverlayBorderNodes`
body into per-region runs; the regions appear in the same order as
`region_order`. Each matrix is stored as its own section keyed by
`overlay/matrix/<src>/<dst>/<mode>` so a partial overlay (e.g. only
car) can be loaded selectively.

The `provenance` field is a SHA-256 (truncated to 16 bytes hex) of the
border-node record bytes — a quick sanity check that the per-region
containers match the EBG node-id space the overlay was built against.

## Why a dense `n_src × n_dst` matrix?

We considered three alternatives:

1. **Single union CCH** (build one CCH over BE+LU together). Rejected:
   defeats the operational independence of per-region containers and
   forces a full re-build to add a single region.
2. **Sparse "L_src + cost + L_dst" stored decomposition** — store
   per-region border-to-border tables only, fold the inter-region
   crossings at query time. Rejected at the *storage* layer because
   it requires the runtime coordinator to do `O(n_borders × n_crossings)`
   work per query, and complicates Arc-sharing of the loaded matrix.
3. **Dense `n_src × n_dst` matrix** — accepted. The runtime coordinator
   does `O(n_src × n_dst)` work per query against a single contiguous
   `[u32]` slice, which is cache-friendly and trivially parallelisable
   across modes.

For BE+LU at 8010 borders the dense matrix is `8010 × 8010 × 4 B ≈
257 MB` per (src, dst, mode) triple. Both directions × `car` ≈ 514 MB,
which mmaps cleanly. Adding `bike` + `foot` would land at ~1.5 GB —
still within OS page-cache budget on a workstation.

## Build cost

The dominant cost is the matrix-build step. For each (src, dst, mode):

- L_src: `n_src_borders × n_resolved` CCH P2P queries on the src CCH.
- L_dst: `n_resolved × n_dst_borders` CCH P2P queries on the dst CCH.

A single bidirectional CCH P2P on Belgium takes ~5 ms (warm L3,
single-threaded). With `n_src = n_dst = n_resolved = 8010`, each side
is 64 M queries × 5 ms = ~89 hours **per direction per mode**.

Total live BE+LU build wall-clock for car-mode-only: ~178 hours
(~7 days). For all four modes: ~30 days.

This is tractable as an offline batch but not interactive. The
**Future work** section below covers two algorithmic improvements that
shrink this by 100×.

## Coordinator runtime cost

For a small overlay (≤100 borders/region), the runtime is dominated
by the egress N-search:

- access: 1 × CCH P2P with N targets ≈ ~50 ms (1-to-100 batched)
- middle: O(100²) = O(10⁴) `u32` reads ≈ <1 ms
- egress: 100 × CCH P2P (1-to-1) ≈ 500 ms total ← dominant

For the BE+LU 8010-border case, that becomes ~40 s per query, which
is unworkable. A pruned-border filter is required for production
(see Future work).

## Future work

1. **Pruned border set per query.** Most cross-region queries only
   benefit from a small subset of borders — those geographically near
   the great-circle line between src and tgt. A bbox or radius filter
   on the border list before the access/egress 1-to-N call cuts the
   N from 8010 to typically 50, which restores sub-second latency.
   Tracked as a follow-up to this PR.

2. **Reverse-CCH wrapper for the egress leg.** `CchQuery` is symmetric
   bidirectional; running 1-to-N from a target via reversal is exactly
   one extra `Cow` flip on the topology. With this we collapse the
   egress N-search to a single 1-to-N call, saving ~95 % of the egress
   wall-clock.

3. **Stored decomposition (option 2 above) for very large border
   counts.** Once we have `n_src × n_dst > 10⁵`, the dense matrix is
   over 4 GB and the build cost is impractical. The decomposed shape
   trades ~10× build cost reduction for ~5× runtime cost increase,
   which is the right tradeoff at that scale.

## Codex consultation

Border-node identification is the most ambiguous part of this design;
the obvious alternatives are:

- **OSM relation tagging**. Use `boundary=administrative` relations
  to identify border-crossing ways at extract time. Rejected because
  this approach depends on OSM tagging quality (incomplete near
  Luxembourg) and does not generalise to non-administrative region
  splits (e.g. compute clusters).
- **Geometric border line**. Maintain a polyline per region pair and
  pair samples by intersection with the line. Rejected because the
  line itself has to come from somewhere — either OSM relations or a
  hand-curated GeoJSON — and the bbox+pair approach gets the same
  answer with no external input.

We did not run a separate codex consultation for this iteration:
the prior agent's algorithm (proven on 21 unit tests + 14 k real
border crossings) is the same algorithm we ship here, only with
slightly tighter thresholds. The synthetic-fixture test covers the
algorithm end-to-end against a brute-force oracle on 81 (src, tgt)
pairs.

## What ships in this PR

- Border-node extraction (`route/src/server/border.rs`). Pure
  algorithm, unit-tested, proven on real BE+LU containers.
- Overlay container format (`route/src/server/overlay.rs`). Round-trip
  serialisation tested.
- Coordinator (`route/src/server/cross_region.rs`). The combinator
  `pick_best_border_pair` is verified against brute-force Dijkstra
  on the union graph for all 81 pairs of a synthetic 2-region fixture.
- `RegionsState::dispatch_p2p_with_overlay` + `P2pPlan` (additive in
  `regions.rs`).
- `cross_region_route_handler` (additive in `route.rs`). Currently
  only mounted when the operator passes `--overlay` to `serve`.
- CLI subcommands `extract-borders` + `build-overlay`.
- `--overlay <PATH>` flag on `serve`.

## What does NOT ship

- A live BE+LU overlay container. The build is ~7 days of CCH P2P
  per direction per mode, which is offline-batch territory; we ship
  the infrastructure + reproducible build command but not the
  container itself.
- Geometry / steps for cross-region routes. The handler returns total
  duration + a 2-point straight-line geometry between the two snaps;
  proper EBG-path concatenation across regions is a follow-up.
- Pruned border set (latency optimisation — see Future work).

The 501 path in the existing `route_handler` is preserved for the
default mount, so non-overlay-aware deployments continue to behave
exactly as in #177.
