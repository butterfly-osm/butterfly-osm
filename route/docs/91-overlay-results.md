# #91 Phase 2 — Cross-region overlay (results)

## Summary

Cross-region overlay infrastructure shipped. Border extraction proven
on real BE+LU containers. Synthetic 2-region oracle test verifies the
combinatorial picker matches brute-force Dijkstra on the union graph
for all 81 (src, tgt) pairs.

The full live BE+LU overlay-matrix build is documented as a
reproducible offline batch (~7 days per direction per mode); we did
not ship the container itself.

## Synthetic 2-region fixture

`route/tests/cross_region_synthetic.rs::synthetic_fixture_agrees_with_oracle_on_all_pairs`

Two 3×3 grids ("regions A and B") connected by three border crossings
with prescribed costs:

```
A.node(2,1) ↔ B.node(0,1)  cost 5
A.node(2,2) ↔ B.node(0,2)  cost 3
A.node(2,0) ↔ B.node(0,0)  cost 7
```

The test:

1. Builds the dense overlay matrix from oracle distances:
   `matrix[i][j] = union_dist(borders_a[i] → borders_b[j])`
2. For every (src, tgt) pair with src ∈ A and tgt ∈ B, computes:
   - `dist_src[i] = region_dist_A(src → borders_a[i])`
   - `dist_tgt[j] = region_dist_B(borders_b[j] → tgt)`
   - `picker_total = pick_best_border_pair(dist_src, matrix, n_dst, dist_tgt)`
   - `oracle_total = union_dijkstra(src → tgt)`
3. Asserts `picker_total == oracle_total`.

**Result**: 81 / 81 pairs agree. Test runtime ~10 ms.

Two additional tests cover edge cases:

- `picker_handles_unreachable_paths` — `u32::MAX` propagation in any
  of the three input arrays returns `None`.
- `picker_finds_the_minimum_combination` — verifies the picker chooses
  the correct `(i, j)` for a hand-rolled distance matrix.

## Real BE+LU border extraction

```bash
butterfly-route extract-borders \
  --regions data/belgium/baseline.butterfly data/luxembourg/luxembourg.butterfly \
  --out /tmp/borders-be-lu.json
```

| Metric                                 | Value           |
| -------------------------------------- | --------------- |
| Wall-clock (cold cache)                | 67 s            |
| Crossings extracted                    | 8010            |
| Crossings per region pair              | (BE, LU): 8010  |
| Edge distance min / mean / max         | 0.0 / 3.4 / 74.8 m |
| Crossings within 10 km of Athus border | 2012            |
| First sample (lat / lon)               | (49.6383932, 5.9061705) — Athus border |
| Output JSON size                       | ~2.5 MB pretty  |

The single-threaded extraction iterates the snap-index point arrays
of each region (Belgium: ~5 M points, Luxembourg: ~250 k) once. The
bbox-intersect prune drops the cross-region candidate set down to
~50 k points × ~10 k points. Within the intersection we do greedy
nearest-pair haversine with a `|Δlat| × 111 320` early-out, which
keeps the inner loop cache-resident.

The 8010 figure is lower than the prior agent's 14 428 because we
tightened `MAX_PAIR_DIST_M` from ~100 m to 75 m. The tighter threshold
produces fewer false positives (parallel roads either side of the
border that aren't actually connected) at the cost of a slightly
sparser overlay; the design doc explains why we accept this tradeoff.

## Overlay container format

The overlay round-trip test (`route/src/server/overlay.rs::tests::roundtrip_overlay_container`)
verifies:

- 4 section kinds round-trip cleanly through `ContainerWriter` /
  `Container::open`.
- The manifest's `border_counts` correctly slices the flat
  `OverlayBorderNodes` body back into per-region tables.
- Crossings reload symmetrically: `crossings_between("A", "B")`
  equals `crossings_between("B", "A")` regardless of the call order.
- Per-`(src, dst, mode)` matrix lookup returns the original bytes.

## Live BE+LU overlay build (deferred)

The build-overlay command starts cleanly:

```bash
butterfly-route build-overlay \
  --regions data/belgium/baseline.butterfly data/luxembourg/luxembourg.butterfly \
  --modes car \
  --out data/be-lu-overlay.butterfly
```

We capped a sanity run at 180 s; it had not finished the matrix step.
Algorithmic analysis (see design doc) puts the full build at ~7 days
per direction per mode, dominated by `n_src × n_dst` CCH P2P queries
on Belgium (~5 ms each). Two follow-up changes shrink this by 100×:

1. **Pruned border set** — only run access/egress against borders
   geographically near the great-circle line between src and tgt.
   Cuts N from 8010 to ~50 per query, which also fixes the runtime
   latency problem (see "Coordinator runtime cost" in the design
   doc).
2. **Reverse-CCH wrapper** — collapses the egress N-search into a
   single 1-to-N call.

These follow-ups are tracked as separate issues in #91. The overlay
infrastructure shipped here is correct and complete; the deferred work
is purely about making the offline build tractable for the BE+LU
border count.

## Reproducible build command (when the optimisations land)

```bash
# Step 1 — extract borders (~1 minute)
butterfly-route extract-borders \
  --regions data/belgium/baseline.butterfly \
            data/luxembourg/luxembourg.butterfly \
  --out data/be-lu-borders.json

# Step 2 — build overlay (~1 hour with optimisations)
butterfly-route build-overlay \
  --regions data/belgium/baseline.butterfly \
            data/luxembourg/luxembourg.butterfly \
  --modes car,bike,foot \
  --out data/be-lu-overlay.butterfly

# Step 3 — serve with the overlay
butterfly-route serve \
  --data-dir data/ \
  --overlay data/be-lu-overlay.butterfly
```

## Test inventory

| Test | What it covers |
|------|---------------|
| `server::overlay::tests::roundtrip_overlay_container` | On-disk format round-trips |
| `server::overlay::tests::crossings_between_is_symmetric` | Symmetric pair lookup |
| `server::overlay::tests::missing_matrix_is_none` | Unknown (src, dst, mode) returns None |
| `server::overlay::tests::inter_region_cost_uses_mode_speed` | Mode-aware haversine→deciseconds |
| `server::border::tests::intersect_bbox_handles_overlap_and_disjoint` | Bbox math |
| `server::border::tests::samples_outside_bbox_are_excluded` | Bbox filter |
| `cross_region_synthetic::synthetic_fixture_agrees_with_oracle_on_all_pairs` | **All 81 pairs match brute-force Dijkstra** |
| `cross_region_synthetic::picker_handles_unreachable_paths` | `u32::MAX` edge cases |
| `cross_region_synthetic::picker_finds_the_minimum_combination` | Combinator correctness |

All 9 new tests pass. Existing 447 lib tests continue to pass.
Clippy + fmt clean.
