# #188 cross-region polyline — fix verification

## Bug

`cross_region_route_inner` returned correct distance/duration but
emitted a 2-point straight-line polyline `[src, dst]`, missing all
road geometry across both legs and the border crossing.

## Fix

`route/src/server/route.rs::cross_region_route_inner`:

1. Translate the chosen `src_border_ebg` / `dst_border_ebg` back to
   per-region CCH ranks via each region's `orig_to_rank` mapping.
2. Run two CCH P2P queries with path recovery — one in `src_state`
   (`src_rank → src_border_rank`) and one in `dst_state`
   (`dst_border_rank → dst_rank`) — via the new private helper
   `leg_points_and_distance`. Each returns a vector of `Point`s and a
   precise length-mm-derived distance.
3. Stitch the access leg, the source-side border representative, the
   destination-side border representative, and the egress leg into a
   single deduplicated `Point` sequence via the new
   `stitch_cross_region_polyline` helper.
4. Encode the full sequence into the requested geometry format
   (polyline6 / GeoJSON / points). Distance is the sum of the two
   leg lengths plus the haversine of the border crossing edge.

`stitch_cross_region_polyline` is `pub` so the synthetic 2-region
test can verify polyline assembly without spinning up two real
`ServerState` instances.

## Live verification

Server boot:

```
butterfly-route serve \
  --data-dir /tmp/be-lu-regions \
  --overlay data/be-lu-overlay.butterfly \
  --port 3001 --transport rest
```

Query (Brussels → Luxembourg City, the exact case from #188):

```
curl 'http://localhost:3001/route?src_lon=4.3525&src_lat=50.8467&dst_lon=6.1296&dst_lat=49.6116&mode=car'
```

Response:

| field | value |
|---|---|
| `distance_m` | 220042.47 |
| `duration_s` | 8893.6 |
| `geometry.polyline` length (chars) | 12566 |
| **decoded polyline points** | **2664** |

Compared to #188 which reported a 2-point degenerate line.

### Sketch of decoded path (every ~88th point of 2664)

| idx | lon | lat | note |
|---:|---:|---:|---|
| 0 | 4.3516 | 50.8462 | Brussels (src snap) |
| 176 | 4.3758 | 50.8162 | South Brussels |
| 528 | 4.5386 | 50.7483 | E411 motorway |
| 880 | 4.9855 | 50.3830 | Marche-en-Famenne area |
| 1320 | 5.2503 | 49.9178 | Bastogne |
| 1672 | 5.7701 | 49.6737 | Athus / BE-LU border |
| 1936 | 5.9145 | 49.6176 | Steinfort, Luxembourg |
| 2200 | 6.0550 | 49.5981 | Luxembourg-Strassen |
| 2552 | 6.1105 | 49.6104 | Luxembourg-Hollerich |
| 2663 | 6.1305 | 49.6127 | Luxembourg-Ville (dst snap) |

The polyline crosses the BE/LU border at Athus (~5.77, 49.67), as
expected for the E411 → A4 motorway corridor.

## Tests

`route/tests/cross_region_synthetic.rs`:

- `stitch_cross_region_polyline_emits_more_than_two_points`:
  hand-rolled 8-vertex BE leg + 8-vertex LU leg + 2 border vertices,
  asserts the stitched polyline has > 10 points and is monotonic
  BE → LU.
- `stitch_cross_region_handles_degenerate_legs`: src snap == src
  border (zero-length access leg) — function seeds polyline from
  src_snap rather than collapsing.
- `stitch_cross_region_dedupes_border_overlap_with_leg_endpoints`:
  consecutive duplicate vertices are collapsed.
- `e2e_be_to_lu_polyline_has_road_geometry` (`#[ignore]`): live BE
  + LU + overlay, asserts the polyline has > 100 points after the
  full overlay → CCH → unpack → stitch round trip.

## Numbers

| | before | after |
|---|---:|---:|
| polyline points | 2 | **2664** |
| polyline chars | 18 | 12566 |
| `distance_m` | 186585 (haversine, 12% under) | **220042** (sum of legs + border) |
| `duration_s` | 8893.6 | 8893.6 (unchanged) |

`distance_m` shifted because the previous code reported a haversine
src→dst distance (which underestimates road distance by ~16%); the
fixed code sums actual EBG-edge `length_mm` for both legs plus the
border haversine. Duration unchanged because it was already pulled
straight from `solution.total_cost`.
