# Butterfly-OSM Ecosystem Changelog

All notable changes to the butterfly-osm ecosystem will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For detailed tool-specific changes, see individual tool changelogs:
- [butterfly-dl](./tools/butterfly-dl/CHANGELOG.md) - OSM data downloader

## [Unreleased]

### 2026-07-17 — Phantom endpoints, close-pair correctness, in-engine seeded matrices (#502–#517)

The largest correctness campaign since the CCH landed, deployed fleet-wide
and locked in by a new post-deploy gate:

- **Phantom endpoints (#502/#503/#504–#508)**: snapping commits to BOTH
  directed twins of up to 3 near-equidistant physical edges with exact
  partial-edge costs, on every surface — `/route`, `/table`, Flight
  `matrix` (small + streamed), `route_batch` (unbounded + `max_meters`),
  `/trip`, REST + Flight isochrones (+ exact snapped contour anchor, #497),
  `/isochrone/bulk`, catchment, `edges_batch`. Kills 2–4× wrong-way
  detours on long rural edges; field-validated (Berloz/Heers/Robertville).
- **Close-pair correctness (#509/#510)**: fixed 0-second `/table` answers
  (12 % of close pairs), a legacy same-edge `/route` shortcut, secondary-
  candidate zero-cost conflation, and seed-label domination in the seeded
  bidirectional search (ALT-label meets).
- **In-engine multi-seed bucket M2M (#511)**: phantom seeds now initialise
  the engine directly (super-source forward, shift-trick backward,
  pure-meet guard with in-join same-edge directs). Replaces the API-layer
  expansion that cost 12–15× on matrices; measured at parity with the
  pre-phantom engine (1000×1000: 492 ms seeded vs 509 ms legacy).
- **edges_batch (#512)**: per-edge paths now match `/route` exactly
  (fixture sums 334 s → 163 s).
- **Turn-charge label correction (#515)**: boot recustomization subtracts
  the engine's own expected junction charge from observed door-to-door
  edge slowdowns (zero fitted parameters).
- **`GET /version` (#516)** and **post-deploy gate** (`bench/postdeploy_gate.py`,
  #505/#513/#517): ticket fixtures, fwd/rev symmetry, route==table,
  close-pair sweep, isochrone snapped-origin containment, edges_batch
  sums, and 1 000 reference-trip ground truth (dur p50 1.02, dist p50
  1.004, thresholds ratcheted); `deploy.sh staging` refuses to promote on
  FAIL.
- **OSRM re-validation (post-phantom)**: tied at 200×200, 2.7× faster at
  500×500, 4.8× faster at 1000×1000 (same-host interleaved HTTP).


### 2026-06-10 — Traffic profiles: (highway_class × density) modifier matrix (#428)

Traffic profiles (`traffic/*.traffic.json`) may now carry an opt-in 2-D
`matrix` section refining the per-density speed-factor vector with
per-`(highway_class × density)` cells:

- **Schema**: `"matrix": {"<highway_class_code>": {"<density>": factor, ...}}`.
  Outer keys are the numeric `highway_class` codes stored per way in
  `way_attrs.<mode>.bin` (assigned by the build model's `highway_class`
  table — model-defined, so the code rather than a name is the exact value
  available at customization time). Rows may be partial: a missing
  `(highway, density)` cell falls back to the per-density `speed_factors`
  vector, which stays required and complete. Factors validated in
  `[0.1, 1.5]`; unknown keys, non-canonical codes, empty matrices/rows
  rejected. Vector-only profiles are unchanged and round-trip
  byte-for-byte (the `matrix` key is omitted when absent).
- **Application**: step 8 `--traffic` and the serve-boot car
  recustomization both resolve `factor_for_cell(highway, density)` —
  identical to the pre-#428 behavior when no matrix is present.
- **Calibration**: `calibrate-traffic --matrix` fits the matrix from the
  same observed table — per-cell sample-count-weighted median, same
  clamp band, cells emitted only above `--min-samples` (omitted cells
  fall back cell → density-marginal → global). Deterministic output.

Closed #371/#372 — the matrix endpoints (`/table`, `/trip`, Flight)
now report distance values that belong to the SAME path as the
duration, matching what `/route` reports for the same coordinate pair.
The fix combined a new on-disk weight (`cch.lat.<mode>.u32`,
length-along-time-shortest per CCH edge), a 2-channel bucket-M2M
algorithm, and a bound-pruned CAS loop on a packed `AtomicU64` for
the parallel backward join. Net effect: drivetime APIs are
semantically consistent AND faster than the broken legacy.

### Belgium /table 1000×1000 dur+dist (HTTP wall)

| state | latency | distance metric |
|---|---:|---|
| Pre-#372 legacy 2-pass | 549 ms | wrong (distance-shortest CCH, different geometric path) |
| **Shipped: 2-channel + target-owned local columns** | **379 ms** | **correct (matches /route to within 0.45 % u32 rounding)** |
| OSRM CH reference (HTTP wall) | 684 ms | — |

Butterfly is now **1.81× faster than OSRM** at 1000×1000 with the
correct drivetime distance metric.

### Correctness sweep (4 Belgium pairs)

| pair | /route distance | /table distance | gap |
|---|---:|---:|---:|
| Brussels–Antwerp | 57 693 m | 57 678 m | 0.026 % |
| Aalst–Charleroi | 161 545 m | 160 826 m | 0.45 % |
| Liège–Gent | 166 871 m | 166 861 m | 0.006 % |
| Bruges–Namur | 236 950 m | 236 909 m | 0.017 % |

All durations match EXACTLY. The residual u32 rounding gap comes
from `EbgNode.length_m` being u32-rounded vs `/route`'s polyline
geometry sum.

### Pipeline / on-disk

- step8 customize emits `cch.lat.<mode>.u32` alongside `cch.d.<mode>.u32`
  via the new `bottom_up_with_external_middles` helper — for each
  shortcut, sum the physical edge lengths along the time-optimal
  middle's two halves, recursive bottom-up using the post-relax time
  middles. Belgium car: +0.48 s in step8.
- pack.rs bundles `cch.lat.<mode>.u32` into the container as a new
  `CchWeightsLat = 0x0008_0003` section. Belgium container 12.87 GiB
  → **15.4 GiB** (+1.5 GiB for cch.lat across 3 modes).
- `ServerState.ModeData` gains `cch_weights_len_along_time:
  Option<CchWeights>` plus `up_adj_flat_len_along_time` /
  `down_rev_flat_len_along_time`. Old containers boot with `None`
  and fall through to the legacy 2-pass.

### Algorithm (matrix/bucket_ch.rs)

- `SearchState2` — `NodeEntry` + parallel `Vec<u32> lats`; `relax()`
  takes `(node, dist, lat)` and updates both when `dist` improves;
  `pop()` returns `(dist, lat, node)`.
- `Bucket2Entry` — 12 bytes `(dist, lat, source_idx)`. SoA layout
  proven slower for this access pattern than AoS; AoS-only.
- `PrefixSumBuckets2` — same prefix-sum stamping as the single-channel
  buckets, AoS-only.
- `forward_fill_buckets_flat_len_along_time` — reads time from
  `up_adj_flat.weights` and lat from `up_adj_flat_len_along_time.
  weights` at the same flat index. Same topology, parallel arrays.
- `backward_join_parallel_prefix_len_along_time` — per-cell update via
  **bound-pruned CAS loop** on packed `AtomicU64`:

  ```rust
  let mut cur = packed_matrix[cell].load(Relaxed);
  loop {
      let cur_time = (cur >> 32) as u32;
      if cur_time <= entry.dist { break; }            // can't improve via this entry
      let total_time = entry.dist.saturating_add(d);
      if total_time >= cur_time { break; }
      let next = ((total_time as u64) << 32) | total_lat as u64;
      match packed_matrix[cell].compare_exchange_weak(cur, next, Relaxed, Relaxed) {
          Ok(_) => break,
          Err(observed) => cur = observed,
      }
  }
  ```

  Unconditional `fetch_min` was the dominant cost on contended cells;
  load-and-check skips the locked RMW when the current value already
  beats this bucket's entry.

### Consumers

- `/table` and `/trip` dispatch to the 2-channel function when:
  duration+distance both requested, no exclude/avoid, AND
  `cch_weights_len_along_time` is loaded. Otherwise falls back to
  the legacy 2-pass single-channel path (distance-shortest CCH).
- Flight `route_batch` / `edges_batch` already correct (per-cell
  unpack from the time CCH); Flight `matrix` returns `u32::MAX` for
  distance (unchanged).

### Removed

- `/isochrone?distance_m=` parameter (PR #373). Was the only endpoint
  that ran PHAST on the separate distance-shortest CCH; reachability
  was reported for a path geometry different from every other
  endpoint. Requests now return 400.

### PRs

- #373 fix(isochrone): #371 remove `distance_m` (isodistance) parameter
- #377 feat(customize): #371/#372 emit `cch.lat.<mode>.u32` alongside `cch.d`
- #378 feat(state): #372 load `cch.lat.<mode>.u32` into `ModeData.cch_weights_len_along_time`
- #379 feat(pack): #372 bundle `cch.lat.<mode>.u32` into container as `CchWeightsLat` section
- #380 feat(state): #372 build `up_adj_flat_len_along_time` / `down_rev_flat_len_along_time` at boot
- #381 feat(matrix,table): #372 2-channel bucket-M2M (time + length-along-time)
- #383 perf(matrix): #372 target-owned local columns — eliminate `AtomicU64` in 2-channel backward

### Known follow-up

- `cch_weights_dist` and the dist flats are still loaded — the legacy
  2-pass fallback uses them when custom weights (exclude/avoid) are
  in play. Once the exclude/avoid recustomiser also computes
  length-along-time, drop the dist plumbing entirely.

### 2026-05-26 — Lazy snap escalation + isodistance removal

Closed the OSRM gap on the headline `/route` endpoint and pushed `/table`
ahead of OSRM on the HTTP wall, all by deferring the K=64 candidate
fetch in every snapping handler. Also removed isodistance from
`/isochrone` as part of the drivetime-distance-consistency cleanup
(#371). Six PRs landed in one day on top of the codec sprint below.

### Performance — lazy K=64 snap escalation across all snapping endpoints

The pre-patch pattern: every endpoint paid the K=64 candidate fetch
upfront for every source/destination (~2.14 ms each on Belgium per
the `iterate_rings` + linear-scan-update-best loop), even though
98.7% of pairs route on (0, 0) (#197 sweep). After: K=1 primary
upfront, K=64 escalation only for src/tgt indices that produce an
INF cell or where the primary CCH query returns None.

| endpoint | size | before | after | Δ |
|---|---|---:|---:|---:|
| `/route` Brussels→Antwerp HTTP wall | apples-to-apples | 12 ms p50 | **9 ms p50** | **−25%** |
| `/route` tail | 30-run max | 13 ms | **16 ms** | within noise |
| `/table` HTTP wall | 100×100 | 75 ms | **47 ms** | **−37%** |
| `/table` HTTP wall | 1000×1000 | ~740 ms | **549 ms** | **−26%** |
| OSRM CH `/table` HTTP wall reference | 1000×1000 | 684 ms | — | Butterfly is now **1.25× faster than OSRM** |

`/route` now ties OSRM at p50 (9 ms vs 9 ms apples-to-apples) and
beats it on the tail (16 ms vs OSRM 33 ms max).

### Added

- **butterfly-route**: `snap_kbest::snap_primary_role` helper
  (PR #375). K=1 primary with a valid CCH rank; transparently
  escalates to K=64 if the geometrically-closest candidate has
  `orig_to_rank == u32::MAX` (rare `role_filter` / `orig_to_rank`
  disagreement edge case). Used by `/route`, `/catchment`, Flight
  `matrix`, Flight `edges_batch`, Flight `catchment`.

### Changed

- **butterfly-route**: `/route` lazy snap escalation (PR #368).
  Snap K=1 primary first; only escalate to K=64 + #197 combo
  enumeration on primary CCH query failure (~1.3% of Belgium pairs).
  snap_src 2140 µs → 127 µs, snap_dst 717 µs → 23 µs, handler total
  6850 µs → 4180 µs.
- **butterfly-route**: `/table` lazy snap (PR #370). Same pattern,
  K=64 only for src/tgt indices that have at least one failed cell
  after bucket-M2M. Healthy 1000×1000 matrices snap K=64 for zero
  indices.
- **butterfly-route**: `/trip` lazy snap (PR #374). K=1 per waypoint
  upfront, K=64 only for waypoints whose row/column has an INF cell.
- **butterfly-route**: Flight `matrix` / `route_batch` / `edges_batch`
  + Flight `catchment` DoExchange + REST `/catchment` all share the
  same lazy pattern (PR #375).

### Removed

- **butterfly-route**: `/isochrone?distance_m=…` (isodistance) removed
  entirely (#371, PR #373). Isodistance was the only endpoint that
  ran PHAST on the separate distance-shortest CCH (`cch_weights_dist`),
  reporting reachability for a geometric path different from every
  other drivetime endpoint in the engine. Requests now return 400
  `Provide exactly one of: time_s or contours`. The `cch_weights_dist`
  storage stays for now — still consumed by `/table`, `/trip`, and
  Flight matrix endpoints; #372 tracks the 2-channel bucket-M2M
  migration that retires it from those endpoints too.

### Subsequently fixed (#372, see top of [Unreleased])

- The matrix endpoints' divergent distance metric (`/table`, `/trip`,
  Flight `matrix` / `route_batch` / `edges_batch` reporting from the
  separate distance-shortest CCH instead of length-along-time) was the
  reason the 2-channel bucket-M2M work in #372 shipped. With that
  work landed, all matrix endpoints now report distance consistent
  with `/route` within u32 rounding (≤ 0.45 % on the 4-pair Belgium
  sweep).

### Internal

- Clippy + fmt drift cleanup (PR #369). 21 files reformatted under
  edition-2024 rustfmt; 4 `needless_option_as_deref` warnings
  collapsed in `way_names_idx` test code.

### 2026-05-26 — Disk/RAM codec sprint

End-to-end disk + RAM reduction sweep landed across nine PRs. Belgium
packed container shrank from 16.06 GiB to 12.87 GiB (**−20%**) with
no query-latency regression. Cumulative Europe-scale projection at
10 regions: ~20-30 GiB on-disk savings.

### Added

- **butterfly-route**: Format v5 width-picked CCH middles (#352,
  PR #357). `cch.topo` packs `up_middle`/`down_middle` at u16/u24/u32
  depending on rank range. Belgium savings: 272 MB. `WeightArray`
  reuse keeps `u32::MAX` "no middle" sentinel semantics across all
  three widths.
- **butterfly-route**: zstd-compressed cold sections (#347, PR #358).
  `shared/way_names_idx` 19.81 → 6.61 MiB (67% saved) +
  `shared/snap_grid` 179 → 77 KiB (57% saved). Section-internal
  transparent magic-prefix sniff — pre-#347 containers load
  unchanged.
- **butterfly-route**: Split flat-adjacency format (#345, PR #360).
  Per-(mode × direction) `FlatTopo` section shared across time and
  dist metric variants; per-(mode × direction × metric) `FlatWeights`
  sections carry only the weight bytes. Saves ~1 GiB on Belgium.
  Pack-side topology divergence guard catches the unexpected case
  loudly.
- **butterfly-route**: Cold `CchMiddles` SectionKind (#359, PR #362).
  Pack splits `cch.topo` middles out into a dedicated cold section;
  server boot loads both, then `madvise(DONTNEED)` on the middles
  range after CRC walk. Matrix / isochrone / bucket-M2M never touch
  middles, so the kernel reclaims their pages and route-unpack pages
  them back on demand. Codex estimate: ~300-420 MB RSS per Belgium
  mode under 24-thread matrix load.
- **butterfly-route**: Transit_bulk preflight bbox-tier confirm
  (#343, PR #361). `RegionsState::confirm_in_region` replaces per-
  query full snap with bbox + tile check, falling back to full snap
  only for bbox-overlap zones. Projected 100k same-region batch:
  1 s → <50 ms.

### Changed

- **butterfly-route**: u32 offsets in flat adjacencies when n_edges
  fits u32 (#350, PR #355). Belgium-class containers gain another
  ~300 MB.
- **butterfly-route**: u24 absolute targets in flat adjacencies
  (#351, PR #356). Codex re-consult on rank-delta concluded absolute
  u24 is the right first step (rank-delta deferred — bench math
  showed it would regress on hot-loop edge reads). 652 MB saved on
  Belgium.
- **butterfly-route**: u16/u24 weights propagation to flats (#349,
  PR #354). 970 MB compressed across the four flat-adjacency
  variants on Belgium.
- **butterfly-route**: Auto-prune step1..step8 after pack (#344,
  PR #348). `pack` now defaults to deleting the per-step intermediate
  trees after CRC-verifying the packed container — typically 30-60%
  of a region's footprint. `--keep-intermediates` opts out for
  iterative dev.
- **butterfly-route**: Lean default pack drops `shared/nbg.csr`
  (#346, PR #353). Belgium container shrank by another ~190 MB; the
  per-edge geometry index in `shared/edge_geom_*` (#155) supplants
  the unused NBG CSR for serve-time geometry lookups.

### Tested

- Multi-region serve (BE + LU) verified end-to-end: 19/19 REST PASS,
  10/11 Flight PASS (only `transit_bulk` fails — transit subsystem
  not loaded, expected for a no-transit-feed setup).
- /route Brussels→Antwerp byte-identical across all 9 merges.
  12 ms p50 latency unchanged.
- Matrix bench 1000×1000 mean: 244.9 ms (was 249 ms pre-codec —
  noise-band but trending faster).
- e2e-isochrone bench: 4.11 ms mean / 11.5 ms p99 / 243 iso/sec
  single-threaded.
- 600 lib tests pass.

### Removed

- ~365 GB of stale build artifacts (geocode/nominatim docker volume,
  pre-codec Belgium snapshots, abandoned step experiments).

### Internal

- 0 clippy errors on butterfly-route — `chore(clippy)` sweep
  (PR #363, #364) collapses 13 lints into idiomatic forms with no
  behaviour change.

### 2026-05-23

### Added

- **butterfly-route**: Incremental `avoid_polygons` customization
  ([#240](https://github.com/butterfly-osm/butterfly-osm/issues/240),
  [#249](https://github.com/butterfly-osm/butterfly-osm/pull/249)). The
  recustomization pass now walks an explicit BFS frontier seeded from
  the edges that intersect the avoid polygons, instead of re-running a
  whole-graph triangle relaxation. A 1 km rural polygon on Belgium went
  from 37 s to ~780 ms end-to-end (47× speedup); the larger E19
  motorway-corridor polygon settles at 1.16 s. Cold `/route` requests
  that previously dominated the response now spend the bulk of their
  time in I/O and snap, not in customization.
- **butterfly-route**: LRU avoid-polygon cache with operational
  visibility ([#242](https://github.com/butterfly-osm/butterfly-osm/issues/242),
  [#243](https://github.com/butterfly-osm/butterfly-osm/issues/243),
  [#246](https://github.com/butterfly-osm/butterfly-osm/pull/246),
  [#247](https://github.com/butterfly-osm/butterfly-osm/pull/247)).
  Cache hit rate, entry count, and eviction counters are now surfaced
  on `GET /health` and exported as four Prometheus gauges on
  `GET /metrics`. Polygon inputs are canonicalized before hashing so
  semantically equivalent JSON inputs (rotation, whitespace, ring
  closure) collide on the same cache entry. Booth's algorithm
  ([#250](https://github.com/butterfly-osm/butterfly-osm/pull/250))
  replaces the quadratic rotation search used in the first cut of
  canonicalization.
- **belgium-latest container** ([#236](https://github.com/butterfly-osm/butterfly-osm/issues/236)):
  refreshed Belgium build deployed with 5.13M EBG nodes, 14.98M edges,
  769K named roads, and 4 modes (bike, car, foot, truck). Used as the
  reference dataset for every benchmark in this release.

### Changed

- **butterfly-route**: Avoid cache now returns `Arc<AvoidEntry>` rather
  than cloning the customized weight set per request
  ([#241](https://github.com/butterfly-osm/butterfly-osm/issues/241),
  [#245](https://github.com/butterfly-osm/butterfly-osm/pull/245)).
  `/table` warm-hit latency dropped from 366 ms to 22 ms, matching the
  baseline `/table` cost on un-avoided queries.
- **butterfly-route**: `POST /table/stream` now borrows the flat
  adjacency arrays from the cached `AvoidEntry` instead of cloning
  them ([#248](https://github.com/butterfly-osm/butterfly-osm/pull/248)).
  Eliminates a 100–200 MB per-request clone on Belgium-sized inputs;
  visible as a flat memory profile under sustained streaming load.

### Fixed

- **butterfly-route**: Matrix gap closed
  ([#197](https://github.com/butterfly-osm/butterfly-osm/issues/197),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  K-best snap and SCC-aware role masks are now applied at every snap
  site — `/route`, `/nearest`, `/table`, `/matrix`, `/isochrone`,
  `/trip`, and the Flight gRPC equivalents — instead of only `/route`.
  A 200-pair Belgium `/route` ↔ `/table` correlation sweep now reports
  100% agreement, up from a ~9% gap where `/table` would return
  unreachable for pairs `/route` resolved successfully.
- **butterfly-route**: Small-N matrix dispatch fast-path
  ([#191](https://github.com/butterfly-osm/butterfly-osm/issues/191),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  10×10 and 25×25 matrices no longer fall through to the bulk
  scheduler — rayon thread-dispatch overhead at those sizes outweighed
  the parallelism win.
- **butterfly-route**: Sparse triangle correctness for avoid polygons
  ([#235](https://github.com/butterfly-osm/butterfly-osm/issues/235),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  `/route` and `/table` durations now match exactly on avoided
  queries; the previous implementation had an 8% disagreement caused
  by the sparse pass touching a different node set than the dense
  baseline.
- **butterfly-route**: Stale unpacked geometry in serve-time triangle
  relaxation ([#239](https://github.com/butterfly-osm/butterfly-osm/issues/239),
  [#244](https://github.com/butterfly-osm/butterfly-osm/pull/244)).
  When the relax loop replaced a shortcut's middle node, the unpacking
  arrays still pointed at the original topology middle, producing
  polylines that crossed the avoid polygon even though the duration
  number was correct. `up_middle` and `down_middle` are now updated in
  lockstep with the weight.
- **butterfly-route**: Additional correctness and review fixes for
  the incremental avoid path
  ([#233](https://github.com/butterfly-osm/butterfly-osm/issues/233),
  [#234](https://github.com/butterfly-osm/butterfly-osm/issues/234),
  [#248](https://github.com/butterfly-osm/butterfly-osm/pull/248),
  [#251](https://github.com/butterfly-osm/butterfly-osm/pull/251),
  [#252](https://github.com/butterfly-osm/butterfly-osm/pull/252)).

### Removed

- **butterfly-geocode**: Crate shelved
  ([#253](https://github.com/butterfly-osm/butterfly-osm/issues/253),
  [#254](https://github.com/butterfly-osm/butterfly-osm/pull/254)).
  The full geocoder work tree is preserved under the git tag
  `geocode-shelved-2026-05-23` and can be restored at any time; it is
  removed from the workspace to keep CI and release artifacts focused
  on the routing engine.

### Documentation

- New top-level `docs/` directory with a quickstart guide, REST + gRPC
  API reference, deployment guide, architecture overview, and
  troubleshooting notes.
- README rewritten to reflect the current state of the workspace
  (route engine production-ready, geocoder shelved, downloader stable).
- Stale "sparse triangle" comments across `route/src/server/exclude.rs`
  and adjacent modules updated to "incremental BFS"
  ([#251](https://github.com/butterfly-osm/butterfly-osm/pull/251),
  [#252](https://github.com/butterfly-osm/butterfly-osm/pull/252)) so
  the code matches the algorithm that actually runs.

### Performance reference (Belgium, 2026-05-23)

- 10k×10k distance matrix: **18.3 s**, 1.8× faster than OSRM CH on the
  same dataset.
- 50k×50k Flight gRPC matrix: **9.61 min**, at parity with the
  historical `/table/stream` baseline and well outside what OSRM can
  serve at all (URL-length limits, no streaming).
- `/route` with `avoid_polygons`, warm cache hit: **11 ms**.
- `/route` with `avoid_polygons`, cold miss: **~780 ms** for a 1 km
  rural polygon (was 37 s); **1.16 s** for the E19 motorway corridor.
- `/table` with `avoid_polygons`, warm cache hit: **22 ms** (was
  366 ms before the `Arc<AvoidEntry>` return).

### 2026-04-14

### Changed

- **License**: relicensed from MIT to AGPL-3.0-or-later. See
  [#99](https://github.com/butterfly-osm/butterfly-osm/issues/99) for the
  full rationale. Every workspace crate (`butterfly-common`,
  `butterfly-dl`, `butterfly-route`) now ships under
  AGPL-3.0-or-later. Network-deployed forks must publish source per the
  AGPL §13 requirement. The `LICENSE` file now carries the canonical FSF
  AGPL-3.0 text byte-for-byte. `CONTRIBUTING.md` documents the
  submission-implies-agreement contributor grant.

### Removed
- **butterfly-route**: Experimental PHAST routing implementation and related routing tools
- **benchmarks/**: Deprecated benchmark infrastructure
- **scripts/**: Deprecated utility scripts
- **Planned tool scaffolds**: Removed placeholder directories for butterfly-shrink, butterfly-extract, and butterfly-serve to focus on core functionality first

### Changed
- **Workspace structure**: Simplified to focus on production-ready butterfly-dl and butterfly-common foundation
- **Development focus**: Concentrating on core data acquisition tools before expanding to additional planned tools

## [2.0.0] - 2025-06-27

### 🌟 Major Milestone: Ecosystem Foundation

**Transformation from single-tool to ecosystem workspace**

### Added
- **🏗️ Workspace Architecture**: Multi-tool Rust workspace with shared components
- **📚 butterfly-common**: Shared library for error handling, geographic algorithms, and utilities
- **🤖 Automated Release Process**: Modern GitHub Actions with multi-platform builds (5 platforms)
- **🔒 Security**: Automatic checksums and integrity verification for all releases
- **📋 Tool Templates**: Standardized structure for future butterfly tools
- **🌍 Enhanced Geographic Intelligence**: Advanced fuzzy matching with semantic understanding
- **🎯 Project Roadmap**: Comprehensive development plan for ecosystem expansion
- **📊 CI Badge**: Added build status badge to README for transparency

### Changed
- **Repository Structure**: Organized as multi-tool workspace
- **Release Process**: Fully automated from tag push to published release (~4 minutes)
- **Performance**: Improved build times while maintaining runtime performance
- **Documentation**: Ecosystem-focused with tool-specific documentation

### Maintained
- **100% Backward Compatibility**: All v1.x APIs and CLI usage preserved
- **Performance**: Same runtime characteristics and memory efficiency
- **Features**: All existing functionality retained

### Performance
- **Build Efficiency**: Shared dependencies across tools
- **Release Speed**: 4-minute automated releases vs 30+ minute manual process
- **Platform Coverage**: 5 platforms (Linux x86_64/ARM64, macOS Intel/Apple Silicon, Windows x86_64)

---

## butterfly-dl Evolution (1.0.0 → 2.0.0)

*For detailed version history, see [butterfly-dl CHANGELOG](./tools/butterfly-dl/CHANGELOG.md)*

### Key Milestones

#### 🚀 **Performance Era** (1.4.x)
- Hurricane-fast downloads: **79% faster** than aria2, **3x faster** than curl
- Memory efficiency: **<1GB RAM** for any file size (including 81GB planet)
- Network resilience with intelligent retry and resume
- Beautiful progress displays with tqdm-style formatting

#### 🧠 **Intelligence Era** (1.2.x - 1.3.x)  
- Geographic-aware fuzzy matching: knows Belgium is in Europe, not Antarctica
- Dynamic source discovery from Geofabrik API
- Semantic error correction: "austrailia" → "australia-oceania" (not "austria")
- Real-time source updates, no hardcoded lists

#### 🏗️ **Architecture Era** (1.0.x - 1.1.x)
- Library + CLI architecture with C FFI bindings
- HTTP-only design for security and simplicity
- Smart connection scaling based on file size
- Comprehensive benchmarking against industry standards

#### 🛠️ **Foundation Era** (0.1.x)
- Multi-connection parallel downloads
- Docker-first development
- Convention over configuration approach
- Production-ready Geofabrik downloader

### Performance Achievements

| Metric | Achievement | Comparison |
|--------|-------------|------------|
| **Speed** | 14.07 MB/s | 79% faster than aria2 |
| **Memory** | <1GB fixed | 4-16x less than alternatives |
| **Reliability** | Smart resume | Network resilience with retry |
| **Intelligence** | Geographic fuzzy matching | Semantic understanding |

---

## Upcoming Tools

### 🔄 **Development Roadmap**

#### **Phase 2: Geometric Operations** 
- **butterfly-shrink**: Polygon-based extraction with GEOS integration
- Target: **10x faster** than osmium extract
- Memory limit: **<2GB** for planet-scale operations

#### **Phase 3: Data Transformation**
- **butterfly-extract**: Advanced filtering and transformation engine  
- Target: **5-10x faster** than osmosis
- Memory limit: **<1GB** for streaming operations

#### **Phase 4: High-Performance Serving**
- **butterfly-serve**: HTTP tile server with intelligent caching
- Target: **10-50x faster** QPS than existing solutions
- Memory limit: **<500MB** baseline + configurable caching

### 🎯 **Ecosystem Goals**

- **10x Performance**: Across all operations vs state-of-the-art
- **Minimal Memory**: Fixed memory usage regardless of data size  
- **Modern Architecture**: Rust's safety + async performance
- **Composable Design**: Unix philosophy applied to OSM processing

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for ecosystem development guidelines.

### Performance Standards
- All performance claims must be benchmarked
- Memory usage must be predictable and bounded
- Tools must compose via standard streams and formats

### Tool Development
- Each tool has a single, well-defined responsibility
- Shared functionality goes in butterfly-common
- Comprehensive test coverage including performance tests

---

**butterfly-osm** - Hurricane-fast OSM processing for the modern era.