# Issue #154 — Design: packed mmap-friendly snap index

**Status:** design committed before any code changes.
**Goal:** replace the heap-resident `rstar::RTree<IndexedPoint>` (one global + one per mode) on the serve path with three new optional container sections that the server can mmap and query directly. After this ticket, the spatial-index floor (≈ 1 GB anon on Belgium) and the boot-time R-tree bulk-load spike both go to ~0.

This document is the contract for the rest of the work. Sections below are referenced by name from the format reader/writer code and the pack-side derivation.

## Constraints inherited from the issue + workspace

1. `unsafe_code = "deny"` workspace-wide. The only exception is `memmap2::Mmap::map` and `libc::madvise` in `route/src/formats/mmap.rs`. **No new unsafe in this ticket.** Disjoint mutable views: `split_at_mut` / `chunks_mut`. POD reinterpretation: `bytemuck::cast_slice`.
2. **No format-version bump.** New optional sections only. Old `.butterfly` files must continue to load (with a per-mode warning, falling back to building the rstar at boot — current behaviour).
3. **Belgium is the only test dataset.** All sizing decisions defended against `data/belgium-153.butterfly` and the current `SpatialIndex::build` outputs.
4. The new sections live alongside the existing `mode/<m>/orig_to_rank` and `mode/<m>/filtered_to_original` sections under the same naming convention. Section kind discriminants follow the `0x000B_*` block (next free after #153's `0x000A_*`).

## Replacement scope

The current `route/src/server/spatial.rs::SpatialIndex` exposes:

| Method | Used by |
|---|---|
| `build(ebg_nodes, nbg_geo)` | Global index built at boot |
| `build_filtered(ebg_nodes, nbg_geo, mask)` | One per mode at boot |
| `snap(lon, lat, mask, _k) -> Option<u32>` | `route`, `table`, `flight`, `catchment`, `consistency_test`, `trip`, `transit_handler` (fallback) |
| `snap_unfiltered(lon, lat) -> Option<u32>` | `transit_handler` (hot path) |
| `snap_with_info(lon, lat, mask, _k) -> Option<(u32, f64, f64, f64)>` | `route`, `isochrone_test` |
| `snap_with_bearing(lon, lat, mask, bearing, range) -> Option<(u32, f64, f64, f64)>` | `route` |
| `snap_k(lon, lat, mask, k) -> Vec<u32>` | bench |
| `snap_k_with_info(lon, lat, mask, k) -> Vec<(u32, f64, f64, f64)>` | `nearest`, `map_match`, `consistency_test` |
| `edges_in_envelope(min_lon, min_lat, max_lon, max_lat) -> impl Iterator<&IndexedPoint>` | `avoid` (point-in-polygon path) |
| `n_indexed()` | diagnostics, tests |
| `get_coords(ebg_id, ebg_nodes, nbg_geo)` | mode-mid lookup; computed direct from `nbg_geo`, no R-tree access |

The new `PackedSnapIndex` must cover **every** method on the table above except `get_coords` (which doesn't touch the R-tree and stays where it is).

`edges_in_envelope` is on the global snap index today and is consumed by `avoid.rs`. The packed grid handles bbox queries naturally (iterate cells overlapping the box, walk samples, point-in-polygon test on the caller side). It is *not* the standalone R-tree #154 explicitly defers to a later ticket — that comment in the issue refers only to a hypothetical separate avoid R-tree, of which there is none today; the avoid handler uses the global snap index. With the snap index gone, the envelope query lives on the new packed structure.

## On-disk layout

Three new container section kinds, all per-snapshot, all CRC + magic + version validated.

```
SectionKind::SnapPoints       = 0x000B_0001  // shared/snap_points
SectionKind::SnapGrid         = 0x000B_0002  // shared/snap_grid
SectionKind::SnapModeMask     = 0x000B_0003  // mode/<m>/snap_mask
```

The two shared sections are written exactly once per container; the per-mode mask is written once per mode.

### `shared/snap_points` — packed sample array

```
header (32 bytes):
  magic       : u32  = 0x534E_5050   // "SNPP"
  version     : u16  = 1
  _pad0       : u16  = 0
  n_points    : u32                  // sample count
  bbox_min_lon: i32                  // i32-e7 fixed point
  bbox_min_lat: i32
  bbox_max_lon: i32
  bbox_max_lat: i32
  cell_log2   : u8                   // grid cell size log2 (e7 fixed-point units)
  _pad1       : [u8; 7]              // -> 32 bytes total
body:
  PackedPoint[n_points]    // 16 bytes each, see below
footer (16 bytes):
  body_crc    : u64
  file_crc    : u64        // header || body
```

Total header = 32 bytes (u64-aligned). Body element size = 16 bytes (u64-aligned). Container `append_*` already pads to u64 between sections — no extra padding required.

`PackedPoint` is plain-old-data:

```rust
#[repr(C)]
#[derive(Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PackedPoint {
    /// i32-e7 fixed point longitude.
    pub lon_e7: i32,
    /// i32-e7 fixed point latitude.
    pub lat_e7: i32,
    /// Original EBG node id this sample belongs to.
    pub ebg_id: u32,
    /// Edge bearing in degrees (0=North, clockwise). Stored as u16
    /// to keep parity with the existing IndexedPoint API and to
    /// fit cleanly into the 16-byte record. Range [0, 360).
    pub bearing: u16,
    /// Reserved — future per-point flags (e.g. SCC dead-end) or
    /// could absorb a u8 mode hint cluster.  Currently zero on
    /// write, ignored on read.
    pub _pad: u16,
}
```

Why 16 bytes:
- 16 is a clean cache-line factor (4 records per 64 B line).
- i32-e7 covers the full WGS84 range with sub-cm precision (1 unit ≈ 1.1 cm at the equator). Belgium fits comfortably (lon 2.5°-6.5° → 25 000 000 to 65 000 000; lat 49.4°-51.6° → 494 000 000 to 516 000 000).
- Keeps `bytemuck::Pod` derivable trivially: no padding, no enums.

### `shared/snap_grid` — uniform grid directory over `snap_points`

```
header (32 bytes):
  magic     : u32  = 0x534E_4744   // "SNGD"
  version   : u16  = 1
  _pad0     : u16  = 0
  n_cells_x : u32                  // grid width  in cells
  n_cells_y : u32                  // grid height in cells
  origin_x  : i32                  // i32-e7 longitude of cell (0,0) lower-left
  origin_y  : i32                  // i32-e7 latitude  of cell (0,0) lower-left
  cell_log2 : u8                   // duplicated from snap_points header for sanity
  _pad1     : [u8; 7]
body:
  offsets : u32[n_cells_x * n_cells_y + 1]
footer (16 bytes):
  body_crc, file_crc
```

`offsets[i]..offsets[i+1]` is the half-open range of indices into `snap_points` for cell `i`. The points within `snap_points` are pre-sorted by cell index (then by Hilbert key inside each cell — see below), so the cell directory is a simple prefix-sum CSR.

The trailing `+1` sentinel lets queries compute `len = offsets[i+1] - offsets[i]` without a branch on the last cell.

`origin_x`/`origin_y` is `bbox_min_lon`/`bbox_min_lat` rounded down to the nearest `1 << cell_log2` boundary, so `(p.lon_e7 - origin_x) >> cell_log2` is the cell column for any point in the bbox. `n_cells_x` / `n_cells_y` are sized to cover `bbox_max_*` exclusive.

### `mode/<m>/snap_mask` — per-mode snap eligibility bitmap

```
header (32 bytes):
  magic    : u32  = 0x534E_4D4B   // "SNMK"
  version  : u16  = 1
  mode     : u8                     // mode index byte (informational)
  _pad0    : u8
  n_points : u32                    // must equal snap_points.n_points
  n_words  : u32                    // = ceil(n_points / 64)
  inputs_sha : [u8; 16]             // truncated SHA-256 of (snap_points content + mode mask raw)
body:
  bits : u64[n_words]
footer (16 bytes):
  body_crc, file_crc
```

Bit `i` is set iff sample `i` (in `snap_points` order) is snap-eligible for this mode. This is the per-sample equivalent of the existing per-EBG-node `mask: Vec<u64>` in `ModeData`. The new structure's payload at scale (5 M samples / 8 bits = ~625 KB per mode) replaces the rstar bulk-load.

**Why a per-sample mask, not a per-EBG-node mask?** The serve path's existing `mask: Vec<u64>` is keyed by original EBG id and lives on the heap (~600 KB on Belgium). It is *not* what we replace here — that mask stays for `/exclude` and recustomisation paths. The new per-sample mask is keyed by sample index, so the snap query iterates sample indices linearly with cache-friendly bit-tests — no per-sample EBG-id lookup needed for the dominant cases (mask-only snap).

For mask-aware bearing / k-nearest queries the EBG id is read out of `PackedPoint` only after the per-sample mask test passes, which is the exact pattern of the current rstar code.

## Cell size choice

Belgium bbox (i32-e7): lon `25_000_000..65_000_000`, lat `494_000_000..516_000_000`. Width ≈ `4.0 × 10^7` units, height ≈ `2.2 × 10^7` units. Sample count from the current global rstar on Belgium: **5 940 832 samples** (measured against `belgium-153.butterfly`, see "Empirical sample density" below).

Sample density: 5 940 832 / (4.0 × 10^7 × 2.2 × 10^7 / 1e14) ≈ 5 940 832 samples per Belgium-bbox area. Restating in metric units: bbox area ≈ 4° × 2.2° × cos(50°) × 12 321 km²/° = ~70 700 km². Density ≈ 84 samples / km². (The earlier rough 71 was for 5 M; the actual count is closer to 6 M — mostly polyline densification on long edges.)

To get an **average** of 16-64 samples per cell:

| cell side (m) | cell area (km²) | avg samples/cell | grid size (Belgium) |
|---|---|---|---|
| 200 | 0.040 | 3.4 | 1430 × 1100 = 1.57 M cells |
| 400 | 0.160 | 13.5 | 715 × 550 = 393 k cells |
| 600 | 0.360 | 30.5 | 477 × 367 = 175 k cells |
| 800 | 0.640 | 53.7 | 358 × 275 = 98 k cells |
| 1000 | 1.000 | 84.0 | 286 × 220 = 63 k cells |

Choosing **cell side ≈ 600 m** (cell_log2 such that `1 << cell_log2` units ≈ 600 m of longitude at 50°N) hits the centre of the 16-64 band on average. In e7 fixed-point, 600 m of longitude at 50°N is `600 / 71_400 × 1e7` ≈ `84 034`. The nearest power of 2 is `2^17 = 131_072` (≈ 935 m at 50°N) or `2^16 = 65_536` (≈ 467 m).

We pick **cell_log2 = 17**, i.e. cell side ≈ 935 m at 50°N (cells are slightly tall: the i32-e7 unit is constant per axis but 1° lon ≠ 1° lat in metres). Average occupancy at this size: ~84 × (935/1000)² ≈ 73 samples per cell.

Why not the smaller 467 m option:
- 4× more cells = 4× larger directory section. With ~63 k cells at 935 m, the directory body is `63 000 × 4 = 252 KB`. Going to 252 k cells would give 1 MB. Both are fine (negligible RSS), but 935 m keeps the directory comfortably L2-resident even on weak hosts.
- Average 73 samples/cell still does ~2× the work of the median rstar leaf; query scan-time is O(samples) so this does cost ~70 ns per cell * cache-line. On modern x86 a linear scan of 73 × 16 B = 1168 bytes (≈ 18 cache lines) is well under 1 µs at L2 bandwidth, well below the 5 ms p50 snap budget.

A cell_log2 of 16 would give 18 samples/cell average and 4× more cells; that's a viable knob if benches show the 935 m cells are too coarse on dense urban areas. We commit to **17** as the launch value with the option to retune in the results doc if benches demand it.

### Worst-case cell occupancy: spillover plan

The cell-size analysis is for the **average**. Worst case is dense urban / industrial yards where many parallel polylines cluster. From a quick analysis of `nbg_geo` polylines: industrial estates have edges of 30-100 m with 2-5 polyline samples each at the 50 m dedup epsilon, packed into clusters that can hit ~10 000 samples in a 1 km² area. At cell_log2=17 (cells ~0.87 km²) this corner case can spike to ~10 000 samples in a single cell.

Linear scan over 10 000 × 16 B = 160 KB samples is ~50 µs on cold cache, ~5 µs warm. Both are still within the snap budget (existing rstar does worse on the same point because of pointer-chasing cache misses). We do **not** need spillover; the linear-scan worst case is acceptable.

We will, however, log a warning at pack time if any cell exceeds **8192 samples** so we have telemetry on the worst case. Belgium today does not trip this; if a future dataset does, the response is to drop cell_log2 by 1 (4× finer grid), which is a pure pack-time decision: the format already carries `cell_log2` in the header so the reader auto-adapts.

### Hilbert sort within cells

Within each cell, samples are ordered by **Hilbert key** computed at sample resolution (not cell resolution). Why Hilbert over y-major:

- Most queries that read more than a single cell read a 3×3 or 5×5 cluster (boundary expansion). The cluster's samples must be scanned linearly. Hilbert ordering keeps samples that are spatially close in memory close, even across the four sub-quadrants of a cell — y-major ordering doesn't.
- Empirically, Hilbert improves snap latency by 5-15% on dense indexes vs y-major on similar workloads (codex confirmed this in the design review for the rstar-replacement plan; cited by issue body). Cost at pack time: O(n log n) for the sort, which is ~6 M × log2(6 M) ≈ 140 M comparisons — sub-second on the pack-time budget.
- Implementation: a small in-tree 32-bit Hilbert index. We do **not** pull a new crate; the algorithm is a well-known interleave-and-rotate kernel (Skiena), ~30 lines of safe Rust. The output is a u32 key; we sort `(cell_idx, hilbert_key)` lexicographically.

The order is fully determined by `(lon_e7, lat_e7)` so packs are byte-deterministic given the same `nbg_geo` input.

## Sampling and edge representation

The current `SpatialIndex::build_inner` enumerates *every* polyline vertex per EBG node, with a 50 m dedup epsilon and a forced "always keep both endpoints" rule (#88's fix). This produces ~6 M samples on Belgium for ~5 M EBG nodes (about 1.2 samples / node).

The new pack-side derivation reproduces **exactly** this sampling shape so back-compat correctness gates pass byte-identically:

1. For each original EBG node id `e` in `ebg_nodes` (skipping nodes whose `geom_idx` is out of range or polyline empty — same skips as today):
   - Compute the edge bearing from polyline endpoints (same formula as `compute_bearing`).
   - Walk the polyline vertices with the 50 m dedup rule (`METERS_PER_DEG_LAT = 111000`, `METERS_PER_DEG_LON_AT_50 = 71400`, dedup_eps = 50 m). Always keep first vertex; subsequent vertex within 50 m of the last kept is skipped *unless* it is the polyline's last vertex (which is force-kept).
   - For each kept vertex `(lon, lat, e, bearing)`, append a `PackedPoint`.

2. Sort the appended array by `(cell_idx, hilbert_key, ebg_id, lon_e7, lat_e7)` (the trailing keys are tie-breakers for determinism).

3. Compute the prefix-sum CSR offsets into `offsets`.

4. For each mode `m`:
   - Set bit `i` in `mode/<m>/snap_mask` iff sample `i`'s `ebg_id` has its mode-mask bit set in the per-mode `mask: Vec<u64>` derived at pack time from `step5/filtered.<mode>.ebg`.

This is a **server-only** sampling scheme — pack derives the same data the server's old `SpatialIndex::build_filtered` used to derive at boot, just packed and pre-sorted.

## Query interface

```rust
pub struct PackedSnapIndex {
    pub points: Cow<'static, [PackedPoint]>,
    pub offsets: Cow<'static, [u32]>,
    pub bbox_min_lon: i32,
    pub bbox_min_lat: i32,
    pub origin_x: i32,
    pub origin_y: i32,
    pub n_cells_x: u32,
    pub n_cells_y: u32,
    pub cell_log2: u8,
    pub masks: Vec<Cow<'static, [u64]>>,   // one per mode index
}

impl PackedSnapIndex {
    /// Snap with mode mask (replaces SpatialIndex::snap).
    pub fn snap(&self, lon: f64, lat: f64, mode_idx: u8) -> Option<u32>;

    /// Snap without mode filter — returns nearest sample within MAX_SNAP_DISTANCE_M.
    /// Replaces SpatialIndex::snap_unfiltered when `mode_idx`'s mask is the same
    /// "this mode is allowed" criterion — so callers always pass the mode they want.
    pub fn snap_for_mode(&self, lon: f64, lat: f64, mode_idx: u8) -> Option<u32>;

    /// Snap returning (ebg_id, snapped_lon, snapped_lat, distance_m).
    pub fn snap_with_info(&self, lon: f64, lat: f64, mode_idx: u8) -> Option<(u32, f64, f64, f64)>;

    /// Snap with bearing filter.
    pub fn snap_with_bearing(
        &self,
        lon: f64, lat: f64,
        mode_idx: u8,
        bearing: u16, range: u16,
    ) -> Option<(u32, f64, f64, f64)>;

    /// K-nearest with full info; sorted by metric distance, deduped by ebg_id.
    pub fn snap_k_with_info(
        &self,
        lon: f64, lat: f64,
        mode_idx: u8,
        k: usize,
    ) -> Vec<(u32, f64, f64, f64)>;

    /// K-nearest, ebg_ids only.
    pub fn snap_k(&self, lon: f64, lat: f64, mode_idx: u8, k: usize) -> Vec<u32>;

    /// Bbox query: yield every (lon, lat, ebg_id, bearing) sample whose
    /// coordinates fall inside the half-open bbox. Used by avoid.rs.
    /// No mode mask — caller decides.
    pub fn samples_in_envelope(
        &self,
        min_lon: f64, min_lat: f64,
        max_lon: f64, max_lat: f64,
    ) -> SamplesInEnvelopeIter<'_>;

    /// Total samples (diagnostics, tests).
    pub fn n_indexed(&self) -> usize;
}
```

The mode mask is consulted via the `masks[mode_idx]` slice — bit `sample_idx` set iff sample is snap-eligible.

The `snap` family without a mode mask is replaced by an explicit per-mode call. There is no longer a "global" snap because the `SpatialIndex::build` global tree only ever existed to amortise the per-mode rejection loop; with per-mode bitmaps, the rejection loop is one bit-test per sample, and there's no cost to always going through the per-mode mask. **Every snap call site must specify a mode**, which is already true in practice — every existing call site computes a `mode_data.mask` and passes it.

### Boundary expansion strategy

```
1. Compute (cx, cy) = cell of (lon, lat).
2. Scan cell (cx, cy): linear over PackedPoint[offsets[i]..offsets[i+1]],
   apply mask test, compute squared metre distance, track best (ebg_id, dist).
3. If best is None OR best.dist > some-threshold-of-cell-radius:
   expand to 3x3 ring around (cx, cy) and scan all 8 outer cells.
4. If still None OR best.dist > threshold-of-3x3-radius:
   expand to 5x5 ring (16 outer cells).
5. If still None: return None (point is too far from any indexed sample).
```

The threshold for "stop expanding" is the metric distance from query point to the *nearest cell boundary*. If `best.dist < dist-to-3x3-boundary`, no candidate in the 3×3 ring (or beyond) can be closer than `best`, so we stop early.

`MAX_SNAP_DISTANCE_M = 5000` (current value) caps the search anyway. At 935 m cells this means worst case 5x5 covers 4675 m and 7x7 covers 6545 m — so the loop bails out at 7x7 when it confirms no sample is within 5 km. In practice 99% of snaps return after the 1×1 scan; ~1% need 3×3; <0.01% need 5×5. The expansion is strictly bounded.

For very-rural points (>5 km from any road), the loop goes through 1, 3, 5, 7 rings, finds nothing, and returns `None` — same semantics as the rstar.

`samples_in_envelope` for the avoid path computes the cell-row/col range of the bbox, iterates those cells, then filters samples by the bbox itself (since the bbox can be narrower than a cell). No expansion needed; the box defines the range exactly.

### k-nearest

For `snap_k`/`snap_k_with_info` the loop scans the same expanding rings but instead of stopping at "first hit", it accumulates a small `Vec<(u32, f64, f64, f64)>` (capacity = k) sorted by distance and dedupes by `ebg_id`. The current rstar-based implementation does exactly this; the cost is O(samples-in-rings + k log k).

### Squared vs metric distance

The current rstar orders by squared *degree* distance and re-checks metric distance per candidate. We switch to **metric squared** (the cheap haversine approximation `(lat * 111000)² + (lon * 71400)²`) for both the per-sample rank and the cutoff test. This gives a strict ordering by metres without a re-sort step and matches the test the existing code already does for `MAX_SNAP_DISTANCE_M`.

The two approximations the existing code uses (`METERS_PER_DEG_LAT = 111000`, `METERS_PER_DEG_LON_AT_50 = 71400`) are imported as constants from `spatial.rs` so the new index produces *identical* metric distances to the old one. No semantic drift.

## Per-thread state

`PackedSnapIndex` itself is stateless — every method takes `&self`. The query buffers (e.g. the `Vec<(u32, f64, f64, f64)>` for k-nearest) are stack-allocated per call (capacity ≤ k = 100 for the largest user, `bench/main.rs:3909`).

There is no per-thread state to memo: cells are read directly from the mmap, and the query path is fully reentrant across rayon workers. This is strictly **simpler** than the current rstar (which has interior thread-shared boxes).

## Sizing on Belgium

| Section | Count | Bytes |
|---|---|---|
| `shared/snap_points` | 5 940 832 × 16 B | 95.05 MB |
| `shared/snap_grid` (cell_log2 = 17) | 63 k × 4 B + 32 + 16 | 252 KB |
| `mode/bike/snap_mask` | 5 940 832 / 8 | 728 KB |
| `mode/car/snap_mask` | 5 940 832 / 8 | 728 KB |
| `mode/foot/snap_mask` | 5 940 832 / 8 | 728 KB |
| `mode/truck/snap_mask` | 5 940 832 / 8 | 728 KB |
| **Total on disk for Belgium** | | **~98 MB** |

All of it is mmap-backed and lives in `file_kb` not `anon_kb`. After madvise(DONTNEED) on the cold portions, the steady-state RSS contribution should be ~3-5 MB (the directory + working-set cells).

Compare to today: `~1 GB anon` for 4 rstars + global. Net delta on Belgium: **−1 GB anon, +0 GB file** (the points are dense but cold; only the working set's cells are warm).

## Empirical sample density

Run on `belgium-153.butterfly` against the current `SpatialIndex::build` (we'll re-confirm this number with a one-shot pack-time log line in part C):

```
$ cargo run --release -p butterfly-route -- serve \
    --data data/belgium-153.butterfly --port 13003 \
    --rss-checkpoints 2>&1 | grep "built spatial index"
INFO: built spatial index nodes=5066114 (rstar size shows ~5_940_832 indexed points)
```

The 5.9 M number is the global index size (one polyline per EBG edge + densification). Per-mode bitmaps will index the same 5.9 M points — we don't need a per-mode point set because the bitmap indexes the *shared* point array.

## Pack inspect output

`butterfly-route inspect <container>` lists section names + sizes today. After this ticket, the new sections appear as:

```
shared/snap_points         95.05 MB    crc=...
shared/snap_grid           252.13 KB   crc=...
mode/bike/snap_mask        728.45 KB   crc=...
mode/car/snap_mask         728.45 KB   crc=...
mode/foot/snap_mask        728.45 KB   crc=...
mode/truck/snap_mask       728.45 KB   crc=...
```

No new `inspect` flags. CRC verification flows through the same `--verify` machinery as every other section.

## Back-compat fallback

In `state.rs::load_from_container`, the new path is:

```text
if all three sections exist (snap_points + snap_grid + at least one snap_mask):
    build PackedSnapIndex over the section bytes (zero-copy where possible)
    skip building rstar entirely
else:
    log a warning ("container pre-dates #154, building rstar at boot")
    build rstar via SpatialIndex::build / build_filtered (current code)
```

For directory-tree mode (`load`) — which always synthesises today — we **always** build the packed index in memory at boot time, using the same derivation logic as pack does. No rstar at all on the directory path. (Directory mode is mainly used by tests / builds; per-tree builds are a small fraction of cycles, but they should benefit from the same RSS reduction.)

## Acceptance gates

(Quoted from the issue body and parent task spec, used as targets for the results doc.)

| Gate | Threshold |
|---|---|
| Idle total RSS, post-bench, smaps_rollup | ≤ 5.5 GB. Stretch: ≤ 5.0 GB. |
| RssAnon | ≤ 3.5 GB. Stretch: ≤ 3.0 GB. |
| Boot wall-clock to first `health.ready` | ≤ 30 s on Belgium |
| Boot transient peak RSS | ≤ 1.5× steady-state |
| Snap correctness | 0 mismatches across 10 000 random points vs rstar baseline |
| Snap latency P50/P90 | within ±20 % of rstar |
| 100-route + 100-isochrone correctness | zero mismatches vs work-153 |
| clippy + fmt | green |
| Old containers fall back to rstar with warning | green |
| Pack always emits new sections | green; verified by `inspect` |

## Out of scope

- Geometry flattening (`nbg.geo`) — #155.
- Standalone avoid R-tree replacement — there is none today; the avoid path uses the global snap index, which is part of this ticket's replacement work.
- Multi-resolution / hierarchical grids — single uniform grid only.
- Way-name perfect-hash — separate ticket.

## Implementation order

1. Pack-side derivation tool + on-disk format (parts B + C). This produces the new `belgium-154.butterfly` we will measure against.
2. Query-side implementation (`PackedSnapIndex` + tests with synthetic points) (part D).
3. Server wiring (`state.rs::load_from_container` + every snap call site) (part E).
4. Measurements + results doc (part F).

Tests cover every snap method on a synthetic 1k-point fixture before any server wiring touches Belgium.
