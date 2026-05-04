# Issue #155 — Design: flat mmappable edge geometry substrate

**Status:** design committed before any code changes.
**Goal:** replace `NbgGeo.polylines: Vec<PolyLine>` (nested heap `Vec<Vec<i32>>`) on the serve path with two new optional container sections that the server mmaps and queries directly. After this ticket, the polyline floor (~330 MB heap on Belgium) goes to ~0; the bytes live as cold file pages and demand-page only when a route response needs them.

This document is the contract for the rest of the work. Sections below are referenced by name from the format reader/writer code and the pack-side derivation.

## Constraints inherited from the issue + workspace

1. `unsafe_code = "deny"` workspace-wide. The only exception is `memmap2::Mmap::map` and `libc::madvise` in `route/src/formats/mmap.rs`. **No new unsafe in this ticket.** POD reinterpretation: `bytemuck::cast_slice`.
2. **No format-version bump.** New optional sections only. Old `.butterfly` files must continue to load (with one warning, falling back to the legacy `nbg.geo` reader's heap polylines — same code path that runs today).
3. **Belgium is the only test dataset.** All sizing decisions defended against the Belgium-154 numbers in `route/docs/154-results.md`.
4. **Magic + version validation in BOTH `read_from_bytes` AND `read_from_bytes_zero_copy`.** PR #156 lesson; #154's reader does this and we copy the pattern.
5. The new sections live alongside the existing `shared/snap_*` sections under the next discriminant block (`0x000C_*`).

## Replacement scope

The current `NbgGeo` struct (in `route/src/formats/nbg_geo.rs`) carries:

```rust
pub struct NbgGeo {
    pub n_edges_und: u64,
    pub edges: Vec<NbgEdge>,           // 36-byte records — kept (used for first_osm_way_id)
    pub polylines: Vec<PolyLine>,      // nested Vec<Vec<i32>> — REPLACED
}
```

`PolyLine` is a heap pair of `Vec<i32>` (lat / lon i32-e7). Per-edge sizes are tiny (~5-10 vertices on average) but the **outer Vec headers** alone consume ~96 MB on Belgium (24 B × 4 M edges), and the inner Vec point bodies pin another ~240 MB. Total: ~330 MB heap, all anon, all on every server boot.

The replacement is two new sections (offsets + points), both flat `[u32]` / `[i32]` arrays that mmap zero-copy via `bytemuck::cast_slice`.

## On-disk layout

Two new container section kinds, both per-snapshot, both CRC + magic + version validated.

```
SectionKind::EdgeGeomOffsets = 0x000C_0001  // shared/edge_geom_offsets
SectionKind::EdgeGeomPoints  = 0x000C_0002  // shared/edge_geom_points
```

Both shared sections are written exactly once per container.

### `shared/edge_geom_offsets` — CSR offset table

```
header (32 bytes):
  magic       : u32  = 0x45474F46  // "EGOF"
  version     : u16  = 1
  _pad0       : u16  = 0
  n_edges     : u32                // edge count (= NbgGeo.n_edges_und as u32)
  n_points    : u32                // total point count, equals offsets[n_edges]
  _pad1       : [u8; 16]           // -> 32 bytes
body:
  offsets : u32[n_edges + 1]       // cumulative point counts; offsets[0] = 0
                                   // offsets[n_edges] = n_points
footer (16 bytes):
  body_crc : u64
  file_crc : u64                    // header || body
```

The trailing `+1` sentinel lets readers compute `len = offsets[i+1] - offsets[i]` without a branch on the last edge — same invariant the snap_grid section uses.

Why u32 point counts: the CSR is over **point counts** (not byte offsets) so each entry is a vertex index into `edge_geom_points`. Belgium has ~30 M vertices total — well under `u32::MAX`. A continent-scale dataset (~3 B vertices) would need u64; we'll cross that bridge when we hit it (the edge count itself is u32 already, so other limits bite first).

Header is 32 bytes (u64-aligned). Body element size = 4 bytes; with `n_edges + 1` u32 entries the body is naturally 4-byte aligned. Container `append_*` already pads to u64 between sections — no extra padding required.

### `shared/edge_geom_points` — interleaved (lon_e7, lat_e7)

```
header (32 bytes):
  magic        : u32  = 0x45475054  // "EGPT"
  version      : u16  = 1
  _pad0        : u16  = 0
  n_points     : u32                // must equal offsets section's n_points
  bbox_min_lon : i32                // i32-e7 fixed point
  bbox_min_lat : i32
  bbox_max_lon : i32
  bbox_max_lat : i32
  _pad1        : [u8; 4]            // -> 32 bytes
body:
  pts_e7 : i32[2 * n_points]        // [lon0, lat0, lon1, lat1, ...]
footer (16 bytes):
  body_crc : u64
  file_crc : u64
```

Why `(lon, lat)` order (not `(lat, lon)` like NbgGeo's PolyLine):

- The legacy `PolyLine` struct stored two separate Vecs (`lat_fxp`, `lon_fxp`). Order doesn't matter when they're independent vectors.
- The new flat layout interleaves them, so we pick the (lon, lat) order to match every f64 conversion in the codebase that emits `lon, lat` first (e.g. `Point { lon, lat }`, `[lon, lat]` GeoJSON arrays). Most of the hot path's existing code reads `polyline.lon_fxp[i]` first when emitting a point, so this matches the natural ordering.
- The `PackedPoint` struct (#154's snap-index sample format) is also `lon_e7` first. Consistency with the snap index keeps the i32-e7 access pattern uniform across the two new substrates.

Body element size = 4 bytes. With `2 * n_points` i32 entries, the body is naturally 4-byte aligned. No extra padding.

The bbox is informational — used for diagnostics + an optional `inspect`-time sanity check. The packing pipeline computes it from the points (same loop, near-zero cost) so future readers can verify a container is for the expected region without parsing the full body.

## Reader API

Both readers follow the existing `formats/snap_index.rs` pattern: owning + zero-copy paths, both validating magic + version + CRC.

```rust
// route/src/formats/edge_geom.rs

pub const EDGE_GEOM_OFFSETS_MAGIC: u32 = 0x45474F46;  // "EGOF"
pub const EDGE_GEOM_POINTS_MAGIC: u32  = 0x45475054;  // "EGPT"
const EDGE_GEOM_VERSION: u16 = 1;

pub struct EdgeGeomOffsets {
    pub n_edges: u32,
    pub n_points: u32,
    pub offsets: Cow<'static, [u32]>,  // length n_edges + 1
}

pub struct EdgeGeomPoints {
    pub n_points: u32,
    pub bbox_min_lon: i32,
    pub bbox_min_lat: i32,
    pub bbox_max_lon: i32,
    pub bbox_max_lat: i32,
    /// Interleaved [lon_e7, lat_e7, lon_e7, lat_e7, ...]; length = 2 * n_points.
    pub points: Cow<'static, [i32]>,
}

pub struct EdgeGeomOffsetsFile;
pub struct EdgeGeomPointsFile;

impl EdgeGeomOffsetsFile {
    pub fn encode(&EdgeGeomOffsets) -> Vec<u8>;
    pub fn read_from_bytes(&[u8]) -> Result<EdgeGeomOffsets>;
    pub fn read_from_bytes_zero_copy(&'static [u8]) -> Result<EdgeGeomOffsets>;
    pub fn write<P: AsRef<Path>>(path: P, x: &EdgeGeomOffsets) -> Result<()>;
}

impl EdgeGeomPointsFile { /* same shape */ }
```

The body's `[u32]` / `[i32]` slice is read via `bytemuck::cast_slice` directly off the mmap'd byte slice. Alignment: i32/u32 require 4-byte alignment. The container's per-section pad is 8-byte, so this naturally holds. We assert the alignment in `read_from_bytes_zero_copy` (debug-only; identical to the snap_index format).

## In-memory access type

```rust
// route/src/server/edge_geom.rs

pub struct EdgeGeometry {
    offsets: Cow<'static, [u32]>,
    points:  Cow<'static, [i32]>,
}

impl EdgeGeometry {
    pub fn from_sections(off: EdgeGeomOffsets, pts: EdgeGeomPoints) -> Result<Self> { ... }

    /// Build in-memory from a (heap-loaded) NbgGeo. Used by the legacy
    /// fallback when the container pre-dates #155.
    pub fn from_legacy_polylines(geo: &NbgGeo) -> Self { ... }

    pub fn n_edges(&self) -> usize { self.offsets.len().saturating_sub(1) }

    /// Cheap O(1) lookup. Returns an empty view for out-of-range or
    /// zero-length polylines. Never panics.
    #[inline]
    pub fn polyline(&self, edge_id: u32) -> EdgePolyline<'_> {
        let i = edge_id as usize;
        if i + 1 >= self.offsets.len() {
            return EdgePolyline::EMPTY;
        }
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        let pts = &self.points[start * 2 .. end * 2];
        EdgePolyline { pts_lon_lat_e7: pts }
    }
}

#[derive(Clone, Copy)]
pub struct EdgePolyline<'a> {
    pts_lon_lat_e7: &'a [i32],
}

impl<'a> EdgePolyline<'a> {
    pub const EMPTY: Self = Self { pts_lon_lat_e7: &[] };

    #[inline] pub fn len(&self) -> usize { self.pts_lon_lat_e7.len() / 2 }
    #[inline] pub fn is_empty(&self) -> bool { self.pts_lon_lat_e7.is_empty() }

    /// (lon_e7, lat_e7) at vertex `i`. O(1), no float conversion.
    #[inline]
    pub fn at_e7(&self, i: usize) -> (i32, i32) {
        (self.pts_lon_lat_e7[i * 2], self.pts_lon_lat_e7[i * 2 + 1])
    }

    /// (lon, lat) in degrees. O(1), one int→float divide pair per call.
    #[inline]
    pub fn at(&self, i: usize) -> (f64, f64) {
        let (lon, lat) = self.at_e7(i);
        (lon as f64 / 1e7, lat as f64 / 1e7)
    }

    /// Lazy iterator over `(lon, lat)` in degrees.
    pub fn iter(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.pts_lon_lat_e7
            .chunks_exact(2)
            .map(|c| (c[0] as f64 / 1e7, c[1] as f64 / 1e7))
    }

    /// Lazy iterator over `(lon_e7, lat_e7)` in i32 fixed-point.
    pub fn iter_e7(&self) -> impl Iterator<Item = (i32, i32)> + '_ {
        self.pts_lon_lat_e7.chunks_exact(2).map(|c| (c[0], c[1]))
    }
}
```

The borrow form (`EdgePolyline`) is the new shape every hot-path consumer migrates to. The owned `Vec<(f64, f64)>` form is reserved for legacy fallback paths in tests / build code that still expect an owned vec.

## Pack-side derivation

`route/src/pack.rs::pack_edge_geometry` (new function, called from `pack_butterfly`):

```text
1. Open NbgGeo (already done by pack_snap_index — share via the same loaded copy).
2. Allocate `offsets: Vec<u32>` of length `n_edges + 1`.
3. For edge_id in 0..n_edges:
       offsets[edge_id] = points.len() / 2
       for vertex in polylines[edge_id]:
           push lon_e7
           push lat_e7
   offsets[n_edges] = points.len() / 2
4. Compute bbox from `points` slice (single linear scan).
5. Encode + append both sections.
```

The pack tool's existing snap-index derivation reads NbgGeo to walk vertices for the dedup-50m sample set. We pre-load NbgGeo once and pass it to both `pack_snap_index` and `pack_edge_geometry` to avoid re-reading the file. (Implementation detail; not a contract.)

The output is **byte-deterministic**: edge IDs are densely indexed, vertices within each polyline are in their NbgGeo source order, and `bytemuck::cast_slice` is endian-explicit (little-endian via i32/u32 to-le-bytes during encode).

## Server load path

In `state.rs::load_from_container`, the new dispatch:

```text
if both shared/edge_geom_offsets and shared/edge_geom_points exist in the container:
    edge_geom = EdgeGeometry::from_sections(zero-copy reads)
    Drop NbgGeo.polylines from heap — load NbgGeo with .polylines empty
        (or load NbgGeo as before but never read polylines from it after construction)
else:
    log warning "container pre-dates #155, using heap polylines"
    Load NbgGeo from shared/nbg.geo as today
    edge_geom = EdgeGeometry::from_legacy_polylines(&nbg_geo)
```

For the directory-tree path (`load`), we **always** build `EdgeGeometry` from the heap-loaded NbgGeo via `from_legacy_polylines`. This is fine: the directory path is for build-tree development, not production serving.

The simplest implementation keeps the legacy `NbgGeo.polylines: Vec<PolyLine>` field in `NbgGeo` and just **never reads from it on the serve path** when the new sections are present. The server's `ServerState` exposes `edge_geom: EdgeGeometry` instead, and every consumer migrated.

But there's a cleaner option: load `NbgGeo` with `polylines` left empty when the container has the new sections. This means we have to either (a) split the read into two methods or (b) make `polylines` an empty Vec by default. We'll pick (a) — a new `NbgGeoFile::read_from_bytes_metadata_only` that reads only the header + `edges` array, never the trailing polyline blob. This drops all polyline bytes from the heap entirely.

Add an RSS checkpoint `phase=load.edge_geom` after loading, so the `155-results.md` doc shows the win.

## Sizing on Belgium

Belgium edge count: ~4.0 M (`n_edges_und`). Total polyline vertices on Belgium: ~30 M (estimate; precise number is logged at pack time).

| Section                       | Count                       | Bytes        |
|-------------------------------|-----------------------------|--------------|
| `shared/edge_geom_offsets`    | 4 M × 4 B + 4 B sentinel    | 16 MB        |
| `shared/edge_geom_points`     | 30 M × 8 B                  | 240 MB       |
| **Total on disk for Belgium** |                             | **~256 MB**  |

The legacy heap shape (`Vec<PolyLine>`) consumes:

- `Vec<PolyLine>` outer header: `Vec` is 24 B; n=4 M → 96 MB.
- For each `PolyLine`: 2 × `Vec<i32>` headers = 48 B. n=4 M → 192 MB.
- Inner data: 2 × ~8 vertices × 4 B = 64 B per edge → ~256 MB.
- Total: **~544 MB** anon RSS just for polylines.

Net delta after #155: **~544 MB anon → ~0 MB anon** (file pages don't count as anon, and only the working set's pages are warm under steady-state load).

Total RSS: file pages add 256 MB cold but the Belgium container baseline already has these bytes (NbgGeo's polyline blob lives in `shared/nbg.geo` already; we're just exposing it differently). So the **container disk size grows by ~256 MB** but the **process RSS drops by 544 MB anon** because we no longer materialise the heap structure.

Expected post-#155 numbers (against the work-154 baseline of 3.78 GB total / 0.97 GB anon):

- Total RSS: ≤ 3.5 GB (gate).
- RssAnon: ≤ 0.7 GB (gate). Stretch: ≤ 0.5 GB. The polyline anon was the largest remaining heap chunk after the snap-index migration; dropping it removes the dominant remaining anon source.
- Disk container size: +256 MB (acceptable; small compared to the per-mode CCH topo + weights).

## Polyline vertex count → exact bytes

We can pin down the exact number with a one-shot pack-time log:

```
$ ./target/release/butterfly-route pack --data-dir data/belgium-v4-pack --out /tmp/x.butterfly
... + [ NN MiB] shared/edge_geom_points    <- (edge geom, NN_M points)
```

The packing logs the section size and point count; the results doc records the actual number.

## Pack inspect output

`butterfly-route inspect <container>` lists section names + sizes today. After this ticket, the new sections appear as:

```
shared/edge_geom_offsets   16.00 MB    crc=...
shared/edge_geom_points    240.00 MB   crc=...
```

No new `inspect` flags. CRC verification flows through the same `--verify` machinery.

## Back-compat fallback

In `state.rs::load_from_container`, the new path is:

```text
match (shared/edge_geom_offsets, shared/edge_geom_points) {
    (Some(off_bytes), Some(pts_bytes)) =>
        edge_geom = EdgeGeometry::from_sections_zero_copy(off_bytes, pts_bytes)?
        // NbgGeo loaded WITHOUT polylines (header + edges only)
    _ =>
        log warning "container pre-dates #155, using heap polylines (~544 MB anon — re-pack to drop)"
        Load full NbgGeo (with .polylines)
        edge_geom = EdgeGeometry::from_legacy_polylines(&nbg_geo)
}
```

For directory-tree mode (`load`) — still synthesises from the file always — we build `EdgeGeometry::from_legacy_polylines(&nbg_geo)`. Directory mode keeps the heap polyline shape; if you want #155's RSS savings, pack a container.

## Acceptance gates

(Quoted from the parent task spec, used as targets for the results doc.)

| Gate | Threshold |
|---|---|
| Idle total RSS, post-bench, smaps_rollup | ≤ 3.5 GB |
| RssAnon | ≤ 0.7 GB |
| Route geometry latency P50/P90 | within ±10 % of work-154 |
| 100-route correctness vs work-154 | byte-identical route geometries |
| 100-isochrone correctness vs work-154 | within ±10 % geometry drift acceptable |
| Old containers fall back with warning | green; correctness check |
| Pack always emits new sections | green; verified by `inspect` |
| clippy + fmt | green |
| Magic + version validation in BOTH reader paths | unit-tested |

## Out of scope

- **Polyline simplification / lossy encoding** — out of scope; ship lossless first.
- **Geometry-side delta / varint encoding** — possible follow-up, but RSS is the gate, not disk size.
- **Sharing geometry with the snap-index sample point array (#154)** — interesting but couples two changes; revisit after both ship.
- **Flattening `NbgGeo.edges` to a mmappable `[NbgEdgeRecord]`** — separate ticket. The serve path only reads `first_osm_way_id` from this array (~140 MB on Belgium). Material but disjoint.
- **Way-name perfect-hash** — separate ticket.

## Implementation order

1. Format file (`route/src/formats/edge_geom.rs`) + unit tests on synthetic data. (commit 3)
2. Pack-side derivation (`route/src/pack.rs::pack_edge_geometry`). (commit 4)
3. In-memory access type (`route/src/server/edge_geom.rs`) — `EdgeGeometry` + `EdgePolyline` + tests. (commit 5)
4. Migrate hot-path consumers (geometry / route / types / trip / isochrone_handler / map_match / transit_handler / catchment / flight). (commit 6)
5. Server wiring (`state.rs::load_from_container`) + RSS checkpoint. (commit 7)
6. Measurements + results doc (`route/docs/155-results.md`). (commit 8)

Tests cover every `EdgePolyline` accessor on a synthetic 100-edge fixture before any server wiring touches Belgium.
