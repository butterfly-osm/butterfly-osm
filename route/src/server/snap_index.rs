//! Packed mmap-friendly snap index (#154).
//!
//! Replaces the heap-resident `rstar::RTree<IndexedPoint>` (one global +
//! one per mode) on the serve path with a uniform-grid CSR over a
//! shared, Hilbert-sorted `[PackedPoint]` array plus per-mode
//! `snap_mask` bitmaps. After this lever:
//!
//! - The per-mode rstar floor (~1 GB anon on Belgium across 4 modes) is
//!   gone. The new structure is mmap-backed at boot when the container
//!   carries the new sections.
//! - Boot wall-clock to /health drops because we skip rstar bulk-load
//!   entirely on the container path.
//! - Snap latency is comparable to rstar's, with the cell scan amortising
//!   against L1-L2 cache lines instead of pointer-chased rstar nodes.
//!
//! See `route/docs/154-design.md` for the full design rationale.

use crate::formats::mmap::ArcCow;
use crate::formats::snap_index::{PackedPoint, SnapGrid, SnapMask, SnapPoints};
use crate::formats::{EbgNodes, NbgGeo};

// ---------- Constants -------------------------------------------------------

/// Maximum snap distance in meters. Same value the legacy SpatialIndex
/// used; not configurable.
pub const MAX_SNAP_DISTANCE_M: f64 = 5000.0;

/// Approximate meters per degree at Belgian latitudes (~50°N). Same
/// constants the legacy SpatialIndex used so metric distances are
/// byte-identical between the two implementations.
pub const METERS_PER_DEG_LAT: f64 = 111_000.0;
pub const METERS_PER_DEG_LON_AT_50: f64 = 71_400.0;

/// Cell-size log2 in i32-e7 fixed-point units. `1 << 17 = 131_072`
/// units ≈ 935 m of longitude at 50°N. See design doc for derivation.
pub const DEFAULT_CELL_LOG2: u8 = 17;

/// 50 m polyline-vertex dedup epsilon (squared, in metres). Identical
/// to the legacy `SpatialIndex::build_inner` rule (#88's fix).
const DEDUP_EPS_M: f64 = 50.0;

/// Maximum bbox-expansion ring radius (in cells). Capped large enough
/// that MAX_SNAP_DISTANCE_M is fully covered along the *smallest* cell
/// axis (longitude at 50°N — ~935 m per cell at cell_log2 = 17). The
/// loop's metric early-exit ends scans much earlier than this in
/// practice; this is just the worst-case ceiling for points where
/// nothing snaps.
///
/// Math: ceil(MAX_SNAP_DISTANCE_M / cell_size_lon_m_at_50N) =
/// ceil(5000 / 935) = 6. We round up to 8 for safety against the
/// haversine approximation drift.
const MAX_RING_RADIUS: i32 = 8;

// ---------- Builder ---------------------------------------------------------

/// Build a `(SnapPoints, SnapGrid, per-mode SnapMask...)` set from an
/// EBG + NBG geometry pair plus per-mode masks.
///
/// `mode_masks` is indexed by *something the caller decides* — for the
/// pack tool, it's per-discovered-mode-name; for the server's directory
/// loader, it's per-`ModeData`. The builder doesn't care which; it just
/// produces one `SnapMask` per entry in the slice, in the same order.
///
/// The sampling rule reproduces the legacy `SpatialIndex::build_inner`
/// shape exactly:
///   * For each EBG node:
///     - Skip if `geom_idx >= nbg_geo.polylines.len()` or the polyline
///       is empty.
///     - Compute the edge bearing from polyline endpoints.
///     - Walk the polyline vertices with the 50 m dedup rule. Always
///       keep the first vertex; skip subsequent vertices within 50 m of
///       the last kept *unless* the candidate is the polyline's last
///       vertex (which is force-kept).
///     - Each kept vertex becomes a `PackedPoint(lon_e7, lat_e7,
///       ebg_id, bearing)`.
///
/// The resulting array is sorted by `(cell_idx, hilbert_key, ebg_id,
/// lon_e7, lat_e7)` for byte-determinism.
///
/// The per-mode masks are computed by walking the sorted samples and
/// testing `mode_masks[m]` (an EBG-id-indexed bitset) against each
/// sample's `ebg_id`.
pub struct SnapIndexBuild {
    pub points: SnapPoints,
    pub grid: SnapGrid,
    pub masks: Vec<SnapMask>,
}

/// Per-mode input to `build_snap_index`: a name (used as the mode byte
/// + log label) plus a reference to that mode's EBG-id-indexed mask.
pub struct SnapBuilderMode<'a> {
    pub mode_byte: u8,
    pub mask: &'a [u64],
    /// Truncated SHA-256 of `(snap_points content || ebg-id mask raw
    /// bytes)`. Caller computes; builder only stamps. Pass `[0; 16]`
    /// when you don't have a fingerprint to attach.
    pub inputs_sha: [u8; 16],
}

pub fn build_snap_index(
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
    modes: &[SnapBuilderMode<'_>],
    cell_log2: u8,
) -> SnapIndexBuild {
    // ---- 1. Walk every (ebg_id, polyline-vertex) → PackedPoint ----------
    let mut tmp: Vec<PackedPoint> = Vec::with_capacity(ebg_nodes.n_nodes as usize);

    let dedup_eps2 = DEDUP_EPS_M * DEDUP_EPS_M;

    for (ebg_id, node) in ebg_nodes.nodes.iter().enumerate() {
        let geom_idx = node.geom_idx as usize;
        if geom_idx >= nbg_geo.polylines.len() {
            continue;
        }
        let polyline = &nbg_geo.polylines[geom_idx];
        if polyline.lat_fxp.is_empty() {
            continue;
        }
        let n_pts = polyline.lat_fxp.len();

        let bearing = if n_pts >= 2 {
            let lat1 = polyline.lat_fxp[0] as f64 / 1e7;
            let lon1 = polyline.lon_fxp[0] as f64 / 1e7;
            let lat2 = polyline.lat_fxp[n_pts - 1] as f64 / 1e7;
            let lon2 = polyline.lon_fxp[n_pts - 1] as f64 / 1e7;
            compute_bearing(lat1, lon1, lat2, lon2)
        } else {
            0u16
        };

        let mut last_kept_lon = f64::INFINITY;
        let mut last_kept_lat = f64::INFINITY;
        for i in 0..n_pts {
            let lon = polyline.lon_fxp[i] as f64 / 1e7;
            let lat = polyline.lat_fxp[i] as f64 / 1e7;

            if last_kept_lon.is_finite() {
                let dlat = (lat - last_kept_lat) * METERS_PER_DEG_LAT;
                let dlon = (lon - last_kept_lon) * METERS_PER_DEG_LON_AT_50;
                if dlat * dlat + dlon * dlon < dedup_eps2 && i + 1 < n_pts {
                    continue;
                }
            }

            tmp.push(PackedPoint {
                lon_e7: polyline.lon_fxp[i],
                lat_e7: polyline.lat_fxp[i],
                ebg_id: ebg_id as u32,
                bearing,
                _pad: 0,
            });
            last_kept_lon = lon;
            last_kept_lat = lat;
        }
    }

    // ---- 2. Compute bbox + grid origin ---------------------------------
    let (bbox_min_lon, bbox_min_lat, bbox_max_lon, bbox_max_lat) = if tmp.is_empty() {
        (0i32, 0i32, 0i32, 0i32)
    } else {
        let mut min_lon = i32::MAX;
        let mut max_lon = i32::MIN;
        let mut min_lat = i32::MAX;
        let mut max_lat = i32::MIN;
        for p in &tmp {
            if p.lon_e7 < min_lon {
                min_lon = p.lon_e7;
            }
            if p.lon_e7 > max_lon {
                max_lon = p.lon_e7;
            }
            if p.lat_e7 < min_lat {
                min_lat = p.lat_e7;
            }
            if p.lat_e7 > max_lat {
                max_lat = p.lat_e7;
            }
        }
        (min_lon, min_lat, max_lon, max_lat)
    };

    let cell_size = 1i32 << cell_log2;
    // Origin = bbox_min rounded down to a cell boundary so cell index
    // arithmetic stays non-negative for any sample inside the bbox.
    let origin_x = floor_to_cell(bbox_min_lon, cell_log2);
    let origin_y = floor_to_cell(bbox_min_lat, cell_log2);
    // Cells must cover bbox_max exclusively, so add 1 cell of slack.
    let n_cells_x = if tmp.is_empty() {
        1u32
    } else {
        // (bbox_max_lon - origin_x) / cell_size + 1
        let span_x = (bbox_max_lon as i64) - (origin_x as i64);
        ((span_x / cell_size as i64) + 1) as u32
    };
    let n_cells_y = if tmp.is_empty() {
        1u32
    } else {
        let span_y = (bbox_max_lat as i64) - (origin_y as i64);
        ((span_y / cell_size as i64) + 1) as u32
    };

    // ---- 3. Compute (cell_idx, hilbert_key) for every sample, sort -----
    // Hilbert key is computed at sample resolution within the bbox so
    // even within a cell, samples remain spatially clustered.
    let n_cells = n_cells_x as usize * n_cells_y as usize;
    let cell_keys: Vec<(u32, u32, usize)> = tmp
        .iter()
        .enumerate()
        .map(|(idx, p)| {
            let cx = ((p.lon_e7 as i64 - origin_x as i64) / cell_size as i64) as u32;
            let cy = ((p.lat_e7 as i64 - origin_y as i64) / cell_size as i64) as u32;
            let cell_idx = cy * n_cells_x + cx;
            // Normalise sample-resolution coords into an unsigned u32
            // for the Hilbert function. We keep the offset relative to
            // the origin so all values are non-negative.
            let rel_x = (p.lon_e7 as i64 - origin_x as i64) as u64;
            let rel_y = (p.lat_e7 as i64 - origin_y as i64) as u64;
            // Clamp to u32 — bbox is small enough that this fits.
            let hil = hilbert_xy_to_d(rel_x as u32, rel_y as u32);
            (cell_idx, hil, idx)
        })
        .collect();

    let mut sort_keys: Vec<(u32, u32, u32, i32, i32, usize)> = cell_keys
        .into_iter()
        .map(|(c, h, idx)| {
            let p = &tmp[idx];
            (c, h, p.ebg_id, p.lon_e7, p.lat_e7, idx)
        })
        .collect();
    sort_keys.sort_unstable();

    // Reorder `tmp` according to sort_keys; produce both the sorted
    // points and a parallel cell_idx array for the directory.
    let mut sorted_points: Vec<PackedPoint> = Vec::with_capacity(tmp.len());
    let mut sorted_cells: Vec<u32> = Vec::with_capacity(tmp.len());
    for (c, _h, _id, _lon, _lat, idx) in &sort_keys {
        sorted_points.push(tmp[*idx]);
        sorted_cells.push(*c);
    }
    drop(tmp);
    drop(sort_keys);

    // ---- 4. Build the CSR offsets[n_cells + 1] -------------------------
    let mut offsets = vec![0u32; n_cells + 1];
    // Count
    for &c in &sorted_cells {
        offsets[c as usize + 1] += 1;
    }
    // Prefix-sum
    for i in 1..offsets.len() {
        offsets[i] += offsets[i - 1];
    }

    // Worst-case cell occupancy log
    let worst = (1..offsets.len())
        .map(|i| offsets[i] - offsets[i - 1])
        .max()
        .unwrap_or(0);
    if worst > 8192 {
        tracing::warn!(
            worst_cell_samples = worst,
            cell_log2,
            "snap_index: worst-case cell occupancy exceeds 8192; \
             consider lowering cell_log2 by 1 if snap latency degrades"
        );
    } else {
        tracing::info!(
            n_points = sorted_points.len(),
            n_cells,
            cell_log2,
            worst_cell_samples = worst,
            "snap_index: built points + grid"
        );
    }

    // ---- 5. Build per-mode bitmasks -----------------------------------
    let n_points = sorted_points.len();
    let n_words = n_points.div_ceil(64);

    let masks: Vec<SnapMask> = modes
        .iter()
        .map(|m| {
            let mut bits = vec![0u64; n_words];
            for (sample_idx, sample) in sorted_points.iter().enumerate() {
                let eid = sample.ebg_id as usize;
                let word = eid / 64;
                let bit = eid % 64;
                if word < m.mask.len() && (m.mask[word] & (1u64 << bit)) != 0 {
                    let sword = sample_idx / 64;
                    let sbit = sample_idx % 64;
                    bits[sword] |= 1u64 << sbit;
                }
            }
            SnapMask {
                mode: m.mode_byte,
                n_points: n_points as u32,
                inputs_sha: m.inputs_sha,
                bits: ArcCow::from_vec(bits),
            }
        })
        .collect();

    SnapIndexBuild {
        points: SnapPoints {
            n_points: n_points as u32,
            bbox_min_lon,
            bbox_min_lat,
            bbox_max_lon,
            bbox_max_lat,
            cell_log2,
            points: ArcCow::from_vec(sorted_points),
        },
        grid: SnapGrid {
            n_cells_x,
            n_cells_y,
            origin_x,
            origin_y,
            cell_log2,
            offsets: ArcCow::from_vec(offsets),
        },
        masks,
    }
}

#[inline]
fn floor_to_cell(v: i32, cell_log2: u8) -> i32 {
    let cell = 1i64 << cell_log2;
    // Floor division for signed integers. The bbox is always positive
    // for Belgium (lon ≥ 25_000_000, lat ≥ 494_000_000), but we handle
    // the negative case for general robustness.
    let v = v as i64;
    let q = v.div_euclid(cell);
    (q * cell) as i32
}

/// Compute bearing in degrees (0=North, clockwise) from point 1 to point 2.
/// Mirrors `SpatialIndex::compute_bearing` exactly.
fn compute_bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> u16 {
    let dlat_m = (lat2 - lat1) * METERS_PER_DEG_LAT;
    let dlon_m = (lon2 - lon1) * METERS_PER_DEG_LON_AT_50;
    let angle_rad = dlon_m.atan2(dlat_m);
    let deg = angle_rad.to_degrees();
    ((deg + 360.0) % 360.0) as u16
}

/// Compute Hilbert distance for a 2D point in a `2^32 × 2^32` grid
/// (Skiena's iterative algorithm, 32 bits each axis -> u64 result, but
/// we truncate to u32 since 32-bit Hilbert keys are enough for a
/// per-sample sort and fit in our sort tuple).
///
/// The output ordering is what matters; the absolute key value is
/// opaque to callers.
fn hilbert_xy_to_d(mut x: u32, mut y: u32) -> u32 {
    // 2^16 grid is more than enough resolution to disambiguate samples
    // within a Belgium-sized bbox at i32-e7 precision (the bbox is
    // ~4×10^7 units wide; truncating to u16 still gives ~600 unit
    // buckets, far finer than any cell).
    let order = 16u32;
    let mut d: u32 = 0;
    // Truncate to lower `order` bits (top bits are mostly redundant
    // for samples inside the bbox).
    x &= (1u32 << order) - 1;
    y &= (1u32 << order) - 1;
    let mut s = (1u32 << (order - 1)) as i32;
    while s > 0 {
        let rx = if (x & s as u32) != 0 { 1 } else { 0 };
        let ry = if (y & s as u32) != 0 { 1 } else { 0 };
        d = d.wrapping_add(((s as u32) * (s as u32)) * ((3 * rx) ^ ry) as u32);
        // Rotate.
        if ry == 0 {
            if rx == 1 {
                x = (s as u32 - 1).wrapping_sub(x);
                y = (s as u32 - 1).wrapping_sub(y);
            }
            std::mem::swap(&mut x, &mut y);
        }
        s >>= 1;
    }
    d
}

// ---------- Query interface -------------------------------------------------

/// Packed snap index. Holds `SnapPoints` + `SnapGrid` shared by all
/// modes, plus one `SnapMask` per mode (indexed by the same `mode_idx`
/// the caller uses for `ModeData`).
///
/// All three sections come from either zero-copy mmap views (container
/// path) or owned in-memory builds (directory path or back-compat).
/// Either way, query code is identical.
pub struct PackedSnapIndex {
    pub points: SnapPoints,
    pub grid: SnapGrid,
    /// One per mode, indexed by mode_idx. Empty Vec means "no mode
    /// masks loaded yet" (reject every snap with None until install).
    pub masks: Vec<SnapMask>,
}

impl PackedSnapIndex {
    /// Total samples in the shared point array.
    pub fn n_indexed(&self) -> usize {
        self.points.points.len()
    }

    /// Number of registered modes.
    pub fn n_modes(&self) -> usize {
        self.masks.len()
    }

    /// Snap to the nearest mode-eligible sample. Returns the EBG node
    /// id of the chosen sample, or None if no sample is within
    /// MAX_SNAP_DISTANCE_M.
    pub fn snap(&self, lon: f64, lat: f64, mode_idx: u8) -> Option<u32> {
        self.snap_with_info(lon, lat, mode_idx)
            .map(|(id, _, _, _)| id)
    }

    /// Snap with full info: (ebg_id, snapped_lon, snapped_lat, dist_m).
    pub fn snap_with_info(&self, lon: f64, lat: f64, mode_idx: u8) -> Option<(u32, f64, f64, f64)> {
        self.snap_with_info_filtered(lon, lat, mode_idx, None)
    }

    /// Snap with full info, additionally constrained by an optional
    /// EBG-id-indexed `edge_filter` bitmap (used by exclude/avoid).
    /// When `edge_filter` is None, this is identical to
    /// [`snap_with_info`]. When Some, after passing the per-sample
    /// mask, the candidate is also rejected if its EBG-id bit is clear
    /// in `edge_filter`.
    pub fn snap_with_info_filtered(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        edge_filter: Option<&[u64]>,
    ) -> Option<(u32, f64, f64, f64)> {
        self.snap_with_info_filtered_role(lon, lat, mode_idx, edge_filter, None)
    }

    /// Snap with full info, constrained by an `edge_filter`
    /// (exclude/avoid) AND an optional `role_filter` (#197 directional
    /// snap). The `role_filter` is the EBG-id-indexed bitset built at
    /// boot for either the `src` role (`mode_data.has_outbound`) or
    /// the `dst` role (`mode_data.has_inbound`). Both filters are
    /// AND'd: a candidate must have its bit set in BOTH `edge_filter`
    /// and `role_filter` (when those filters are present).
    ///
    /// When `role_filter` is None, this is identical to
    /// [`snap_with_info_filtered`].
    pub fn snap_with_info_filtered_role(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        edge_filter: Option<&[u64]>,
        role_filter: Option<&[u64]>,
    ) -> Option<(u32, f64, f64, f64)> {
        let mask = self.masks.get(mode_idx as usize)?;
        let mut best: Option<(u32, f64, f64, f64)> = None;
        let max2 = MAX_SNAP_DISTANCE_M * MAX_SNAP_DISTANCE_M;

        self.iterate_rings(lon, lat, |sample_idx, p| -> Option<f64> {
            if !mask_bit_set(&mask.bits, sample_idx) {
                return None;
            }
            if let Some(ef) = edge_filter
                && !mask_bit_set(ef, p.ebg_id as usize)
            {
                return None;
            }
            if let Some(rf) = role_filter
                && !mask_bit_set(rf, p.ebg_id as usize)
            {
                return None;
            }
            let (d2, plon, plat) = sample_distance2(lon, lat, p);
            if d2 > max2 {
                return None;
            }
            let beat = match best {
                Some(b) => d2 < b.3 * b.3,
                None => true,
            };
            if beat {
                best = Some((p.ebg_id, plon, plat, d2.sqrt()));
            }
            Some(d2)
        });
        best
    }

    /// Convenience: like [`snap`] but also constrained by a dynamic
    /// EBG-id-indexed `edge_filter` (exclude/avoid path).
    pub fn snap_filtered(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        edge_filter: Option<&[u64]>,
    ) -> Option<u32> {
        self.snap_with_info_filtered(lon, lat, mode_idx, edge_filter)
            .map(|(id, _, _, _)| id)
    }

    /// Convenience: like [`snap_filtered`] but also constrained by an
    /// optional `role_filter` (#197 directional snap). Use this from
    /// query handlers that know whether the snap is acting as a source
    /// (use `mode_data.has_outbound`) or destination (use
    /// `mode_data.has_inbound`).
    pub fn snap_filtered_role(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        edge_filter: Option<&[u64]>,
        role_filter: Option<&[u64]>,
    ) -> Option<u32> {
        self.snap_with_info_filtered_role(lon, lat, mode_idx, edge_filter, role_filter)
            .map(|(id, _, _, _)| id)
    }

    /// Snap with bearing filter. `bearing` and `range` are degrees.
    pub fn snap_with_bearing(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        bearing: u16,
        range: u16,
    ) -> Option<(u32, f64, f64, f64)> {
        self.snap_with_bearing_filtered(lon, lat, mode_idx, bearing, range, None)
    }

    /// Snap with bearing filter + optional EBG-id-indexed edge filter.
    pub fn snap_with_bearing_filtered(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        bearing: u16,
        range: u16,
        edge_filter: Option<&[u64]>,
    ) -> Option<(u32, f64, f64, f64)> {
        self.snap_with_bearing_filtered_role(lon, lat, mode_idx, bearing, range, edge_filter, None)
    }

    /// Snap with bearing filter, edge filter (exclude/avoid), AND
    /// `role_filter` (#197 directional snap). See
    /// [`snap_with_info_filtered_role`] for role-filter semantics.
    #[allow(clippy::too_many_arguments)]
    pub fn snap_with_bearing_filtered_role(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        bearing: u16,
        range: u16,
        edge_filter: Option<&[u64]>,
        role_filter: Option<&[u64]>,
    ) -> Option<(u32, f64, f64, f64)> {
        let mask = self.masks.get(mode_idx as usize)?;
        let mut best: Option<(u32, f64, f64, f64)> = None;
        let max2 = MAX_SNAP_DISTANCE_M * MAX_SNAP_DISTANCE_M;

        self.iterate_rings(lon, lat, |sample_idx, p| -> Option<f64> {
            if !mask_bit_set(&mask.bits, sample_idx) {
                return None;
            }
            if let Some(ef) = edge_filter
                && !mask_bit_set(ef, p.ebg_id as usize)
            {
                return None;
            }
            if let Some(rf) = role_filter
                && !mask_bit_set(rf, p.ebg_id as usize)
            {
                return None;
            }
            if !bearing_matches(p.bearing, bearing, range) {
                return None;
            }
            let (d2, plon, plat) = sample_distance2(lon, lat, p);
            if d2 > max2 {
                return None;
            }
            let beat = match best {
                Some(b) => d2 < b.3 * b.3,
                None => true,
            };
            if beat {
                best = Some((p.ebg_id, plon, plat, d2.sqrt()));
            }
            Some(d2)
        });
        best
    }

    /// K-nearest with full info; results sorted by metric distance,
    /// deduped by `ebg_id` (only the closest sample per edge is kept).
    pub fn snap_k_with_info(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        k: usize,
    ) -> Vec<(u32, f64, f64, f64)> {
        self.snap_k_with_info_filtered(lon, lat, mode_idx, k, None)
    }

    pub fn snap_k_with_info_filtered(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        k: usize,
        edge_filter: Option<&[u64]>,
    ) -> Vec<(u32, f64, f64, f64)> {
        self.snap_k_with_info_filtered_role(lon, lat, mode_idx, k, edge_filter, None)
    }

    /// K-nearest with full info, edge filter, AND `role_filter` (#197
    /// directional snap). See [`snap_with_info_filtered_role`] for
    /// role-filter semantics.
    pub fn snap_k_with_info_filtered_role(
        &self,
        lon: f64,
        lat: f64,
        mode_idx: u8,
        k: usize,
        edge_filter: Option<&[u64]>,
        role_filter: Option<&[u64]>,
    ) -> Vec<(u32, f64, f64, f64)> {
        if k == 0 {
            return Vec::new();
        }
        let Some(mask) = self.masks.get(mode_idx as usize) else {
            return Vec::new();
        };

        // K-nearest with deterministic early-exit:
        //
        // Each iterate_rings callback that accepts a sample returns
        // `Some(d²)`. iterate_rings tracks `best_accepted_d²` as the
        // MIN of all returned d²s, and exits a ring whose next-inner
        // edge is already farther than that. If we always return d²
        // we mimic single-best snap and may stop too early for K-best.
        //
        // To get K-correct early-exit, we accumulate the closest sample
        // per (ebg_id), and track the K-th best d² seen so far. Once
        // we have ≥k distinct edges we return THAT d² instead of d² —
        // any further ring whose inner edge exceeds √(K-th d²) cannot
        // beat our current top-K. We use a sorted Vec keyed by d², so
        // tie-breaking is deterministic (insertion-order stable for
        // equal d²).
        //
        // Without this early-exit, snap_k iterated the full 5 km
        // radius — fine for a single /route call, but pathological
        // for /table (200+ snaps per request).
        let max2 = MAX_SNAP_DISTANCE_M * MAX_SNAP_DISTANCE_M;
        let mut best: Vec<(u32, f64, f64, f64, f64)> = Vec::with_capacity(k * 2); // (ebg_id, plon, plat, d, d²)

        self.iterate_rings(lon, lat, |sample_idx, p| -> Option<f64> {
            if !mask_bit_set(&mask.bits, sample_idx) {
                return None;
            }
            if let Some(ef) = edge_filter
                && !mask_bit_set(ef, p.ebg_id as usize)
            {
                return None;
            }
            if let Some(rf) = role_filter
                && !mask_bit_set(rf, p.ebg_id as usize)
            {
                return None;
            }
            let (d2, plon, plat) = sample_distance2(lon, lat, p);
            if d2 > max2 {
                return None;
            }
            // Update per-edge best: linear scan keeps tie-breaking
            // deterministic (first-encountered wins on exact ties),
            // and K is small (≤ 64) so the linear scan beats a heap
            // by ~2x in microbenchmarks.
            let mut updated = false;
            for entry in best.iter_mut() {
                if entry.0 == p.ebg_id {
                    if d2 < entry.4 {
                        *entry = (p.ebg_id, plon, plat, d2.sqrt(), d2);
                    }
                    updated = true;
                    break;
                }
            }
            if !updated {
                best.push((p.ebg_id, plon, plat, d2.sqrt(), d2));
            }
            // Return the K-th smallest d² once we have ≥k distinct
            // edges — drives iterate_rings' early-exit without losing
            // any candidate that would have made it into the top-K.
            if best.len() >= k {
                let mut ds: Vec<f64> = best.iter().map(|e| e.4).collect();
                // select_nth_unstable is O(n), faster than sort for our
                // small `k * 2`-ish vec. Use the K-th smallest.
                ds.select_nth_unstable_by(k - 1, |a, b| {
                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                });
                Some(ds[k - 1])
            } else {
                None
            }
        });

        best.sort_by(|a, b| a.4.partial_cmp(&b.4).unwrap_or(std::cmp::Ordering::Equal));
        best.truncate(k);
        best.into_iter()
            .map(|(id, plon, plat, d, _)| (id, plon, plat, d))
            .collect()
    }

    /// K-nearest, ebg_ids only.
    pub fn snap_k(&self, lon: f64, lat: f64, mode_idx: u8, k: usize) -> Vec<u32> {
        self.snap_k_with_info(lon, lat, mode_idx, k)
            .into_iter()
            .map(|(id, _, _, _)| id)
            .collect()
    }

    /// Yield every sample whose `(lon, lat)` lies in the half-open
    /// `[min_lon, max_lon] x [min_lat, max_lat]` bbox. No mode filter
    /// (the avoid path doesn't want one).
    pub fn samples_in_envelope(
        &self,
        min_lon: f64,
        min_lat: f64,
        max_lon: f64,
        max_lat: f64,
    ) -> Vec<EnvelopeSample> {
        let mut out: Vec<EnvelopeSample> = Vec::new();
        if self.points.points.is_empty() {
            return out;
        }
        let cell = 1i64 << self.points.cell_log2;
        let min_lon_e7 = (min_lon * 1e7) as i64;
        let max_lon_e7 = (max_lon * 1e7) as i64;
        let min_lat_e7 = (min_lat * 1e7) as i64;
        let max_lat_e7 = (max_lat * 1e7) as i64;
        let origin_x = self.grid.origin_x as i64;
        let origin_y = self.grid.origin_y as i64;
        let n_cells_x = self.grid.n_cells_x as i64;
        let n_cells_y = self.grid.n_cells_y as i64;

        let cx_min = ((min_lon_e7 - origin_x) / cell).max(0).min(n_cells_x - 1) as u32;
        let cx_max = ((max_lon_e7 - origin_x) / cell).max(0).min(n_cells_x - 1) as u32;
        let cy_min = ((min_lat_e7 - origin_y) / cell).max(0).min(n_cells_y - 1) as u32;
        let cy_max = ((max_lat_e7 - origin_y) / cell).max(0).min(n_cells_y - 1) as u32;

        for cy in cy_min..=cy_max {
            for cx in cx_min..=cx_max {
                let cell_idx = cy * self.grid.n_cells_x + cx;
                let (start, end) = self.cell_range(cell_idx as usize);
                for p in &self.points.points[start..end] {
                    let plon = p.lon_e7 as f64 / 1e7;
                    let plat = p.lat_e7 as f64 / 1e7;
                    if plon >= min_lon && plon <= max_lon && plat >= min_lat && plat <= max_lat {
                        out.push(EnvelopeSample {
                            lon: plon,
                            lat: plat,
                            ebg_id: p.ebg_id,
                            bearing: p.bearing,
                        });
                    }
                }
            }
        }
        out
    }

    // ---- Internal helpers ------------------------------------------------

    fn cell_range(&self, cell_idx: usize) -> (usize, usize) {
        let off = self.grid.offsets.as_ref();
        if cell_idx + 1 >= off.len() {
            return (0, 0);
        }
        (off[cell_idx] as usize, off[cell_idx + 1] as usize)
    }

    /// Walk concentric cell rings around the query point. The visitor
    /// returns `Some(d2_m)` for samples it ACCEPTS (passing mode mask,
    /// edge filter, bearing filter, etc.) and `None` for rejected
    /// samples. iterate_rings tracks only the best ACCEPTED squared
    /// distance for the early-exit cutoff. This is critical: if we
    /// counted physically-close-but-rejected samples toward the cutoff,
    /// the loop could terminate before reaching mode-eligible samples
    /// in outer rings, and snap_filtered queries near pedestrianized
    /// areas (or at bbox edges) would silently fail.
    fn iterate_rings<F>(&self, lon: f64, lat: f64, mut visit: F)
    where
        F: FnMut(usize, &PackedPoint) -> Option<f64>,
    {
        if self.points.points.is_empty() {
            return;
        }

        let lon_e7 = (lon * 1e7) as i64;
        let lat_e7 = (lat * 1e7) as i64;
        let cell_size = 1i64 << self.points.cell_log2;
        let origin_x = self.grid.origin_x as i64;
        let origin_y = self.grid.origin_y as i64;
        let n_cells_x = self.grid.n_cells_x as i64;
        let n_cells_y = self.grid.n_cells_y as i64;

        // Query cell coordinates (may be outside the grid for far-away
        // points; we clamp at iteration time).
        let qcx = ((lon_e7 - origin_x) / cell_size) as i32;
        let qcy = ((lat_e7 - origin_y) / cell_size) as i32;

        let cell_size_lon_m = cell_size as f64 / 1e7 * METERS_PER_DEG_LON_AT_50;
        let cell_size_lat_m = cell_size as f64 / 1e7 * METERS_PER_DEG_LAT;

        let mut best_accepted_d2: Option<f64> = None;

        for ring in 0..=MAX_RING_RADIUS {
            let cx_min = qcx - ring;
            let cx_max = qcx + ring;
            let cy_min = qcy - ring;
            let cy_max = qcy + ring;

            for cy in cy_min..=cy_max {
                if cy < 0 || cy >= n_cells_y as i32 {
                    continue;
                }
                for cx in cx_min..=cx_max {
                    if cx < 0 || cx >= n_cells_x as i32 {
                        continue;
                    }
                    // Skip cells we already visited in inner rings.
                    if ring > 0 && (cx > cx_min && cx < cx_max) && (cy > cy_min && cy < cy_max) {
                        continue;
                    }
                    let cell_idx = (cy as i64 * n_cells_x + cx as i64) as usize;
                    let (start, end) = self.cell_range(cell_idx);
                    let pts = &self.points.points[start..end];
                    for (offset, p) in pts.iter().enumerate() {
                        if let Some(d2) = visit(start + offset, p) {
                            best_accepted_d2 = Some(match best_accepted_d2 {
                                Some(b) => b.min(d2),
                                None => d2,
                            });
                        }
                    }
                }
            }

            // Early exit: if the best ACCEPTED so far is closer than the
            // distance from query to the next ring's inner edge, no
            // further accepted sample can beat it.
            if let Some(b) = best_accepted_d2 {
                let next_ring = (ring + 1) as f64;
                let inner_m_lon = next_ring * cell_size_lon_m;
                let inner_m_lat = next_ring * cell_size_lat_m;
                let inner_m = inner_m_lon.min(inner_m_lat);
                if b < inner_m * inner_m {
                    return;
                }
            }
        }
    }
}

/// Public envelope-sample row, returned by `samples_in_envelope`.
#[derive(Debug, Clone, Copy)]
pub struct EnvelopeSample {
    pub lon: f64,
    pub lat: f64,
    pub ebg_id: u32,
    pub bearing: u16,
}

#[inline]
fn mask_bit_set(bits: &[u64], i: usize) -> bool {
    let word = i / 64;
    let bit = i % 64;
    word < bits.len() && (bits[word] & (1u64 << bit)) != 0
}

#[inline]
fn sample_distance2(lon: f64, lat: f64, p: &PackedPoint) -> (f64, f64, f64) {
    let plon = p.lon_e7 as f64 / 1e7;
    let plat = p.lat_e7 as f64 / 1e7;
    let dlat = (plat - lat) * METERS_PER_DEG_LAT;
    let dlon = (plon - lon) * METERS_PER_DEG_LON_AT_50;
    (dlat * dlat + dlon * dlon, plon, plat)
}

/// Bearing-match check (mirrors `SpatialIndex::bearing_matches`).
fn bearing_matches(candidate: u16, requested: u16, range: u16) -> bool {
    let diff = (candidate as i32 - requested as i32).unsigned_abs() as u16;
    let diff = diff.min(360 - diff);
    diff <= range
}

// ---------- Tests -----------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::ebg_nodes::EbgNode;
    use crate::formats::nbg_geo::PolyLine;

    fn make_synthetic() -> (EbgNodes, NbgGeo) {
        // 5x5 grid of EBG nodes, each pointing at a single-vertex polyline.
        // Spacing: 0.001° (~111 m lat, 71 m lon) — ensures multiple per cell.
        let mut nodes: Vec<EbgNode> = Vec::new();
        let mut polys: Vec<PolyLine> = Vec::new();
        for j in 0..5 {
            for i in 0..5 {
                let lat = 50.0 + 0.001 * j as f64;
                let lon = 4.0 + 0.001 * i as f64;
                polys.push(PolyLine {
                    lat_fxp: vec![(lat * 1e7) as i32],
                    lon_fxp: vec![(lon * 1e7) as i32],
                });
                nodes.push(EbgNode {
                    tail_nbg: 0,
                    head_nbg: 0,
                    geom_idx: (j * 5 + i) as u32,
                    length_mm: 0,
                    class_bits: 0,
                    primary_way: 0,
                });
            }
        }
        let n_nodes = nodes.len() as u32;
        let ebg = EbgNodes {
            n_nodes,
            created_unix: 0,
            inputs_sha: [0; 32],
            nodes: crate::formats::ArcCow::from_vec(nodes),
        };
        let geo = NbgGeo {
            n_edges_und: 0,
            edges: Vec::new(),
            polylines: polys,
        };
        (ebg, geo)
    }

    fn full_mask(n_ebg: usize) -> Vec<u64> {
        let mut m = vec![0u64; n_ebg.div_ceil(64)];
        for i in 0..n_ebg {
            m[i / 64] |= 1u64 << (i % 64);
        }
        m
    }

    fn build_for_test() -> PackedSnapIndex {
        let (ebg, geo) = make_synthetic();
        let mask = full_mask(ebg.n_nodes as usize);
        let modes = vec![SnapBuilderMode {
            mode_byte: 0,
            mask: &mask,
            inputs_sha: [0; 16],
        }];
        let built = build_snap_index(&ebg, &geo, &modes, DEFAULT_CELL_LOG2);
        PackedSnapIndex {
            points: built.points,
            grid: built.grid,
            masks: built.masks,
        }
    }

    #[test]
    fn build_packs_every_node() {
        let idx = build_for_test();
        assert_eq!(idx.n_indexed(), 25, "5x5 grid should produce 25 samples");
        assert_eq!(idx.n_modes(), 1);
    }

    #[test]
    fn snap_returns_nearest() {
        let idx = build_for_test();
        // Query at the (2,2) node exactly.
        let lon = 4.002;
        let lat = 50.002;
        let id = idx.snap(lon, lat, 0).expect("snap");
        // Expected EBG id = j*5 + i = 2*5 + 2 = 12.
        assert_eq!(id, 12);
    }

    #[test]
    fn snap_with_info_returns_distance() {
        let idx = build_for_test();
        let lon = 4.0;
        let lat = 50.0;
        let (id, plon, plat, d) = idx.snap_with_info(lon, lat, 0).unwrap();
        assert_eq!(id, 0);
        assert!((plon - 4.0).abs() < 1e-6);
        assert!((plat - 50.0).abs() < 1e-6);
        assert!(d < 1.0);
    }

    #[test]
    fn snap_returns_none_for_far_point() {
        let idx = build_for_test();
        // 1000 km away
        assert!(idx.snap(0.0, 0.0, 0).is_none());
    }

    #[test]
    fn k_nearest_returns_k_distinct_edges() {
        let idx = build_for_test();
        let lon = 4.002;
        let lat = 50.002;
        let v = idx.snap_k_with_info(lon, lat, 0, 4);
        assert_eq!(v.len(), 4);
        // Distinct ebg ids
        let mut ids: Vec<u32> = v.iter().map(|x| x.0).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 4);
        // Sorted by distance
        for w in v.windows(2) {
            assert!(w[0].3 <= w[1].3 + 1e-9);
        }
    }

    #[test]
    fn samples_in_envelope_returns_box_contents() {
        let idx = build_for_test();
        let v = idx.samples_in_envelope(4.001, 50.001, 4.003, 50.003);
        // Samples at i=1,2,3 and j=1,2,3 -> 9 samples.
        assert_eq!(v.len(), 9);
    }

    #[test]
    fn empty_input_handled() {
        let ebg = EbgNodes {
            n_nodes: 0,
            created_unix: 0,
            inputs_sha: [0; 32],
            nodes: crate::formats::ArcCow::from_vec(Vec::new()),
        };
        let geo = NbgGeo {
            n_edges_und: 0,
            edges: Vec::new(),
            polylines: vec![],
        };
        let modes = Vec::<SnapBuilderMode<'_>>::new();
        let built = build_snap_index(&ebg, &geo, &modes, DEFAULT_CELL_LOG2);
        let idx = PackedSnapIndex {
            points: built.points,
            grid: built.grid,
            masks: built.masks,
        };
        assert_eq!(idx.n_indexed(), 0);
        assert!(idx.snap(4.0, 50.0, 0).is_none());
    }

    #[test]
    fn mode_mask_filters_correctly() {
        let (ebg, geo) = make_synthetic();
        // Mask only the corner node 0
        let mut m = vec![0u64; (ebg.n_nodes as usize).div_ceil(64)];
        m[0] |= 1u64;
        let modes = vec![SnapBuilderMode {
            mode_byte: 0,
            mask: &m,
            inputs_sha: [0; 16],
        }];
        let built = build_snap_index(&ebg, &geo, &modes, DEFAULT_CELL_LOG2);
        let idx = PackedSnapIndex {
            points: built.points,
            grid: built.grid,
            masks: built.masks,
        };
        // Querying near the (2,2) node with the mask only allowing
        // node 0 should snap to node 0 (~3 cells away).
        let id = idx.snap(4.002, 50.002, 0);
        assert_eq!(id, Some(0));
    }

    #[test]
    fn bearing_filter_rejects_outside_range() {
        // Synthetic two-vertex polylines with known endpoint bearings.
        // Build two EBG nodes: one going North (bearing ~0), one East.
        let polys = vec![
            PolyLine {
                lat_fxp: vec![500_000_000, 500_010_000],
                lon_fxp: vec![40_000_000, 40_000_000],
            }, // 0.001° N -> bearing 0
            PolyLine {
                lat_fxp: vec![500_000_000, 500_000_000],
                lon_fxp: vec![40_000_000, 40_010_000],
            }, // 0.001° E -> bearing 90
        ];
        let nodes = vec![
            EbgNode {
                tail_nbg: 0,
                head_nbg: 0,
                geom_idx: 0,
                length_mm: 0,
                class_bits: 0,
                primary_way: 0,
            },
            EbgNode {
                tail_nbg: 0,
                head_nbg: 0,
                geom_idx: 1,
                length_mm: 0,
                class_bits: 0,
                primary_way: 0,
            },
        ];
        let ebg = EbgNodes {
            n_nodes: 2,
            created_unix: 0,
            inputs_sha: [0; 32],
            nodes: crate::formats::ArcCow::from_vec(nodes),
        };
        let geo = NbgGeo {
            n_edges_und: 0,
            edges: Vec::new(),
            polylines: polys,
        };
        let mask = full_mask(2);
        let modes = vec![SnapBuilderMode {
            mode_byte: 0,
            mask: &mask,
            inputs_sha: [0; 16],
        }];
        let built = build_snap_index(&ebg, &geo, &modes, DEFAULT_CELL_LOG2);
        let idx = PackedSnapIndex {
            points: built.points,
            grid: built.grid,
            masks: built.masks,
        };
        // Query at the shared start point (lon=4.0, lat=50.0).
        // Bearing filter for North (0±20°) should match node 0.
        // Bearing for East (90±20°) should match node 1.
        let id_n = idx.snap_with_bearing(4.0, 50.0, 0, 0, 20).map(|x| x.0);
        let id_e = idx.snap_with_bearing(4.0, 50.0, 0, 90, 20).map(|x| x.0);
        assert_eq!(id_n, Some(0));
        assert_eq!(id_e, Some(1));
    }
}
