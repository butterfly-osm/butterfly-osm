//! #502: phantom endpoint construction — multi-candidate directional snapping.
//!
//! The spatial snap projects a query point onto ONE directed EBG half-edge, but
//! both directions of the physical edge are equally valid departure/arrival
//! states (they are co-located in the index; the historical single pick was an
//! R-tree iteration-order tie-break). Committing to the direction that points
//! away from the destination forces traversing the remaining edge the wrong way
//! — 2-4× detours on long rural chains (#502/#503).
//!
//! A `PhantomEnd` therefore carries UP TO TWO directed seeds (the snapped edge
//! and its twin `ebg_id ^ 1`) with exact partial-edge costs:
//!
//! - CCH arc weights are charge-on-entry (`w(head_edge) + turn`), so a source
//!   label at edge `e` means "standing on `e`, about to leave via its head".
//!   Seeding `e` costs the REMAINDER of `e` from the snap point:
//!   `(1-f)·w(e)` for the stored direction, `f·w(twin)` for the twin, where
//!   `f` is the arc-length fraction of the snap along the stored geometry.
//! - A backward (target) label at edge `d` has already paid FULL `w(d)`, but
//!   the journey stops at the snap point — an overpay of `suffix(d)` =
//!   `(1-f_d)·w(d)` along `d`'s traversal direction. Negative seeds are not
//!   representable, so targets use a per-end constant shift:
//!   `seed(d) = shift - suffix(d)` with `shift = max suffix`; the caller
//!   subtracts `shift` from the final best (see `query_phantom`).

use crate::profile_abi::Mode;

use super::edge_geom::EdgeGeometry;
use super::state::{ModeData, ServerState};
use super::types::SnapRole;
use crate::formats::EbgNodes;

/// One directed seed of a phantom endpoint.
#[derive(Debug, Clone, Copy)]
pub struct PhantomSeed {
    /// Original EBG node id (directed edge).
    pub ebg_id: u32,
    /// Rank-space id in this mode's CCH.
    pub rank: u32,
    /// RAW partial in the TIME channel: cost of the remainder of this directed
    /// edge from the snap point to its head — `(1-frac)·w(edge)`. Sources seed
    /// with it directly; targets interpret it as the SUFFIX overpay (see
    /// [`PhantomEnd::query_seeds_and_shift`]) or subtract it in API-level
    /// reductions.
    pub part_time: u32,
    /// Same partial in METERS: `(1-frac)·length_m`.
    pub part_len: u32,
    /// Arc-length fraction of the snap point along THIS directed edge's
    /// traversal (0 = at its tail, 1 = at its head). For geometry clipping.
    pub frac: f64,
    /// Whether this seed may participate in SAME-EDGE direct/zero-move
    /// evaluations. True for the primary (geometrically best) edge and for
    /// secondary edges the point projects ONTO (interior). False for
    /// CLAMPED secondary projections: two distinct points clamping onto the
    /// same end of a shared side street collapse to equal fractions and
    /// fabricate a 0-cost "direct" move (live bug: 0 s vs a true 30-70 s
    /// drive). Such seeds still participate in the network query, where the
    /// junction-entry approximation is fine.
    pub direct_ok: bool,
}

/// A snapped query endpoint with its directional seeds.
#[derive(Debug, Clone)]
pub struct PhantomEnd {
    /// 1-2 directional seeds (twin dropped when inaccessible in this mode).
    pub seeds: Vec<PhantomSeed>,
    /// The geometrically snapped point (projection on the edge).
    pub snapped_lon: f64,
    pub snapped_lat: f64,
    pub snap_distance_m: f64,
    /// The primary (geometrically snapped) directed edge — for debug output
    /// and single-seed fallbacks.
    pub primary_ebg: u32,
}

impl PhantomEnd {
    /// The seed's stored fraction for a given ebg id (used to clip geometry).
    pub fn frac_of(&self, ebg_id: u32) -> Option<f64> {
        self.seeds
            .iter()
            .find(|s| s.ebg_id == ebg_id)
            .map(|s| s.frac)
    }

    /// The full seed for a given ebg id (direct-move eligibility + fraction).
    pub fn seed_of(&self, ebg_id: u32) -> Option<&PhantomSeed> {
        self.seeds.iter().find(|s| s.ebg_id == ebg_id)
    }

    /// Time-channel query seeds `(rank, cost)` + the shift to subtract from
    /// the final raw best, per role:
    /// - `Src`: cost = raw partial (remainder to head), shift = 0.
    /// - `Dst`: reaching edge `d` pays full `w(d)` but the journey stops at
    ///   the snap — overpay = `part_time(d)`. Negative labels aren't
    ///   representable, so cost = `shift - part_time` with
    ///   `shift = max part_time`; caller subtracts `shift` from the best.
    pub fn query_seeds_and_shift(&self, role: SnapRole) -> (Vec<(u32, u32)>, u32) {
        match role {
            SnapRole::Dst => {
                let shift = self.seeds.iter().map(|s| s.part_time).max().unwrap_or(0);
                (
                    self.seeds
                        .iter()
                        .map(|s| (s.rank, shift - s.part_time))
                        .collect(),
                    shift,
                )
            }
            _ => (
                self.seeds.iter().map(|s| (s.rank, s.part_time)).collect(),
                0,
            ),
        }
    }
}

/// Project (lon, lat) onto the edge's stored polyline; return the arc-length
/// fraction of the closest point along the STORED direction, in [0, 1].
// `pub(crate)` only so the unit tests below can pin the geometry directly;
// behavior is unchanged.
pub(crate) fn projection_fraction(
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    ebg_id: u32,
    lon: f64,
    lat: f64,
) -> (f64, bool) {
    let node = &ebg_nodes.nodes[ebg_id as usize];
    let poly = edge_geom.polyline(node.geom_idx);
    let n = poly.len();
    if n < 2 {
        return (0.5, false);
    }
    // planar approximation, consistent with the snap index's projection
    let mlat = 111_320.0_f64;
    let mlon = 111_320.0 * (lat.to_radians().cos());
    let px = lon * mlon;
    let py = lat * mlat;

    let mut best_d2 = f64::INFINITY;
    let mut best_arc = 0.0_f64;
    let mut total = 0.0_f64;
    let mut prev = poly.at(0);
    let mut acc = 0.0_f64;
    for i in 1..n {
        let cur = poly.at(i);
        let (x1, y1) = (prev.0 * mlon, prev.1 * mlat);
        let (x2, y2) = (cur.0 * mlon, cur.1 * mlat);
        let (dx, dy) = (x2 - x1, y2 - y1);
        let seg_len = (dx * dx + dy * dy).sqrt();
        let t = if seg_len > 0.0 {
            (((px - x1) * dx + (py - y1) * dy) / (seg_len * seg_len)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let (cx, cy) = (x1 + t * dx, y1 + t * dy);
        let d2 = (px - cx) * (px - cx) + (py - cy) * (py - cy);
        if d2 < best_d2 {
            best_d2 = d2;
            best_arc = acc + t * seg_len;
        }
        acc += seg_len;
        total = acc;
        prev = cur;
    }
    if total > 0.0 {
        let f = (best_arc / total).clamp(0.0, 1.0);
        // Interior = the point actually projects ONTO the edge, not past an
        // end. A sliver of tolerance (~0.5 m on a 500 m edge) keeps genuine
        // endpoint projections classified as clamped.
        let interior = best_arc > 1e-3 * total && best_arc < total * (1.0 - 1e-3);
        (f, interior)
    } else {
        (0.5, false)
    }
}

/// True when `ebg_id` is a valid seed in this mode under the given role +
/// dynamic edge filter: mode-accessible weight, contracted rank, role mask.
fn seed_valid(
    mode_data: &ModeData,
    role: SnapRole,
    edge_filter: Option<&[u64]>,
    ebg_id: u32,
) -> bool {
    let i = ebg_id as usize;
    if i >= mode_data.node_weights.len() {
        return false;
    }
    let w = mode_data.node_weights[i];
    if w == 0 || w == u32::MAX {
        return false;
    }
    if mode_data.orig_to_rank[i] == u32::MAX {
        return false;
    }
    let bit_ok = |m: &[u64]| (i / 64) < m.len() && (m[i / 64] >> (i % 64)) & 1 == 1;
    if !bit_ok(&mode_data.mask) {
        return false;
    }
    if let Some(rf) = role.role_filter(mode_data)
        && !bit_ok(rf)
    {
        return false;
    }
    if let Some(ef) = edge_filter
        && !bit_ok(ef)
    {
        return false;
    }
    true
}

/// Build a phantom endpoint: snap, then seed both directions of the snapped
/// physical edge with exact partial-edge costs. `role` decides the cost form
/// (source remainder vs target shifted suffix).
pub fn build_phantom_end(
    state: &ServerState,
    mode_data: &ModeData,
    mode: Mode,
    lon: f64,
    lat: f64,
    role: SnapRole,
    edge_filter: Option<&[u64]>,
) -> Option<PhantomEnd> {
    let role_filter = role.role_filter(mode_data);
    let primary_tuple = state.snap_index.snap_with_info_filtered_role(
        lon,
        lat,
        mode.0,
        edge_filter,
        role_filter,
    )?;
    phantom_from_primary(state, mode_data, primary_tuple, lon, lat, role, edge_filter)
}

/// Build a phantom endpoint from an ALREADY-RESOLVED primary snap tuple
/// `(ebg_id, snapped_lon, snapped_lat, snap_distance_m)` — used by handlers
/// that have their own snap flow (K-best escalation, bearing filters).
/// Build a phantom endpoint from K nearest candidates: seeds the twins of up
/// to `MAX_PHANTOM_EDGES` distinct physical edges whose snap distance is
/// within `SNAP_SLACK_M` (or 20 %) of the best — two parallel roads at
/// near-equal distance are BOTH plausible endpoints and the search must be
/// allowed to pick (#502 Robertville: correct road was 12 m further than a
/// track whose both directions detour 15 km).
pub fn phantom_from_candidates(
    state: &ServerState,
    mode_data: &ModeData,
    candidates: &[(u32, f64, f64, f64)],
    lon: f64,
    lat: f64,
    role: SnapRole,
    edge_filter: Option<&[u64]>,
) -> Option<PhantomEnd> {
    const MAX_PHANTOM_EDGES: usize = 3;
    const SNAP_SLACK_M: f64 = 20.0;
    let best_d = candidates.first()?.3;
    let slack = (best_d + SNAP_SLACK_M).max(best_d * 1.2);

    let mut end: Option<PhantomEnd> = None;
    let mut edges_used: Vec<u32> = Vec::new();
    for &cand in candidates {
        if cand.3 > slack {
            break;
        }
        let base = cand.0 & !1u32;
        if edges_used.contains(&base) {
            continue;
        }
        if edges_used.len() >= MAX_PHANTOM_EDGES {
            break;
        }
        let is_secondary = !edges_used.is_empty();
        if let Some(pe) = phantom_from_primary_inner(
            state,
            mode_data,
            cand,
            lon,
            lat,
            role,
            edge_filter,
            is_secondary,
        ) {
            edges_used.push(base);
            match &mut end {
                None => end = Some(pe),
                Some(acc) => acc.seeds.extend(pe.seeds),
            }
        }
    }
    end
}

pub fn phantom_from_primary(
    state: &ServerState,
    mode_data: &ModeData,
    primary_tuple: (u32, f64, f64, f64),
    lon: f64,
    lat: f64,
    role: SnapRole,
    edge_filter: Option<&[u64]>,
) -> Option<PhantomEnd> {
    phantom_from_primary_inner(
        state,
        mode_data,
        primary_tuple,
        lon,
        lat,
        role,
        edge_filter,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
fn phantom_from_primary_inner(
    state: &ServerState,
    mode_data: &ModeData,
    primary_tuple: (u32, f64, f64, f64),
    lon: f64,
    lat: f64,
    role: SnapRole,
    edge_filter: Option<&[u64]>,
    is_secondary: bool,
) -> Option<PhantomEnd> {
    let (primary, plon, plat, dist_m) = primary_tuple;

    // Twin pair of the snapped physical edge (forward = even, reverse = odd).
    let base = primary & !1u32;
    let fwd = base; // stored geometry direction (tail = u)
    let rev = base | 1;

    // Fraction along the STORED direction; the reverse twin traverses the same
    // geometry backward, so its own fraction is 1 - f.
    //
    // `direct_ok` marks PRIMARY-edge seeds only. Secondary seeds (nearby
    // parallel/side streets within snap slack) exist to give the network
    // query directional alternatives — they do NOT claim the point is ON
    // that edge, so they must not participate in same-edge direct/zero-move
    // evaluations: two points near a shared side street both project onto
    // its stem at nearly the same spot and fabricate a ~0-cost "direct"
    // between points a real 30-190 s drive apart (live /table 0-second bug).
    let (f_stored, _interior) =
        projection_fraction(&state.ebg_nodes, &state.edge_geom, fwd, lon, lat);
    let direct_ok = !is_secondary;

    let mut seeds: Vec<PhantomSeed> = Vec::with_capacity(2);
    let mut push = |ebg_id: u32, frac_along_self: f64| {
        if seed_valid(mode_data, role, edge_filter, ebg_id) {
            let w = mode_data.node_weights[ebg_id as usize] as f64;
            let len = state.ebg_nodes.nodes[ebg_id as usize].length_m as f64;
            let rem = 1.0 - frac_along_self;
            seeds.push(PhantomSeed {
                ebg_id,
                rank: mode_data.orig_to_rank[ebg_id as usize],
                part_time: (rem * w).round() as u32,
                part_len: (rem * len).round() as u32,
                frac: frac_along_self,
                direct_ok,
            });
        }
    };
    push(fwd, f_stored);
    push(rev, 1.0 - f_stored);
    if seeds.is_empty() {
        return None;
    }

    Some(PhantomEnd {
        seeds,
        snapped_lon: plon,
        snapped_lat: plat,
        snap_distance_m: dist_m,
        primary_ebg: primary,
    })
}

// =============================================================================
// #502: matrix seed expansion — shared by /table (REST) and Flight `matrix`.
// Each endpoint's directional seeds become extra rows/columns for the bucket
// engine (which stays untouched); `reduce_*` collapses the expanded result
// back to S×T with exact partial-edge adjustments.
// =============================================================================

/// Expanded seed lists for one matrix axis.
pub struct SeedExpansion {
    /// Rank per expanded row/column (engine input).
    pub exp_ranks: Vec<u32>,
    /// (start, len) span into `exp_ranks` per ORIGINAL endpoint index.
    pub spans: Vec<(usize, usize)>,
    /// (time_part, len_part) per expanded row/column.
    pub parts: Vec<(u32, u32, bool)>,
}

impl SeedExpansion {
    /// Build from per-endpoint seed sets `(rank, time_part, len_part,
    /// direct_ok)`. Empty sets (invalid endpoints) get one placeholder so
    /// spans align; their cells are masked by the caller's validity flags as
    /// before.
    pub fn build(seedsets: &[Vec<(u32, u32, u32, bool)>]) -> Self {
        let mut exp_ranks = Vec::new();
        let mut spans = Vec::with_capacity(seedsets.len());
        let mut parts = Vec::new();
        for seeds in seedsets {
            let start = exp_ranks.len();
            for &(r, t, l, ok) in seeds {
                exp_ranks.push(r);
                parts.push((t, l, ok));
            }
            if seeds.is_empty() {
                exp_ranks.push(0);
                parts.push((0, 0, false));
            }
            spans.push((start, exp_ranks.len() - start));
        }
        Self {
            exp_ranks,
            spans,
            parts,
        }
    }

    /// Max time part — bounded searches widen their threshold by
    /// `src.slack() + tgt.slack()` so seed adjustments can't cut valid cells.
    pub fn slack(&self) -> u32 {
        self.parts.iter().map(|p| p.0).max().unwrap_or(0)
    }

    /// Reduce an expanded TIME matrix (rows = self, cols = tgt) to S×T.
    /// `carry` (e.g. length-along-time) is read at the time-argmin so the
    /// two channels stay path-consistent.
    pub fn reduce_time(
        &self,
        tgt: &SeedExpansion,
        m: &[u32],
        carry: Option<&[u32]>,
    ) -> (Vec<u32>, Option<Vec<u32>>) {
        self.reduce_inner(tgt, m, carry, false)
    }

    /// Reduce an expanded DISTANCE matrix with LENGTH partials.
    pub fn reduce_len(&self, tgt: &SeedExpansion, m: &[u32]) -> Vec<u32> {
        self.reduce_inner(tgt, m, None, true).0
    }

    fn reduce_inner(
        &self,
        tgt: &SeedExpansion,
        m: &[u32],
        carry: Option<&[u32]>,
        use_len_parts: bool,
    ) -> (Vec<u32>, Option<Vec<u32>>) {
        let n_exp_t = tgt.exp_ranks.len();
        let n_s = self.spans.len();
        let n_t = tgt.spans.len();
        let mut out = vec![u32::MAX; n_s * n_t];
        let mut out_c = carry.map(|_| vec![u32::MAX; n_s * n_t]);
        let pick = |p: &(u32, u32, bool)| if use_len_parts { p.1 } else { p.0 };
        for (i, &(ss, sl)) in self.spans.iter().enumerate() {
            for (j, &(ts, tl)) in tgt.spans.iter().enumerate() {
                let mut best = i64::MAX;
                let mut best_rc = (0usize, 0usize);
                for r in ss..ss + sl {
                    for c in ts..ts + tl {
                        let v = m[r * n_exp_t + c];
                        if v == u32::MAX {
                            continue;
                        }
                        // A same-rank combo is the engine's zero-cost identity
                        // cell — a pure seed-seed meet standing in for the
                        // SAME-EDGE direct move. Only valid when both seeds
                        // actually project onto the edge (direct_ok); clamped
                        // secondary projections fabricate 0-cost moves between
                        // distinct points (live 0-second /table bug).
                        if self.exp_ranks[r] == tgt.exp_ranks[c]
                            && !(self.parts[r].2 && tgt.parts[c].2)
                        {
                            continue;
                        }
                        let adj =
                            v as i64 + pick(&self.parts[r]) as i64 - pick(&tgt.parts[c]) as i64;
                        // NEGATIVE adj is an INVALID pure seed-seed meet, not a
                        // path: any REAL path into the target edge has paid its
                        // full charge-on-entry (v >= w(dst) >= tgt part), so
                        // v + src_part - tgt_part >= 0 always. Only the
                        // engine's zero-cost same-rank cell (source seed ==
                        // target seed, journey would run BACKWARD along the
                        // edge) can go negative. Clamping it to 0 emitted 0 s
                        // for ~12% of close pairs (src ahead of dst on the
                        // same edge). Reject instead — the cell stays MAX and
                        // the K-best P2P rescue computes the true loop.
                        if (0..best).contains(&adj) {
                            best = adj;
                            best_rc = (r, c);
                        }
                    }
                }
                if best != i64::MAX {
                    out[i * n_t + j] = best as u32;
                    if let (Some(oc), Some(cm)) = (&mut out_c, carry) {
                        let (r, c) = best_rc;
                        let lv = cm[r * n_exp_t + c];
                        if lv != u32::MAX {
                            let ladj = lv as i64 + self.parts[r].1 as i64 - tgt.parts[c].1 as i64;
                            oc[i * n_t + j] = ladj.max(0) as u32;
                        }
                    }
                }
            }
        }
        (out, out_c)
    }
}

/// #506: seeded PHAST init `(seeds, exact snapped anchor)` for a center.
pub type CenterSeeds = (Vec<(u32, u32)>, Option<(f64, f64)>);

/// #506: multi-seed PHAST init for an isochrone/catchment center.
///
/// Snaps `(lon, lat)` with K=8 role-aware candidates and converts the phantom
/// seeds into `(rank, cost)` pairs for the seeded PHAST variants:
/// - depart (`is_reverse == false`): cost = remainder of the edge past the
///   snap point (`part_time`)
/// - arrive (`is_reverse == true`): cost = entry-to-snap prefix
///   (`w(edge) − part_time`)
///
/// Returns the seeds plus the exact snapped point (the #497 contour anchor).
/// Falls back to a single zero-cost seed at `fallback_rank` when no phantom
/// end can be built (isolated candidates, filtered edges).
#[allow(clippy::too_many_arguments)]
pub fn isochrone_center_seeds(
    state: &ServerState,
    mode_data: &ModeData,
    mode: Mode,
    lon: f64,
    lat: f64,
    role: SnapRole,
    snap_mask: Option<&[u64]>,
    is_reverse: bool,
    fallback_rank: u32,
) -> CenterSeeds {
    let k = state.snap_index.snap_k_with_info_filtered_role(
        lon,
        lat,
        mode.0,
        8,
        snap_mask,
        role.role_filter(mode_data),
    );
    match phantom_from_candidates(state, mode_data, &k, lon, lat, role, snap_mask) {
        Some(pe) => {
            let anchor = Some((pe.snapped_lon, pe.snapped_lat));
            let seeds = pe
                .seeds
                .iter()
                .map(|sd| {
                    let cost = if is_reverse {
                        mode_data.node_weights[sd.ebg_id as usize].saturating_sub(sd.part_time)
                    } else {
                        sd.part_time
                    };
                    (sd.rank, cost)
                })
                .collect();
            (seeds, anchor)
        }
        None => (vec![(fallback_rank, 0)], None),
    }
}

#[cfg(test)]
mod tests {
    //! Pure-geometry / pure-arithmetic unit tests for the phantom-endpoint
    //! construction. No server, no Belgium container: `projection_fraction`
    //! runs on a hand-built single-edge geometry with exactly derivable
    //! expected fractions, `query_seeds_and_shift` and `SeedExpansion` run on
    //! plain in-memory structs.
    use super::*;
    use crate::formats::ArcCow;
    use crate::formats::ebg_nodes::{EbgNode, EbgNodes};
    use crate::formats::edge_geom::{EdgeGeomOffsets, EdgeGeomPoints};
    use crate::server::edge_geom::EdgeGeometry;
    use crate::server::types::SnapRole;

    /// Build a one-edge `(EbgNodes, EdgeGeometry)` from a polyline of
    /// (lon_deg, lat_deg) vertices. The single EBG node has `geom_idx == 0`.
    fn single_edge(pts_deg: &[(f64, f64)]) -> (EbgNodes, EdgeGeometry) {
        let n = pts_deg.len() as u32;
        let mut flat: Vec<i32> = Vec::with_capacity(pts_deg.len() * 2);
        for &(lon, lat) in pts_deg {
            flat.push((lon * 1e7).round() as i32);
            flat.push((lat * 1e7).round() as i32);
        }
        let min_lon = pts_deg
            .iter()
            .map(|p| (p.0 * 1e7).round() as i32)
            .min()
            .unwrap_or(0);
        let max_lon = pts_deg
            .iter()
            .map(|p| (p.0 * 1e7).round() as i32)
            .max()
            .unwrap_or(0);
        let min_lat = pts_deg
            .iter()
            .map(|p| (p.1 * 1e7).round() as i32)
            .min()
            .unwrap_or(0);
        let max_lat = pts_deg
            .iter()
            .map(|p| (p.1 * 1e7).round() as i32)
            .max()
            .unwrap_or(0);
        let off = EdgeGeomOffsets {
            n_edges: 1,
            n_points: n,
            offsets: ArcCow::from_vec(vec![0u32, n]),
        };
        let points = EdgeGeomPoints {
            n_points: n,
            bbox_min_lon: min_lon,
            bbox_min_lat: min_lat,
            bbox_max_lon: max_lon,
            bbox_max_lat: max_lat,
            points: ArcCow::from_vec(flat),
        };
        let geom = EdgeGeometry::from_sections(off, points).unwrap();
        let node = EbgNode {
            tail_nbg: 0,
            head_nbg: 0,
            geom_idx: 0,
            length_m: 100,
            class_bits: 0,
            primary_way: 0,
        };
        let ebg = EbgNodes {
            n_nodes: 1,
            created_unix: 0,
            inputs_sha: [0u8; 32],
            nodes: ArcCow::from_vec(vec![node]),
        };
        (ebg, geom)
    }

    // --- projection_fraction ------------------------------------------------

    #[test]
    fn projection_fraction_endpoints_and_midpoint_of_straight_segment() {
        // Horizontal edge at lat=0 from lon 0 -> lon 1. At lat=0 the planar
        // metres/degree in lon is constant, so arc length is exactly linear
        // in lon and the fractions are hand-derivable.
        let (ebg, geom) = single_edge(&[(0.0, 0.0), (1.0, 0.0)]);

        let (f_start, int_start) = projection_fraction(&ebg, &geom, 0, 0.0, 0.0);
        assert!(
            f_start.abs() < 1e-9,
            "start vertex -> fraction 0, got {f_start}"
        );
        assert!(
            !int_start,
            "an endpoint projection is CLAMPED, not interior"
        );

        let (f_end, int_end) = projection_fraction(&ebg, &geom, 0, 1.0, 0.0);
        assert!(
            (f_end - 1.0).abs() < 1e-9,
            "end vertex -> fraction 1, got {f_end}"
        );
        assert!(!int_end, "an endpoint projection is CLAMPED, not interior");

        let (f_mid, int_mid) = projection_fraction(&ebg, &geom, 0, 0.5, 0.0);
        assert!(
            (f_mid - 0.5).abs() < 1e-9,
            "midpoint -> fraction 0.5, got {f_mid}"
        );
        assert!(int_mid, "a mid-edge projection IS interior");
    }

    #[test]
    fn projection_fraction_is_clamped_to_unit_interval_past_the_ends() {
        // A point beyond the head projects onto the head (fraction 1); a point
        // before the tail projects onto the tail (fraction 0). Both are the
        // clamp behaviour the seed math relies on (no negative / >1 partials).
        let (ebg, geom) = single_edge(&[(0.0, 0.0), (1.0, 0.0)]);
        let (f_past, _) = projection_fraction(&ebg, &geom, 0, 5.0, 0.0);
        assert!(
            (f_past - 1.0).abs() < 1e-9,
            "past-the-end clamps to 1, got {f_past}"
        );
        let (f_before, _) = projection_fraction(&ebg, &geom, 0, -3.0, 0.0);
        assert!(
            f_before.abs() < 1e-9,
            "before-the-start clamps to 0, got {f_before}"
        );
    }

    #[test]
    fn projection_fraction_perpendicular_offset_does_not_move_the_fraction() {
        // A point off to the side of the midpoint still projects to the
        // midpoint: the arc-length fraction is unchanged (0.5), only the
        // squared distance grows. Locks the "project onto, not snap-to-vertex"
        // property.
        let (ebg, geom) = single_edge(&[(0.0, 0.0), (1.0, 0.0)]);
        let (f, interior) = projection_fraction(&ebg, &geom, 0, 0.5, 0.01);
        assert!(
            (f - 0.5).abs() < 1e-6,
            "perpendicular offset keeps fraction 0.5, got {f}"
        );
        assert!(interior);
    }

    #[test]
    fn projection_fraction_uses_arc_length_not_vertex_index() {
        // Three collinear vertices with UNEQUAL segment lengths: lon 0, 0.001,
        // 0.003 (seg1 length : seg2 length = 1 : 2, total = 3 units). A query
        // at lon 0.002 is the midpoint of seg2, so its arc length is
        // seg1 + 0.5*seg2 = 1 + 1 = 2 units -> fraction 2/3. A vertex-index
        // scheme would wrongly report 0.75 (3/4 of the way past vertex index).
        let (ebg, geom) = single_edge(&[(0.0, 0.0), (0.001, 0.0), (0.003, 0.0)]);
        let (f, interior) = projection_fraction(&ebg, &geom, 0, 0.002, 0.0);
        assert!(
            (f - 2.0 / 3.0).abs() < 1e-6,
            "arc-length fraction must be 2/3, got {f}"
        );
        assert!(interior);
    }

    #[test]
    fn projection_fraction_degenerate_single_point_polyline() {
        // A polyline with <2 vertices has no direction; the function returns
        // the documented (0.5, false) sentinel rather than dividing by zero.
        let (ebg, geom) = single_edge(&[(0.0, 0.0)]);
        let (f, interior) = projection_fraction(&ebg, &geom, 0, 0.0, 0.0);
        assert!(
            (f - 0.5).abs() < 1e-12,
            "single-point sentinel fraction is 0.5"
        );
        assert!(!interior);
    }

    // --- PhantomEnd::query_seeds_and_shift ----------------------------------

    fn seed(ebg_id: u32, rank: u32, part_time: u32, direct_ok: bool) -> PhantomSeed {
        PhantomSeed {
            ebg_id,
            rank,
            part_time,
            part_len: part_time, // irrelevant to the time-channel tests
            frac: 0.0,
            direct_ok,
        }
    }

    fn phantom(seeds: Vec<PhantomSeed>) -> PhantomEnd {
        PhantomEnd {
            primary_ebg: seeds[0].ebg_id,
            seeds,
            snapped_lon: 0.0,
            snapped_lat: 0.0,
            snap_distance_m: 0.0,
        }
    }

    #[test]
    fn source_seeds_are_the_raw_partials_with_zero_shift() {
        // A SOURCE label seeds each directed edge with the REMAINDER cost to
        // its head (`part_time`) and needs no shift.
        let pe = phantom(vec![seed(0, 10, 30, true), seed(1, 11, 70, true)]);
        for role in [SnapRole::Src, SnapRole::Either] {
            let (seeds, shift) = pe.query_seeds_and_shift(role);
            assert_eq!(shift, 0, "source role carries no shift");
            assert_eq!(seeds, vec![(10, 30), (11, 70)]);
        }
    }

    #[test]
    fn dst_seeds_shift_by_the_max_suffix_so_every_cost_is_nonnegative() {
        // A DESTINATION label overpays `suffix = part_time` on the target
        // edge. Negative seeds aren't representable, so the encoding uses
        // cost = shift - part_time with shift = max part_time. Invariants:
        //   * shift == max(part_time)
        //   * the deepest-suffix seed encodes to cost 0
        //   * cost + part_time == shift for EVERY seed (the caller subtracts
        //     shift from the final best to recover the true suffix reduction)
        let parts = [30u32, 70, 55];
        let pe = phantom(vec![
            seed(0, 10, parts[0], true),
            seed(1, 11, parts[1], true),
            seed(2, 12, parts[2], true),
        ]);
        let (seeds, shift) = pe.query_seeds_and_shift(SnapRole::Dst);
        assert_eq!(shift, 70, "shift is the max suffix");
        assert_eq!(seeds[1], (11, 0), "the max-suffix seed encodes to cost 0");
        for (i, &(rank, cost)) in seeds.iter().enumerate() {
            assert_eq!(rank, 10 + i as u32);
            assert_eq!(
                cost + parts[i],
                shift,
                "cost + suffix must equal the constant shift for seed {i}"
            );
        }
    }

    #[test]
    fn frac_of_and_seed_of_locate_seeds_by_ebg_id() {
        let pe = phantom(vec![
            PhantomSeed {
                frac: 0.25,
                ..seed(4, 10, 30, true)
            },
            PhantomSeed {
                frac: 0.75,
                ..seed(5, 11, 70, false)
            },
        ]);
        assert_eq!(pe.frac_of(5), Some(0.75));
        assert_eq!(pe.frac_of(999), None);
        assert!(pe.seed_of(4).unwrap().direct_ok);
        assert!(!pe.seed_of(5).unwrap().direct_ok);
        assert!(pe.seed_of(999).is_none());
    }

    // --- SeedExpansion ------------------------------------------------------

    #[test]
    fn seed_expansion_build_spans_and_placeholder_for_empty_sets() {
        // endpoint 0: two seeds; endpoint 1: none (invalid) -> one placeholder.
        let sets = vec![vec![(10u32, 3u32, 1u32, true), (11, 5, 2, false)], vec![]];
        let exp = SeedExpansion::build(&sets);
        // exp_ranks = [10, 11, 0(placeholder)]
        assert_eq!(exp.exp_ranks, vec![10, 11, 0]);
        // spans: endpoint 0 -> (0,2); endpoint 1 -> (2,1) placeholder.
        assert_eq!(exp.spans, vec![(0, 2), (2, 1)]);
        // parts carry (time, len, direct_ok); placeholder is (0,0,false).
        assert_eq!(exp.parts, vec![(3, 1, true), (5, 2, false), (0, 0, false)]);
        // slack is the max TIME part across all expanded rows.
        assert_eq!(exp.slack(), 5);
    }

    #[test]
    fn reduce_time_applies_source_plus_minus_target_partial() {
        // 1 source seed (rank 10, +5 s) x 1 target seed (rank 20, -7 s).
        // Engine raw time = 100 -> adjusted = 100 + 5 - 7 = 98. The carry
        // (length-along-time) is read at the SAME argmin cell: 50 + 1 - 2 = 49.
        let src = SeedExpansion::build(&[vec![(10u32, 5u32, 1u32, true)]]);
        let tgt = SeedExpansion::build(&[vec![(20u32, 7u32, 2u32, true)]]);
        let m = vec![100u32];
        let carry = vec![50u32];
        let (out, out_c) = src.reduce_time(&tgt, &m, Some(&carry));
        assert_eq!(out, vec![98]);
        assert_eq!(out_c.unwrap(), vec![49]);
    }

    #[test]
    fn reduce_len_uses_the_length_partials() {
        // reduce_len picks the LENGTH partials (index .1): 200 + 3 - 4 = 199.
        let src = SeedExpansion::build(&[vec![(10u32, 5u32, 3u32, true)]]);
        let tgt = SeedExpansion::build(&[vec![(20u32, 7u32, 4u32, true)]]);
        let m = vec![200u32];
        assert_eq!(src.reduce_len(&tgt, &m), vec![199]);
    }

    #[test]
    fn reduce_time_rejects_clamped_secondary_same_rank_meet() {
        // Same rank on both sides is the engine's zero-cost identity cell. It
        // may ONLY stand in for a real same-edge direct move when BOTH seeds
        // truly project onto the edge (direct_ok). Here the source seed is a
        // CLAMPED secondary (direct_ok = false), so the meet is a fabricated
        // 0-cost move and must be rejected -> cell stays u32::MAX (#502/#509).
        let src = SeedExpansion::build(&[vec![(10u32, 5u32, 1u32, false)]]);
        let tgt = SeedExpansion::build(&[vec![(10u32, 5u32, 1u32, true)]]);
        let m = vec![0u32]; // zero-cost identity cell
        let (out, _) = src.reduce_time(&tgt, &m, None);
        assert_eq!(out, vec![u32::MAX], "clamped same-rank meet is not a path");
    }

    #[test]
    fn reduce_time_rejects_negative_same_edge_meet_but_keeps_positive() {
        // Both seeds direct_ok and same rank (same physical edge). The
        // identity cell v=0 gives adj = src_part - tgt_part. When the source
        // sits AHEAD of the destination on the edge (src_part < tgt_part) adj
        // is negative -> an invalid backward "path" -> reject (stays MAX,
        // #509: clamping to 0 emitted spurious 0 s). When the source is BEHIND
        // (src_part > tgt_part) adj is the true positive suffix difference.
        let tgt = SeedExpansion::build(&[vec![(10u32, 10u32, 0u32, true)]]);
        let m = vec![0u32];

        let src_ahead = SeedExpansion::build(&[vec![(10u32, 4u32, 0u32, true)]]);
        let (neg, _) = src_ahead.reduce_time(&tgt, &m, None);
        assert_eq!(
            neg,
            vec![u32::MAX],
            "negative same-edge meet rejected, not clamped to 0"
        );

        let src_behind = SeedExpansion::build(&[vec![(10u32, 16u32, 0u32, true)]]);
        let (pos, _) = src_behind.reduce_time(&tgt, &m, None);
        assert_eq!(pos, vec![6], "behind-on-edge meet = 16 - 10 = 6 s");
    }

    #[test]
    fn reduce_time_takes_the_minimum_over_the_seed_cross_product() {
        // One source with two directional seeds x one target with two seeds:
        // a 2x2 expanded matrix collapses to a single S*T cell = the minimum
        // adjusted cost over all four (src_seed, tgt_seed) combinations.
        let src = SeedExpansion::build(&[vec![(10u32, 0u32, 0u32, true), (11, 0, 0, true)]]);
        let tgt = SeedExpansion::build(&[vec![(20u32, 0u32, 0u32, true), (21, 0, 0, true)]]);
        // exp matrix row-major, n_exp_t = 2:
        //  (10->20)=90 (10->21)=40 / (11->20)=70 (11->21)=55  -> min = 40
        let m = vec![90u32, 40, 70, 55];
        let (out, _) = src.reduce_time(&tgt, &m, None);
        assert_eq!(
            out,
            vec![40],
            "reduce takes the min over the seed cross-product"
        );
    }
}
