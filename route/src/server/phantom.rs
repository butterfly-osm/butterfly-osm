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
    /// Seed cost in the TIME channel (deciseconds/seconds — same unit as
    /// `node_weights`). Source: partial remainder; target: `shift - suffix`.
    pub cost: u32,
    /// Arc-length fraction of the snap point along THIS directed edge's
    /// traversal (0 = at its tail, 1 = at its head). For geometry clipping.
    pub frac: f64,
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
    /// Target-side shift (0 for sources). Final duration = raw - shift.
    pub shift: u32,
}

impl PhantomEnd {
    /// The seed's stored fraction for a given ebg id (used to clip geometry).
    pub fn frac_of(&self, ebg_id: u32) -> Option<f64> {
        self.seeds
            .iter()
            .find(|s| s.ebg_id == ebg_id)
            .map(|s| s.frac)
    }
}

/// Project (lon, lat) onto the edge's stored polyline; return the arc-length
/// fraction of the closest point along the STORED direction, in [0, 1].
fn projection_fraction(
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    ebg_id: u32,
    lon: f64,
    lat: f64,
) -> f64 {
    let node = &ebg_nodes.nodes[ebg_id as usize];
    let poly = edge_geom.polyline(node.geom_idx);
    let n = poly.len();
    if n < 2 {
        return 0.5;
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
        (best_arc / total).clamp(0.0, 1.0)
    } else {
        0.5
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
        if let Some(pe) = phantom_from_primary(state, mode_data, cand, lon, lat, role, edge_filter)
        {
            edges_used.push(base);
            match &mut end {
                None => end = Some(pe),
                Some(acc) => acc.seeds.extend(pe.seeds),
            }
        }
    }
    let mut end = end?;
    // Re-derive the target shift across ALL merged seeds: per-edge shifts are
    // inconsistent with each other, so rebuild from raw suffixes.
    if matches!(role, SnapRole::Dst) && !end.seeds.is_empty() {
        // undo per-edge shifts back to suffixes, then apply a global shift
        // (seed = shift_edge - suffix → suffix = shift_edge - seed; the per-edge
        // shift is not stored per seed, so recompute suffix from weights).
        for s in &mut end.seeds {
            let w = mode_data.node_weights[s.ebg_id as usize] as f64;
            s.cost = ((1.0 - s.frac) * w).round() as u32; // suffix again
        }
        let shift = end.seeds.iter().map(|s| s.cost).max().unwrap_or(0);
        for s in &mut end.seeds {
            s.cost = shift - s.cost;
        }
        end.shift = shift;
    }
    Some(end)
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
    let (primary, plon, plat, dist_m) = primary_tuple;

    // Twin pair of the snapped physical edge (forward = even, reverse = odd).
    let base = primary & !1u32;
    let fwd = base; // stored geometry direction (tail = u)
    let rev = base | 1;

    // Fraction along the STORED direction; the reverse twin traverses the same
    // geometry backward, so its own fraction is 1 - f.
    let f_stored = projection_fraction(&state.ebg_nodes, &state.edge_geom, fwd, lon, lat);

    let mut seeds: Vec<PhantomSeed> = Vec::with_capacity(2);
    let mut push = |ebg_id: u32, frac_along_self: f64| {
        if seed_valid(mode_data, role, edge_filter, ebg_id) {
            let w = mode_data.node_weights[ebg_id as usize] as f64;
            // Source: cost from snap to this edge's head = (1 - frac)·w.
            // Target: suffix (overpay) from snap to head = (1 - frac)·w too —
            //         converted to a shifted seed below.
            let part = ((1.0 - frac_along_self) * w).round() as u32;
            seeds.push(PhantomSeed {
                ebg_id,
                rank: mode_data.orig_to_rank[ebg_id as usize],
                cost: part,
                frac: frac_along_self,
            });
        }
    };
    push(fwd, f_stored);
    push(rev, 1.0 - f_stored);
    if seeds.is_empty() {
        return None;
    }

    let shift = match role {
        // Target: seeds currently hold suffix(d); rewrite to shift - suffix.
        SnapRole::Dst => {
            let shift = seeds.iter().map(|s| s.cost).max().unwrap_or(0);
            for s in &mut seeds {
                s.cost = shift - s.cost;
            }
            shift
        }
        // Source (and Either): seeds are the forward remainders as-is.
        _ => 0,
    };

    Some(PhantomEnd {
        seeds,
        snapped_lon: plon,
        snapped_lat: plat,
        snap_distance_m: dist_m,
        primary_ebg: primary,
        shift,
    })
}
