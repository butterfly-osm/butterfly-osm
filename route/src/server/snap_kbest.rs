//! K-best snap + combo-fallback helpers shared by /route, /table, /trip,
//! catchment, and the Flight endpoints.
//!
//! With the connectivity-aware role masks (state::build_role_masks) the
//! K-best fallback now only kicks in for same-geometry directional
//! ambiguity (snap landed on the wrong direction of a divided
//! carriageway) and dynamic recustomisation cases (exclude/avoid
//! cutting the only outbound transition). It is no longer the primary
//! correctness mechanism — that work moved into the role masks — but
//! it still covers the residual ~0.1 % of pairs where the primary
//! (i=0, j=0) pair doesn't connect on the current weight vector.
//!
//! ## Combo enumeration
//!
//! Given K source candidates and K destination candidates sorted by
//! snap distance, we visit `(i, j)` in `(i+j)` ascending order then by
//! `i` ascending. That produces `(0,0), (0,1), (1,0), (0,2), (1,1),
//! (2,0), …` — first the second-best dst, then the second-best src,
//! then both, etc. The first combo that yields a finite distance
//! wins.
//!
//! A `MAX_FALLBACK_COMBOS` cap bounds tail latency on truly
//! disconnected pairs (or pairs where every combo within the K-best
//! window straddles two unreachable components after the role-mask
//! filter). 200 combos covers `(i+j) ≤ ~19` which empirically matches
//! /route's hit rate on Belgium random pairs.

use crate::profile_abi::Mode;

use super::query::CchQuery;
use super::state::{ModeData, ServerState};
use super::types::SnapRole;

/// Default cap on per-query (or per-cell) combo enumeration. Matches
/// /route's historical value, which keeps the 200-pair Belgium sweep
/// at zero `/route`-only-success regressions.
pub const DEFAULT_MAX_FALLBACK_COMBOS: usize = 200;

/// Build the `(i+j)`-ordered combo list for K-best fallback. Caps at
/// `max_combos` so callers don't have to repeat that truncation.
pub fn combo_order(k_src: usize, k_dst: usize, max_combos: usize) -> Vec<(usize, usize)> {
    let mut order = Vec::with_capacity((k_src * k_dst).min(max_combos));
    for sum in 0..(k_src + k_dst) {
        for i in 0..k_src {
            if let Some(j) = sum.checked_sub(i)
                && j < k_dst
            {
                order.push((i, j));
                if order.len() >= max_combos {
                    return order;
                }
            }
        }
    }
    order
}

/// Snap the K=1 primary with the #197 role filter — the cheap path
/// before any K=64 escalation. Returns the primary tuple
/// `(ebg_id, snapped_lon, snapped_lat, snap_distance_m)` plus the
/// rank, or `None` if no candidate exists.
///
/// If the geometrically-closest candidate has `orig_to_rank == u32::MAX`
/// (not in this mode's contracted graph — rare; usually the role_filter
/// already excludes such nodes), this function transparently escalates
/// to a K=64 fetch and returns the closest candidate that DOES have a
/// valid rank. So callers never have to re-snap to recover from that
/// edge case — they pay K=64 only on the (rare) miss.
///
/// Mirrors the lazy-snap pattern used inline in /route (#368), /table
/// (#370), and /trip (#374). Use this for any handler that uses the
/// primary on the happy path and only needs the K=64 list to feed the
/// combo fallback when the primary CCH query returns None.
pub fn snap_primary_role(
    state: &ServerState,
    mode_data: &ModeData,
    mode: Mode,
    lon: f64,
    lat: f64,
    role: SnapRole,
    snap_mask: Option<&[u64]>,
) -> Option<((u32, f64, f64, f64), u32)> {
    let role_filter = role.role_filter(mode_data);
    if let Some((orig_id, plon, plat, d)) =
        state
            .snap_index
            .snap_with_info_filtered_role(lon, lat, mode.0, snap_mask, role_filter)
    {
        let rank = mode_data.orig_to_rank[orig_id as usize];
        if rank != u32::MAX {
            return Some(((orig_id, plon, plat, d), rank));
        }
    }
    // K=1 closest had u32::MAX rank (role_filter and orig_to_rank
    // disagreed on this node — typically very rare). Escalate to K=64
    // and pick the first candidate with a valid rank. Preserves the
    // pre-#368 behaviour of always finding the closest contracted
    // neighbour when one exists within MAX_SNAP_DISTANCE_M.
    let kbest = snap_k_pair_role(state, mode_data, mode, lon, lat, role, snap_mask, 64);
    let rank = kbest.primary_rank()?;
    let primary = kbest.primary?;
    Some((primary, rank))
}

/// Snap K candidates per src/dst with the directional #197 role filter,
/// dropping any candidate whose rank is `u32::MAX` (not contracted in
/// this mode's CCH). Returns the rank-space candidate lists, the
/// snapped lon/lat of each primary, and `valid` flags.
///
/// `snap_mask` is the per-mode + exclude/avoid edge filter built by
/// the caller (typically `mode_data.mask` for the vanilla path).
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub fn snap_k_pair_role(
    state: &ServerState,
    mode_data: &ModeData,
    mode: Mode,
    lon: f64,
    lat: f64,
    role: SnapRole,
    snap_mask: Option<&[u64]>,
    k: usize,
) -> KBestSnap {
    let role_filter = role.role_filter(mode_data);
    let cands = state.snap_index.snap_k_with_info_filtered_role(
        lon,
        lat,
        mode.0,
        k,
        snap_mask,
        role_filter,
    );
    let ranks: Vec<u32> = cands
        .iter()
        .filter_map(|(orig_id, _, _, _)| {
            let r = mode_data.orig_to_rank[*orig_id as usize];
            if r == u32::MAX { None } else { Some(r) }
        })
        .collect();
    let primary = cands.first().copied();
    KBestSnap { primary, ranks }
}

/// Output of [`snap_k_pair_role`]: primary candidate's full info plus
/// the rank-space candidate list (after dropping invalid ranks).
pub struct KBestSnap {
    /// (ebg_id, snapped_lon, snapped_lat, snap_distance_m) of the
    /// nearest candidate, or `None` if no candidate snapped.
    pub primary: Option<(u32, f64, f64, f64)>,
    /// Sorted (by snap distance) rank-space candidates with valid
    /// `orig_to_rank` entries. May be empty even if `primary` is set,
    /// when the primary's rank is u32::MAX (e.g. node accessible per
    /// the filter but not contracted into the CCH).
    pub ranks: Vec<u32>,
}

impl KBestSnap {
    /// Primary rank, or `None` if no valid candidate.
    pub fn primary_rank(&self) -> Option<u32> {
        self.ranks.first().copied()
    }
}

/// Run a single P2P query with K-best (i+j)-combo fallback. Returns
/// the chosen `(src_rank, dst_rank, QueryResult)` on success, `None`
/// when every combo in the bounded enumeration yields no path.
///
/// This mirrors the inline logic in `/route`'s handler and replaces
/// the per-pair fallback that used to live inline in the Flight
/// endpoints, catchment, and other consumers.
pub fn p2p_with_kbest_fallback(
    query: &CchQuery,
    src_ranks: &[u32],
    dst_ranks: &[u32],
    max_combos: usize,
) -> Option<(u32, u32, super::query::QueryResult)> {
    let combos = combo_order(src_ranks.len(), dst_ranks.len(), max_combos);
    for (i, j) in combos {
        let s = src_ranks[i];
        let d = dst_ranks[j];
        if s == d {
            continue;
        }
        if let Some(r) = query.query(s, d) {
            return Some((s, d, r));
        }
    }
    None
}
