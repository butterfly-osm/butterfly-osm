//! Cross-region routing coordinator (#91 Phase 2).
//!
//! Given a precomputed [`OverlayCluster`] plus per-region road state,
//! [`solve_cross_region`] answers a P2P query whose source and target
//! sit in different regions:
//!
//! ```text
//!   src ──CCH──► src_border_i ─edge_i,j─► dst_border_j ──CCH──► tgt
//!         dist_src[i]         overlay[i,j]         dist_tgt[j]
//! ```
//!
//! The implementation consults the precomputed overlay matrix for the
//! middle term, and runs two CCH 1-to-N searches per query for the
//! access and egress legs.
//!
//! The runtime cost is dominated by the two CCH P2P loops (one per
//! border node on each side). For small overlays (≤100 borders/region)
//! this is well under 50 ms; for the BE+LU 14k-border case it scales
//! linearly. Future work (tracked in `91-overlay-design.md`) will swap
//! in a "pruned border set" — only run access/egress against borders
//! within a bbox of the source/target — to keep latency bounded.

use std::sync::Arc;

use super::overlay::OverlayCluster;
use super::query::CchQuery;
use super::state::ServerState;
use crate::profile_abi::Mode;

#[allow(clippy::needless_range_loop)] // index-based two-loop is cleaner here
/// Pure combinatorial kernel of [`solve_cross_region`]. Picks the best
/// `(i, j)` border pair given precomputed access / overlay / egress
/// distance arrays.
///
/// Exposed standalone so the synthetic 2-region oracle test can verify
/// the picker without spinning up a real `ServerState`.
///
/// Returns `(best_total, best_i, best_j)` or `None` if no combination
/// is reachable.
pub fn pick_best_border_pair(
    dist_src: &[u32],
    matrix_row_major: &[u32],
    n_dst: usize,
    dist_tgt: &[u32],
) -> Option<(u32, u32, u32)> {
    debug_assert_eq!(matrix_row_major.len(), dist_src.len() * n_dst);
    debug_assert_eq!(dist_tgt.len(), n_dst);
    let mut best_total = u32::MAX;
    let mut best_i = u32::MAX;
    let mut best_j = u32::MAX;
    for i in 0..dist_src.len() {
        let d_s = dist_src[i];
        if d_s == u32::MAX {
            continue;
        }
        let row_off = i * n_dst;
        for j in 0..n_dst {
            let m = matrix_row_major[row_off + j];
            if m == u32::MAX {
                continue;
            }
            let d_t = dist_tgt[j];
            if d_t == u32::MAX {
                continue;
            }
            let total = d_s.saturating_add(m).saturating_add(d_t);
            if total < best_total {
                best_total = total;
                best_i = i as u32;
                best_j = j as u32;
            }
        }
    }
    if best_total == u32::MAX {
        None
    } else {
        Some((best_total, best_i, best_j))
    }
}

/// One leg of a cross-region route. Each leg lives in one region's
/// CCH, so the caller can independently unpack it for geometry / step
/// reconstruction by passing the leg's `(src_rank, dst_rank)` to the
/// per-region path-reconstruction code.
#[derive(Debug, Clone)]
pub struct CrossLeg {
    /// Region id this leg lives in.
    pub region: String,
    /// Leg endpoint at the source side, given as a CCH rank in `region`.
    pub src_rank: u32,
    /// Leg endpoint at the destination side, given as a CCH rank in `region`.
    pub dst_rank: u32,
    /// Total cost of this leg (in mode units — deciseconds for time).
    /// `u32::MAX` if the leg is unreachable in this region's CCH.
    pub cost: u32,
}

/// Result of a cross-region P2P solve.
#[derive(Debug, Clone)]
pub struct CrossSolution {
    /// Total cost (access + overlay + egress) in mode units.
    pub total_cost: u32,
    /// Access leg in the source region.
    pub access: CrossLeg,
    /// Egress leg in the destination region.
    pub egress: CrossLeg,
    /// Index of the chosen border in the source region.
    pub src_border_idx: u32,
    /// Index of the chosen border in the destination region.
    pub dst_border_idx: u32,
    /// EBG node id of the chosen src-side border (in the source region's
    /// EBG space). The handler uses this to materialise the
    /// access-leg geometry tail.
    pub src_border_ebg: u32,
    /// EBG node id of the chosen dst-side border (in the destination
    /// region's EBG space).
    pub dst_border_ebg: u32,
}

/// Solve a cross-region P2P query. Returns `None` if no path exists
/// (typically because the regions share no border crossings, or every
/// reachable border combination is `u32::MAX`).
#[allow(clippy::too_many_arguments)]
pub fn solve_cross_region(
    src_state: &Arc<ServerState>,
    src_region: &str,
    src_rank: u32,
    dst_state: &Arc<ServerState>,
    dst_region: &str,
    dst_rank: u32,
    mode_name: &str,
    overlay: &OverlayCluster,
) -> Option<CrossSolution> {
    let matrix = overlay.matrix(src_region, dst_region, mode_name)?;
    let src_borders = overlay.region_borders(src_region);
    let dst_borders = overlay.region_borders(dst_region);

    if src_borders.is_empty() || dst_borders.is_empty() {
        return None;
    }
    debug_assert_eq!(matrix.len(), src_borders.len() * dst_borders.len());

    // Translate src borders → CCH ranks in src region.
    let src_mode_idx = *src_state.mode_lookup.get(mode_name)?;
    let src_mode_data = src_state.get_mode(Mode(src_mode_idx));
    let dst_mode_idx = *dst_state.mode_lookup.get(mode_name)?;
    let dst_mode_data = dst_state.get_mode(Mode(dst_mode_idx));

    let src_border_ranks: Vec<u32> = src_borders
        .iter()
        .map(|b| src_mode_data.orig_to_rank[b.ebg_node as usize])
        .collect();
    let dst_border_ranks: Vec<u32> = dst_borders
        .iter()
        .map(|b| dst_mode_data.orig_to_rank[b.ebg_node as usize])
        .collect();

    // ---- Access leg: src → every src border ------------------------
    let src_query = CchQuery::new(src_state, Mode(src_mode_idx));
    let dist_src: Vec<u32> = {
        // Filter ranks to the reachable subset to avoid O(MAX) entries.
        let dists = src_query.distances_one_to_many(src_rank, &src_border_ranks);
        dists.into_iter().map(|d| d.unwrap_or(u32::MAX)).collect()
    };

    // ---- Egress leg: every dst border → tgt ------------------------
    // We need d(border_j → tgt). The distance-only CCH supports
    // 1-to-N starting at a single source. We invert the loop direction:
    // run 1-to-N from each dst_border to tgt — but that's N searches.
    // Equivalent and cheaper: run a single 1-to-N from tgt to all dst
    // borders on the *reverse* graph, but we don't have a reverse-CCH
    // wrapper. So we accept N bidirectional CCH P2P queries here. For
    // typical overlay sizes (≤100 borders) that's <50ms total.
    let dst_query = CchQuery::new(dst_state, Mode(dst_mode_idx));
    let mut dist_tgt: Vec<u32> = vec![u32::MAX; dst_border_ranks.len()];
    for (j, &b_rank) in dst_border_ranks.iter().enumerate() {
        if b_rank == u32::MAX {
            continue;
        }
        if let Some(d) = dst_query.distance(b_rank, dst_rank) {
            dist_tgt[j] = d;
        }
    }

    // ---- Combine ---------------------------------------------------
    let n_dst = dst_borders.len();
    let (best_total, best_i, best_j) = pick_best_border_pair(&dist_src, matrix, n_dst, &dist_tgt)?;

    let i = best_i as usize;
    let j = best_j as usize;
    Some(CrossSolution {
        total_cost: best_total,
        access: CrossLeg {
            region: src_region.to_string(),
            src_rank,
            dst_rank: src_border_ranks[i],
            cost: dist_src[i],
        },
        egress: CrossLeg {
            region: dst_region.to_string(),
            src_rank: dst_border_ranks[j],
            dst_rank,
            cost: dist_tgt[j],
        },
        src_border_idx: best_i,
        dst_border_idx: best_j,
        src_border_ebg: src_borders[i].ebg_node,
        dst_border_ebg: dst_borders[j].ebg_node,
    })
}
