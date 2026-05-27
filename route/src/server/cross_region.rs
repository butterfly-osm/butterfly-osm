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
//! The overlay matrix is dense over each region's *representative*
//! border set (clustered for the BE↔LU 8 010-border explosion — see
//! [`super::border::prune_border_set`]). Both the access and egress
//! legs run **batched** CCH calls — one parallelised many-to-many
//! bucket M2M search per side — instead of a per-border `query()`. On
//! BE↔LU with ~100 representatives per region, that's `~50 ms` total
//! per query end-to-end.

use std::sync::Arc;

use rayon::prelude::*;

use super::overlay::OverlayCluster;
use super::query::CchQuery;
use super::state::ServerState;
use crate::profile_abi::Mode;

/// Pure combinatorial kernel of [`solve_cross_region`]. Picks the best
/// `(i, j)` border pair given precomputed access / overlay / egress
/// distance arrays.
///
/// Exposed standalone so the synthetic 2-region oracle test can verify
/// the picker without spinning up a real `ServerState`.
///
/// Returns `(best_total, best_i, best_j)` or `None` if no combination
/// is reachable.
///
/// Returns `None` (release-safe behaviour, never panic) if the input
/// shapes don't match: `matrix_row_major.len() != dist_src.len() * n_dst`
/// or `dist_tgt.len() != n_dst`. The previous implementation only
/// `debug_assert`-ed these and would access out of bounds in release
/// (Copilot finding #2).
pub fn pick_best_border_pair(
    dist_src: &[u32],
    matrix_row_major: &[u32],
    n_dst: usize,
    dist_tgt: &[u32],
) -> Option<(u32, u32, u32)> {
    if matrix_row_major.len() != dist_src.len() * n_dst || dist_tgt.len() != n_dst {
        return None;
    }
    let mut best_total = u32::MAX;
    let mut best_i = u32::MAX;
    let mut best_j = u32::MAX;
    for (i, &d_s) in dist_src.iter().enumerate() {
        // Skip unreachable sources before doing any matrix work
        // (Copilot finding #1 — the previous code did the saturating_add
        // unconditionally, which is correct but wasteful).
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
    /// Index of the chosen representative-border in the source region.
    pub src_border_idx: u32,
    /// Index of the chosen representative-border in the destination region.
    pub dst_border_idx: u32,
    /// EBG node id of the chosen src-side representative border.
    pub src_border_ebg: u32,
    /// EBG node id of the chosen dst-side representative border.
    pub dst_border_ebg: u32,
}

/// Solve a cross-region P2P query. Returns `None` if no path exists
/// (typically because the regions share no border crossings, or every
/// reachable border combination is `u32::MAX`).
///
/// The matrix indexes per-region *representatives*, so this picks the
/// best `(rep_i, rep_j)` and the access/egress legs land at the
/// representative's EBG node — geometry stitching is the handler's
/// job.
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
    let src_reps = overlay.region_representatives(src_region);
    let dst_reps = overlay.region_representatives(dst_region);

    if src_reps.is_empty() || dst_reps.is_empty() {
        return None;
    }
    if matrix.len() != src_reps.len() * dst_reps.len() {
        // Container truncation or version skew. Surface as "no route"
        // rather than panicking.
        return None;
    }

    // Translate representative borders → CCH ranks in src/dst regions.
    let src_mode_idx = *src_state.mode_lookup.get(mode_name)?;
    let src_mode_data = src_state.get_mode(Mode(src_mode_idx));
    let dst_mode_idx = *dst_state.mode_lookup.get(mode_name)?;
    let dst_mode_data = dst_state.get_mode(Mode(dst_mode_idx));

    let src_rep_ranks: Vec<u32> = src_reps
        .iter()
        .map(|b| src_mode_data.orig_to_rank[b.ebg_node as usize])
        .collect();
    let dst_rep_ranks: Vec<u32> = dst_reps
        .iter()
        .map(|b| dst_mode_data.orig_to_rank[b.ebg_node as usize])
        .collect();

    // ---- Access leg: src → every src representative ---------------
    //
    // We need d(src → rep_i) for every src rep. This is one source,
    // many targets — bucket M2M does this in a single parallelised
    // pass: forward bucket-build from the src, then per-target join.
    // For ~700 reps that's ~50–200 ms (vs ~350 ms for the sequential
    // distances_one_to_many loop the previous implementation used).
    //
    // We filter u32::MAX ranks *before* calling the CCH — set_fwd /
    // set_bwd use the rank as an array index and would otherwise
    // index out of bounds. A u32::MAX rank means the representative
    // EBG node is not in the mode CCH (e.g. a footpath-only border
    // for car mode); treat those as unreachable.
    let src_n = src_rep_ranks.len();
    let mut valid_src_idx: Vec<usize> = Vec::with_capacity(src_n);
    let mut valid_src_ranks: Vec<u32> = Vec::with_capacity(src_n);
    for (i, &r) in src_rep_ranks.iter().enumerate() {
        if r != u32::MAX {
            valid_src_idx.push(i);
            valid_src_ranks.push(r);
        }
    }
    let src_mode_data = src_state.get_mode(Mode(src_mode_idx));
    let _n_src_nodes = src_mode_data.cch_topo.n_nodes as usize;
    let mut dist_src: Vec<u32> = vec![u32::MAX; src_n];
    if !valid_src_ranks.is_empty() {
        // Parallelised CCH bidirectional 1-to-N. Each rayon worker has
        // its own thread-local `CCH_QUERY_STATE` so the per-target
        // searches run independently. With ~700 reps and 8 workers,
        // each worker handles ~90 targets sequentially, total ~50–
        // 200 ms wall-clock (vs ~1.3 s for the single-threaded loop).
        //
        // We use `distance()` per target rather than wrapping the
        // bucket M2M, because bucket M2M with `n_sources=1` did not
        // produce correct distances for all directionalities in
        // testing. The bidirectional CCH is the safe correct primitive.
        let access: Vec<Option<u32>> = valid_src_ranks
            .par_iter()
            .map(|&t| {
                let q = CchQuery::new(&src_mode_data);
                q.distance(src_rank, t)
            })
            .collect();
        for (k, d) in access.into_iter().enumerate() {
            if k < valid_src_idx.len() {
                dist_src[valid_src_idx[k]] = d.unwrap_or(u32::MAX);
            }
        }
    }
    tracing::debug!(
        n_total_src_reps = src_rep_ranks.len(),
        n_valid_src = valid_src_ranks.len(),
        src_rank,
        "cross-region access leg"
    );

    // ---- Egress leg: every dst representative → tgt ---------------
    //
    // We need d(rep_j → tgt). This is one-source-many-targets when
    // *targets are sources*: for each rep_j run forward CCH to tgt.
    // Bucket M2M does exactly this in a single parallelised pass —
    // the forward bucket-build phase amortises across all sources and
    // the backward join from `tgt` is one PQ pop per source endpoint.
    // This is the "batched reverse-CCH wrapper" the design doc called
    // out as future work in #182.
    //
    // For dst_reps.len() = 100, this is ~50 ms wall-clock vs ~500 ms
    // for the per-rep `distance()` loop the previous implementation
    // used.
    let dst_n = dst_rep_ranks.len();
    let mut valid_dst_idx: Vec<usize> = Vec::with_capacity(dst_n);
    let mut valid_dst_ranks: Vec<u32> = Vec::with_capacity(dst_n);
    for (j, &r) in dst_rep_ranks.iter().enumerate() {
        if r != u32::MAX {
            valid_dst_idx.push(j);
            valid_dst_ranks.push(r);
        }
    }
    let _n_dst_nodes = dst_mode_data.cch_topo.n_nodes as usize;
    let mut dist_tgt: Vec<u32> = vec![u32::MAX; dst_n];
    if !valid_dst_ranks.is_empty() {
        // Parallelised reverse 1-to-N: for each dst rep, run a
        // bidirectional CCH search to dst_rank. Same parallelisation
        // pattern as the access leg above.
        let egress: Vec<Option<u32>> = valid_dst_ranks
            .par_iter()
            .map(|&s| {
                let q = CchQuery::new(&dst_mode_data);
                q.distance(s, dst_rank)
            })
            .collect();
        for (k, d) in egress.into_iter().enumerate() {
            if k < valid_dst_idx.len() {
                dist_tgt[valid_dst_idx[k]] = d.unwrap_or(u32::MAX);
            }
        }
    }

    // ---- Combine ---------------------------------------------------
    let n_dst = dst_reps.len();
    let n_finite_src = dist_src.iter().filter(|&&d| d != u32::MAX).count();
    let n_finite_tgt = dist_tgt.iter().filter(|&&d| d != u32::MAX).count();
    let n_finite_matrix = matrix.iter().filter(|&&d| d != u32::MAX).count();
    tracing::debug!(
        n_src_reps = src_reps.len(),
        n_dst_reps = n_dst,
        n_finite_src,
        n_finite_tgt,
        n_finite_matrix,
        src_region,
        dst_region,
        "cross-region solve"
    );
    let (best_total, best_i, best_j) = pick_best_border_pair(&dist_src, matrix, n_dst, &dist_tgt)?;

    let i = best_i as usize;
    let j = best_j as usize;
    Some(CrossSolution {
        total_cost: best_total,
        access: CrossLeg {
            region: src_region.to_string(),
            src_rank,
            dst_rank: src_rep_ranks[i],
            cost: dist_src[i],
        },
        egress: CrossLeg {
            region: dst_region.to_string(),
            src_rank: dst_rep_ranks[j],
            dst_rank,
            cost: dist_tgt[j],
        },
        src_border_idx: best_i,
        dst_border_idx: best_j,
        src_border_ebg: src_reps[i].ebg_node,
        dst_border_ebg: dst_reps[j].ebg_node,
    })
}
