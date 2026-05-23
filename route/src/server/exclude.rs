//! Exclude feature: re-customize CCH weights to block toll/ferry/motorway edges.
//!
//! At startup, builds per-EBG-edge exclude flags from way attributes.
//! At query time, selects or computes excluded weight sets with cached results.
//!
//! The recustomization algorithm:
//! 1. Clone existing CCH weights
//! 2. Set base (non-shortcut) edges whose original EBG edge is excluded to u32::MAX
//! 3. Recompute all shortcut weights bottom-up (shortcuts through excluded edges get u32::MAX)
//! 4. Run triangle relaxation to find alternative paths through non-excluded edges

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

use rayon::prelude::*;

use crate::formats::way_attrs;
use crate::formats::{CchTopo, CchWeights, EbgNodes};
use crate::matrix::bucket_ch::{DownAdjFlat, DownReverseAdjFlat, UpAdjFlat};
use crate::profile_abi::class_bits;

/// Exclude flags (bitmask, per EBG edge)
pub const EXCLUDE_TOLL: u8 = 1; // bit 0
pub const EXCLUDE_FERRY: u8 = 2; // bit 1
pub const EXCLUDE_MOTORWAY: u8 = 4; // bit 2

/// Cached exclude weight set (time + distance metrics)
pub struct ExcludeWeights {
    pub time_weights: CchWeights,
    pub dist_weights: CchWeights,
    pub time_up_flat: UpAdjFlat,
    pub time_down_flat: DownReverseAdjFlat,
    pub time_down_fwd_flat: DownAdjFlat,
    pub dist_up_flat: UpAdjFlat,
    pub dist_down_flat: DownReverseAdjFlat,
    pub dist_down_fwd_flat: DownAdjFlat,
}

/// Parse exclude parameter string into bitmask.
/// Accepts comma-separated tokens: toll, ferry, motorway.
/// Returns 0 for empty/whitespace-only input.
pub fn parse_exclude(s: &str) -> Result<u8, String> {
    let mut mask = 0u8;
    for token in s.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        match token.to_lowercase().as_str() {
            "toll" => mask |= EXCLUDE_TOLL,
            "ferry" => mask |= EXCLUDE_FERRY,
            "motorway" => mask |= EXCLUDE_MOTORWAY,
            other => {
                return Err(format!(
                    "Unknown exclude token: '{}'. Valid: toll, ferry, motorway",
                    other
                ));
            }
        }
    }
    Ok(mask)
}

/// Parse an optional exclude parameter into `Option<u8>`.
/// Returns `None` if the parameter is absent, empty, or all-whitespace.
pub fn parse_exclude_option(exclude: &Option<String>) -> Result<Option<u8>, String> {
    match exclude {
        Some(s) => {
            let mask = parse_exclude(s)?;
            if mask == 0 { Ok(None) } else { Ok(Some(mask)) }
        }
        None => Ok(None),
    }
}

/// Build per-EBG-edge exclude flags from way attributes.
/// Returns Vec<u8> indexed by original EBG edge ID.
///
/// Each byte encodes which exclude categories apply:
/// - bit 0: toll road
/// - bit 1: ferry
/// - bit 2: motorway (highway_class 1 or 2)
pub fn build_edge_exclude_flags(
    ebg_nodes: &EbgNodes,
    way_attrs_path: &std::path::Path,
) -> anyhow::Result<Vec<u8>> {
    if !way_attrs_path.exists() {
        tracing::warn!(
            path = %way_attrs_path.display(),
            "way_attrs not found, exclude feature disabled"
        );
        return Ok(vec![0u8; ebg_nodes.n_nodes as usize]);
    }

    let attrs = way_attrs::read_all(way_attrs_path)?;
    build_edge_exclude_flags_from_attrs(ebg_nodes, &attrs)
}

/// Same as `build_edge_exclude_flags` but takes pre-loaded attrs (e.g.
/// decoded from a mmap-backed `mode/<mode>/way_attrs` section).
pub fn build_edge_exclude_flags_from_attrs(
    ebg_nodes: &EbgNodes,
    attrs: &[way_attrs::WayAttr],
) -> anyhow::Result<Vec<u8>> {
    // Build lookup: way_id (lower 32 bits) → exclude flags
    let mut way_flags: rustc_hash::FxHashMap<u32, u8> = rustc_hash::FxHashMap::default();
    for attr in attrs {
        let way_id_32 = (attr.way_id & 0xFFFF_FFFF) as u32;
        let mut flags = 0u8;

        if (attr.output.class_bits & (1 << class_bits::TOLL)) != 0 {
            flags |= EXCLUDE_TOLL;
        }
        if (attr.output.class_bits & (1 << class_bits::FERRY)) != 0 {
            flags |= EXCLUDE_FERRY;
        }
        // Motorway = highway_class 1 (motorway) or 2 (motorway_link)
        if attr.output.highway_class >= 1 && attr.output.highway_class <= 2 {
            flags |= EXCLUDE_MOTORWAY;
        }

        if flags != 0 {
            way_flags.insert(way_id_32, flags);
        }
    }

    // Build per-edge flags from primary_way lookup
    let edge_flags: Vec<u8> = ebg_nodes
        .nodes
        .iter()
        .map(|node| way_flags.get(&node.primary_way).copied().unwrap_or(0))
        .collect();

    let toll_count = edge_flags
        .iter()
        .filter(|&&f| f & EXCLUDE_TOLL != 0)
        .count();
    let ferry_count = edge_flags
        .iter()
        .filter(|&&f| f & EXCLUDE_FERRY != 0)
        .count();
    let motorway_count = edge_flags
        .iter()
        .filter(|&&f| f & EXCLUDE_MOTORWAY != 0)
        .count();
    tracing::info!(
        toll = toll_count,
        ferry = ferry_count,
        motorway = motorway_count,
        total_edges = edge_flags.len(),
        "built edge exclude flags"
    );

    Ok(edge_flags)
}

/// Build combined snap mask that excludes edges matching the exclude pattern.
/// Returns a new mask where excluded edges are cleared (set to 0).
pub fn build_exclude_mask(
    base_mask: &[u64],
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
) -> Vec<u64> {
    base_mask
        .iter()
        .enumerate()
        .map(|(word_idx, &word)| {
            let mut filtered = word;
            for bit in 0..64 {
                let edge_id = word_idx * 64 + bit;
                if edge_id < edge_exclude_flags.len()
                    && (edge_exclude_flags[edge_id] & exclude_mask) != 0
                {
                    filtered &= !(1u64 << bit);
                }
            }
            filtered
        })
        .collect()
}

/// Re-customize CCH weights with excluded edges set to u32::MAX.
///
/// Algorithm:
/// 1. For base (non-shortcut) CCH edges: if the target's original EBG edge is excluded,
///    set weight to u32::MAX. Otherwise keep existing weight.
/// 2. For shortcut edges: recompute as w(u,m) + w(m,v) using modified weights.
/// 3. Process bottom-up by rank (ascending) for correct dependency order.
/// 4. Run triangle relaxation to find alternative paths where shortcuts were blocked.
pub fn recustomize_weights(
    topo: &CchTopo,
    base_weights: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> CchWeights {
    let n_nodes = topo.n_nodes as usize;

    let is_excluded = |orig_id: usize| -> bool {
        orig_id < edge_exclude_flags.len() && (edge_exclude_flags[orig_id] & exclude_mask) != 0
    };

    // Build sorted down indices for correct dependency order
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end {
                return Vec::new();
            }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();

    let mut up_weights = vec![u32::MAX; topo.up_targets.len()];
    let mut down_weights = vec![u32::MAX; topo.down_targets.len()];

    // Bottom-up pass
    for rank in 0..n_nodes {
        let u = rank;

        // DOWN edges (sorted by target rank)
        for &i in &sorted_down_indices[u] {
            let v = topo.down_targets[i] as usize;
            if !topo.down_is_shortcut.bit(i) {
                // Base edge: check target for exclusion
                let v_filtered = topo.rank_to_filtered[v] as usize;
                let v_orig = filtered_to_original[v_filtered] as usize;

                if is_excluded(v_orig) {
                    down_weights[i] = u32::MAX;
                } else {
                    down_weights[i] = base_weights.down[i];
                }
            } else {
                // Shortcut: recompute from components
                let m = topo.down_middle[i] as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                down_weights[i] = w_um.saturating_add(w_mv);
            }
        }

        // UP edges
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i] as usize;
            if !topo.up_is_shortcut.bit(i) {
                let v_filtered = topo.rank_to_filtered[v] as usize;
                let v_orig = filtered_to_original[v_filtered] as usize;

                if is_excluded(v_orig) {
                    up_weights[i] = u32::MAX;
                } else {
                    up_weights[i] = base_weights.up[i];
                }
            } else {
                let m = topo.up_middle[i] as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                up_weights[i] = w_um.saturating_add(w_mv);
            }
        }
    }

    // Triangle relaxation to find alternative paths
    let rev_down = build_reverse_down_adj(topo);
    triangle_relax(topo, &mut up_weights, &mut down_weights, &rev_down);

    CchWeights {
        up: up_weights.into(),
        down: down_weights.into(),
        up_middle: base_weights.up_middle.clone(),
        down_middle: base_weights.down_middle.clone(),
    }
}

/// Like `recustomize_weights` but uses sparse triangle relaxation.
///
/// After the bottom-up pass, identifies which edges changed vs base weights,
/// then only processes triangle relaxation on dirty nodes (nodes with at least
/// one changed incident edge). Subsequent passes only process newly-dirtied
/// nodes, making later passes much faster.
///
/// For avoid polygons affecting ~1% of edges, this is ~10x faster than full
/// triangle relaxation while producing identical results.
fn recustomize_weights_sparse_triangle(
    topo: &CchTopo,
    base_weights: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> CchWeights {
    let n_nodes = topo.n_nodes as usize;

    let is_excluded = |orig_id: usize| -> bool {
        orig_id < edge_exclude_flags.len() && (edge_exclude_flags[orig_id] & exclude_mask) != 0
    };

    // Build sorted down indices for correct dependency order
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end {
                return Vec::new();
            }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();

    let mut up_weights = vec![u32::MAX; topo.up_targets.len()];
    let mut down_weights = vec![u32::MAX; topo.down_targets.len()];
    let mut changed_up_edges: Vec<ChangedEdge> = Vec::new();
    let mut changed_down_edges: Vec<ChangedEdge> = Vec::new();

    // Bottom-up pass — collect changed edges as we go so the triangle
    // relax pass 1 can iterate only those instead of sweeping every node.
    for rank in 0..n_nodes {
        let u = rank;

        // DOWN edges (sorted by target rank)
        for &i in &sorted_down_indices[u] {
            let v = topo.down_targets[i] as usize;
            let new_weight = if !topo.down_is_shortcut.bit(i) {
                let v_filtered = topo.rank_to_filtered[v] as usize;
                let v_orig = filtered_to_original[v_filtered] as usize;
                if is_excluded(v_orig) {
                    u32::MAX
                } else {
                    base_weights.down[i]
                }
            } else {
                let m = topo.down_middle[i] as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                w_um.saturating_add(w_mv)
            };
            down_weights[i] = new_weight;
            if new_weight != base_weights.down[i] {
                changed_down_edges.push(ChangedEdge {
                    source: u as u32,
                    target: v as u32,
                });
            }
        }

        // UP edges
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i] as usize;
            let new_weight = if !topo.up_is_shortcut.bit(i) {
                let v_filtered = topo.rank_to_filtered[v] as usize;
                let v_orig = filtered_to_original[v_filtered] as usize;
                if is_excluded(v_orig) {
                    u32::MAX
                } else {
                    base_weights.up[i]
                }
            } else {
                let m = topo.up_middle[i] as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                w_um.saturating_add(w_mv)
            };
            up_weights[i] = new_weight;
            if new_weight != base_weights.up[i] {
                changed_up_edges.push(ChangedEdge {
                    source: u as u32,
                    target: v as u32,
                });
            }
        }
    }

    tracing::debug!(
        changed_up_edges = changed_up_edges.len(),
        changed_down_edges = changed_down_edges.len(),
        "sparse: bottom-up done, changed-edge set collected"
    );

    // Build reverse DOWN adjacency for triangle relaxation
    let rev_down = build_reverse_down_adj(topo);

    // Sparse triangle relaxation: only process dirty nodes
    sparse_triangle_relax(
        topo,
        &mut up_weights,
        &mut down_weights,
        &rev_down,
        &changed_up_edges,
        &changed_down_edges,
    );

    CchWeights {
        up: up_weights.into(),
        down: down_weights.into(),
        up_middle: base_weights.up_middle.clone(),
        down_middle: base_weights.down_middle.clone(),
    }
}

/// A weight that differs from base after the bottom-up customization
/// pass. Collected during bottom-up so triangle relaxation can iterate
/// only those edges instead of sweeping every node.
#[derive(Clone, Copy, Debug)]
struct ChangedEdge {
    source: u32,
    target: u32,
}

#[inline]
fn push_dirty_once(dirty: &[AtomicBool], out: &mut Vec<usize>, node: usize) {
    if !dirty[node].swap(true, Ordering::Relaxed) {
        out.push(node);
    }
}

#[inline]
fn mark_potential_middles(
    topo: &CchTopo,
    dirty: &[AtomicBool],
    out: &mut Vec<usize>,
    x: usize,
    y: usize,
) {
    let down_start = topo.down_offsets[x] as usize;
    let down_end = topo.down_offsets[x + 1] as usize;
    for &m_u32 in &topo.down_targets[down_start..down_end] {
        let m = m_u32 as usize;
        if m == y {
            continue;
        }
        if find_edge_index(m, y, &topo.up_offsets, &topo.up_targets).is_some() {
            push_dirty_once(dirty, out, m);
        }
    }
}

/// Sparse triangle relaxation. Pass 1's dirty set is bounded by the
/// changed-edge list collected during bottom-up:
///   (a) For each changed UP edge x→y: mark the SOURCE x dirty (the
///       only m whose next-pass iteration reads w_xy as w_my).
///   (b) For each changed DOWN edge x→y: mark the TARGET y dirty (the
///       only m whose iteration reads w_xy as w_xm).
///   (c) For each changed edge (x, y): enumerate potential middles m
///       (DOWN neighbours of x that have a UP edge to y) and mark
///       dirty[m]. This catches the "shortcut x→y went INF and the
///       triangle that rescues it uses an m_alt with unchanged
///       incident edges" case (#238).
///
/// Subsequent passes use **one-sided** propagation. An improved UP
/// edge x→y is consumed only by triangles centered at x. An improved
/// DOWN edge x→y is consumed only by triangles centered at y. Marking
/// both endpoints (as the earlier version did) doubles dirty-set churn
/// for no correctness benefit.
fn sparse_triangle_relax(
    topo: &CchTopo,
    up_weights: &mut Vec<u32>,
    down_weights: &mut Vec<u32>,
    rev_down: &ReverseDownAdj,
    changed_up_edges: &[ChangedEdge],
    changed_down_edges: &[ChangedEdge],
) {
    let n_nodes = topo.n_nodes as usize;

    let atomic_up: Vec<AtomicU32> = up_weights.drain(..).map(AtomicU32::new).collect();
    let atomic_down: Vec<AtomicU32> = down_weights.drain(..).map(AtomicU32::new).collect();

    let dirty: Vec<AtomicBool> = (0..n_nodes).map(|_| AtomicBool::new(false)).collect();

    // Build the initial dirty set as an actual Vec (not a bitvector
    // scan) so subsequent passes don't pay O(n_nodes) to enumerate it.
    let mut dirty_nodes: Vec<usize> = Vec::new();

    // (a) Per-edge apex marking. For changed UP edges, that's the source;
    // for changed DOWN edges, that's the target.
    let from_up: Vec<usize> = changed_up_edges
        .par_iter()
        .fold(Vec::new, |mut local, edge| {
            push_dirty_once(&dirty, &mut local, edge.source as usize);
            local
        })
        .reduce(Vec::new, |mut a, mut b| {
            a.append(&mut b);
            a
        });
    let from_down: Vec<usize> = changed_down_edges
        .par_iter()
        .fold(Vec::new, |mut local, edge| {
            push_dirty_once(&dirty, &mut local, edge.target as usize);
            local
        })
        .reduce(Vec::new, |mut a, mut b| {
            a.append(&mut b);
            a
        });
    dirty_nodes.extend(from_up);
    dirty_nodes.extend(from_down);

    // (b) Potential middles per changed edge. Bounded by O(changed × deg).
    let middles_up: Vec<usize> = changed_up_edges
        .par_iter()
        .fold(Vec::new, |mut local, edge| {
            mark_potential_middles(
                topo,
                &dirty,
                &mut local,
                edge.source as usize,
                edge.target as usize,
            );
            local
        })
        .reduce(Vec::new, |mut a, mut b| {
            a.append(&mut b);
            a
        });
    let middles_down: Vec<usize> = changed_down_edges
        .par_iter()
        .fold(Vec::new, |mut local, edge| {
            mark_potential_middles(
                topo,
                &dirty,
                &mut local,
                edge.source as usize,
                edge.target as usize,
            );
            local
        })
        .reduce(Vec::new, |mut a, mut b| {
            a.append(&mut b);
            a
        });
    dirty_nodes.extend(middles_up);
    dirty_nodes.extend(middles_down);

    tracing::debug!(
        changed_up_edges = changed_up_edges.len(),
        changed_down_edges = changed_down_edges.len(),
        dirty_nodes = dirty_nodes.len(),
        total_nodes = n_nodes,
        "sparse triangle relax: initial dirty set"
    );

    let mut pass = 0u32;
    while !dirty_nodes.is_empty() {
        pass += 1;

        // Clear dirty flags so the next pass's per-edge marks can
        // detect "already enqueued for this pass" via swap.
        dirty_nodes
            .par_iter()
            .for_each(|&m| dirty[m].store(false, Ordering::Relaxed));

        let pass_updates = AtomicU64::new(0);

        let next_dirty_nodes: Vec<usize> = dirty_nodes
            .par_iter()
            .fold(Vec::new, |mut next_dirty, &m| {
                let rev_start = rev_down.offsets[m] as usize;
                let rev_end = rev_down.offsets[m + 1] as usize;

                for i_rev in rev_start..rev_end {
                    let x = rev_down.sources[i_rev] as usize;
                    let edge_idx_xm = rev_down.edge_idx[i_rev];
                    let w_xm = atomic_down[edge_idx_xm].load(Ordering::Relaxed);

                    if w_xm == u32::MAX {
                        continue;
                    }

                    let up_start = topo.up_offsets[m] as usize;
                    let up_end = topo.up_offsets[m + 1] as usize;

                    for i_my in up_start..up_end {
                        let y = topo.up_targets[i_my] as usize;
                        if y == x {
                            continue;
                        }

                        let w_my = atomic_up[i_my].load(Ordering::Relaxed);
                        if w_my == u32::MAX {
                            continue;
                        }

                        let new_weight = w_xm.saturating_add(w_my);

                        if y > x {
                            if let Some(idx) =
                                find_edge_index(x, y, &topo.up_offsets, &topo.up_targets)
                            {
                                let old = atomic_up[idx].fetch_min(new_weight, Ordering::Relaxed);
                                if new_weight < old {
                                    pass_updates.fetch_add(1, Ordering::Relaxed);
                                    // Improved UP x→y is consumed only by triangles
                                    // centered at x. One-sided propagation.
                                    push_dirty_once(&dirty, &mut next_dirty, x);
                                }
                            }
                        } else if let Some(idx) =
                            find_edge_index(x, y, &topo.down_offsets, &topo.down_targets)
                        {
                            let old = atomic_down[idx].fetch_min(new_weight, Ordering::Relaxed);
                            if new_weight < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                                // Improved DOWN x→y is consumed only by triangles
                                // centered at y. One-sided propagation.
                                push_dirty_once(&dirty, &mut next_dirty, y);
                            }
                        }
                    }
                }
                next_dirty
            })
            .reduce(Vec::new, |mut a, mut b| {
                a.append(&mut b);
                a
            });

        let pu = pass_updates.into_inner();
        tracing::debug!(
            pass,
            updates = pu,
            dirty_nodes = dirty_nodes.len(),
            next_dirty_nodes = next_dirty_nodes.len(),
            "sparse triangle relaxation"
        );
        if pu == 0 {
            break;
        }
        if pass >= 50 {
            tracing::warn!(
                pass,
                updates = pu,
                "sparse triangle relaxation hit 50-pass cap without converging — \
                 weights may still be improvable; falling through with current state"
            );
            break;
        }

        dirty_nodes = next_dirty_nodes;
    }

    *up_weights = atomic_up.into_iter().map(AtomicU32::into_inner).collect();
    *down_weights = atomic_down.into_iter().map(AtomicU32::into_inner).collect();
}

/// Compute time-only exclude weights (for P2P route queries).
///
/// Skips distance recustomization and flat adjacency builds.
/// Uses sparse triangle relaxation (~10x faster than full) for correct routing.
pub fn compute_exclude_weights_time_only(
    topo: &CchTopo,
    base_time: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> CchWeights {
    let start = std::time::Instant::now();

    let time_weights = recustomize_weights_sparse_triangle(
        topo,
        base_time,
        edge_exclude_flags,
        exclude_mask,
        filtered_to_original,
    );

    tracing::info!(
        exclude_mask,
        elapsed_ms = start.elapsed().as_millis(),
        "computed exclude weights (time-only, sparse triangle)"
    );

    time_weights
}

/// Compute full exclude weight set (time + distance) with flat adjacencies.
pub fn compute_exclude_weights(
    topo: &CchTopo,
    base_time: &CchWeights,
    base_dist: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> ExcludeWeights {
    let start = std::time::Instant::now();

    // Re-customize time and distance weights in parallel
    let (time_weights, dist_weights) = rayon::join(
        || {
            recustomize_weights(
                topo,
                base_time,
                edge_exclude_flags,
                exclude_mask,
                filtered_to_original,
            )
        },
        || {
            recustomize_weights(
                topo,
                base_dist,
                edge_exclude_flags,
                exclude_mask,
                filtered_to_original,
            )
        },
    );

    // Build flat adjacencies for matrix/isochrone
    let (time_up_flat, time_down_flat) = rayon::join(
        || UpAdjFlat::build(topo, &time_weights),
        || DownReverseAdjFlat::build(topo, &time_weights),
    );
    let time_down_fwd_flat = DownAdjFlat::build(topo, &time_weights);
    let (dist_up_flat, dist_down_flat) = rayon::join(
        || UpAdjFlat::build(topo, &dist_weights),
        || DownReverseAdjFlat::build(topo, &dist_weights),
    );
    let dist_down_fwd_flat = DownAdjFlat::build(topo, &dist_weights);

    tracing::info!(
        exclude_mask,
        elapsed_ms = start.elapsed().as_millis(),
        "computed exclude weights"
    );

    ExcludeWeights {
        time_weights,
        dist_weights,
        time_up_flat,
        time_down_flat,
        time_down_fwd_flat,
        dist_up_flat,
        dist_down_flat,
        dist_down_fwd_flat,
    }
}

// --- Internal helpers ---

#[inline]
fn find_edge_weight(u: usize, v: usize, offsets: &[u64], targets: &[u32], weights: &[u32]) -> u32 {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    if start >= end {
        return u32::MAX;
    }
    match targets[start..end].binary_search(&(v as u32)) {
        Ok(idx) => weights[start + idx],
        Err(_) => u32::MAX,
    }
}

#[inline]
fn find_edge_index(u: usize, v: usize, offsets: &[u64], targets: &[u32]) -> Option<usize> {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    if start >= end {
        return None;
    }
    targets[start..end]
        .binary_search(&(v as u32))
        .ok()
        .map(|idx| start + idx)
}

struct ReverseDownAdj {
    offsets: Vec<u64>,
    sources: Vec<u32>,
    edge_idx: Vec<usize>,
}

fn build_reverse_down_adj(topo: &CchTopo) -> ReverseDownAdj {
    let n_nodes = topo.n_nodes as usize;

    let mut counts = vec![0u64; n_nodes];
    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            counts[topo.down_targets[i] as usize] += 1;
        }
    }

    let mut offsets = vec![0u64; n_nodes + 1];
    for m in 0..n_nodes {
        offsets[m + 1] = offsets[m] + counts[m];
    }

    let total = offsets[n_nodes] as usize;
    let mut sources = vec![0u32; total];
    let mut edge_idx = vec![0usize; total];
    let mut insert = vec![0u64; n_nodes];

    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let m = topo.down_targets[i] as usize;
            let pos = (offsets[m] + insert[m]) as usize;
            sources[pos] = u as u32;
            edge_idx[pos] = i;
            insert[m] += 1;
        }
    }

    ReverseDownAdj {
        offsets,
        sources,
        edge_idx,
    }
}

/// Triangle relaxation: find shorter paths through alternative intermediate nodes.
/// Iterates until convergence (no more weight decreases).
fn triangle_relax(
    topo: &CchTopo,
    up_weights: &mut Vec<u32>,
    down_weights: &mut Vec<u32>,
    rev_down: &ReverseDownAdj,
) {
    let n_nodes = topo.n_nodes as usize;

    // Convert to atomic arrays for lock-free parallel relaxation
    let atomic_up: Vec<AtomicU32> = up_weights.drain(..).map(AtomicU32::new).collect();
    let atomic_down: Vec<AtomicU32> = down_weights.drain(..).map(AtomicU32::new).collect();

    let mut pass = 0u32;
    loop {
        pass += 1;
        let pass_updates = AtomicU64::new(0);

        (0..n_nodes).into_par_iter().for_each(|m| {
            let rev_start = rev_down.offsets[m] as usize;
            let rev_end = rev_down.offsets[m + 1] as usize;

            for i_rev in rev_start..rev_end {
                let x = rev_down.sources[i_rev] as usize;
                let edge_idx_xm = rev_down.edge_idx[i_rev];
                let w_xm = atomic_down[edge_idx_xm].load(Ordering::Relaxed);

                if w_xm == u32::MAX {
                    continue;
                }

                let up_start = topo.up_offsets[m] as usize;
                let up_end = topo.up_offsets[m + 1] as usize;

                for i_my in up_start..up_end {
                    let y = topo.up_targets[i_my] as usize;
                    if y == x {
                        continue;
                    }

                    let w_my = atomic_up[i_my].load(Ordering::Relaxed);
                    if w_my == u32::MAX {
                        continue;
                    }

                    let new_weight = w_xm.saturating_add(w_my);

                    if y > x {
                        // UP edge from x to y
                        if let Some(idx) = find_edge_index(x, y, &topo.up_offsets, &topo.up_targets)
                        {
                            let old = atomic_up[idx].fetch_min(new_weight, Ordering::Relaxed);
                            if new_weight < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        // DOWN edge from x to y
                        if let Some(idx) =
                            find_edge_index(x, y, &topo.down_offsets, &topo.down_targets)
                        {
                            let old = atomic_down[idx].fetch_min(new_weight, Ordering::Relaxed);
                            if new_weight < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        });

        let pu = pass_updates.into_inner();
        tracing::debug!(pass, updates = pu, "exclude triangle relaxation");
        if pu == 0 {
            break;
        }
        if pass >= 50 {
            tracing::warn!(
                pass,
                updates = pu,
                "full triangle relaxation hit 50-pass cap without converging — \
                 weights may still be improvable; falling through with current state"
            );
            break;
        }
    }

    *up_weights = atomic_up.into_iter().map(AtomicU32::into_inner).collect();
    *down_weights = atomic_down.into_iter().map(AtomicU32::into_inner).collect();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_exclude_single() {
        assert_eq!(parse_exclude("toll").unwrap(), EXCLUDE_TOLL);
        assert_eq!(parse_exclude("ferry").unwrap(), EXCLUDE_FERRY);
        assert_eq!(parse_exclude("motorway").unwrap(), EXCLUDE_MOTORWAY);
    }

    #[test]
    fn test_parse_exclude_multiple() {
        let mask = parse_exclude("toll,ferry").unwrap();
        assert_eq!(mask, EXCLUDE_TOLL | EXCLUDE_FERRY);
    }

    #[test]
    fn test_parse_exclude_all() {
        let mask = parse_exclude("toll,ferry,motorway").unwrap();
        assert_eq!(mask, EXCLUDE_TOLL | EXCLUDE_FERRY | EXCLUDE_MOTORWAY);
    }

    #[test]
    fn test_parse_exclude_case_insensitive() {
        assert_eq!(parse_exclude("Toll").unwrap(), EXCLUDE_TOLL);
        assert_eq!(parse_exclude("MOTORWAY").unwrap(), EXCLUDE_MOTORWAY);
    }

    #[test]
    fn test_parse_exclude_whitespace() {
        let mask = parse_exclude("toll , ferry").unwrap();
        assert_eq!(mask, EXCLUDE_TOLL | EXCLUDE_FERRY);
    }

    #[test]
    fn test_parse_exclude_invalid_token() {
        let err = parse_exclude("toll,highway").unwrap_err();
        assert!(err.contains("highway"));
    }

    #[test]
    fn test_parse_exclude_empty() {
        // Empty string returns 0 (no exclude), callers treat 0 as None
        assert_eq!(parse_exclude("").unwrap(), 0);
        assert_eq!(parse_exclude("  ").unwrap(), 0);
        assert_eq!(parse_exclude(",").unwrap(), 0);
        assert_eq!(parse_exclude(" , , ").unwrap(), 0);
    }

    #[test]
    fn test_parse_exclude_dedup() {
        // Duplicate tokens should just OR the same bits
        let mask = parse_exclude("toll,toll").unwrap();
        assert_eq!(mask, EXCLUDE_TOLL);
    }

    #[test]
    fn test_build_exclude_mask_clears_bits() {
        let base_mask: Vec<u64> = vec![0xFFFF_FFFF_FFFF_FFFF]; // all edges accessible
        let edge_flags = vec![
            0u8,
            EXCLUDE_TOLL,
            0,
            EXCLUDE_FERRY,
            0,
            0,
            0,
            0, // edges 0-7
            0,
            0,
            EXCLUDE_MOTORWAY,
            0,
            0,
            0,
            0,
            0, // edges 8-15
        ];

        // Exclude toll: should clear bit 1
        let mask = build_exclude_mask(&base_mask, &edge_flags, EXCLUDE_TOLL);
        assert_eq!(mask[0] & (1u64 << 1), 0); // edge 1 cleared
        assert_ne!(mask[0] & (1u64 << 0), 0); // edge 0 still set
        assert_ne!(mask[0] & (1u64 << 3), 0); // edge 3 still set (ferry, not toll)

        // Exclude toll + ferry: should clear bits 1 and 3
        let mask = build_exclude_mask(&base_mask, &edge_flags, EXCLUDE_TOLL | EXCLUDE_FERRY);
        assert_eq!(mask[0] & (1u64 << 1), 0); // toll cleared
        assert_eq!(mask[0] & (1u64 << 3), 0); // ferry cleared
        assert_ne!(mask[0] & (1u64 << 10), 0); // motorway still set
    }
}
