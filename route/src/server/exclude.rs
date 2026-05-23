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

use std::sync::atomic::{AtomicU64, Ordering};

/// Pack (weight, middle_rank) into a single u64 for atomic fetch_min.
/// Weight in high 32 bits so fetch_min minimizes by weight first. Middle
/// in low 32 bits comes along for the ride — when the relax improves
/// the weight it atomically records the m that produced it.
///
/// Build-time customization does the same dance in customization.rs;
/// duplicated here so exclude.rs stays self-contained.
#[inline]
fn pack_wm(weight: u32, middle: u32) -> u64 {
    ((weight as u64) << 32) | (middle as u64)
}

#[inline]
fn unpack_weight(packed: u64) -> u32 {
    (packed >> 32) as u32
}

#[inline]
fn unpack_middle(packed: u64) -> u32 {
    packed as u32
}

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

    // Triangle relaxation to find alternative paths. The relax updates
    // (weight, middle) as a tuple via AtomicU64, so middles drift to
    // whichever m actually produces the current best weight. Stale
    // middles → wrong unpack geometry (#239).
    let rev_down = build_reverse_down_adj(topo);
    let (up_middles, down_middles) =
        triangle_relax(topo, &mut up_weights, &mut down_weights, &rev_down);

    CchWeights {
        up: up_weights.into(),
        down: down_weights.into(),
        up_middle: up_middles.into(),
        down_middle: down_middles.into(),
    }
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

    // Incremental BFS recustomization (#240). Walks only edges
    // transitively dependent on polygon-flagged base edges.
    let time_weights = recustomize_weights_incremental(
        topo,
        base_time,
        edge_exclude_flags,
        exclude_mask,
        filtered_to_original,
    );

    tracing::info!(
        exclude_mask,
        elapsed_ms = start.elapsed().as_millis(),
        "computed exclude weights (time-only, incremental)"
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

    // Re-customize time and distance weights in parallel.
    // Uses the incremental BFS algorithm (#240) — touches only edges
    // transitively dependent on polygon-flagged base edges, so work
    // is bounded by polygon size rather than graph size.
    let (time_weights, dist_weights) = rayon::join(
        || {
            recustomize_weights_incremental(
                topo,
                base_time,
                edge_exclude_flags,
                exclude_mask,
                filtered_to_original,
            )
        },
        || {
            recustomize_weights_incremental(
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

/// Triangle relaxation: find shorter paths through alternative intermediate
/// nodes. Iterates until convergence (no more weight decreases).
///
/// Packs (weight, middle_rank) into AtomicU64 so the relax updates both
/// atomically — when m gives a strictly better weight, the slot's middle
/// becomes m. This is what `unpack_path` follows; without it the unpack
/// follows the stale topo middle and the geometry can cross an avoided
/// region even when the duration correctly reflects the detour (#239).
///
/// Returns `(up_middles, down_middles)` populated with the relaxed
/// middles; the caller's mutable `up_weights` / `down_weights` are
/// written in-place with the relaxed weights.
fn triangle_relax(
    topo: &CchTopo,
    up_weights: &mut Vec<u32>,
    down_weights: &mut Vec<u32>,
    rev_down: &ReverseDownAdj,
) -> (Vec<u32>, Vec<u32>) {
    let n_nodes = topo.n_nodes as usize;

    // Pack (weight, middle) into AtomicU64; initial middle = topo's
    // contraction middle for each edge.
    let atomic_up: Vec<AtomicU64> = up_weights
        .iter()
        .zip(topo.up_middle.iter())
        .map(|(&w, &m)| AtomicU64::new(pack_wm(w, m)))
        .collect();
    let atomic_down: Vec<AtomicU64> = down_weights
        .iter()
        .zip(topo.down_middle.iter())
        .map(|(&w, &m)| AtomicU64::new(pack_wm(w, m)))
        .collect();

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
                let w_xm = unpack_weight(atomic_down[edge_idx_xm].load(Ordering::Relaxed));

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

                    let w_my = unpack_weight(atomic_up[i_my].load(Ordering::Relaxed));
                    if w_my == u32::MAX {
                        continue;
                    }

                    let new_weight = w_xm.saturating_add(w_my);
                    let new_packed = pack_wm(new_weight, m as u32);

                    if y > x {
                        if let Some(idx) = find_edge_index(x, y, &topo.up_offsets, &topo.up_targets)
                        {
                            let old = atomic_up[idx].fetch_min(new_packed, Ordering::Relaxed);
                            if new_packed < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else if let Some(idx) =
                        find_edge_index(x, y, &topo.down_offsets, &topo.down_targets)
                    {
                        let old = atomic_down[idx].fetch_min(new_packed, Ordering::Relaxed);
                        if new_packed < old {
                            pass_updates.fetch_add(1, Ordering::Relaxed);
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

    let n_up = atomic_up.len();
    let n_down = atomic_down.len();
    let mut up_middles = Vec::with_capacity(n_up);
    let mut down_middles = Vec::with_capacity(n_down);
    up_weights.clear();
    down_weights.clear();
    up_weights.reserve(n_up);
    down_weights.reserve(n_down);
    for a in atomic_up {
        let packed = a.into_inner();
        up_weights.push(unpack_weight(packed));
        up_middles.push(unpack_middle(packed));
    }
    for a in atomic_down {
        let packed = a.into_inner();
        down_weights.push(unpack_weight(packed));
        down_middles.push(unpack_middle(packed));
    }
    (up_middles, down_middles)
}

// ============================================================================
// #240 Incremental recustomization
// ============================================================================
//
// The from-scratch path (recustomize_weights / recustomize_weights_sparse_triangle)
// runs an O(|edges|) bottom-up over every CCH edge regardless of polygon size.
// On Belgium that's ~12 M shortcut recomputations and ~8 s sequential — even
// for a polygon covering 10 base edges in a rural area.
//
// The incremental version starts from the BASE weights + base middles and
// only re-evaluates edges that depend, transitively, on a polygon-flagged
// base edge. Cost is O(|touched_shortcuts| × deg) rather than O(|edges|).
//
// Algorithm:
//   1. Initialise (up_weights, down_weights, up_middle, down_middle) to the
//      base build-time values — those are already triangle-relaxed for the
//      no-avoid graph.
//   2. Seed a BFS queue with every CCH base edge whose underlying OSM edge
//      is in the polygon. For each, mark it as needing recomputation.
//   3. Pop edges from the queue, recompute their (weight, middle) by
//      considering every triangle (x, m, y) where x = edge.source and
//      y = edge.target. If the result changed, write it and enqueue every
//      edge that uses this one as a triangle leg.
//   4. Terminate when the queue is empty.
//
// Correctness: each edge's recomputation considers all incident triangles,
// so the final (weight, middle) matches what a full triangle relaxation
// would produce. Convergence is guaranteed because weights only decrease
// and are bounded by the base value.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeDir {
    Up,
    Down,
}

#[derive(Clone, Copy, Debug)]
struct EdgeRef {
    dir: EdgeDir,
    idx: usize,
    source: usize,
    target: usize,
}

/// Incrementally recustomize CCH weights starting from `base_weights` after
/// the avoid/exclude mask flags some base edges as INF.
///
/// Returns a new `CchWeights` with the relaxed weights AND relaxed middles
/// — `unpack_path` must follow the relaxed middles to emit the correct
/// geometry (#239).
pub fn recustomize_weights_incremental(
    topo: &CchTopo,
    base_weights: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> CchWeights {
    let start = std::time::Instant::now();
    let mut up_weights = base_weights.up.to_vec();
    let mut down_weights = base_weights.down.to_vec();
    let mut up_middle = if base_weights.up_middle.len() == topo.up_targets.len() {
        base_weights.up_middle.to_vec()
    } else {
        topo.up_middle.to_vec()
    };
    let mut down_middle = if base_weights.down_middle.len() == topo.down_targets.len() {
        base_weights.down_middle.to_vec()
    } else {
        topo.down_middle.to_vec()
    };

    let mut queued_up = vec![false; topo.up_targets.len()];
    let mut queued_down = vec![false; topo.down_targets.len()];
    let mut queue: std::collections::VecDeque<EdgeRef> = std::collections::VecDeque::new();
    let mut seeded = 0usize;
    let n_nodes = topo.n_nodes as usize;

    // Seed: every CCH BASE edge whose underlying OSM edge is in the
    // polygon. Shortcuts inherit through the BFS propagation.
    for source in 0..n_nodes {
        let up_start = topo.up_offsets[source] as usize;
        let up_end = topo.up_offsets[source + 1] as usize;
        for idx in up_start..up_end {
            if !topo.up_is_shortcut.bit(idx)
                && cch_base_edge_excluded(
                    topo.up_targets[idx] as usize,
                    topo,
                    edge_exclude_flags,
                    exclude_mask,
                    filtered_to_original,
                )
            {
                push_edge(
                    &mut queue,
                    &mut queued_up,
                    &mut queued_down,
                    EdgeRef {
                        dir: EdgeDir::Up,
                        idx,
                        source,
                        target: topo.up_targets[idx] as usize,
                    },
                );
                seeded += 1;
            }
        }
        let down_start = topo.down_offsets[source] as usize;
        let down_end = topo.down_offsets[source + 1] as usize;
        for idx in down_start..down_end {
            if !topo.down_is_shortcut.bit(idx)
                && cch_base_edge_excluded(
                    topo.down_targets[idx] as usize,
                    topo,
                    edge_exclude_flags,
                    exclude_mask,
                    filtered_to_original,
                )
            {
                push_edge(
                    &mut queue,
                    &mut queued_up,
                    &mut queued_down,
                    EdgeRef {
                        dir: EdgeDir::Down,
                        idx,
                        source,
                        target: topo.down_targets[idx] as usize,
                    },
                );
                seeded += 1;
            }
        }
    }

    // Reverse DOWN adjacency: for each m, which sources x have a DOWN
    // edge x→m? Needed by enqueue_dependents to walk triangles centred
    // at the lower apex when an UP edge changes.
    let rev_down = build_reverse_down_adj(topo);

    let mut recomputed = 0usize;
    let mut changed_weight = 0usize;
    let mut changed_middle = 0usize;

    while let Some(edge) = queue.pop_front() {
        match edge.dir {
            EdgeDir::Up => queued_up[edge.idx] = false,
            EdgeDir::Down => queued_down[edge.idx] = false,
        }
        recomputed += 1;

        let (new_weight, new_middle) = recompute_edge_weight(
            edge,
            topo,
            base_weights,
            edge_exclude_flags,
            exclude_mask,
            filtered_to_original,
            &up_weights,
            &down_weights,
        );

        let (old_weight, old_middle) = match edge.dir {
            EdgeDir::Up => (up_weights[edge.idx], up_middle[edge.idx]),
            EdgeDir::Down => (down_weights[edge.idx], down_middle[edge.idx]),
        };

        if new_weight == old_weight && new_middle == old_middle {
            continue;
        }

        match edge.dir {
            EdgeDir::Up => {
                up_weights[edge.idx] = new_weight;
                up_middle[edge.idx] = new_middle;
            }
            EdgeDir::Down => {
                down_weights[edge.idx] = new_weight;
                down_middle[edge.idx] = new_middle;
            }
        }

        if new_middle != old_middle {
            changed_middle += 1;
        }
        if new_weight != old_weight {
            changed_weight += 1;
            enqueue_dependents(
                edge,
                topo,
                &rev_down,
                &mut queue,
                &mut queued_up,
                &mut queued_down,
            );
        }
    }

    tracing::debug!(
        seeded_edges = seeded,
        recomputed_edges = recomputed,
        changed_weight_edges = changed_weight,
        changed_middle_edges = changed_middle,
        elapsed_ms = start.elapsed().as_millis(),
        "incremental CCH recustomization"
    );

    CchWeights {
        up: up_weights.into(),
        down: down_weights.into(),
        up_middle: up_middle.into(),
        down_middle: down_middle.into(),
    }
}

#[inline]
fn push_edge(
    queue: &mut std::collections::VecDeque<EdgeRef>,
    queued_up: &mut [bool],
    queued_down: &mut [bool],
    edge: EdgeRef,
) {
    let queued = match edge.dir {
        EdgeDir::Up => &mut queued_up[edge.idx],
        EdgeDir::Down => &mut queued_down[edge.idx],
    };
    if !*queued {
        *queued = true;
        queue.push_back(edge);
    }
}

/// True if the CCH base edge with the given target rank corresponds to
/// an OSM edge that is in the polygon/exclude flag set.
#[inline]
fn cch_base_edge_excluded(
    target_rank: usize,
    topo: &CchTopo,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> bool {
    let filtered = topo.rank_to_filtered[target_rank] as usize;
    let Some(&orig) = filtered_to_original.get(filtered) else {
        return false;
    };
    edge_exclude_flags
        .get(orig as usize)
        .is_some_and(|flags| flags & exclude_mask != 0)
}

/// Pick the best (weight, middle) for `edge` by considering its
/// direct base value (if base) and every triangle through the
/// current up_weights / down_weights.
#[allow(clippy::too_many_arguments)]
fn recompute_edge_weight(
    edge: EdgeRef,
    topo: &CchTopo,
    base_weights: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
    up_weights: &[u32],
    down_weights: &[u32],
) -> (u32, u32) {
    let is_shortcut = match edge.dir {
        EdgeDir::Up => topo.up_is_shortcut.bit(edge.idx),
        EdgeDir::Down => topo.down_is_shortcut.bit(edge.idx),
    };

    // Start from the base value (or INF if this base edge is itself
    // excluded). The triangle scan below can only improve it.
    let base_excluded = !is_shortcut
        && cch_base_edge_excluded(
            edge.target,
            topo,
            edge_exclude_flags,
            exclude_mask,
            filtered_to_original,
        );
    let mut best_weight = if base_excluded {
        u32::MAX
    } else {
        match edge.dir {
            EdgeDir::Up => base_weights.up[edge.idx],
            EdgeDir::Down => base_weights.down[edge.idx],
        }
    };
    let mut best_middle = match edge.dir {
        EdgeDir::Up => topo.up_middle[edge.idx],
        EdgeDir::Down => topo.down_middle[edge.idx],
    };
    let mut best_packed = pack_wm(best_weight, best_middle);

    // Iterate every candidate middle m: m has DOWN edge from source
    // (rank(m) < rank(source)) and UP edge to target.
    let down_start = topo.down_offsets[edge.source] as usize;
    let down_end = topo.down_offsets[edge.source + 1] as usize;
    for (offset, &m_u32) in topo.down_targets[down_start..down_end].iter().enumerate() {
        let i_xm = down_start + offset;
        let m = m_u32 as usize;
        if m == edge.target {
            continue;
        }
        let w_xm = down_weights[i_xm];
        if w_xm == u32::MAX {
            continue;
        }
        let Some(i_my) = find_edge_index(m, edge.target, &topo.up_offsets, &topo.up_targets) else {
            continue;
        };
        let w_my = up_weights[i_my];
        if w_my == u32::MAX {
            continue;
        }
        let packed = pack_wm(w_xm.saturating_add(w_my), m as u32);
        if packed < best_packed {
            best_packed = packed;
            best_weight = unpack_weight(packed);
            best_middle = unpack_middle(packed);
        }
    }

    (best_weight, best_middle)
}

/// Enqueue every edge whose recomputation depends on `edge`. When an
/// UP edge m→y changes, all triangles x→m→y need re-examination — the
/// affected output edges are (x, y) for every x that has a DOWN edge
/// to m. Symmetric for DOWN edges via the upper apex.
fn enqueue_dependents(
    edge: EdgeRef,
    topo: &CchTopo,
    rev_down: &ReverseDownAdj,
    queue: &mut std::collections::VecDeque<EdgeRef>,
    queued_up: &mut [bool],
    queued_down: &mut [bool],
) {
    match edge.dir {
        EdgeDir::Up => {
            // Improved m→y (with m = edge.source, y = edge.target).
            // Affected: every (x, y) where x→m DOWN exists.
            let m = edge.source;
            let y = edge.target;
            let rev_start = rev_down.offsets[m] as usize;
            let rev_end = rev_down.offsets[m + 1] as usize;
            for slot in rev_start..rev_end {
                let x = rev_down.sources[slot] as usize;
                if x == y {
                    continue;
                }
                push_existing_edge(x, y, topo, queue, queued_up, queued_down);
            }
        }
        EdgeDir::Down => {
            // Improved x→m DOWN (with x = edge.source, m = edge.target).
            // Affected: every (x, y) where m→y UP exists.
            let x = edge.source;
            let m = edge.target;
            let up_start = topo.up_offsets[m] as usize;
            let up_end = topo.up_offsets[m + 1] as usize;
            for i_my in up_start..up_end {
                let y = topo.up_targets[i_my] as usize;
                if x == y {
                    continue;
                }
                push_existing_edge(x, y, topo, queue, queued_up, queued_down);
            }
        }
    }
}

#[inline]
fn push_existing_edge(
    source: usize,
    target: usize,
    topo: &CchTopo,
    queue: &mut std::collections::VecDeque<EdgeRef>,
    queued_up: &mut [bool],
    queued_down: &mut [bool],
) {
    if target > source {
        if let Some(idx) = find_edge_index(source, target, &topo.up_offsets, &topo.up_targets) {
            push_edge(
                queue,
                queued_up,
                queued_down,
                EdgeRef {
                    dir: EdgeDir::Up,
                    idx,
                    source,
                    target,
                },
            );
        }
    } else if let Some(idx) =
        find_edge_index(source, target, &topo.down_offsets, &topo.down_targets)
    {
        push_edge(
            queue,
            queued_up,
            queued_down,
            EdgeRef {
                dir: EdgeDir::Down,
                idx,
                source,
                target,
            },
        );
    }
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
