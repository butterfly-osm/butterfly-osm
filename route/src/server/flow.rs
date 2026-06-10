//! #460 `edges_flow` — server-side weighted flow accumulation on the
//! predecessor tree.
//!
//! `edges_batch` streams every pair's full edge list (~370 rows/pair) only
//! for the consumer to `GROUP BY (node_from, node_to)` it straight back
//! down — a measured ~2,000× compression ratio thrown away on the wire.
//! `edges_flow` keeps the same routing core (snap → source-group → K-lane
//! restricted tree, #438/#439) but accumulates each pair's `weight` on its
//! path server-side and emits ONE row per traversed edge per `group`.
//!
//! ## Why accumulation runs on CCH arcs, not unpacked EBG edges
//!
//! A pair's tree path is ~10-40 CCH arcs (UP\*DOWN\*) but unpacks to ~370
//! EBG edges — the unpack is the dominant per-pair cost (#436). So:
//!
//! 1. **Deposit** each pair's weight on its tagged CCH arcs (cheap chain
//!    walk via [`tree_lane_path_arcs`]) plus one deposit on the source EBG
//!    rank (the unpacked path starts with the source node itself).
//! 2. **Cascade** shortcut flow to its two children once per DISTINCT
//!    `(group, arc)`: a shortcut `u→v` with middle `m` expands to
//!    `(u→m, m→v)` exactly as `unpack` does. `rank(middle(child)) <
//!    rank(middle(parent))` (the child's bypassed node was contracted
//!    earlier), so popping a max-heap ordered by middle rank processes
//!    every parent before its children — each arc's flow is final when
//!    popped, and the cascade is a single pass.
//! 3. **Flush** base-arc flow to EBG ranks (a base UP/DOWN arc `u→v`
//!    contributes the rank it appends to the unpacked path: `v`), map
//!    ranks to OSM endpoints, and merge the fallback rows.
//!
//! Pairs the tree can't serve (snap misses, singleton groups, lane
//! backtrack misses) ride the existing per-pair path and fold their
//! emitted rows directly into the OSM-keyed accumulator — bit-identical
//! routing semantics to `edges_batch`, only the aggregation differs.
//!
//! Determinism: batches compute in parallel but merge in batch order, the
//! cascade pops `(middle, arc, group)` tuples (total order), and the final
//! rows sort by `(group, osm_from, osm_to)` — byte-identical reruns.

use rustc_hash::FxHashMap as HashMap;

use rayon::prelude::*;

use super::flight::{GroupedTarget, PairEdges, PerPairWork};
use super::state::{ModeData, ServerState};
use crate::model::types::Mode;
use crate::range::tree_phast::{
    ARC_MASK, DOWN_BIT, TREE_LANES, TreeSettle, tree_lane_path_arcs, tree_settle_restricted_batch,
};

/// Request-level bookkeeping for conservation checks (#460).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct FlowSummary {
    pub n_pairs: u64,
    pub n_unreachable: u64,
    pub total_weight_in: f64,
    pub total_weight_assigned: f64,
}

/// One output row: accumulated flow on a directed edge for a class.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlowRow {
    pub group: u32,
    pub osm_from: i64,
    pub osm_to: i64,
    pub flow: f64,
}

/// Per-batch accumulator — merged in batch order for determinism.
#[derive(Default)]
struct BatchAcc {
    /// `(group, tagged CCH arc) → flow` from tree-served pairs.
    arc_flow: HashMap<(u32, u32), f64>,
    /// `(group, EBG rank) → flow` — source-rank deposits (the unpacked
    /// path includes the source EBG node itself).
    rank_flow: HashMap<(u32, u32), f64>,
    /// `(group, osm_from, osm_to) → flow` from per-pair fallback rows.
    row_flow: HashMap<(u32, i64, i64), f64>,
    n_unreachable: u64,
    assigned: f64,
}

impl BatchAcc {
    fn merge_into(self, total: &mut BatchAcc) {
        for (k, v) in self.arc_flow {
            *total.arc_flow.entry(k).or_default() += v;
        }
        for (k, v) in self.rank_flow {
            *total.rank_flow.entry(k).or_default() += v;
        }
        for (k, v) in self.row_flow {
            *total.row_flow.entry(k).or_default() += v;
        }
        total.n_unreachable += self.n_unreachable;
        total.assigned += self.assigned;
    }

    fn fold_pair_rows(&mut self, group: u32, weight: f64, rows: &[super::flight::EdgeRow]) {
        if rows.is_empty() {
            self.n_unreachable += 1;
            return;
        }
        for r in rows {
            *self
                .row_flow
                .entry((group, r.osm_from, r.osm_to))
                .or_default() += weight;
        }
        self.assigned += weight;
    }
}

/// One K-lane batch: settle, then deposit every served target's weight on
/// its CCH arcs + the source rank; misses fall back to the per-pair path
/// LAST (fallback queries reuse thread-local scratch the lane walk needs).
fn process_flow_batch(
    state: &ServerState,
    mode_data: &ModeData,
    mode: Mode,
    batch: &[&(u32, Vec<GroupedTarget>)],
    weights: &[f64],
    groups: &[u32],
) -> BatchAcc {
    let mut acc = BatchAcc::default();
    let sources: Vec<u32> = batch.iter().map(|(s, _)| *s).collect();
    let union_targets: Vec<u32> = batch
        .iter()
        .flat_map(|(_, ts)| ts.iter().filter_map(|t| t.dst_rank))
        .collect();

    let settled = !union_targets.is_empty()
        && tree_settle_restricted_batch(
            &mode_data.cch_topo,
            &mode_data.cch_weights,
            &mode_data.down_rev_flat,
            &sources,
            &union_targets,
        ) == TreeSettle::Ok;

    let mut fallbacks: Vec<&GroupedTarget> = Vec::new();
    let mut arc_buf: Vec<u32> = Vec::with_capacity(64);
    for (k, (src_rank, targets)) in batch.iter().enumerate() {
        for t in targets {
            let g = groups[t.query_idx as usize];
            let w = weights[t.query_idx as usize];
            let hit = if settled {
                t.dst_rank.and_then(|dst| {
                    tree_lane_path_arcs(&mode_data.cch_topo, k, *src_rank, dst, &mut arc_buf)
                })
            } else {
                None
            };
            match hit {
                Some(_dist) => {
                    for &arc in &arc_buf {
                        *acc.arc_flow.entry((g, arc)).or_default() += w;
                    }
                    *acc.rank_flow.entry((g, *src_rank)).or_default() += w;
                    acc.assigned += w;
                }
                None => fallbacks.push(t),
            }
        }
    }
    let query = super::query::CchQuery::new(mode_data);
    for t in fallbacks {
        let g = groups[t.query_idx as usize];
        let w = weights[t.query_idx as usize];
        let pe =
            super::flight::edges_for_pair(state, mode_data, mode, &query, t.query_idx, &t.pair);
        acc.fold_pair_rows(g, w, &pe.rows);
    }
    acc
}

/// Middle rank of a tagged shortcut arc (mirrors `unpack`'s relaxed-middle
/// preference: `CchWeights` middles post-triangle-relaxation, falling back
/// to the contraction middles).
#[inline]
fn arc_middle(mode_data: &ModeData, tagged: u32) -> u32 {
    let idx = (tagged & ARC_MASK) as usize;
    if tagged & DOWN_BIT != 0 {
        if idx < mode_data.cch_weights.down_middle.len() {
            mode_data.cch_weights.down_middle[idx]
        } else {
            mode_data.cch_topo.down_middle.get(idx)
        }
    } else if idx < mode_data.cch_weights.up_middle.len() {
        mode_data.cch_weights.up_middle[idx]
    } else {
        mode_data.cch_topo.up_middle.get(idx)
    }
}

#[inline]
fn arc_is_shortcut(mode_data: &ModeData, tagged: u32) -> bool {
    let idx = (tagged & ARC_MASK) as usize;
    if tagged & DOWN_BIT != 0 {
        mode_data.cch_topo.down_is_shortcut.bit(idx)
    } else {
        mode_data.cch_topo.up_is_shortcut.bit(idx)
    }
}

/// Owner (source node) and target of a tagged arc.
#[inline]
fn arc_endpoints(mode_data: &ModeData, tagged: u32) -> (u32, u32) {
    let idx = (tagged & ARC_MASK) as usize;
    if tagged & DOWN_BIT != 0 {
        (
            crate::range::tree_phast::arc_owner(&mode_data.cch_topo.down_offsets, idx) as u32,
            mode_data.cch_topo.down_targets[idx],
        )
    } else {
        (
            crate::range::tree_phast::arc_owner(&mode_data.cch_topo.up_offsets, idx) as u32,
            mode_data.cch_topo.up_targets[idx],
        )
    }
}

/// Cascade ONE group's shortcut flow down to base arcs, folding base-arc
/// flow into a fresh per-group rank map. Single pass: max-heap by middle
/// rank — every parent pops before its children (child middles are
/// strictly lower-rank), so each arc's flow is final when popped.
fn cascade_group(mode_data: &ModeData, arc_flow: Vec<(u32, f64)>) -> HashMap<u32, f64> {
    use std::collections::BinaryHeap;

    let mut rank_flow: HashMap<u32, f64> = HashMap::default();
    let mut flow: HashMap<u32, f64> = HashMap::default();
    // (middle, tagged) — total order keeps pop order deterministic.
    let mut heap: BinaryHeap<(u32, u32)> = BinaryHeap::new();

    let seed = |mode_data: &ModeData,
                flow: &mut HashMap<u32, f64>,
                heap: &mut BinaryHeap<(u32, u32)>,
                rank_flow: &mut HashMap<u32, f64>,
                tagged: u32,
                f: f64| {
        if arc_is_shortcut(mode_data, tagged) {
            let fresh = {
                let e = flow.entry(tagged).or_default();
                let was_zero = *e == 0.0;
                *e += f;
                was_zero
            };
            if fresh {
                heap.push((arc_middle(mode_data, tagged), tagged));
            }
        } else {
            let (_, v) = arc_endpoints(mode_data, tagged);
            *rank_flow.entry(v).or_default() += f;
        }
    };

    for (tagged, f) in arc_flow {
        seed(mode_data, &mut flow, &mut heap, &mut rank_flow, tagged, f);
    }

    while let Some((_, tagged)) = heap.pop() {
        let Some(f) = flow.remove(&tagged) else {
            continue; // duplicate heap entry — already cascaded
        };
        let (u, v) = arc_endpoints(mode_data, tagged);
        let m = arc_middle(mode_data, tagged);
        // Children exactly as unpack resolves them: DOWN u→m, UP m→v.
        let down_child = super::unpack::find_down_edge(&mode_data.cch_topo, u as usize, m)
            .map(|i| i as u32 | DOWN_BIT);
        let up_child =
            super::unpack::find_up_edge(&mode_data.cch_topo, m as usize, v).map(|i| i as u32);
        debug_assert!(
            down_child.is_some() && up_child.is_some(),
            "shortcut without resolvable children (arc {tagged:#x}, middle {m})"
        );
        for child in [down_child, up_child].into_iter().flatten() {
            seed(mode_data, &mut flow, &mut heap, &mut rank_flow, child, f);
        }
    }
    rank_flow
}

/// Cascade shortcut flow down to base arcs per GROUP — groups carry
/// independent flows, so they cascade in parallel with zero coordination —
/// then fold every group's base-arc flow into `rank_flow` in ascending
/// group order (deterministic).
fn cascade_arc_flow(
    mode_data: &ModeData,
    arc_flow: HashMap<(u32, u32), f64>,
    rank_flow: &mut HashMap<(u32, u32), f64>,
    parallel: bool,
) {
    let mut per_group: HashMap<u32, Vec<(u32, f64)>> = HashMap::default();
    for ((g, tagged), f) in arc_flow {
        per_group.entry(g).or_default().push((tagged, f));
    }
    let mut group_list: Vec<(u32, Vec<(u32, f64)>)> = per_group.into_iter().collect();
    group_list.sort_unstable_by_key(|(g, _)| *g);

    let cascaded: Vec<(u32, HashMap<u32, f64>)> = if parallel {
        group_list
            .into_par_iter()
            .map(|(g, arcs)| (g, cascade_group(mode_data, arcs)))
            .collect()
    } else {
        group_list
            .into_iter()
            .map(|(g, arcs)| (g, cascade_group(mode_data, arcs)))
            .collect()
    };
    for (g, ranks) in cascaded {
        for (rank, f) in ranks {
            *rank_flow.entry((g, rank)).or_default() += f;
        }
    }
}

/// Compute accumulated flow rows for weighted OD `pairs`. `weights` and
/// `groups` are indexed by pair (`query_idx`). Returns rows sorted by
/// `(group, osm_from, osm_to)` plus the conservation summary.
pub fn compute_edges_flow(
    state: &ServerState,
    mode_data: &ModeData,
    mode: Mode,
    pairs: &[[f64; 4]],
    weights: &[f64],
    groups: &[u32],
    parallel: bool,
) -> (Vec<FlowRow>, FlowSummary) {
    assert_eq!(pairs.len(), weights.len());
    assert_eq!(pairs.len(), groups.len());

    let (group_vec, per_pair_work) =
        super::flight::group_pairs(state, mode_data, mode, pairs, parallel);

    // Locality batching (#438): rank-sort before chunking keeps each
    // batch's union selection tight.
    let mut group_refs: Vec<&(u32, Vec<GroupedTarget>)> = group_vec.iter().collect();
    group_refs.sort_unstable_by_key(|(src, _)| *src);
    let batches: Vec<Vec<&(u32, Vec<GroupedTarget>)>> =
        group_refs.chunks(TREE_LANES).map(|c| c.to_vec()).collect();

    // Parallel compute, DETERMINISTIC merge: collect per-batch accs in
    // batch order, then fold left.
    let batch_accs: Vec<BatchAcc> = if parallel {
        batches
            .par_iter()
            .map(|b| process_flow_batch(state, mode_data, mode, b, weights, groups))
            .collect()
    } else {
        batches
            .iter()
            .map(|b| process_flow_batch(state, mode_data, mode, b, weights, groups))
            .collect()
    };
    let mut total = BatchAcc::default();
    for acc in batch_accs {
        acc.merge_into(&mut total);
    }

    // Per-pair work (snap misses + singletons): existing per-pair path,
    // rows folded by query_idx order (deterministic).
    let pair_results: Vec<PairEdges> = if parallel {
        per_pair_work
            .par_iter()
            .map(|w: &PerPairWork| {
                let query = super::query::CchQuery::new(mode_data);
                super::flight::process_per_pair_work(state, mode_data, mode, &query, w)
            })
            .collect()
    } else {
        let query = super::query::CchQuery::new(mode_data);
        per_pair_work
            .iter()
            .map(|w| super::flight::process_per_pair_work(state, mode_data, mode, &query, w))
            .collect()
    };
    for pe in &pair_results {
        let g = groups[pe.query_idx as usize];
        let w = weights[pe.query_idx as usize];
        total.fold_pair_rows(g, w, &pe.rows);
    }

    // Cascade CCH-arc flow to EBG ranks.
    let arc_flow = std::mem::take(&mut total.arc_flow);
    cascade_arc_flow(mode_data, arc_flow, &mut total.rank_flow, parallel);

    // Flush: rank → EBG node → OSM endpoints, merged with fallback rows.
    let mut out: HashMap<(u32, i64, i64), f64> = std::mem::take(&mut total.row_flow);
    for ((g, rank), f) in std::mem::take(&mut total.rank_flow) {
        let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        let ebg_id = mode_data.filtered_to_original[filt_id as usize];
        let node = &state.ebg_nodes.nodes[ebg_id as usize];
        let osm_from = state
            .nbg_node_to_osm
            .get(node.tail_nbg as usize)
            .copied()
            .unwrap_or(0);
        let osm_to = state
            .nbg_node_to_osm
            .get(node.head_nbg as usize)
            .copied()
            .unwrap_or(0);
        *out.entry((g, osm_from, osm_to)).or_default() += f;
    }

    let mut rows: Vec<FlowRow> = out
        .into_iter()
        .map(|((group, osm_from, osm_to), flow)| FlowRow {
            group,
            osm_from,
            osm_to,
            flow,
        })
        .collect();
    rows.sort_unstable_by(|a, b| {
        (a.group, a.osm_from, a.osm_to).cmp(&(b.group, b.osm_from, b.osm_to))
    });

    let total_weight_in: f64 = weights.iter().sum();
    let summary = FlowSummary {
        n_pairs: pairs.len() as u64,
        n_unreachable: total.n_unreachable,
        total_weight_in,
        total_weight_assigned: total.assigned,
    };
    (rows, summary)
}
