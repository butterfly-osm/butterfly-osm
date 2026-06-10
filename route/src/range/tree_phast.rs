//! #438: predecessor-tracking bounded PHAST — the per-source shortest-path
//! TREE that `edges_batch` slices per target.
//!
//! One tree per source replaces one bidirectional CCH query per (source,
//! target) pair: after `tree_settle(source, threshold)`, every target's path
//! is a `tree_backtrack(target)` — a parent-chain walk plus the existing
//! shortcut unpack. Zero per-target searches (the #439 grouped path still ran
//! one backward search per target; this removes those too).
//!
//! Design (codex-reviewed, butterfly-osm#438):
//! - Upward phase: PQ Dijkstra over the topo UP arrays (mirrors
//!   [`crate::range::phast::PhastEngine::query_adaptive`]), recording the
//!   winning UP arc per improved node.
//! - Downward phase: rank-descending block-gated scan over the topo DOWN
//!   arrays, recording the winning DOWN arc per improved node. Block gating
//!   bounds the scan to blocks reachable within `threshold` — exact for every
//!   node with true distance ≤ threshold (same guarantee the isochrone path
//!   relies on).
//! - Parent storage: ONE tagged `u32` per node — the topo arc index with the
//!   top bit set for DOWN arcs. The parent NODE is derived at backtrack time
//!   by ownership binary-search over the offsets arrays (≈23 steps per hop,
//!   ~10-40 hops per path — negligible). This halves the extra write
//!   bandwidth on the memory-bound scan vs storing `(node, arc)`.
//! - Tree paths are `UP* DOWN*` (rank-ordered downward scan can never insert
//!   an UP arc after a DOWN arc), so the backtrack splits cleanly at the apex
//!   and feeds [`crate::server::unpack::unpack_path`] unchanged.
//!
//! Scratch arrays are thread-local, generation-stamped (O(1) per-tree reset),
//! and registered with the idle compactor via
//! [`crate::server::evictable::EvictableCell`] (~60 MB per worker thread on
//! Belgium, freed when idle).

use crate::formats::{CchTopo, CchWeights};
use crate::matrix::bucket_ch::{DAryHeap, DownReverseAdjFlat};
use crate::server::evictable::EvictableCell;
use crate::server::query::HANDLE_NONE;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Block size for the gated downward scan — matches
/// [`crate::range::phast::BLOCK_SIZE`].
const BLOCK_SIZE: usize = 4096;

/// Direction tag on the parent arc: set ⇒ DOWN arc, clear ⇒ UP arc.
const DOWN_BIT: u32 = 1 << 31;
const ARC_MASK: u32 = DOWN_BIT - 1;

struct TreeScratch {
    dist: Vec<u32>,
    /// Tagged winning arc per node (`DOWN_BIT` | topo arc idx). Valid only
    /// where `gen[node] == epoch`.
    parent: Vec<u32>,
    generation: Vec<u32>,
    epoch: u32,
    /// Origin + threshold of the CURRENT settled tree (validity check for
    /// backtracks; a settle for a new origin bumps the epoch).
    origin: u32,
    threshold: u32,
    /// RPHAST selection visited-stamps + the selected-rank list (reused
    /// across groups; `sel` is cleared per settle, stamps are epoch-based).
    sel_gen: Vec<u32>,
    sel_epoch: u32,
    sel: Vec<u32>,
    /// #438 K-lane: selection-position map (pos+1, valid where sel_gen ==
    /// sel_epoch) + compact per-selection lane arrays (K × |sel|).
    sel_pos: Vec<u32>,
    lane_dist: Vec<u32>,
    lane_parent: Vec<u32>,
    /// Lever-1 (#438): reused 4-ary decrease-key heap + handle slots for the
    /// upward sweep (replaces a per-settle std::BinaryHeap with lazy dupes).
    pq: DAryHeap,
    handles: Vec<u32>,
}

impl TreeScratch {
    fn new(n: usize) -> Self {
        Self {
            dist: vec![u32::MAX; n],
            parent: vec![0; n],
            generation: vec![0; n],
            epoch: 0,
            origin: u32::MAX,
            threshold: 0,
            sel_gen: vec![0; n],
            sel_epoch: 0,
            sel: Vec::new(),
            sel_pos: vec![0; n],
            lane_dist: Vec::new(),
            lane_parent: Vec::new(),
            pq: DAryHeap::new(1024),
            handles: vec![HANDLE_NONE; n],
        }
    }

    #[inline]
    fn start_tree(&mut self, origin: u32, threshold: u32) {
        self.epoch = self.epoch.wrapping_add(1);
        if self.epoch == 0 {
            self.generation.iter_mut().for_each(|g| *g = 0);
            self.epoch = 1;
        }
        self.origin = origin;
        self.threshold = threshold;
    }

    #[inline]
    fn start_selection(&mut self) {
        self.sel_epoch = self.sel_epoch.wrapping_add(1);
        if self.sel_epoch == 0 {
            self.sel_gen.iter_mut().for_each(|g| *g = 0);
            self.sel_epoch = 1;
        }
        self.sel.clear();
    }

    #[inline]
    fn get(&self, node: usize) -> u32 {
        if self.generation[node] == self.epoch {
            self.dist[node]
        } else {
            u32::MAX
        }
    }

    /// Push-or-decrease into the reused 4-ary heap (mirrors
    /// CchQueryState::push_fwd; `set` clears stale handles on first touch).
    #[inline]
    fn push_up(&mut self, node: u32, dist: u32) {
        if self.handles[node as usize] != HANDLE_NONE {
            let h = self.handles[node as usize];
            self.pq.decrease(h, dist, node, &mut self.handles);
        } else {
            self.pq.push(dist, node, &mut self.handles);
        }
    }

    #[inline]
    fn pop_up(&mut self) -> Option<(u32, u32)> {
        self.pq.pop(&mut self.handles)
    }

    #[inline]
    fn set(&mut self, node: usize, dist: u32, tagged_arc: u32) {
        if self.generation[node] != self.epoch {
            self.handles[node] = HANDLE_NONE;
        }
        self.dist[node] = dist;
        self.parent[node] = tagged_arc;
        self.generation[node] = self.epoch;
    }
}

thread_local! {
    /// Per-thread tree scratch (#409/#410 pattern): evictable so idle worker
    /// threads return the ~60 MB to the OS.
    static TREE_SCRATCH: EvictableCell<TreeScratch> = const { EvictableCell::new() };
}

/// Outcome of [`tree_settle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeSettle {
    /// Tree settled; backtracks are valid for nodes with dist ≤ threshold.
    Ok,
    /// The origin itself was invalid (should not happen for a snapped rank).
    BadOrigin,
}

/// Settle the bounded shortest-path tree from `origin` (a CCH rank) up to
/// `threshold` (same weight unit as the CCH weights — seconds for TIME).
///
/// MUST be followed — on the SAME thread, with no intervening `tree_settle` —
/// by [`tree_backtrack`] calls for this origin. Exactness: every node whose
/// true distance is ≤ `threshold` has exact `dist` + a valid parent chain
/// (block gating only skips blocks unreachable within the threshold). Nodes
/// beyond the threshold may be unset or carry partial values — `tree_backtrack`
/// reports them as misses so the caller can retry with a larger threshold or
/// fall back to the per-pair query.
pub fn tree_settle(
    topo: &CchTopo,
    weights: &CchWeights,
    origin: u32,
    threshold: u32,
) -> TreeSettle {
    let n = topo.n_nodes as usize;
    if (origin as usize) >= n {
        return TreeSettle::BadOrigin;
    }
    TREE_SCRATCH.with(|cell| {
        cell.with_or_init(
            || TreeScratch::new(n),
            |s| {
                if s.dist.len() != n {
                    *s = TreeScratch::new(n);
                }
                s.start_tree(origin, threshold);
                s.set(origin as usize, 0, 0);

                // Block-activity bitset for the gated downward scan.
                let n_blocks = n.div_ceil(BLOCK_SIZE);
                let mut active = vec![0u64; n_blocks.div_ceil(64)];
                let ob = origin as usize / BLOCK_SIZE;
                active[ob / 64] |= 1u64 << (ob % 64);

                // ---- Phase 1: upward PQ sweep (records UP parent arcs) ----
                let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
                pq.push(Reverse((0, origin)));
                while let Some(Reverse((d, u))) = pq.pop() {
                    if d > threshold {
                        break; // all remaining heap entries are ≥ d
                    }
                    if d > s.get(u as usize) {
                        continue; // stale
                    }
                    let start = topo.up_offsets[u as usize] as usize;
                    let end = topo.up_offsets[u as usize + 1] as usize;
                    for i in start..end {
                        let w = weights.up.get(i);
                        if w == u32::MAX {
                            continue;
                        }
                        let v = topo.up_targets[i];
                        let nd = d.saturating_add(w);
                        if nd < s.get(v as usize) {
                            s.set(v as usize, nd, i as u32); // UP arc (no tag)
                            pq.push(Reverse((nd, v)));
                            if nd <= threshold {
                                let vb = v as usize / BLOCK_SIZE;
                                active[vb / 64] |= 1u64 << (vb % 64);
                            }
                        }
                    }
                }

                // ---- Phase 2: rank-descending gated downward scan ----
                // (records DOWN parent arcs)
                for block_idx in (0..n_blocks).rev() {
                    if (active[block_idx / 64] >> (block_idx % 64)) & 1 == 0 {
                        continue;
                    }
                    let rank_start = block_idx * BLOCK_SIZE;
                    let rank_end = ((block_idx + 1) * BLOCK_SIZE).min(n);
                    for u in (rank_start..rank_end).rev() {
                        let d_u = s.get(u);
                        if d_u == u32::MAX || d_u > threshold {
                            continue;
                        }
                        let start = topo.down_offsets[u] as usize;
                        let end = topo.down_offsets[u + 1] as usize;
                        for i in start..end {
                            let w = weights.down.get(i);
                            if w == u32::MAX {
                                continue;
                            }
                            let v = topo.down_targets[i] as usize;
                            let nd = d_u.saturating_add(w);
                            if nd < s.get(v) {
                                s.set(v, nd, i as u32 | DOWN_BIT); // DOWN arc
                                if nd <= threshold {
                                    let vb = v / BLOCK_SIZE;
                                    active[vb / 64] |= 1u64 << (vb % 64);
                                }
                            }
                        }
                    }
                }
                TreeSettle::Ok
            },
        )
    })
}

/// #438: RPHAST-style EXACT restricted tree settle — codex's "(c) is the exact
/// answer". No distance bound, no retries: the downward scan is restricted to
/// the reverse-DOWN ancestry of the group's targets (the SELECTION), which for
/// ~30 targets is a few thousand nodes instead of the 5M-rank full scan.
///
/// Correctness: every shortest path source→t is `UP* DOWN*`; its DOWN suffix
/// lies entirely inside t's reverse-DOWN ancestry ⊆ selection, and the apex
/// gets its exact distance from the (exhaustive) upward sweep. Scanning the
/// selection in descending rank order propagates exact distances to every
/// target — same guarantee as the full scan, at a fraction of the work.
/// Relaxations INTO non-selected children are allowed (their dists are dead
/// ends, never scanned, never backtracked through) so the hot loop needs no
/// membership test.
///
/// `down_rev` supplies the reverse-DOWN adjacency for the selection DFS (INF
/// arcs are filtered at its build — they can't carry a shortest path).
/// TEMP #438 instrumentation: summed ns per phase across all restricted
/// settles (selection DFS / upward sweep / restricted scan). Read+reset by
/// the bench; near-zero overhead when not read.
pub static TREE_PHASE_NS: [std::sync::atomic::AtomicU64; 3] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

pub fn tree_settle_restricted(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev: &DownReverseAdjFlat,
    origin: u32,
    targets: &[u32],
) -> TreeSettle {
    let n = topo.n_nodes as usize;
    if (origin as usize) >= n {
        return TreeSettle::BadOrigin;
    }
    TREE_SCRATCH.with(|cell| {
        cell.with_or_init(
            || TreeScratch::new(n),
            |s| {
                if s.dist.len() != n {
                    *s = TreeScratch::new(n);
                }
                // No bound: every settled node is backtrackable.
                s.start_tree(origin, u32::MAX);
                s.set(origin as usize, 0, 0);

                // ---- Selection: reverse-DOWN ancestry of the targets ----
                let _t = std::time::Instant::now();
                s.start_selection();
                let mut stack: Vec<u32> = Vec::with_capacity(targets.len() * 4);
                for &t in targets {
                    let ti = t as usize;
                    if ti < n && s.sel_gen[ti] != s.sel_epoch {
                        s.sel_gen[ti] = s.sel_epoch;
                        s.sel.push(t);
                        stack.push(t);
                    }
                }
                while let Some(v) = stack.pop() {
                    let start = down_rev.offsets[v as usize] as usize;
                    let end = down_rev.offsets[v as usize + 1] as usize;
                    for slot in start..end {
                        let u = down_rev.sources[slot];
                        let ui = u as usize;
                        if s.sel_gen[ui] != s.sel_epoch {
                            s.sel_gen[ui] = s.sel_epoch;
                            s.sel.push(u);
                            stack.push(u);
                        }
                    }
                }
                // Descending rank order for the scan.
                s.sel.sort_unstable_by(|a, b| b.cmp(a));
                TREE_PHASE_NS[0].fetch_add(
                    _t.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                let _t = std::time::Instant::now();

                // ---- Phase 1: exhaustive upward sweep (UP parents) ----
                upward_sweep_body(s, topo, weights, down_rev, origin);
                TREE_PHASE_NS[1].fetch_add(
                    _t.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                let _t = std::time::Instant::now();
                // ---- Phase 2: restricted downward scan (DOWN parents) ----
                // s.sel is moved out to appease the borrow checker (s is
                // mutably borrowed inside the loop), then restored.
                let sel = std::mem::take(&mut s.sel);
                for &u in &sel {
                    let ui = u as usize;
                    let d_u = s.get(ui);
                    if d_u == u32::MAX {
                        continue;
                    }
                    let start = topo.down_offsets[ui] as usize;
                    let end = topo.down_offsets[ui + 1] as usize;
                    for i in start..end {
                        let w = weights.down.get(i);
                        if w == u32::MAX {
                            continue;
                        }
                        let v = topo.down_targets[i] as usize;
                        let nd = d_u.saturating_add(w);
                        if nd < s.get(v) {
                            s.set(v, nd, i as u32 | DOWN_BIT); // DOWN arc
                        }
                    }
                }
                s.sel = sel;
                TREE_PHASE_NS[2].fetch_add(
                    _t.elapsed().as_nanos() as u64,
                    std::sync::atomic::Ordering::Relaxed,
                );
                TreeSettle::Ok
            },
        )
    })
}

/// A target's path sliced out of the settled tree, in the exact shape
/// [`crate::server::unpack::unpack_path`] consumes.
#[derive(Debug)]
pub struct TreePath {
    /// Total distance (weight units) — identical to the bidirectional query's.
    pub distance: u32,
    /// The apex (highest-rank node on the path) — plays `meeting_node`.
    pub apex: u32,
    /// `(node, up_arc_idx)` ordered source→apex (`node` is ignored by unpack).
    pub forward_parent: Vec<(u32, u32)>,
    /// `(down_arc_source_node, down_arc_idx)` ordered target→apex (unpack
    /// iterates it reversed, i.e. apex→target).
    pub backward_parent: Vec<(u32, u32)>,
}

/// Backtrack `target` (a CCH rank) out of the tree settled by the last
/// [`tree_settle`] on this thread. Returns `None` when the target was not
/// settled within the threshold (unreachable OR beyond the bound — the caller
/// retries with a larger threshold or falls back to the per-pair query;
/// distinguishing the two here is impossible by design).
pub fn tree_backtrack(topo: &CchTopo, origin: u32, target: u32) -> Option<TreePath> {
    let n = topo.n_nodes as usize;
    if (target as usize) >= n {
        return None;
    }
    TREE_SCRATCH.with(|cell| {
        cell.with_or_init(
            || TreeScratch::new(n),
            |s| {
                // Validity: the settled tree must be for this origin and sized
                // for this graph (a graph swap or missing settle ⇒ miss; the
                // caller's fallback recomputes correctly).
                if s.dist.len() != n || s.origin != origin {
                    return None;
                }
                let d_t = s.get(target as usize);
                if d_t == u32::MAX || d_t > s.threshold {
                    return None;
                }
                if target == origin {
                    return Some(TreePath {
                        distance: 0,
                        apex: origin,
                        forward_parent: vec![],
                        backward_parent: vec![],
                    });
                }

                // Walk target → origin. The chain is DOWN* then UP* (the
                // reverse of the path's UP* DOWN* shape).
                let mut backward: Vec<(u32, u32)> = Vec::new();
                let mut forward_rev: Vec<(u32, u32)> = Vec::new();
                let mut cur = target;
                let mut seen_up = false;
                // Path length is bounded by the CCH search-space depth
                // (~hundreds); the cap is a corruption backstop, not a limit
                // that real paths approach.
                for _ in 0..100_000 {
                    if cur == origin {
                        // forward_rev was collected walking apex→origin, so its
                        // FIRST element is the apex-side UP arc — the apex is
                        // that arc's UP target. With no UP arcs the apex is the
                        // highest DOWN source (the last one pushed, nearest the
                        // origin side of the DOWN chain).
                        let apex = if let Some(&(_, apex_up)) = forward_rev.first() {
                            topo.up_targets[apex_up as usize]
                        } else if let Some(&(src, _)) = backward.last() {
                            src
                        } else {
                            origin
                        };
                        // forward_rev was collected apex→source; unpack wants
                        // source→apex.
                        forward_rev.reverse();
                        return Some(TreePath {
                            distance: d_t,
                            apex,
                            forward_parent: forward_rev,
                            backward_parent: backward,
                        });
                    }
                    if s.generation[cur as usize] != s.epoch {
                        return None; // chain left the settled set — corrupt/miss
                    }
                    let tagged = s.parent[cur as usize];
                    let arc = (tagged & ARC_MASK) as usize;
                    if tagged & DOWN_BIT != 0 {
                        // DOWN arc: cur was improved via DOWN arc `arc` whose
                        // target is cur; its SOURCE is the owner of `arc` in
                        // down_offsets (ownership binary search).
                        if seen_up {
                            return None; // UP after DOWN cannot happen — corrupt
                        }
                        let src = arc_owner(&topo.down_offsets, arc) as u32;
                        backward.push((src, arc as u32));
                        cur = src;
                    } else {
                        // UP arc: cur was improved via UP arc `arc` whose
                        // target is cur; its source is the owner in up_offsets.
                        seen_up = true;
                        let src = arc_owner(&topo.up_offsets, arc) as u32;
                        forward_rev.push((src, arc as u32));
                        cur = src;
                    }
                }
                None
            },
        )
    })
}

/// Find the node that owns arc index `arc` in a CSR `offsets` array
/// (`offsets[node] <= arc < offsets[node+1]`). Binary search — ~23 steps on a
/// 5M-node graph.
#[inline]
fn arc_owner(offsets: &[u64], arc: usize) -> usize {
    let a = arc as u64;
    // partition_point returns the first index with offsets[idx] > arc;
    // the owner is that index - 1.
    offsets.partition_point(|&o| o <= a) - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arc_owner_basics() {
        let offsets: Vec<u64> = vec![0, 3, 3, 7, 10];
        assert_eq!(arc_owner(&offsets, 0), 0);
        assert_eq!(arc_owner(&offsets, 2), 0);
        assert_eq!(arc_owner(&offsets, 3), 2); // node 1 is empty
        assert_eq!(arc_owner(&offsets, 6), 2);
        assert_eq!(arc_owner(&offsets, 7), 3);
        assert_eq!(arc_owner(&offsets, 9), 3);
    }
}

/// #438 K-lane: lanes per batch (matches the matrix subsystem's K).
pub const TREE_LANES: usize = 8;
const LANE_NONE: u32 = u32::MAX;

/// #438 K-lane batched settle: up to [`TREE_LANES`] sources share ONE union
/// selection and ONE restricted descending-rank scan — each DOWN arc's topo
/// data is read once and relaxed for all lanes (the scan was 73% of tree CPU).
/// Per-source UP sweeps seed the lanes; UP parent chains are NOT retained —
/// before backtracking lane `k`, the caller MUST [`tree_resweep`]`(sources[k])`
/// (sweeps are ~14%, paid twice by design).
pub fn tree_settle_restricted_batch(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev: &DownReverseAdjFlat,
    sources: &[u32],
    union_targets: &[u32],
) -> TreeSettle {
    let n = topo.n_nodes as usize;
    let k_lanes = sources.len();
    if k_lanes == 0 || k_lanes > TREE_LANES || sources.iter().any(|&s| (s as usize) >= n) {
        return TreeSettle::BadOrigin;
    }
    TREE_SCRATCH.with(|cell| {
        cell.with_or_init(
            || TreeScratch::new(n),
            |s| {
                if s.dist.len() != n {
                    *s = TreeScratch::new(n);
                }
                // Selection: union reverse-DOWN ancestry of ALL lanes' targets.
                s.start_selection();
                let mut stack: Vec<u32> = Vec::with_capacity(union_targets.len() * 4);
                for &t in union_targets {
                    let ti = t as usize;
                    if ti < n && s.sel_gen[ti] != s.sel_epoch {
                        s.sel_gen[ti] = s.sel_epoch;
                        s.sel.push(t);
                        stack.push(t);
                    }
                }
                while let Some(v) = stack.pop() {
                    let start = down_rev.offsets[v as usize] as usize;
                    let end = down_rev.offsets[v as usize + 1] as usize;
                    for slot in start..end {
                        let u = down_rev.sources[slot];
                        let ui = u as usize;
                        if s.sel_gen[ui] != s.sel_epoch {
                            s.sel_gen[ui] = s.sel_epoch;
                            s.sel.push(u);
                            stack.push(u);
                        }
                    }
                }
                s.sel.sort_unstable_by(|a, b| b.cmp(a));
                let sel_n = s.sel.len();
                // Positions AFTER sorting (sel_pos[node] = idx+1, sel_gen-gated).
                for (i, &u) in s.sel.iter().enumerate() {
                    s.sel_pos[u as usize] = (i + 1) as u32;
                }

                // Lane arrays (compact, cache-resident).
                s.lane_dist.clear();
                s.lane_dist.resize(k_lanes * sel_n, u32::MAX);
                s.lane_parent.clear();
                s.lane_parent.resize(k_lanes * sel_n, LANE_NONE);

                // Seed: per-source UP sweep (single-lane scratch), copy dists
                // of selection nodes into the lane.
                let sel_snapshot = std::mem::take(&mut s.sel);
                for (k, &src) in sources.iter().enumerate() {
                    s.start_tree(src, u32::MAX);
                    s.set(src as usize, 0, 0);
                    upward_sweep_body(s, topo, weights, down_rev, src);
                    let base = k * sel_n;
                    for (i, &u) in sel_snapshot.iter().enumerate() {
                        let d = s.get(u as usize);
                        if d != u32::MAX {
                            s.lane_dist[base + i] = d;
                        }
                    }
                }
                s.sel = sel_snapshot;

                // ONE restricted scan, all lanes per arc.
                let sel_view = std::mem::take(&mut s.sel);
                for (i, &u) in sel_view.iter().enumerate() {
                    let ui = u as usize;
                    let start = topo.down_offsets[ui] as usize;
                    let end = topo.down_offsets[ui + 1] as usize;
                    for arc in start..end {
                        let w = weights.down.get(arc);
                        if w == u32::MAX {
                            continue;
                        }
                        let v = topo.down_targets[arc] as usize;
                        if s.sel_gen[v] != s.sel_epoch {
                            continue; // outside selection — dead end
                        }
                        let j = (s.sel_pos[v] - 1) as usize;
                        for k in 0..k_lanes {
                            let di = k * sel_n + i;
                            let d = s.lane_dist[di];
                            if d == u32::MAX {
                                continue;
                            }
                            let nd = d.saturating_add(w);
                            let dj = k * sel_n + j;
                            if nd < s.lane_dist[dj] {
                                s.lane_dist[dj] = nd;
                                s.lane_parent[dj] = arc as u32;
                            }
                        }
                    }
                }
                s.sel = sel_view;
                TreeSettle::Ok
            },
        )
    })
}

/// #438 K-lane: re-run the UP sweep for ONE lane's source into the
/// single-lane scratch so [`tree_lane_backtrack`] can reconstruct UP parent
/// chains. MUST be called (same thread) after the batch settle and before
/// that lane's backtracks; does NOT touch the lane arrays or selection.
pub fn tree_resweep(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev: &DownReverseAdjFlat,
    source: u32,
) {
    let n = topo.n_nodes as usize;
    TREE_SCRATCH.with(|cell| {
        cell.with_or_init(
            || TreeScratch::new(n),
            |s| {
                if s.dist.len() != n {
                    *s = TreeScratch::new(n);
                }
                s.start_tree(source, u32::MAX);
                s.set(source as usize, 0, 0);
                upward_sweep_body(s, topo, weights, down_rev, source);
            },
        )
    });
}

/// #438 K-lane: backtrack `target` out of lane `k` (DOWN chain from the lane
/// arrays; UP chain from the single-lane scratch left by [`tree_resweep`]).
pub fn tree_lane_backtrack(topo: &CchTopo, k: usize, source: u32, target: u32) -> Option<TreePath> {
    let n = topo.n_nodes as usize;
    TREE_SCRATCH.with(|cell| {
        cell.with_or_init(
            || TreeScratch::new(n),
            |s| {
                if s.dist.len() != n || s.origin != source {
                    return None; // resweep missing/mismatched — caller falls back
                }
                let sel_n = s.sel.len();
                let ti = target as usize;
                if ti >= n || s.sel_gen[ti] != s.sel_epoch {
                    return None;
                }
                let tpos = (s.sel_pos[ti] - 1) as usize;
                let dist = s.lane_dist[k * sel_n + tpos];
                if dist == u32::MAX {
                    return None;
                }
                if source == target {
                    return Some(TreePath {
                        distance: 0,
                        apex: source,
                        forward_parent: vec![],
                        backward_parent: vec![],
                    });
                }
                // DOWN chain: walk lane parents target→apex.
                let mut backward: Vec<(u32, u32)> = Vec::new();
                let mut cur_pos = tpos;
                let mut cur_node = target;
                for _ in 0..100_000 {
                    let lp = s.lane_parent[k * sel_n + cur_pos];
                    if lp == LANE_NONE {
                        break; // apex (seeded by the UP sweep)
                    }
                    let arc = lp as usize;
                    let src = arc_owner(&topo.down_offsets, arc) as u32;
                    backward.push((src, lp));
                    cur_node = src;
                    if s.sel_gen[cur_node as usize] != s.sel_epoch {
                        return None; // corrupt
                    }
                    cur_pos = (s.sel_pos[cur_node as usize] - 1) as usize;
                }
                let apex = cur_node;
                // UP chain: from the resweep's single-lane parents apex→source.
                if s.get(apex as usize) == u32::MAX {
                    return None; // resweep didn't reach apex — inconsistent
                }
                let mut forward_rev: Vec<(u32, u32)> = Vec::new();
                let mut cur = apex;
                for _ in 0..100_000 {
                    if cur == source {
                        forward_rev.reverse();
                        return Some(TreePath {
                            distance: dist,
                            apex,
                            forward_parent: forward_rev,
                            backward_parent: backward,
                        });
                    }
                    if s.generation[cur as usize] != s.epoch {
                        return None;
                    }
                    let tagged = s.parent[cur as usize];
                    if tagged & DOWN_BIT != 0 {
                        return None; // UP chain can't contain DOWN arcs
                    }
                    let arc = (tagged & ARC_MASK) as usize;
                    let srcn = arc_owner(&topo.up_offsets, arc) as u32;
                    forward_rev.push((srcn, tagged & ARC_MASK));
                    cur = srcn;
                }
                None
            },
        )
    })
}

/// #438: the exhaustive UP sweep (lever-1: reused 4-ary decrease-key heap +
/// stall-on-demand) against the single-lane scratch. Shared by the
/// single-source settle, the K-lane seeding pass, and [`tree_resweep`].
fn upward_sweep_body(
    s: &mut TreeScratch,
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev: &DownReverseAdjFlat,
    origin: u32,
) {
    s.pq.clear();
    s.push_up(origin, 0);
    while let Some((d, u)) = s.pop_up() {
        if d > s.get(u as usize) {
            continue; // stale
        }
        let rs = down_rev.offsets[u as usize] as usize;
        let re = down_rev.offsets[u as usize + 1] as usize;
        let mut stalled = false;
        for slot in rs..re {
            let x = down_rev.sources[slot] as usize;
            if s.generation[x] == s.epoch {
                let dx = s.dist[x];
                if dx != u32::MAX && dx.saturating_add(down_rev.weights.get(slot)) < d {
                    stalled = true;
                    break;
                }
            }
        }
        if stalled {
            continue;
        }
        let start = topo.up_offsets[u as usize] as usize;
        let end = topo.up_offsets[u as usize + 1] as usize;
        for i in start..end {
            let w = weights.up.get(i);
            if w == u32::MAX {
                continue;
            }
            let v = topo.up_targets[i];
            let nd = d.saturating_add(w);
            if nd < s.get(v as usize) {
                s.set(v as usize, nd, i as u32); // UP arc
                s.push_up(v, nd);
            }
        }
    }
}
