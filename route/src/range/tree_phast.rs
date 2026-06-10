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
use crate::server::evictable::EvictableCell;
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

    #[inline]
    fn set(&mut self, node: usize, dist: u32, tagged_arc: u32) {
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
pub fn tree_settle_restricted(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev: &crate::matrix::bucket_ch::DownReverseAdjFlat,
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

                // ---- Phase 1: exhaustive upward PQ sweep (UP parents) ----
                let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
                pq.push(Reverse((0, origin)));
                while let Some(Reverse((d, u))) = pq.pop() {
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
                            s.set(v as usize, nd, i as u32); // UP arc
                            pq.push(Reverse((nd, v)));
                        }
                    }
                }

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
