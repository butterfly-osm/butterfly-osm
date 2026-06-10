//! CCH query algorithm - bidirectional Dijkstra on hierarchy
//!
//! Uses thread-local generation-stamped state to eliminate O(|V|)
//! allocation per query. Distance and parent arrays are allocated
//! once per thread and reused across queries via version stamping.

use crate::formats::CchTopo;
use crate::matrix::bucket_ch::{DAryHeap, DownReverseAdjFlat, INVALID_HANDLE, UpAdjFlat};
use crate::profile_abi::Mode;

/// Local alias for the shared `bucket_ch::INVALID_HANDLE` sentinel
/// (both `u32::MAX`). The alias is `pub(crate)` so the matrix-side
/// bucket code and the CCH query-side code can refer to a single
/// canonical "no live heap handle" marker — see #317 review. Kept
/// as an alias rather than `pub use` so call sites in this module
/// stay terse without forcing the rest of the crate to spell out the
/// full `bucket_ch::INVALID_HANDLE` path.
pub(crate) const HANDLE_NONE: u32 = INVALID_HANDLE;

use super::state::{CchWeights, ServerState};

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub distance: u32,
    pub meeting_node: u32,
    pub forward_parent: Vec<(u32, u32)>,
    pub backward_parent: Vec<(u32, u32)>,
}

// =============================================================================
// THREAD-LOCAL CCH QUERY STATE (eliminates ~80MB allocation per query)
// =============================================================================

/// Thread-local CCH query state with generation stamping.
/// Eliminates O(|V|) initialization per query by using version stamps.
///
/// For Belgium (~5M EBG nodes), this avoids allocating:
/// - 2 × 20MB distance arrays
/// - 2 × 40MB parent arrays
///
/// per query. Instead, these are allocated once per thread and reused.
struct CchQueryState {
    /// Forward distance array (persistent across queries)
    dist_fwd: Vec<u32>,
    /// Backward distance array
    dist_bwd: Vec<u32>,
    /// Forward parent: packed (prev_node, edge_idx)
    parent_fwd: Vec<(u32, u32)>,
    /// Backward parent: packed (prev_node, edge_idx)
    parent_bwd: Vec<(u32, u32)>,
    /// Version stamp per node for forward search
    gen_fwd: Vec<u32>,
    /// Version stamp per node for backward search
    gen_bwd: Vec<u32>,
    /// Per-direction generation epochs (#438). Split from a single
    /// `current_gen` so the shared-forward edges_batch path can FREEZE a
    /// forward tree (`gen_fwd_epoch` fixed) while resetting the backward
    /// search once per target (`gen_bwd_epoch` bumped). `start_query`
    /// bumps BOTH, so every existing bidirectional caller is byte-identical;
    /// `start_backward_only` bumps only the backward epoch.
    gen_fwd_epoch: u32,
    gen_bwd_epoch: u32,
    /// Forward 4-ary heap (decrease-key) — replaces PriorityQueue
    /// (codex #291). Heap entries are `(weight, node_id)` where node_id
    /// is a usize-cast u32. `handles_fwd[node]` is the heap position
    /// only when the node is in-heap *this query*; see the comment on
    /// `handles_fwd` below for the full invariant.
    pq_fwd: DAryHeap,
    pq_bwd: DAryHeap,
    /// Per-node forward heap handle.
    ///
    /// **Not globally valid.** For any node where `gen_fwd[node] !=
    /// current_gen` the handle slot may still carry a stale value left
    /// over from a previous query — only the gen check gives it
    /// meaning. `set_fwd` resets the slot to `HANDLE_NONE` on first
    /// touch this query and `DAryHeap::pop` clears it again on
    /// settlement, so the "in-heap-now" predicate `gen_fwd[node] ==
    /// current_gen && handles_fwd[node] != HANDLE_NONE` is always
    /// sound for callers that read it.
    ///
    /// In practice the callers in this file (`push_fwd`,
    /// `is_stalled_fwd`) only read `handles_fwd` immediately after a
    /// `set_fwd` for the same node, so the gen check is implicit and
    /// the field-level read of `handles_fwd[node] != HANDLE_NONE`
    /// alone is correct.
    ///
    /// PR #317 review: dropped the separate `handles_*_gen` arrays
    /// (~40 MB/thread on Belgium) by folding staleness into `set_fwd`.
    handles_fwd: Vec<u32>,
    handles_bwd: Vec<u32>,
}

impl CchQueryState {
    fn new(n_nodes: usize) -> Self {
        Self {
            dist_fwd: vec![u32::MAX; n_nodes],
            dist_bwd: vec![u32::MAX; n_nodes],
            parent_fwd: vec![(u32::MAX, 0); n_nodes],
            parent_bwd: vec![(u32::MAX, 0); n_nodes],
            gen_fwd: vec![0; n_nodes],
            gen_bwd: vec![0; n_nodes],
            gen_fwd_epoch: 0,
            gen_bwd_epoch: 0,
            pq_fwd: DAryHeap::new(1024),
            pq_bwd: DAryHeap::new(1024),
            handles_fwd: vec![HANDLE_NONE; n_nodes],
            handles_bwd: vec![HANDLE_NONE; n_nodes],
        }
    }

    /// Start a new bidirectional query (O(1) instead of O(n)).
    ///
    /// Bumps BOTH direction epochs, so the behaviour for the existing
    /// bidirectional callers (`query_with_debug`, `distance_bounded`) is
    /// byte-identical to the pre-#438 single-`current_gen` design.
    #[inline]
    fn start_query(&mut self) {
        self.gen_fwd_epoch = self.gen_fwd_epoch.wrapping_add(1);
        self.gen_bwd_epoch = self.gen_bwd_epoch.wrapping_add(1);
        if self.gen_fwd_epoch == 0 || self.gen_bwd_epoch == 0 {
            // Overflow — reset all versions (rare, every ~4B queries).
            // After reset we also wipe the handle arrays so the
            // post-overflow first query starts fully clean (otherwise
            // a node visited just before overflow could carry a stale
            // non-HANDLE_NONE entry into the first query of the next
            // generation cycle, where set_* clears it via the gen
            // check — but that check would now misfire since gen
            // was just reset to 0).
            self.gen_fwd.iter_mut().for_each(|v| *v = 0);
            self.gen_bwd.iter_mut().for_each(|v| *v = 0);
            self.handles_fwd.iter_mut().for_each(|h| *h = HANDLE_NONE);
            self.handles_bwd.iter_mut().for_each(|h| *h = HANDLE_NONE);
            self.gen_fwd_epoch = 1;
            self.gen_bwd_epoch = 1;
        }
        self.pq_fwd.clear();
        self.pq_bwd.clear();
        // handles_fwd / handles_bwd carry over: set_* clears stale
        // entries on first touch this query.
    }

    /// #438: reset ONLY the backward search, preserving a frozen forward
    /// tree (`gen_fwd_epoch` and the forward arrays untouched). Used by
    /// `backward_meet_and_paths` to run many per-target backward searches
    /// against one shared `settle_forward` result.
    #[inline]
    fn start_backward_only(&mut self) {
        self.gen_bwd_epoch = self.gen_bwd_epoch.wrapping_add(1);
        if self.gen_bwd_epoch == 0 {
            self.gen_bwd.iter_mut().for_each(|v| *v = 0);
            self.handles_bwd.iter_mut().for_each(|h| *h = HANDLE_NONE);
            self.gen_bwd_epoch = 1;
        }
        self.pq_bwd.clear();
    }

    /// #438: start a fresh FORWARD-only search (bump only the forward
    /// epoch), leaving the backward arrays for `start_backward_only` to
    /// reset per target. Used by `settle_forward`.
    #[inline]
    fn start_forward_only(&mut self) {
        self.gen_fwd_epoch = self.gen_fwd_epoch.wrapping_add(1);
        if self.gen_fwd_epoch == 0 {
            self.gen_fwd.iter_mut().for_each(|v| *v = 0);
            self.handles_fwd.iter_mut().for_each(|h| *h = HANDLE_NONE);
            self.gen_fwd_epoch = 1;
        }
        self.pq_fwd.clear();
    }

    /// Push `node` onto the forward heap with weight `dist`. Caller
    /// must have called `set_fwd(node, …)` first; that's where stale
    /// handles get cleared on first-touch-this-query.
    #[inline]
    fn push_fwd(&mut self, node: u32, dist: u32) {
        if self.handles_fwd[node as usize] != HANDLE_NONE {
            // Live handle this query — decrease.
            let handle = self.handles_fwd[node as usize];
            self.pq_fwd
                .decrease(handle, dist, node, &mut self.handles_fwd);
        } else {
            // Fresh insert (DAryHeap::push sets handles_fwd[node]).
            self.pq_fwd.push(dist, node, &mut self.handles_fwd);
        }
    }

    #[inline]
    fn push_bwd(&mut self, node: u32, dist: u32) {
        if self.handles_bwd[node as usize] != HANDLE_NONE {
            let handle = self.handles_bwd[node as usize];
            self.pq_bwd
                .decrease(handle, dist, node, &mut self.handles_bwd);
        } else {
            self.pq_bwd.push(dist, node, &mut self.handles_bwd);
        }
    }

    /// Pop the minimum-weight forward heap entry. Returns `(weight, node)`.
    /// `DAryHeap::pop` clears the popped node's `handles_fwd` slot to
    /// `HANDLE_NONE` so subsequent operations recognise this node as
    /// "not in heap".
    #[inline]
    fn pop_fwd(&mut self) -> Option<(u32, u32)> {
        self.pq_fwd.pop(&mut self.handles_fwd)
    }

    #[inline]
    fn pop_bwd(&mut self) -> Option<(u32, u32)> {
        self.pq_bwd.pop(&mut self.handles_bwd)
    }

    // Forward distance accessors
    #[inline]
    fn get_fwd(&self, node: usize) -> u32 {
        if self.gen_fwd[node] == self.gen_fwd_epoch {
            self.dist_fwd[node]
        } else {
            u32::MAX
        }
    }

    #[inline]
    fn set_fwd(&mut self, node: usize, dist: u32, parent: (u32, u32)) {
        // PR #317 review: on first touch this query, clear the stale
        // handle slot. Any push that follows then correctly observes
        // HANDLE_NONE and pushes fresh instead of decrease-keying a
        // dead handle from a previous query's heap.
        if self.gen_fwd[node] != self.gen_fwd_epoch {
            self.handles_fwd[node] = HANDLE_NONE;
        }
        self.dist_fwd[node] = dist;
        self.parent_fwd[node] = parent;
        self.gen_fwd[node] = self.gen_fwd_epoch;
    }

    // Backward distance accessors
    #[inline]
    fn get_bwd(&self, node: usize) -> u32 {
        if self.gen_bwd[node] == self.gen_bwd_epoch {
            self.dist_bwd[node]
        } else {
            u32::MAX
        }
    }

    #[inline]
    fn set_bwd(&mut self, node: usize, dist: u32, parent: (u32, u32)) {
        if self.gen_bwd[node] != self.gen_bwd_epoch {
            self.handles_bwd[node] = HANDLE_NONE;
        }
        self.dist_bwd[node] = dist;
        self.parent_bwd[node] = parent;
        self.gen_bwd[node] = self.gen_bwd_epoch;
    }
}

thread_local! {
    /// Single thread-local CCH query state. Re-initializes when n_nodes
    /// changes. #409/#410: an `EvictableCell` so the idle-compactor can
    /// free this ~185 MB arena regardless of whether the owning thread
    /// is a Tokio worker (where `/route` runs inline) or a rayon worker.
    static CCH_QUERY_STATE: crate::server::evictable::EvictableCell<CchQueryState> =
        const { crate::server::evictable::EvictableCell::new() };
}

/// Reconstruct path from generation-stamped parent arrays
fn reconstruct_path_versioned(
    parent: &[(u32, u32)],
    generation: &[u32],
    current_gen: u32,
    start: u32,
    end: u32,
) -> Vec<(u32, u32)> {
    let mut path = Vec::new();
    let mut current = end;

    while current != start {
        if generation[current as usize] == current_gen {
            let (prev, edge_idx) = parent[current as usize];
            path.push((current, edge_idx));
            current = prev;
        } else {
            break;
        }
    }

    path.reverse();
    path
}

/// Backend selection for `CchQuery` weight reads.
///
/// `Flats` is the post-#149 hot path: the bidirectional search reads
/// weights, parent middles, and topo back-references straight from the
/// `UpAdjFlat`/`DownReverseAdjFlat` that the matrix subsystem already
/// keeps in heap. `cch_weights.up`/`.down` is never touched on this
/// path, which is what makes `madvise(MADV_DONTNEED)` over those byte
/// ranges actually reclaim RSS.
///
/// `CustomWeights` is the cold path used by `with_custom_weights`,
/// i.e. alternative-route penalties and exclude/avoid recustomizations
/// that build per-call weight arrays. After #152 it reuses the SAME
/// `UpAdjFlat`/`DownReverseAdjFlat` topology that the hot path uses —
/// the flats already carry `topo_edge_idx`, so we look up custom
/// weights via `weights.up.get(topo_edge_idx)` / `weights.down.get(topo_edge_idx)`
/// and ignore the flat's embedded weights. This eliminates the
/// duplicate `DownReverseAdj` Vec-of-Vec topology that #149 left
/// stranded on the heap (~320 MB on Belgium across 4 modes).
///
/// The custom-weight loop incurs an INF branch (custom weights may
/// have INF entries from exclude/avoid recustomization); the hot
/// path's flats backend has those edges filtered out at build time.
enum Backend<'a> {
    Flats {
        up_adj_flat: &'a UpAdjFlat,
        down_rev_flat: &'a DownReverseAdjFlat,
    },
    CustomWeights {
        weights: &'a CchWeights,
        up_adj_flat: &'a UpAdjFlat,
        down_rev_flat: &'a DownReverseAdjFlat,
    },
}

/// Bidirectional CCH query.
///
/// After #152, the query reads its topology entirely through the flats
/// (`UpAdjFlat` / `DownReverseAdjFlat`); the `CchTopo` reference is no
/// longer plumbed through this type. Callers that need topology for
/// non-query work (e.g. unpack) hold their own reference to the same
/// `cch_topo` they passed when building the flats.
pub struct CchQuery<'a> {
    backend: Backend<'a>,
    n_nodes: usize,
}

impl<'a> CchQuery<'a> {
    /// Build a query against the given mode's CCH flats.
    ///
    /// #402: The caller MUST hold an `Arc<ModeData>` (via
    /// `state.get_mode(mode)`) for the lifetime of the returned
    /// `CchQuery`. We borrow into that Arc here — letting it drop while
    /// the query is alive would be use-after-free, which the borrow
    /// checker enforces.
    pub fn new(mode_data: &'a super::state::ModeData) -> Self {
        Self {
            backend: Backend::Flats {
                up_adj_flat: &mode_data.up_adj_flat,
                down_rev_flat: &mode_data.down_rev_flat,
            },
            n_nodes: mode_data.cch_topo.n_nodes as usize,
        }
    }

    /// Iterate UP edges out of node `u`, yielding (target, weight, parent_edge_idx).
    ///
    /// `parent_edge_idx` is whatever the unpack code expects in
    /// `state.parent_fwd[v].1` — the topo edge index recovered via
    /// the flat's `topo_edge_idx[slot]`.
    ///
    /// The `Flats` backend never yields INF entries (filtered at
    /// build time). The `CustomWeights` backend reuses the same flat
    /// topology but reads weights from the caller-supplied
    /// `CchWeights`, which can carry INF entries from exclude/avoid
    /// recustomization — those are filtered inline.
    #[inline]
    fn for_up_edges<F: FnMut(u32, u32, u32)>(&self, u: u32, mut f: F) {
        match &self.backend {
            Backend::Flats { up_adj_flat, .. } => {
                let start = up_adj_flat.offsets[u as usize] as usize;
                let end = up_adj_flat.offsets[u as usize + 1] as usize;
                for slot in start..end {
                    let v = up_adj_flat.targets[slot];
                    let w = up_adj_flat.weights.get(slot);
                    let parent_idx = up_adj_flat.topo_edge_idx[slot];
                    f(v, w, parent_idx);
                }
            }
            Backend::CustomWeights {
                weights,
                up_adj_flat,
                ..
            } => {
                let start = up_adj_flat.offsets[u as usize] as usize;
                let end = up_adj_flat.offsets[u as usize + 1] as usize;
                for slot in start..end {
                    let parent_idx = up_adj_flat.topo_edge_idx[slot];
                    let w = weights.up.get(parent_idx as usize);
                    if w == u32::MAX {
                        continue;
                    }
                    let v = up_adj_flat.targets[slot];
                    f(v, w, parent_idx);
                }
            }
        }
    }

    /// Iterate reversed DOWN edges arriving at node `u` from a higher-rank
    /// source, yielding (source, weight, parent_edge_idx).
    #[inline]
    fn for_down_rev_edges<F: FnMut(u32, u32, u32)>(&self, u: u32, mut f: F) {
        match &self.backend {
            Backend::Flats { down_rev_flat, .. } => {
                let start = down_rev_flat.offsets[u as usize] as usize;
                let end = down_rev_flat.offsets[u as usize + 1] as usize;
                for slot in start..end {
                    let x = down_rev_flat.sources[slot];
                    let w = down_rev_flat.weights.get(slot);
                    let parent_idx = down_rev_flat.topo_edge_idx[slot];
                    f(x, w, parent_idx);
                }
            }
            Backend::CustomWeights {
                weights,
                down_rev_flat,
                ..
            } => {
                let start = down_rev_flat.offsets[u as usize] as usize;
                let end = down_rev_flat.offsets[u as usize + 1] as usize;
                for slot in start..end {
                    let parent_idx = down_rev_flat.topo_edge_idx[slot];
                    let w = weights.down.get(parent_idx as usize);
                    if w == u32::MAX {
                        continue;
                    }
                    let x = down_rev_flat.sources[slot];
                    f(x, w, parent_idx);
                }
            }
        }
    }

    /// Create a query with custom weights (for alternative routes with
    /// penalties, exclude/avoid recustomization, transit access/egress).
    ///
    /// Cold path. Reuses the same flat topology that the hot path
    /// uses (see #152) — `up_adj_flat` and `down_rev_flat` provide
    /// `topo_edge_idx`, which is the index into the caller-supplied
    /// `weights.up` / `weights.down`. The flat's embedded weights are
    /// not read in this backend; only its topology is.
    ///
    /// Caller invariant: the custom `weights` are derived from the same
    /// time metric as the flat (i.e. INF entries in `weights` are a
    /// superset of INF entries in the flat's source). Every current
    /// caller satisfies this — exclude/avoid only add INF, multiplicative
    /// alternative-route penalties keep INF as INF (`saturating_mul`),
    /// and the unmodified `cch_weights` paths are bytewise the flat's
    /// source. This invariant is upheld by construction; we filter INF
    /// entries inline in the hot loop as a defensive measure.
    pub fn with_custom_weights(
        topo: &'a CchTopo,
        up_adj_flat: &'a UpAdjFlat,
        down_rev_flat: &'a DownReverseAdjFlat,
        weights: &'a CchWeights,
    ) -> Self {
        Self {
            backend: Backend::CustomWeights {
                weights,
                up_adj_flat,
                down_rev_flat,
            },
            n_nodes: topo.n_nodes as usize,
        }
    }

    /// #272 forward stall-on-demand check — **test helper**.
    ///
    /// The hot-path forward branches in `query_with_debug` and
    /// `distance()` inline this body directly because hoisting the
    /// closure into a function call regresses measured p50 (the
    /// compiler does not fold the FnMut across the call boundary even
    /// with `#[inline(always)]`). This helper exists so tests can
    /// exercise the stall decision independent of the surrounding
    /// search loop.
    ///
    /// Returns `true` if popping `u` with forward distance `d` is on a
    /// non-shortest path — i.e. there exists a DOWN-predecessor `x`
    /// (a node with `x → u` as a DOWN edge, rank(x) > rank(u)) that
    /// forward search has already reached with `dist_fwd[x] + w(x→u) < d`.
    #[cfg(test)]
    fn is_stalled_fwd(&self, state: &CchQueryState, u: u32, d: u32) -> bool {
        let mut stalled = false;
        self.for_down_rev_edges(u, |x, w, _| {
            if stalled {
                return;
            }
            if state.gen_fwd[x as usize] == state.gen_fwd_epoch {
                let dx = state.dist_fwd[x as usize];
                if dx != u32::MAX && dx.saturating_add(w) < d {
                    stalled = true;
                }
            }
        });
        stalled
    }

    /// #272 backward stall-on-demand check — **test helper** (see
    /// [`Self::is_stalled_fwd`] for the rationale on inlining).
    ///
    /// Symmetric to the forward variant but checks for UP successors
    /// `v` (nodes with `u → v` as an UP edge, rank(v) > rank(u)) where
    /// backward search has already reached `v` with
    /// `dist_bwd[v] + w(u→v) < d`.
    #[cfg(test)]
    fn is_stalled_bwd(&self, state: &CchQueryState, u: u32, d: u32) -> bool {
        let mut stalled = false;
        self.for_up_edges(u, |v, w, _| {
            if stalled {
                return;
            }
            if state.gen_bwd[v as usize] == state.gen_bwd_epoch {
                let dv = state.dist_bwd[v as usize];
                if dv != u32::MAX && dv.saturating_add(w) < d {
                    stalled = true;
                }
            }
        });
        stalled
    }

    /// Run bidirectional query from source to target
    pub fn query(&self, source: u32, target: u32) -> Option<QueryResult> {
        self.query_with_debug(source, target, false)
    }

    /// Run bidirectional query with optional debug output
    pub fn query_with_debug(&self, source: u32, target: u32, debug: bool) -> Option<QueryResult> {
        if source == target {
            return Some(QueryResult {
                distance: 0,
                meeting_node: source,
                forward_parent: vec![],
                backward_parent: vec![],
            });
        }

        let n = self.n_nodes;

        CCH_QUERY_STATE.with(|cell| {
            cell.with_or_init(
                || CchQueryState::new(n),
                |state| {
                    // Reinitialize if n_nodes changed (e.g. graph swapped) or
                    // after the idle-compactor freed and we rebuilt fresh.
                    if state.dist_fwd.len() != n {
                        *state = CchQueryState::new(n);
                    }

                    // Start new query (O(1) instead of O(n) memset)
                    state.start_query();

                    // Initialize source and target
                    state.set_fwd(source as usize, 0, (source, 0));
                    state.set_bwd(target as usize, 0, (target, 0));
                    state.push_fwd(source, 0);
                    state.push_bwd(target, 0);

                    // Best meeting point
                    let mut best_dist = u32::MAX;
                    let mut meeting_node = u32::MAX;

                    // Debug counters
                    let mut fwd_settled = 0usize;
                    let mut bwd_settled = 0usize;
                    let mut fwd_relaxed = 0usize;
                    let mut bwd_relaxed = 0usize;

                    // Bidirectional search with early termination
                    while !state.pq_fwd.is_empty() || !state.pq_bwd.is_empty() {
                        // Early termination: if both queue minimums exceed best_dist, stop
                        let fwd_min = state.pq_fwd.peek_min_weight().unwrap_or(u32::MAX);
                        let bwd_min = state.pq_bwd.peek_min_weight().unwrap_or(u32::MAX);
                        if fwd_min >= best_dist && bwd_min >= best_dist {
                            break;
                        }

                        // Forward step — search UP graph. #272 stall-on-demand
                        // body is inlined (not hoisted to `is_stalled_fwd`)
                        // because hoisting prevents the compiler from folding
                        // the closure into the outer loop, costing the measured
                        // p50 speedup.
                        if let Some((d, u)) = state.pop_fwd() {
                            if d > state.get_fwd(u as usize) {
                                // Stale entry — skip
                            } else {
                                let mut stalled = false;
                                self.for_down_rev_edges(u, |x, w, _| {
                                    if stalled {
                                        return;
                                    }
                                    if state.gen_fwd[x as usize] == state.gen_fwd_epoch {
                                        let dx = state.dist_fwd[x as usize];
                                        if dx != u32::MAX && dx.saturating_add(w) < d {
                                            stalled = true;
                                        }
                                    }
                                });
                                if !stalled {
                                    fwd_settled += 1;

                                    // Check meeting point when settling a node
                                    let bwd_d = state.get_bwd(u as usize);
                                    if bwd_d != u32::MAX {
                                        let total = d.saturating_add(bwd_d);
                                        if total < best_dist {
                                            best_dist = total;
                                            meeting_node = u;
                                            if debug {
                                                tracing::debug!(
                                                    meet_node = u,
                                                    dist_fwd = d,
                                                    dist_bwd = bwd_d,
                                                    total,
                                                    "FWD meet"
                                                );
                                            }
                                        }
                                    }

                                    // Relax UP edges
                                    self.for_up_edges(u, |v, w, edge_idx| {
                                        fwd_relaxed += 1;
                                        let new_dist = d.saturating_add(w);
                                        if new_dist < state.get_fwd(v as usize) {
                                            state.set_fwd(v as usize, new_dist, (u, edge_idx));
                                            state.push_fwd(v, new_dist);

                                            // Check meeting when updating
                                            let bwd_v = state.get_bwd(v as usize);
                                            if bwd_v != u32::MAX {
                                                let total = new_dist.saturating_add(bwd_v);
                                                if total < best_dist {
                                                    best_dist = total;
                                                    meeting_node = v;
                                                    if debug {
                                                        tracing::debug!(
                                                            meet_node = v,
                                                            dist_fwd = new_dist,
                                                            dist_bwd = bwd_v,
                                                            total,
                                                            "FWD meet via edge"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    });
                                }
                            }
                        }

                        // Backward step — traverse reversed DOWN edges
                        // (= upward in reversed graph). #272 stall body inlined
                        // for the same reason as the forward branch above.
                        if let Some((d, u)) = state.pop_bwd() {
                            if d > state.get_bwd(u as usize) {
                                // Stale — skip
                            } else {
                                let mut stalled = false;
                                self.for_up_edges(u, |v, w, _| {
                                    if stalled {
                                        return;
                                    }
                                    if state.gen_bwd[v as usize] == state.gen_bwd_epoch {
                                        let dv = state.dist_bwd[v as usize];
                                        if dv != u32::MAX && dv.saturating_add(w) < d {
                                            stalled = true;
                                        }
                                    }
                                });
                                if stalled {
                                    continue;
                                }
                                bwd_settled += 1;

                                // Check meeting point
                                let fwd_d = state.get_fwd(u as usize);
                                if fwd_d != u32::MAX {
                                    let total = d.saturating_add(fwd_d);
                                    if total < best_dist {
                                        best_dist = total;
                                        meeting_node = u;
                                        if debug {
                                            tracing::debug!(
                                                meet_node = u,
                                                dist_fwd = fwd_d,
                                                dist_bwd = d,
                                                total,
                                                "BWD meet"
                                            );
                                        }
                                    }
                                }

                                // Relax reverse DOWN edges
                                self.for_down_rev_edges(u, |x, w, edge_idx| {
                                    bwd_relaxed += 1;
                                    let new_dist = d.saturating_add(w);
                                    if new_dist < state.get_bwd(x as usize) {
                                        state.set_bwd(x as usize, new_dist, (u, edge_idx));
                                        state.push_bwd(x, new_dist);

                                        // Check meeting when updating
                                        let fwd_x = state.get_fwd(x as usize);
                                        if fwd_x != u32::MAX {
                                            let total = new_dist.saturating_add(fwd_x);
                                            if total < best_dist {
                                                best_dist = total;
                                                meeting_node = x;
                                                if debug {
                                                    tracing::debug!(
                                                        meet_node = x,
                                                        dist_fwd = fwd_x,
                                                        dist_bwd = new_dist,
                                                        total,
                                                        "BWD meet via edge"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }

                    if debug {
                        tracing::debug!(
                            fwd_settled,
                            bwd_settled,
                            fwd_relaxed,
                            bwd_relaxed,
                            best_dist,
                            meeting_node,
                            "CCH bidir final"
                        );
                    }

                    if best_dist == u32::MAX {
                        return None;
                    }

                    // Reconstruct paths using generation-stamped parent arrays
                    let forward_parent = reconstruct_path_versioned(
                        &state.parent_fwd,
                        &state.gen_fwd,
                        state.gen_fwd_epoch,
                        source,
                        meeting_node,
                    );
                    let backward_parent = reconstruct_path_versioned(
                        &state.parent_bwd,
                        &state.gen_bwd,
                        state.gen_bwd_epoch,
                        target,
                        meeting_node,
                    );

                    Some(QueryResult {
                        distance: best_dist,
                        meeting_node,
                        forward_parent,
                        backward_parent,
                    })
                },
            )
        })
    }

    /// #438: forward-only UP settle to EXHAUSTION from `source`, frozen in
    /// the thread-local query state for reuse across many targets via
    /// [`Self::backward_meet_and_paths`]. No early termination (no target
    /// yet) and no meeting checks. The relax + stall-on-demand body mirrors
    /// the forward branch of [`Self::query_with_debug`] exactly, so the
    /// settled distances + parent pointers are identical to a per-pair
    /// forward — only computed ONCE and reused for all of the source's
    /// targets.
    ///
    /// MUST be followed (same thread, no intervening `query`/`settle_forward`)
    /// by one or more `backward_meet_and_paths(source, target)` calls.
    ///
    /// Invariant (relied on by the source-grouped edges path): the whole group
    /// — this settle + all its `backward_meet_and_paths` — must finish before
    /// the #402 idle-compactor could evict the thread-local `CchQueryState`. In
    /// practice that holds trivially (the compactor threshold is seconds; a
    /// group's targets complete in microseconds, and each call re-stamps the
    /// cell's last-touch). If eviction ever did fire mid-group it is only a
    /// performance loss, not a correctness bug: the frozen forward is gone, so
    /// each target returns `None` and falls back to the per-pair path, which
    /// recomputes the same result.
    pub fn settle_forward(&self, source: u32) {
        let n = self.n_nodes;
        CCH_QUERY_STATE.with(|cell| {
            cell.with_or_init(
                || CchQueryState::new(n),
                |state| {
                    if state.dist_fwd.len() != n {
                        *state = CchQueryState::new(n);
                    }
                    state.start_forward_only();
                    state.set_fwd(source as usize, 0, (source, 0));
                    state.push_fwd(source, 0);

                    while let Some((d, u)) = state.pop_fwd() {
                        if d > state.get_fwd(u as usize) {
                            continue; // stale
                        }
                        // Stall-on-demand (mirrors query_with_debug fwd branch).
                        let mut stalled = false;
                        self.for_down_rev_edges(u, |x, w, _| {
                            if stalled {
                                return;
                            }
                            if state.gen_fwd[x as usize] == state.gen_fwd_epoch {
                                let dx = state.dist_fwd[x as usize];
                                if dx != u32::MAX && dx.saturating_add(w) < d {
                                    stalled = true;
                                }
                            }
                        });
                        if stalled {
                            continue;
                        }
                        self.for_up_edges(u, |v, w, edge_idx| {
                            let new_dist = d.saturating_add(w);
                            if new_dist < state.get_fwd(v as usize) {
                                state.set_fwd(v as usize, new_dist, (u, edge_idx));
                                state.push_fwd(v, new_dist);
                            }
                        });
                    }
                },
            )
        });
    }

    /// #438: run a BACKWARD search from `target` against the frozen forward
    /// tree left by [`Self::settle_forward`]`(source)`, find the min-cost
    /// meeting node, and reconstruct the forward + backward parent chains.
    /// Returns the same [`QueryResult`] shape as [`Self::query`] (so the
    /// edges_batch unpack is unchanged), or `None` if unreachable.
    ///
    /// Cost: this pays only the backward half + reconstruct — the forward
    /// half is amortised across all of the source's targets. The min-cost
    /// DISTANCE is identical to the per-pair `query`; the exact equal-cost
    /// path may differ on ties (both are valid time-shortest paths).
    ///
    /// MUST be called after `settle_forward(source)` on the SAME thread with
    /// no intervening `query`/`settle_forward` (those bump the forward epoch
    /// and erase the frozen tree).
    pub fn backward_meet_and_paths(&self, source: u32, target: u32) -> Option<QueryResult> {
        if source == target {
            return Some(QueryResult {
                distance: 0,
                meeting_node: source,
                forward_parent: vec![],
                backward_parent: vec![],
            });
        }
        let n = self.n_nodes;
        CCH_QUERY_STATE.with(|cell| {
            cell.with_or_init(
                || CchQueryState::new(n),
                |state| {
                    // #438 review: guard against a graph swap between the
                    // settle_forward and this call (matches settle_forward /
                    // query_with_debug), AND (Copilot review) fail closed when
                    // the frozen forward tree doesn't belong to `source` —
                    // out-of-sequence call, evicted scratch, or a different
                    // source. Reading a foreign tree would compute garbage
                    // meets (or loop on a foreign root sentinel during
                    // reconstruct). The settled root always has dist 0, so the
                    // guard is one stamped read. `None` → the caller's
                    // per-pair fallback recomputes correctly.
                    if state.dist_fwd.len() != n || state.get_fwd(source as usize) != 0 {
                        return None;
                    }
                    state.start_backward_only();
                    state.set_bwd(target as usize, 0, (target, 0));
                    state.push_bwd(target, 0);

                    let mut best_dist = u32::MAX;
                    let mut meeting_node = u32::MAX;

                    // The target may itself sit in the frozen forward tree.
                    let fwd_t = state.get_fwd(target as usize);
                    if fwd_t != u32::MAX {
                        best_dist = fwd_t;
                        meeting_node = target;
                    }

                    while let Some((d, u)) = state.pop_bwd() {
                        if d >= best_dist {
                            break; // early termination — frontier can't improve
                        }
                        if d > state.get_bwd(u as usize) {
                            continue; // stale
                        }
                        // Backward stall-on-demand (mirrors query_with_debug bwd branch).
                        let mut stalled = false;
                        self.for_up_edges(u, |v, w, _| {
                            if stalled {
                                return;
                            }
                            if state.gen_bwd[v as usize] == state.gen_bwd_epoch {
                                let dv = state.dist_bwd[v as usize];
                                if dv != u32::MAX && dv.saturating_add(w) < d {
                                    stalled = true;
                                }
                            }
                        });
                        if stalled {
                            continue;
                        }
                        // Meet against the frozen forward at settle.
                        let fwd_d = state.get_fwd(u as usize);
                        if fwd_d != u32::MAX {
                            let total = d.saturating_add(fwd_d);
                            if total < best_dist {
                                best_dist = total;
                                meeting_node = u;
                            }
                        }
                        self.for_down_rev_edges(u, |x, w, edge_idx| {
                            let new_dist = d.saturating_add(w);
                            if new_dist < state.get_bwd(x as usize) {
                                state.set_bwd(x as usize, new_dist, (u, edge_idx));
                                state.push_bwd(x, new_dist);
                                let fwd_x = state.get_fwd(x as usize);
                                if fwd_x != u32::MAX {
                                    let total = new_dist.saturating_add(fwd_x);
                                    if total < best_dist {
                                        best_dist = total;
                                        meeting_node = x;
                                    }
                                }
                            }
                        });
                    }

                    if best_dist == u32::MAX {
                        return None;
                    }
                    let forward_parent = reconstruct_path_versioned(
                        &state.parent_fwd,
                        &state.gen_fwd,
                        state.gen_fwd_epoch,
                        source,
                        meeting_node,
                    );
                    let backward_parent = reconstruct_path_versioned(
                        &state.parent_bwd,
                        &state.gen_bwd,
                        state.gen_bwd_epoch,
                        target,
                        meeting_node,
                    );
                    Some(QueryResult {
                        distance: best_dist,
                        meeting_node,
                        forward_parent,
                        backward_parent,
                    })
                },
            )
        })
    }
}

/// One-to-many query for distance matrix
pub fn query_one_to_many(
    state: &ServerState,
    mode: Mode,
    source: u32,
    targets: &[u32],
) -> Vec<Option<u32>> {
    let mode_data = state.get_mode(mode);
    let query = CchQuery::new(&mode_data);
    targets
        .iter()
        .map(|&t| query.query(source, t).map(|r| r.distance))
        .collect()
}

impl CchQuery<'_> {
    /// Distance-only bidirectional query.
    ///
    /// Same bidirectional bounded-search algorithm as `query()` but
    /// **skips path reconstruction**. Used by the `/transit` handler's
    /// access / egress 1-to-N loops where only the walking/driving
    /// time is needed and the actual road path is never read.
    ///
    /// Replaces the use of `table_bucket_parallel()` for tiny `1 x k`
    /// shapes — see issue #103. Bucket M2M allocates a per-worker
    /// `SearchState` of ~60 MB on the foot CCH and a dense result
    /// matrix for every call, which is catastrophically wasteful when
    /// the real query is "1 source, 20 targets, no paths". This
    /// variant reuses the existing thread-local `CCH_QUERY_STATE`
    /// (O(1) per-query reset via generation stamps) and allocates
    /// nothing.
    pub fn distance(&self, source: u32, target: u32) -> Option<u32> {
        self.distance_bounded(source, target, u32::MAX)
    }

    /// Distance-only bidirectional query with an upper bound.
    ///
    /// Identical to [`distance`](Self::distance) but abandons the search
    /// as soon as no node reachable within `max_dist` remains unsettled
    /// on either frontier — returning `None` when the true shortest
    /// distance exceeds `max_dist`.
    ///
    /// #411: the ULTRA transfer build issues millions of these for
    /// stop-pairs that are great-circle-close (within the 2 km
    /// candidate radius) but may be network-far. Without a bound, a
    /// pair separated by a river / rail / motorway runs the bidirectional
    /// search to natural completion over a large slice of the 5 M-node
    /// foot EBG, only for the caller to discard the result against
    /// `max_walk_s`. Passing the walk-time budget here prunes every such
    /// pair at the `max_dist` ball. The returned graph is identical —
    /// pruned pairs would have been filtered out anyway — so this is a
    /// pure speed fix, cache-compatible with the unbounded build.
    ///
    /// Correctness of the prune: bidirectional Dijkstra settles nodes in
    /// nondecreasing distance per side, so once `min(fwd_min, bwd_min) >
    /// max_dist`, every node with distance ≤ `max_dist` on either side
    /// is already settled. Any s-t path of length ≤ `max_dist` therefore
    /// meets at an already-settled node and is already reflected in
    /// `best_dist`; no unsettled node can yield a shorter ≤-budget path.
    pub fn distance_bounded(&self, source: u32, target: u32, max_dist: u32) -> Option<u32> {
        if source == target {
            return Some(0);
        }
        // #409: with_or_init stamps the cell's last-touch so the
        // idle-compactor can free CCH_QUERY_STATE on this thread once it
        // goes quiet — including transit access/egress worker threads
        // that only ever call this method, never `query()`.
        let n = self.n_nodes;
        CCH_QUERY_STATE.with(|cell| {
            cell.with_or_init(
                || CchQueryState::new(n),
                |state| {
                    if state.dist_fwd.len() != n {
                        *state = CchQueryState::new(n);
                    }
                    state.start_query();

                    state.set_fwd(source as usize, 0, (source, 0));
                    state.set_bwd(target as usize, 0, (target, 0));
                    state.push_fwd(source, 0);
                    state.push_bwd(target, 0);

                    let mut best_dist = u32::MAX;

                    while !state.pq_fwd.is_empty() || !state.pq_bwd.is_empty() {
                        let fwd_min = state.pq_fwd.peek_min_weight().unwrap_or(u32::MAX);
                        let bwd_min = state.pq_bwd.peek_min_weight().unwrap_or(u32::MAX);
                        if fwd_min >= best_dist && bwd_min >= best_dist {
                            break;
                        }
                        // #411: bound prune — no unsettled node on either side is
                        // within budget, so the answer (if any) is already in
                        // best_dist. `max_dist == u32::MAX` (the unbounded
                        // `distance()` path) makes this branch dead.
                        if fwd_min.min(bwd_min) > max_dist {
                            break;
                        }

                        // Forward step — UP graph. #272 stall-on-demand inline.
                        if let Some((d, u)) = state.pop_fwd()
                            && d <= state.get_fwd(u as usize)
                        {
                            let mut stalled = false;
                            self.for_down_rev_edges(u, |x, w, _| {
                                if stalled {
                                    return;
                                }
                                if state.gen_fwd[x as usize] == state.gen_fwd_epoch {
                                    let dx = state.dist_fwd[x as usize];
                                    if dx != u32::MAX && dx.saturating_add(w) < d {
                                        stalled = true;
                                    }
                                }
                            });
                            if !stalled {
                                let bwd_d = state.get_bwd(u as usize);
                                if bwd_d != u32::MAX {
                                    let total = d.saturating_add(bwd_d);
                                    if total < best_dist {
                                        best_dist = total;
                                    }
                                }
                                self.for_up_edges(u, |v, w, edge_idx| {
                                    let new_dist = d.saturating_add(w);
                                    if new_dist < state.get_fwd(v as usize) {
                                        state.set_fwd(v as usize, new_dist, (u, edge_idx));
                                        state.push_fwd(v, new_dist);
                                        let bwd_v = state.get_bwd(v as usize);
                                        if bwd_v != u32::MAX {
                                            let total = new_dist.saturating_add(bwd_v);
                                            if total < best_dist {
                                                best_dist = total;
                                            }
                                        }
                                    }
                                });
                            }
                        }

                        // Backward step — reversed DOWN graph. #272 stall inline.
                        if let Some((d, u)) = state.pop_bwd()
                            && d <= state.get_bwd(u as usize)
                        {
                            let mut stalled = false;
                            self.for_up_edges(u, |v, w, _| {
                                if stalled {
                                    return;
                                }
                                if state.gen_bwd[v as usize] == state.gen_bwd_epoch {
                                    let dv = state.dist_bwd[v as usize];
                                    if dv != u32::MAX && dv.saturating_add(w) < d {
                                        stalled = true;
                                    }
                                }
                            });
                            if !stalled {
                                let fwd_d = state.get_fwd(u as usize);
                                if fwd_d != u32::MAX {
                                    let total = d.saturating_add(fwd_d);
                                    if total < best_dist {
                                        best_dist = total;
                                    }
                                }
                                self.for_down_rev_edges(u, |x, w, edge_idx| {
                                    let new_dist = d.saturating_add(w);
                                    if new_dist < state.get_bwd(x as usize) {
                                        state.set_bwd(x as usize, new_dist, (u, edge_idx));
                                        state.push_bwd(x, new_dist);
                                        let fwd_x = state.get_fwd(x as usize);
                                        if fwd_x != u32::MAX {
                                            let total = new_dist.saturating_add(fwd_x);
                                            if total < best_dist {
                                                best_dist = total;
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }

                    if best_dist == u32::MAX || best_dist > max_dist {
                        None
                    } else {
                        Some(best_dist)
                    }
                },
            )
        })
    }

    /// Distance-only 1-to-N query. Runs `distance()` for each target
    /// against the same thread-local state. The thread-local's
    /// generation-stamp reset is O(1) per call so this is genuinely
    /// cheap — each target costs one bounded bidirectional search on
    /// the CCH with no allocation in the steady state.
    pub fn distances_one_to_many(&self, source: u32, targets: &[u32]) -> Vec<Option<u32>> {
        targets.iter().map(|&t| self.distance(source, t)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::cch_topo::CchTopo;

    /// Build a minimal 5-node CCH graph for testing.
    ///
    /// Graph (rank = node id for simplicity):
    ///
    ///   UP edges:
    ///     0 → 2 (w=10)
    ///     1 → 2 (w=3)
    ///     2 → 3 (w=7)
    ///     2 → 4 (w=5)
    ///
    ///   DOWN edges (mirror):
    ///     2 → 0 (w=10)
    ///     2 → 1 (w=3)
    ///     3 → 2 (w=7)
    ///     4 → 2 (w=5)
    fn build_test_cch() -> (CchTopo, CchWeights, UpAdjFlat, DownReverseAdjFlat) {
        let n_nodes = 5u32;

        let up_offsets = vec![0u64, 1, 2, 4, 4, 4];
        let up_targets = vec![2u32, 2, 3, 4];
        let up_is_shortcut = vec![false; 4];
        let up_middle = vec![u32::MAX; 4];

        let down_offsets = vec![0u64, 0, 0, 2, 3, 4];
        let down_targets = vec![0u32, 1, 2, 2];
        let down_is_shortcut = vec![false; 4];
        let down_middle = vec![u32::MAX; 4];

        let rank_to_filtered: Vec<u32> = (0..n_nodes).collect();

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: 4,
            inputs_sha: [0u8; 32],
            up_offsets: up_offsets.into(),
            up_targets: up_targets.into(),
            up_is_shortcut: crate::formats::BitsetField::from_bools(&up_is_shortcut),
            up_middle: up_middle.into(),
            down_offsets: down_offsets.into(),
            down_targets: down_targets.into(),
            down_is_shortcut: crate::formats::BitsetField::from_bools(&down_is_shortcut),
            down_middle: down_middle.into(),
            rank_to_filtered: rank_to_filtered.into(),
        };

        let weights = CchWeights {
            up: vec![10u32, 3, 7, 5].into(),
            down: vec![10u32, 3, 7, 5].into(),
            up_middle: vec![].into(),
            down_middle: vec![].into(),
        };

        // Build flats with topo_edge_idx populated (CchQuery hot path).
        let up_adj_flat = UpAdjFlat::build_with(&topo, &weights, true);
        let down_rev_flat = DownReverseAdjFlat::build_with(&topo, &weights, true);

        (topo, weights, up_adj_flat, down_rev_flat)
    }

    #[test]
    fn test_same_node_query() {
        let (topo, weights, up_flat, down_rev_flat) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        let result = query.query(0, 0).expect("same-node query should succeed");
        assert_eq!(result.distance, 0);
        assert_eq!(result.meeting_node, 0);
        assert!(result.forward_parent.is_empty());
        assert!(result.backward_parent.is_empty());
    }

    #[test]
    fn test_basic_shortest_path() {
        let (topo, weights, up_flat, down_rev_flat) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        // Path 0 → 1: 0→UP→2→DOWN→1, cost = 10 + 3 = 13
        let result = query.query(0, 1).expect("path should exist");
        assert_eq!(result.distance, 13);

        // Path 1 → 0: 1→UP→2→DOWN→0, cost = 3 + 10 = 13
        let result = query.query(1, 0).expect("path should exist");
        assert_eq!(result.distance, 13);

        // Path 0 → 2: 0→UP→2, cost = 10
        let result = query.query(0, 2).expect("path should exist");
        assert_eq!(result.distance, 10);
    }

    #[test]
    fn test_multi_hop_path() {
        let (topo, weights, up_flat, down_rev_flat) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        // Path 0 → 3: 0→UP→2→UP→3, cost = 10 + 7 = 17
        let result = query.query(0, 3).expect("path should exist");
        assert_eq!(result.distance, 17);

        // Path 0 → 4: 0→UP→2→UP→4, cost = 10 + 5 = 15
        let result = query.query(0, 4).expect("path should exist");
        assert_eq!(result.distance, 15);

        // Path 1 → 3: 1→UP→2→UP→3, cost = 3 + 7 = 10
        let result = query.query(1, 3).expect("path should exist");
        assert_eq!(result.distance, 10);
    }

    #[test]
    fn test_distance_bounded_prune() {
        // #411: distance_bounded must (a) equal unbounded distance() when
        // the path fits the budget, (b) return None when the true
        // distance exceeds the bound, and (c) preserve the same-node
        // short-circuit even at a zero budget.
        let (topo, weights, up_flat, down_rev_flat) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        // True distances: 0→3 = 17, 0→4 = 15, 1→3 = 10.
        assert_eq!(query.distance(0, 3), Some(17), "unbounded baseline");

        // Unbounded variant delegates to distance_bounded(.., u32::MAX).
        assert_eq!(query.distance_bounded(0, 3, u32::MAX), Some(17));

        // Bound exactly at the true distance still returns it.
        assert_eq!(query.distance_bounded(0, 3, 17), Some(17));

        // Bound one below the true distance prunes to None.
        assert_eq!(query.distance_bounded(0, 3, 16), None);

        // A different pair: true 1→3 = 10.
        assert_eq!(query.distance_bounded(1, 3, 10), Some(10));
        assert_eq!(query.distance_bounded(1, 3, 9), None);

        // Same-node is Some(0) regardless of budget (even 0).
        assert_eq!(query.distance_bounded(2, 2, 0), Some(0));
    }

    #[test]
    fn test_thread_local_state_reuse() {
        // Run many queries to verify thread-local state is correctly reused
        // across queries (generation stamping doesn't leak stale data)
        let (topo, weights, up_flat, down_rev_flat) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        for _ in 0..100 {
            assert_eq!(query.query(0, 1).unwrap().distance, 13);
            assert_eq!(query.query(1, 0).unwrap().distance, 13);
            assert_eq!(query.query(0, 3).unwrap().distance, 17);
            assert_eq!(query.query(1, 4).unwrap().distance, 8); // 3 + 5
            assert_eq!(query.query(0, 0).unwrap().distance, 0);
        }
    }

    #[test]
    fn test_reconstruct_path_versioned_basic() {
        let parent = vec![(u32::MAX, 0), (0, 42), (1, 99)];
        let generation = vec![5, 5, 5];

        // Path from 0 to 2: 0 → 1 (edge 42) → 2 (edge 99)
        let path = reconstruct_path_versioned(&parent, &generation, 5, 0, 2);
        assert_eq!(path.len(), 2);
        assert_eq!(path[0], (1, 42));
        assert_eq!(path[1], (2, 99));
    }

    #[test]
    fn test_reconstruct_path_versioned_stale_gen() {
        let parent = vec![(u32::MAX, 0), (0, 42), (1, 99)];
        let generation = vec![5, 5, 3]; // Node 2 has stale generation

        // Should stop at node 2 because generation[2] != current_gen
        let path = reconstruct_path_versioned(&parent, &generation, 5, 0, 2);
        assert!(path.is_empty()); // Can't trace back from node 2
    }

    #[test]
    fn test_reconstruct_path_versioned_single_step() {
        let parent = vec![(u32::MAX, 0), (0, 7)];
        let generation = vec![1, 1];

        let path = reconstruct_path_versioned(&parent, &generation, 1, 0, 1);
        assert_eq!(path.len(), 1);
        assert_eq!(path[0], (1, 7));
    }

    #[test]
    fn test_no_path_between_disconnected_nodes() {
        // Build a graph where node 3 and 4 have UP edges to nowhere reachable from below
        // Actually in our test graph everything connects through node 2.
        // Let's make a 6-node graph with two components.
        let n_nodes = 6u32;

        let up_offsets = vec![0u64, 1, 2, 2, 3, 4, 4];
        let up_targets = vec![2u32, 2, 5, 5]; // 0→2, 1→2, 3→5, 4→5
        let up_is_shortcut = vec![false; 4];
        let up_middle = vec![u32::MAX; 4];

        let down_offsets = vec![0u64, 0, 0, 2, 2, 2, 4];
        let down_targets = vec![0u32, 1, 3, 4]; // 2→0, 2→1, 5→3, 5→4
        let down_is_shortcut = vec![false; 4];
        let down_middle = vec![u32::MAX; 4];

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: 4,
            inputs_sha: [0u8; 32],
            up_offsets: up_offsets.into(),
            up_targets: up_targets.into(),
            up_is_shortcut: crate::formats::BitsetField::from_bools(&up_is_shortcut),
            up_middle: up_middle.into(),
            down_offsets: down_offsets.into(),
            down_targets: down_targets.into(),
            down_is_shortcut: crate::formats::BitsetField::from_bools(&down_is_shortcut),
            down_middle: down_middle.into(),
            rank_to_filtered: (0..n_nodes).collect::<Vec<_>>().into(),
        };

        let weights = CchWeights {
            up: vec![10u32, 3, 7, 5].into(),
            down: vec![10u32, 3, 7, 5].into(),
            up_middle: vec![].into(),
            down_middle: vec![].into(),
        };

        let up_flat = UpAdjFlat::build_with(&topo, &weights, true);
        let down_rev_flat = DownReverseAdjFlat::build_with(&topo, &weights, true);

        let query = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        // Same component works
        assert_eq!(query.query(0, 1).unwrap().distance, 13);
        // Path 3→4: 3→UP→5→DOWN→4, cost = 7 + 5 = 12
        assert_eq!(query.query(3, 4).unwrap().distance, 12);

        // Cross-component: no path
        assert!(query.query(0, 3).is_none());
        assert!(query.query(3, 1).is_none());
    }

    /// Build a 4-node CCH where the shortest path from 0 to 1 goes
    /// UP-then-DOWN through node 2. After backward search reaches 1's
    /// DOWN-predecessor 2, the forward pop of 1 at its direct UP cost
    /// should fire the forward stall.
    ///
    /// Ranks 0..=3. Edges:
    ///   UP   : 0→1 (100), 0→2 (5),  2→3 (10), 1→3 (100)
    ///   DOWN : 1→0 (100), 2→0 (5),  2→1 (1),  3→1 (80), 3→2 (10)
    ///
    /// SP(0→1) = 0 →UP→ 2 →DOWN→ 1 = 5 + 1 = 6.
    /// SP(0→3) = 0 →UP→ 2 →UP→ 3 = 5 + 10 = 15.
    fn build_stall_test_cch() -> (CchTopo, CchWeights, UpAdjFlat, DownReverseAdjFlat) {
        let n_nodes = 4u32;

        let up_offsets = vec![0u64, 2, 3, 4, 4];
        let up_targets = vec![1u32, 2, 3, 3];
        let up_is_shortcut = vec![false; 4];
        let up_middle = vec![u32::MAX; 4];

        let down_offsets = vec![0u64, 0, 1, 3, 5];
        let down_targets = vec![0u32, 0, 1, 1, 2];
        let down_is_shortcut = vec![false; 5];
        let down_middle = vec![u32::MAX; 5];

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: 9,
            inputs_sha: [0u8; 32],
            up_offsets: up_offsets.into(),
            up_targets: up_targets.into(),
            up_is_shortcut: crate::formats::BitsetField::from_bools(&up_is_shortcut),
            up_middle: up_middle.into(),
            down_offsets: down_offsets.into(),
            down_targets: down_targets.into(),
            down_is_shortcut: crate::formats::BitsetField::from_bools(&down_is_shortcut),
            down_middle: down_middle.into(),
            rank_to_filtered: (0..n_nodes).collect::<Vec<_>>().into(),
        };

        let weights = CchWeights {
            up: vec![100u32, 5, 100, 10].into(),
            down: vec![100u32, 5, 1, 80, 10].into(),
            up_middle: vec![].into(),
            down_middle: vec![].into(),
        };

        let up_flat = UpAdjFlat::build_with(&topo, &weights, true);
        let down_rev_flat = DownReverseAdjFlat::build_with(&topo, &weights, true);

        (topo, weights, up_flat, down_rev_flat)
    }

    #[test]
    fn test_stall_does_not_break_correctness() {
        let (topo, weights, up_flat, down_rev_flat) = build_stall_test_cch();
        let q = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        let r = q.query(0, 1).expect("path 0→1 exists");
        assert_eq!(r.distance, 6, "stall must not break 0→1 correctness");

        let r = q.query(0, 3).expect("path 0→3 exists");
        assert_eq!(r.distance, 15, "stall must not break 0→3 correctness");

        assert_eq!(q.distance(0, 1), Some(6));
        assert_eq!(q.distance(0, 3), Some(15));
    }

    #[test]
    fn test_is_stalled_fwd_fires_on_shorter_predecessor() {
        let (topo, weights, up_flat, down_rev_flat) = build_stall_test_cch();
        let q = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        let mut state = CchQueryState::new(4);
        state.start_query();
        state.set_fwd(2, 5, (0, 0));

        assert!(q.is_stalled_fwd(&state, 1, 100));
        assert!(!q.is_stalled_fwd(&state, 1, 5));

        state.start_query();
        state.set_fwd(2, 5, (0, 0));
        state.start_query();
        assert!(!q.is_stalled_fwd(&state, 1, 100));
    }

    #[test]
    fn test_is_stalled_bwd_fires_on_shorter_successor() {
        let (topo, weights, up_flat, down_rev_flat) = build_stall_test_cch();
        let q = CchQuery::with_custom_weights(&topo, &up_flat, &down_rev_flat, &weights);

        let mut state = CchQueryState::new(4);
        state.start_query();
        state.set_bwd(2, 5, (0, 0));

        assert!(q.is_stalled_bwd(&state, 0, 100));
        assert!(!q.is_stalled_bwd(&state, 0, 10));

        state.start_query();
        assert!(!q.is_stalled_bwd(&state, 0, 100));
    }
}
