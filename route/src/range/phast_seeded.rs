//! Seeded bounded PHAST — ONE engine over direction and channel count.
//!
//! #569: these four scans (forward/reverse × 1-channel/2-channel) used to
//! live in `server/isochrone_handler.rs`, an HTTP handler module, and
//! `matrix::bucket_ch` imported them from there — the matrix engine
//! depending on a web handler. They are engine code: the isochrone path and
//! the lopsided matrix path (#526/#527) run the *same* sweep and must not be
//! able to disagree. They belong next to the other range queries.
//!
//! ## Shape
//!
//! Phase 1 is a bounded upward PQ sweep; phase 2 is a rank-ordered downward
//! scan; phase 3 hands every settled node within `threshold` to `collect`.
//! Two things vary, and only two:
//!
//! - **Direction** ([`ScanDir`]). Forward relaxes UP edges upward and PUSHes
//!   DOWN edges downward, block-gated. Reverse relaxes DOWN-reverse edges
//!   upward and PULLs via UP edges downward over every rank — a PULL cannot
//!   propagate block activation, so it is not gated (a reverse-UP adjacency
//!   would be needed for a PUSH, and we do not build one).
//! - **Channel count** (`C`). `C = 1` is time alone. `C = 2` (#527) carries a
//!   length-along-time channel in lockstep: time is primary, length follows
//!   the improving parent, and equal time is broken by the shorter length
//!   (#530 lazy-lex, mirroring `SearchState2::relax` so this surface cannot
//!   disagree with `/route`/`/table` on equal-duration ties). `C = 1` never
//!   allocates or touches the length channel — every `C >= 2` branch below is
//!   a compile-time constant and folds away.
//!
//! ## State
//!
//! One `PhastSlots` arena per worker thread, keyed by `(direction, mode)`
//! (#569 — it was two thread-locals). The per-mode LRU (#408) still applies
//! per direction, so the steady-state RSS bound is unchanged:
//! `cap × (~80 MB) × 2 (fwd+rev) × n_workers`. The whole arena sits in one
//! [`EvictableCell`] (#409/#410) so the idle-compactor reclaims it from any
//! pool — Tokio workers included.
//!
//! The cell's lock is held for the whole scan, so a scan must never be
//! started from inside another scan's `collect` on the same thread. No
//! caller does: `collect` writes slots and nothing else.

use crate::evictable::EvictableCell;
use crate::matrix::bucket_ch::{DownAdjFlat, DownReverseAdjFlat, UpAdjFlat};
use crate::model::types::{MAX_MODES, Mode};

// =============================================================================
// THREAD-LOCAL PHAST STATE (eliminates 9.6MB memset per query)
// =============================================================================

/// Block size for block-gated downward scan
/// Each block contains BLOCK_SIZE consecutive ranks
const PHAST_BLOCK_SIZE: usize = 4096;

/// Thread-local PHAST state with generation stamping and block gating
/// Eliminates O(n) initialization per query by using version stamps
/// Block gating skips large portions of the graph in downward phase
struct PhastState {
    /// Distance array (persistent across queries)
    dist: Vec<u32>,
    /// Version stamp per node (marks which generation set the distance)
    version: Vec<u32>,
    /// Version stamp per block (marks which blocks have active nodes)
    block_active: Vec<u32>,
    /// Number of blocks
    n_blocks: usize,
    /// Current generation (incremented per query)
    current_gen: u32,
    /// Priority queue (reused across queries)
    pq: std::collections::BinaryHeap<std::cmp::Reverse<(u32, u32)>>,
    /// #527: parallel length-along-time channel, co-stamped with `version`.
    /// Empty until the first 2-channel query grows it — single-channel
    /// isochrones never allocate or touch it.
    len: Vec<u32>,
}

impl PhastState {
    fn new(n_nodes: usize) -> Self {
        let n_blocks = n_nodes.div_ceil(PHAST_BLOCK_SIZE);
        Self {
            dist: vec![u32::MAX; n_nodes],
            version: vec![0; n_nodes],
            block_active: vec![0; n_blocks],
            n_blocks,
            current_gen: 0,
            pq: std::collections::BinaryHeap::with_capacity(n_nodes / 100),
            len: Vec::new(),
        }
    }

    /// #527: ensure the length channel is allocated (2-channel path only).
    #[inline]
    fn ensure_len(&mut self) {
        if self.len.len() != self.dist.len() {
            self.len = vec![u32::MAX; self.dist.len()];
        }
    }
    #[inline]
    fn get_len(&self, node: usize) -> u32 {
        if self.version[node] == self.current_gen {
            self.len[node]
        } else {
            u32::MAX
        }
    }
    /// Set BOTH channels (time primary, length carried). Marks version+block.
    #[inline]
    fn set_dist_len(&mut self, node: usize, dist: u32, len: u32) {
        self.dist[node] = dist;
        self.len[node] = len;
        self.version[node] = self.current_gen;
        let block_idx = node / PHAST_BLOCK_SIZE;
        self.block_active[block_idx] = self.current_gen;
    }

    /// Start a new query (O(1) instead of O(n))
    #[inline]
    fn start_query(&mut self) {
        self.current_gen = self.current_gen.wrapping_add(1);
        if self.current_gen == 0 {
            // Overflow - reset all versions (rare, every ~4B queries)
            self.version.iter_mut().for_each(|v| *v = 0);
            self.block_active.iter_mut().for_each(|v| *v = 0);
            self.current_gen = 1;
        }
        self.pq.clear();
    }

    /// Get distance (returns MAX if not set this query)
    #[inline]
    fn get_dist(&self, node: usize) -> u32 {
        if self.version[node] == self.current_gen {
            self.dist[node]
        } else {
            u32::MAX
        }
    }

    /// Set distance (also marks version and block as active)
    #[inline]
    fn set_dist(&mut self, node: usize, dist: u32) {
        self.dist[node] = dist;
        self.version[node] = self.current_gen;
        // Mark block as active
        let block_idx = node / PHAST_BLOCK_SIZE;
        self.block_active[block_idx] = self.current_gen;
    }

    /// Check if a block is active this query
    #[inline]
    fn is_block_active(&self, block_idx: usize) -> bool {
        self.block_active[block_idx] == self.current_gen
    }

    /// The settled label at `rank`, as the caller's `C` channels. The length
    /// channel is read only when `C >= 2`, where it is guaranteed allocated.
    #[inline]
    fn label<const C: usize>(&self, rank: usize) -> [u32; C] {
        let mut out = [self.dist[rank]; C];
        if C >= 2 {
            out[1] = self.len[rank];
        }
        out
    }
}

/// Adopt `primary` (plus its lazily-computed secondary channel) at `v` if it
/// improves the label there. Returns whether the label moved — the upward
/// sweep pushes on `true`, the downward scan ignores it.
///
/// `secondary` is only invoked when the answer depends on it, so the
/// 1-channel instantiation never reads a length weight and never touches the
/// (unallocated) length channel. It takes the state by shared reference so a
/// PULL caller can read the parent's length inside it while the state is
/// mutably borrowed here.
#[inline(always)]
fn improve<const C: usize>(
    state: &mut PhastState,
    v: usize,
    primary: u32,
    secondary: impl FnOnce(&PhastState) -> u32,
) -> bool {
    let cur = state.get_dist(v);
    if primary < cur {
        if C >= 2 {
            let s = secondary(state);
            state.set_dist_len(v, primary, s);
        } else {
            state.set_dist(v, primary);
        }
        true
    } else if C >= 2 && primary == cur && cur != u32::MAX {
        // #530: lazy lexicographic (time, then length) tie-break — at EQUAL
        // time but strictly shorter length, adopt the shorter label so the
        // improvement propagates to successors. Fires only on genuine
        // equal-time ties (never for strictly-positive single-direction
        // modes), so non-tying modes are byte-identical to the pre-#530 path.
        let s = secondary(state);
        if s < state.get_len(v) {
            state.set_dist_len(v, primary, s);
            return true;
        }
        false
    } else {
        false
    }
}

/// Forward and reverse — the two halves of the slot key.
const N_DIRS: usize = 2;

/// #408: bounded per-thread PHAST state — `Option<PhastState>` slots
/// indexed by `(direction, mode_idx)`, plus a parallel last-used counter
/// used to pick a victim when a direction's live-slot count reaches the LRU
/// capacity. This LRU bounds *peak* RSS while traffic is steady across many
/// modes; #409 wraps the whole `PhastSlots` in an `EvictableCell` so the
/// idle-compactor reclaims the entire arena once the owning thread (Tokio or
/// rayon) goes quiet.
struct PhastSlots {
    slots: [[Option<PhastState>; MAX_MODES]; N_DIRS],
    last_used: [[u64; MAX_MODES]; N_DIRS],
    epoch: u64,
}

impl PhastSlots {
    const fn empty() -> Self {
        Self {
            slots: [const { [const { None }; MAX_MODES] }; N_DIRS],
            last_used: [[0u64; MAX_MODES]; N_DIRS],
            epoch: 0,
        }
    }

    /// Touch the slot for `(dir, mode_idx)`. If the slot is empty and that
    /// direction's live-slot count is already at `cap`, evict its LRU slot
    /// first (excluding `mode_idx` itself). Eviction never crosses
    /// directions: forward and reverse each keep their own `cap` slots, as
    /// they did when they were two separate thread-locals. Caller then
    /// `.get_or_insert_with` on the returned slot reference.
    fn touch(&mut self, dir: usize, mode_idx: usize, cap: usize) -> &mut Option<PhastState> {
        self.epoch = self.epoch.wrapping_add(1);
        self.last_used[dir][mode_idx] = self.epoch;

        if self.slots[dir][mode_idx].is_some() {
            return &mut self.slots[dir][mode_idx];
        }
        let live = self.slots[dir].iter().filter(|s| s.is_some()).count();
        if live >= cap {
            // Find LRU victim (smallest last_used among live slots of this
            // direction, excluding the requested mode_idx).
            let mut victim: Option<(usize, u64)> = None;
            for (i, slot) in self.slots[dir].iter().enumerate() {
                if i == mode_idx || slot.is_none() {
                    continue;
                }
                let lu = self.last_used[dir][i];
                if victim.map(|(_, vlu)| lu < vlu).unwrap_or(true) {
                    victim = Some((i, lu));
                }
            }
            if let Some((vi, _)) = victim {
                self.slots[dir][vi] = None;
            }
        }
        &mut self.slots[dir][mode_idx]
    }
}

/// #408: per-worker LRU capacity for the PHAST mode-slot array.
/// Reads `BUTTERFLY_PHAST_MODE_LRU_CAP` (default 2). Cold-start cost
/// per evicted-then-re-queried mode is one `PhastState::new(n_nodes)`
/// allocation (~80 MB on Belgium); the steady-state RSS bound is
/// `cap × (~80 MB) × 2 (fwd+rev) × n_workers`.
fn phast_mode_lru_cap() -> usize {
    std::env::var("BUTTERFLY_PHAST_MODE_LRU_CAP")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .map(|c| c.clamp(1, MAX_MODES))
        .unwrap_or(2)
}

thread_local! {
    /// #408: PHAST mode-slot LRU, per worker thread, both directions.
    /// #409/#410: wrapped in an `EvictableCell` so the idle-compactor
    /// frees the whole `PhastSlots` arena regardless of which pool owns
    /// the thread — `/isochrone` runs inline on Tokio workers, which
    /// `rayon::broadcast` could not reach.
    static PHAST_STATES: EvictableCell<PhastSlots> = const { EvictableCell::new() };
}

// =============================================================================
// DIRECTION / ADJACENCY ABSTRACTION
// =============================================================================

/// A CSR neighbour list the sweep relaxes. The three flats differ only in
/// where the neighbour id lives (`targets` vs `sources`) and whether INF
/// weights were already filtered out at build time.
pub trait ScanCsr {
    /// `true` iff this flat can still carry `u32::MAX` weights, which the
    /// relaxation must skip. `UpAdjFlat` / `DownAdjFlat` are built INF-free;
    /// the reverse flat's sweep has always checked.
    const SKIP_INF: bool;
    /// Number of nodes this flat is indexed over (`offsets.len() - 1`).
    fn n_nodes(&self) -> usize;
    /// Half-open slot range of `node`'s neighbours.
    fn slots(&self, node: usize) -> (usize, usize);
    /// Neighbour node at slot `i`.
    fn neighbor(&self, i: usize) -> usize;
    /// Weight at slot `i`.
    fn weight(&self, i: usize) -> u32;
}

impl ScanCsr for UpAdjFlat {
    const SKIP_INF: bool = false;
    #[inline(always)]
    fn n_nodes(&self) -> usize {
        self.offsets.len() - 1
    }
    #[inline(always)]
    fn slots(&self, node: usize) -> (usize, usize) {
        (self.offsets[node] as usize, self.offsets[node + 1] as usize)
    }
    #[inline(always)]
    fn neighbor(&self, i: usize) -> usize {
        self.targets[i] as usize
    }
    #[inline(always)]
    fn weight(&self, i: usize) -> u32 {
        self.weights.get(i)
    }
}

impl ScanCsr for DownAdjFlat {
    const SKIP_INF: bool = false;
    #[inline(always)]
    fn n_nodes(&self) -> usize {
        self.offsets.len() - 1
    }
    #[inline(always)]
    fn slots(&self, node: usize) -> (usize, usize) {
        (self.offsets[node] as usize, self.offsets[node + 1] as usize)
    }
    #[inline(always)]
    fn neighbor(&self, i: usize) -> usize {
        self.targets[i] as usize
    }
    #[inline(always)]
    fn weight(&self, i: usize) -> u32 {
        self.weights.get(i)
    }
}

impl ScanCsr for DownReverseAdjFlat {
    const SKIP_INF: bool = true;
    #[inline(always)]
    fn n_nodes(&self) -> usize {
        self.offsets.len() - 1
    }
    #[inline(always)]
    fn slots(&self, node: usize) -> (usize, usize) {
        (self.offsets[node] as usize, self.offsets[node + 1] as usize)
    }
    #[inline(always)]
    fn neighbor(&self, i: usize) -> usize {
        self.sources[i] as usize
    }
    #[inline(always)]
    fn weight(&self, i: usize) -> u32 {
        self.weights.get(i)
    }
}

/// Which way the field runs — it fixes both adjacencies and the shape of the
/// downward phase. Every associated item is a compile-time constant, so a
/// monomorphised scan carries no direction branch.
pub trait ScanDir {
    /// Relaxed by the upward PQ sweep.
    type Up: ScanCsr;
    /// Relaxed by the downward rank scan.
    type Down: ScanCsr;
    /// Downward phase PULLs over every rank instead of PUSHing block-gated.
    const PULL: bool;
    /// Slot-key half: forward and reverse keep separate per-mode arenas.
    const SLOT: usize;
    /// Name in the timing log.
    const NAME: &'static str;
    /// Node count of the CCH hierarchy. Both flats index the same node set;
    /// the count is read off the `UpAdjFlat` side of the pair (forward: the
    /// upward adjacency, reverse: the downward one) — exactly where each of
    /// the four separate scans read it before #569.
    fn n_nodes(up: &Self::Up, down: &Self::Down) -> usize;
}

/// `d(origin → all)` — depart isochrones, forward matrix fields.
pub struct Forward;

impl ScanDir for Forward {
    type Up = UpAdjFlat;
    type Down = DownAdjFlat;
    const PULL: bool = false;
    const SLOT: usize = 0;
    const NAME: &'static str = "forward";
    #[inline(always)]
    fn n_nodes(up: &UpAdjFlat, _down: &DownAdjFlat) -> usize {
        up.n_nodes()
    }
}

/// `d(all → target)` — arrive isochrones, reverse matrix fields. Swaps the
/// adjacencies: upward relaxes DOWN-reverse edges, downward PULLs via UP
/// edges (for each node `v`, from its higher-rank neighbours) because we
/// have no reverse-UP adjacency to PUSH along.
pub struct Reverse;

impl ScanDir for Reverse {
    type Up = DownReverseAdjFlat;
    type Down = UpAdjFlat;
    const PULL: bool = true;
    const SLOT: usize = 1;
    const NAME: &'static str = "reverse";
    #[inline(always)]
    fn n_nodes(_up: &DownReverseAdjFlat, down: &UpAdjFlat) -> usize {
        down.n_nodes()
    }
}

/// The adjacency flats one scan reads: the primary (time) pair plus, for
/// `C = 2`, the length-along-time pair. The `_len` flats share topology with
/// the primary ones (identical offsets + neighbour ids, different weights),
/// so slot `i` aligns across both.
pub struct ScanFlats<'a, D: ScanDir> {
    up: &'a D::Up,
    down: &'a D::Down,
    up_len: &'a D::Up,
    down_len: &'a D::Down,
}

impl<'a, D: ScanDir> ScanFlats<'a, D> {
    /// Time channel only (`C = 1`): the length flats are never read.
    pub fn time(up: &'a D::Up, down: &'a D::Down) -> Self {
        Self {
            up,
            down,
            up_len: up,
            down_len: down,
        }
    }

    /// Time + length-along-time (`C = 2`, #527).
    pub fn with_len(
        up: &'a D::Up,
        down: &'a D::Down,
        up_len: &'a D::Up,
        down_len: &'a D::Down,
    ) -> Self {
        Self {
            up,
            down,
            up_len,
            down_len,
        }
    }
}

// =============================================================================
// THE ENGINE
// =============================================================================

/// Seeded bounded PHAST core: upward PQ sweep, rank-ordered downward scan,
/// then `collect(rank, label)` for every settled node within `threshold`, in
/// increasing rank order.
///
/// Each seed is `(rank, [channel; C])` — a partial cost per channel, i.e. a
/// super-source with non-negative arcs (#506 phantom endpoints), so the
/// bounded sweep and the rank-order scan are unchanged. Seeds are iterated
/// twice (label init, then PQ init), hence the `Clone` bound; nothing is
/// allocated for them.
///
/// #568: callers that want only a handful of ranks (the lopsided matrix path
/// probes the field at the far endpoints' ranks) pass a `collect` that writes
/// them straight into their own slots — no `Vec` of the whole settled set, no
/// hash probe per settled node. The isochrone pipeline wants the whole
/// settled set, and the named surfaces below build the `Vec` it expects.
///
/// Reads weights, neighbour ids and offsets directly from the pre-built flats
/// — never touches `cch_weights.up/.down` on the inner loop. After #149, this
/// is what makes `madvise(MADV_DONTNEED)` over the cch_weights byte ranges
/// actually reclaim RSS.
pub fn run_seeded<const C: usize, D: ScanDir>(
    flats: ScanFlats<'_, D>,
    seeds: impl Iterator<Item = (u32, [u32; C])> + Clone,
    threshold: u32,
    mode: Mode,
    mut collect: impl FnMut(u32, [u32; C]),
) {
    const {
        assert!(
            C == 1 || C == 2,
            "PhastState carries one length channel: C is 1 or 2"
        );
    }
    use std::cmp::Reverse as Rev;

    let ScanFlats {
        up,
        down,
        up_len,
        down_len,
    } = flats;

    let total_start = std::time::Instant::now();
    let n_nodes = D::n_nodes(up, down);
    let mode_idx = mode.index();

    // #408: per-mode LRU within the thread's PhastSlots; #409: the whole
    // PhastSlots is an EvictableCell so the idle-compactor frees it on
    // any thread (incl. Tokio workers running /isochrone inline).
    let cap = phast_mode_lru_cap();
    PHAST_STATES.with(|cell| {
        cell.with_or_init(PhastSlots::empty, |states| {
            let state_slot = states.touch(D::SLOT, mode_idx, cap);

            // Initialize or reinitialize if needed
            let state = state_slot.get_or_insert_with(|| PhastState::new(n_nodes));

            // Verify size matches (in case different datasets)
            if state.dist.len() != n_nodes {
                *state = PhastState::new(n_nodes);
            }

            // Start new query (O(1) instead of O(n) memset)
            state.start_query();
            if C >= 2 {
                state.ensure_len();
            }
            for (r, v) in seeds.clone() {
                if v[0] < state.get_dist(r as usize) {
                    if C >= 2 {
                        state.set_dist_len(r as usize, v[0], v[1]);
                    } else {
                        state.set_dist(r as usize, v[0]);
                    }
                }
            }

            // Count settled nodes during upward phase (#568: a counter, not
            // a Vec — the value was only ever read as `.len()` for the log).
            let mut upward_settled = 0usize;

            // Phase 1: Upward search (PQ-based). Reads weights from the flat,
            // so the hot loop is branch-free w.r.t. weight validity wherever
            // the flat was built INF-free.
            let upward_start = std::time::Instant::now();
            for (r, v) in seeds {
                if state.get_dist(r as usize) == v[0] {
                    state.pq.push(Rev((v[0], r)));
                }
            }

            while let Some(Rev((d, u))) = state.pq.pop() {
                if d > threshold {
                    break;
                }

                if d > state.get_dist(u as usize) {
                    continue; // Stale entry
                }

                upward_settled += 1;
                let l_u = if C >= 2 { state.get_len(u as usize) } else { 0 };

                let (slot_start, slot_end) = up.slots(u as usize);
                for i in slot_start..slot_end {
                    let w = up.weight(i);
                    if D::Up::SKIP_INF && w == u32::MAX {
                        continue;
                    }
                    let v = up.neighbor(i);
                    let new_dist = d.saturating_add(w);
                    if improve::<C>(state, v, new_dist, |_| l_u.saturating_add(up_len.weight(i))) {
                        state.pq.push(Rev((new_dist, v as u32)));
                    }
                }
            }
            let upward_us = upward_start.elapsed().as_micros();

            // Phase 2: downward scan.
            let downward_start = std::time::Instant::now();
            let mut blocks_active = 0usize;
            if !D::PULL {
                // PUSH, block-gated: skip whole blocks with no active nodes.
                for block_idx in (0..state.n_blocks).rev() {
                    if !state.is_block_active(block_idx) {
                        continue;
                    }
                    blocks_active += 1;

                    // Process nodes in this block in reverse rank order
                    let block_start = block_idx * PHAST_BLOCK_SIZE;
                    let block_end = ((block_idx + 1) * PHAST_BLOCK_SIZE).min(n_nodes);

                    for rank in (block_start..block_end).rev() {
                        let d_u = state.get_dist(rank);

                        if d_u == u32::MAX || d_u > threshold {
                            continue;
                        }
                        let l_u = if C >= 2 { state.get_len(rank) } else { 0 };

                        let (slot_start, slot_end) = down.slots(rank);
                        for i in slot_start..slot_end {
                            let w = down.weight(i);
                            if D::Down::SKIP_INF && w == u32::MAX {
                                continue;
                            }
                            let v = down.neighbor(i);
                            // improve() marks the target block as active too.
                            // Its return is ignored: the scan runs in strictly
                            // decreasing rank and DOWN targets rank lower, so
                            // `v` is still visited this pass.
                            improve::<C>(state, v, d_u.saturating_add(w), |_| {
                                l_u.saturating_add(down_len.weight(i))
                            });
                        }
                    }
                }
            } else {
                // PULL: for each node v (decreasing rank), pull from its
                // higher-rank neighbours. Block-gating is NOT usable here —
                // a PULL cannot propagate block activation downward.
                for v in (0..n_nodes).rev() {
                    let (slot_start, slot_end) = down.slots(v);
                    for i in slot_start..slot_end {
                        let w = down.weight(i);
                        if D::Down::SKIP_INF && w == u32::MAX {
                            continue;
                        }
                        let u = down.neighbor(i); // u has higher rank

                        let d_u = state.get_dist(u);
                        if d_u == u32::MAX || d_u > threshold {
                            continue;
                        }

                        improve::<C>(state, v, d_u.saturating_add(w), |st| {
                            st.get_len(u).saturating_add(down_len.weight(i))
                        });
                    }
                }
            }
            let downward_us = downward_start.elapsed().as_micros();

            // Phase 3: hand over the settled nodes within threshold, in
            // increasing rank order. The PUSH side scans only active blocks —
            // much faster than a full n_nodes scan; the PULL side has no
            // block gating to lean on.
            let collect_start = std::time::Instant::now();
            let mut settled_nodes = 0usize;
            if !D::PULL {
                for block_idx in 0..state.n_blocks {
                    if !state.is_block_active(block_idx) {
                        continue;
                    }
                    let block_start = block_idx * PHAST_BLOCK_SIZE;
                    let block_end = ((block_idx + 1) * PHAST_BLOCK_SIZE).min(n_nodes);
                    for rank in block_start..block_end {
                        if state.version[rank] == state.current_gen && state.dist[rank] <= threshold
                        {
                            settled_nodes += 1;
                            collect(rank as u32, state.label::<C>(rank));
                        }
                    }
                }
            } else {
                for rank in 0..n_nodes {
                    if state.version[rank] == state.current_gen && state.dist[rank] <= threshold {
                        settled_nodes += 1;
                        collect(rank as u32, state.label::<C>(rank));
                    }
                }
            }
            let collect_us = collect_start.elapsed().as_micros();
            let total_us = total_start.elapsed().as_micros();

            tracing::debug!(
                dir = D::NAME,
                channels = C,
                threshold_s = threshold,
                upward_us = upward_us,
                downward_us = downward_us,
                collect_us = collect_us,
                total_us = total_us,
                upward_settled = upward_settled,
                settled_nodes = settled_nodes,
                // 0 on the PULL side, which has no block gating.
                blocks_active = blocks_active,
                blocks_total = state.n_blocks,
                "PHAST timing"
            );
        })
    })
}

// =============================================================================
// MATERIALISING SURFACES (the isochrone pipeline's shapes)
// =============================================================================

/// Run PHAST bounded query using thread-local state.
///
/// Returns `Vec<(rank, dist)>` of settled nodes only — avoids the 9.6 MB
/// output allocation a full distance vector would require.
pub fn run_phast_bounded_fast(
    up_adj_flat: &UpAdjFlat,
    down_adj_flat: &DownAdjFlat,
    origin_rank: u32,
    threshold: u32,
    mode: Mode,
) -> Vec<(u32, u32)> {
    run_phast_bounded_fast_seeded(
        up_adj_flat,
        down_adj_flat,
        &[(origin_rank, 0)],
        threshold,
        mode,
    )
}

/// #506: multi-seed variant — phantom isochrone origins. Each seed is
/// `(rank, partial_cost)`; equivalent to a super-source with non-negative
/// arcs, so the bounded upward sweep and the rank-order downward scan are
/// unchanged.
pub fn run_phast_bounded_fast_seeded(
    up_adj_flat: &UpAdjFlat,
    down_adj_flat: &DownAdjFlat,
    seeds: &[(u32, u32)],
    threshold: u32,
    mode: Mode,
) -> Vec<(u32, u32)> {
    let mut result: Vec<(u32, u32)> = Vec::with_capacity(up_adj_flat.n_nodes() / 10);
    run_seeded::<1, Forward>(
        ScanFlats::time(up_adj_flat, down_adj_flat),
        seeds.iter().map(|&(r, c)| (r, [c])),
        threshold,
        mode,
        |rank, v| result.push((rank, v[0])),
    );
    result
}

/// #527: 2-channel seeded bounded PHAST — a length-along-time channel
/// carried in lockstep with the time field (time primary, length follows the
/// improving parent). Returns settled `(rank, time, len_along_time)`.
pub fn run_phast_bounded_fast_seeded_2ch(
    up_adj_flat: &UpAdjFlat,
    down_adj_flat: &DownAdjFlat,
    up_adj_flat_len: &UpAdjFlat,
    down_adj_flat_len: &DownAdjFlat,
    seeds: &[(u32, u32, u32)], // (rank, time_cost, len_cost)
    threshold: u32,
    mode: Mode,
) -> Vec<(u32, u32, u32)> {
    let mut result: Vec<(u32, u32, u32)> = Vec::with_capacity(up_adj_flat.n_nodes() / 10);
    run_seeded::<2, Forward>(
        ScanFlats::with_len(
            up_adj_flat,
            down_adj_flat,
            up_adj_flat_len,
            down_adj_flat_len,
        ),
        seeds.iter().map(|&(r, t, l)| (r, [t, l])),
        threshold,
        mode,
        |rank, v| result.push((rank, v[0], v[1])),
    );
    result
}

/// Run REVERSE PHAST bounded query — computes `d(all → target)` for reverse
/// isochrones.
pub fn run_phast_bounded_fast_reverse(
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    target_rank: u32,
    threshold: u32,
    mode: Mode,
) -> Vec<(u32, u32)> {
    run_phast_bounded_fast_reverse_seeded(
        up_adj_flat,
        down_rev_flat,
        &[(target_rank, 0)],
        threshold,
        mode,
    )
}

/// #506: multi-seed reverse variant (arrive isochrones) — phantom center.
pub fn run_phast_bounded_fast_reverse_seeded(
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    seeds: &[(u32, u32)],
    threshold: u32,
    mode: Mode,
) -> Vec<(u32, u32)> {
    let mut result: Vec<(u32, u32)> = Vec::with_capacity(up_adj_flat.n_nodes() / 10);
    run_seeded::<1, Reverse>(
        ScanFlats::time(down_rev_flat, up_adj_flat),
        seeds.iter().map(|&(r, c)| (r, [c])),
        threshold,
        mode,
        |rank, v| result.push((rank, v[0])),
    );
    result
}

/// #527: 2-channel reverse seeded PHAST — `d(all → target)` with the
/// length-along-time channel carried.
pub fn run_phast_bounded_fast_reverse_seeded_2ch(
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    up_adj_flat_len: &UpAdjFlat,
    down_rev_flat_len: &DownReverseAdjFlat,
    seeds: &[(u32, u32, u32)], // (rank, time_cost, len_cost)
    threshold: u32,
    mode: Mode,
) -> Vec<(u32, u32, u32)> {
    let mut result: Vec<(u32, u32, u32)> = Vec::with_capacity(up_adj_flat.n_nodes() / 10);
    run_seeded::<2, Reverse>(
        ScanFlats::with_len(
            down_rev_flat,
            up_adj_flat,
            down_rev_flat_len,
            up_adj_flat_len,
        ),
        seeds.iter().map(|&(r, t, l)| (r, [t, l])),
        threshold,
        mode,
        |rank, v| result.push((rank, v[0], v[1])),
    );
    result
}

#[cfg(test)]
mod phast_2ch_lex_tests {
    //! #530: the 2-channel seeded bounded PHAST must apply the same
    //! (time, then length) lexicographic tie-break as `/route` (query.rs)
    //! and the bucket matrix (`SearchState2::relax`), so it cannot report a
    //! LONGER length among equal-duration paths. Without the tie-break the
    //! per-node length is first-arriving (PQ pop order), which lets the
    //! PHAST-lopsided 2-channel matrix disagree with `/route` on ties.
    use super::run_phast_bounded_fast_seeded_2ch;
    use crate::formats::{ArcCow, WeightArray};
    use crate::matrix::bucket_ch::{DownAdjFlat, UpAdjFlat};
    use crate::model::types::Mode;

    fn up_flat(offsets: Vec<u64>, targets: Vec<u32>, weights: Vec<u32>) -> UpAdjFlat {
        UpAdjFlat {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets),
            weights: WeightArray::from_vec_u32(weights),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        }
    }

    fn down_flat(offsets: Vec<u64>, targets: Vec<u32>, weights: Vec<u32>) -> DownAdjFlat {
        DownAdjFlat {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets),
            weights: WeightArray::from_vec_u32(weights),
        }
    }

    #[test]
    fn phast_2ch_picks_shorter_length_on_equal_time_tie() {
        // 4-node CCH, node id == rank. All edges are UP (low→high rank):
        //   0→1 (t=3, len=100)   0→2 (t=5, len=1)
        //   1→3 (t=7, len=100)   2→3 (t=5, len=1)
        // Node 3 is reachable via two EQUAL-TIME (=10) paths from seed 0:
        //   via node 1: length 200 — and its prefix (t=3) pops FIRST, so the
        //               order-dependent length would settle at 200.
        //   via node 2: length   2 — pops second (prefix t=5).
        // The lexicographic (time, then length) tie-break must report 2.
        let up_t = up_flat(vec![0, 2, 3, 4, 4], vec![1, 2, 3, 3], vec![3, 5, 7, 5]);
        let up_l = up_flat(vec![0, 2, 3, 4, 4], vec![1, 2, 3, 3], vec![100, 1, 100, 1]);
        // No DOWN edges — this isolates the upward-phase tie.
        let dn_t = down_flat(vec![0, 0, 0, 0, 0], Vec::new(), Vec::new());
        let dn_l = down_flat(vec![0, 0, 0, 0, 0], Vec::new(), Vec::new());

        let seeds = [(0u32, 0u32, 0u32)];
        let out = run_phast_bounded_fast_seeded_2ch(
            &up_t,
            &dn_t,
            &up_l,
            &dn_l,
            &seeds,
            1000,
            Mode::from_u8(0),
        );
        let node3 = out
            .iter()
            .find(|(r, _, _)| *r == 3)
            .expect("node 3 must be settled within threshold");
        assert_eq!(node3.1, 10, "duration is the primary key and must stay 10");
        assert_eq!(
            node3.2, 2,
            "must report the SHORTER equal-time length (2), not the \
             first-arriving 200"
        );
    }

    #[test]
    fn phast_2ch_shorter_length_arriving_first_is_kept() {
        // Mirror image: the SHORTER path now pops first. The result must be
        // unchanged (2), proving the tie-break never regresses a correct
        // first-arriving length. Swap the per-edge times so via-node-2 (the
        // shorter length) has the smaller prefix time.
        let up_t = up_flat(vec![0, 2, 3, 4, 4], vec![1, 2, 3, 3], vec![5, 3, 5, 7]);
        let up_l = up_flat(vec![0, 2, 3, 4, 4], vec![1, 2, 3, 3], vec![100, 1, 100, 1]);
        let dn_t = down_flat(vec![0, 0, 0, 0, 0], Vec::new(), Vec::new());
        let dn_l = down_flat(vec![0, 0, 0, 0, 0], Vec::new(), Vec::new());

        let seeds = [(0u32, 0u32, 0u32)];
        let out = run_phast_bounded_fast_seeded_2ch(
            &up_t,
            &dn_t,
            &up_l,
            &dn_l,
            &seeds,
            1000,
            Mode::from_u8(0),
        );
        let node3 = out
            .iter()
            .find(|(r, _, _)| *r == 3)
            .expect("node 3 must be settled within threshold");
        assert_eq!(node3.1, 10, "duration must stay 10");
        assert_eq!(
            node3.2, 2,
            "shorter length kept regardless of arrival order"
        );
    }
}
