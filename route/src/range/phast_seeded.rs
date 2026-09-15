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
    collect: impl FnMut(u32, [u32; C]),
) {
    run_seeded_gated::<C, 0, D>(flats, seeds, threshold, mode, collect)
}

/// [`run_seeded`] with the bound applied to channel `GATE` instead of the
/// primary one (#613).
///
/// `GATE = 0` is the plain bounded scan: the threshold is a bound on the
/// quantity the search OPTIMISES, so a node beyond it can never lie on an
/// optimal path to a node inside it, and every label inside the bound is
/// exact. That is the isochrone.
///
/// `GATE = 1` bounds the CARRIED channel, and it is a different animal.
/// Skipping the relaxations out of a node whose carried length already
/// exceeds the budget leaves nodes downstream of it labelled from a slower
/// parent — an over-estimate. What survives is exactly this, and it is what
/// the isodistance leans on:
///
/// * every node whose TRUE carried length is within the bound keeps its
///   EXACT label, because the lexicographic (time, then length) optimum is
///   prefix-optimal and length is monotone along a path: every node on such
///   a node's own optimal path is itself within the bound, so no arc of that
///   path is ever skipped;
/// * the nodes that are reported but should not be — true length above the
///   bound, corrupted label below it — are over-estimates on the PRIMARY
///   channel, never under-estimates, since every label is the cost of a real
///   path.
///
/// So the collected primary values are an upper bound on the primary cost of
/// every genuinely admissible node, and their maximum is a sound completeness
/// bound for a second, ordinary `GATE = 0` pass. Never serve a `GATE = 1`
/// field directly.
pub fn run_seeded_gated<const C: usize, const GATE: usize, D: ScanDir>(
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
        assert!(
            GATE == 0 || (GATE == 1 && C == 2),
            "the gate is the primary channel, or the length channel of a 2-channel scan"
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
                // #612: the SAME lexicographic rule `improve` applies. Two
                // seeds can land on one rank; taking the second only when it
                // is strictly faster would keep a LONGER length among equal
                // times, which is precisely the tie-break #530 added to the
                // relaxation so this surface could not disagree with /route.
                let cur = state.get_dist(r as usize);
                let better = v[0] < cur
                    || (C >= 2
                        && v[0] == cur
                        && cur != u32::MAX
                        && v[1] < state.get_len(r as usize));
                if better {
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
                // The PQ is ordered by the PRIMARY channel, so only a primary
                // bound may stop the sweep. A gate on the carried channel has
                // to skip the node and keep popping — length is not monotone
                // in pop order.
                if GATE == 0 && d > threshold {
                    break;
                }

                if d > state.get_dist(u as usize) {
                    continue; // Stale entry
                }
                if GATE == 1 && state.get_len(u as usize) > threshold {
                    continue;
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

                        if d_u == u32::MAX || (GATE == 0 && d_u > threshold) {
                            continue;
                        }
                        let l_u = if C >= 2 { state.get_len(rank) } else { 0 };
                        if GATE == 1 && l_u > threshold {
                            continue;
                        }

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
                        if d_u == u32::MAX || (GATE == 0 && d_u > threshold) {
                            continue;
                        }
                        if GATE == 1 && state.get_len(u) > threshold {
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
            // The gated channel decides membership too, or a length-gated
            // pass would hand out every node the time field happens to reach.
            let within = |state: &PhastState, rank: usize| {
                state.version[rank] == state.current_gen
                    && if GATE == 0 {
                        state.dist[rank] <= threshold
                    } else {
                        state.len[rank] <= threshold
                    }
            };
            if !D::PULL {
                for block_idx in 0..state.n_blocks {
                    if !state.is_block_active(block_idx) {
                        continue;
                    }
                    let block_start = block_idx * PHAST_BLOCK_SIZE;
                    let block_end = ((block_idx + 1) * PHAST_BLOCK_SIZE).min(n_nodes);
                    for rank in block_start..block_end {
                        if within(state, rank) {
                            settled_nodes += 1;
                            collect(rank as u32, state.label::<C>(rank));
                        }
                    }
                }
            } else {
                for rank in 0..n_nodes {
                    if within(state, rank) {
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

/// #612 (isodistance): the same 2-channel field, selected on the LENGTH
/// channel instead of the time one. Returns settled `(rank, time, len)` for
/// every node whose length-along-time is `≤ max_len`.
///
/// **The sweep that is SERVED may never be gated on length.** The
/// isodistance is "reachable within `max_len` metres ALONG THE TIME-SHORTEST
/// PATH" — the same path `/route` and `/table` report, which is the whole
/// reason the metric is consistent this time round (#371/#373). That makes
/// `len` a value carried by the time search, not a value the time search may
/// steer by. Gating the sweep on `len` — skipping relaxations out of a node
/// whose length already exceeds the budget — looks tempting, because length
/// is monotone along a path and the *reachable set* would still be
/// prefix-closed. It is wrong: a node whose time-optimal parent was skipped
/// is then labelled from a SLOWER parent, so its time label is
/// over-estimated and the length it carries is the length of a path the
/// engine would never drive. Exactly the nodes at the budget boundary — a
/// long fast motorway approach versus a short slow one — would flip, and
/// `/table`'s distance for that pair, which IS the truth here, would
/// disagree.
///
/// Filtering inside `collect` rather than afterwards is what keeps this
/// affordable in memory: the whole-graph settled set is ~2.4 M nodes on
/// Belgium and a `Vec` of it would be ~28 MB per query, where the
/// admissible set of a 5 km isodistance is ~24 k.
///
/// Nor can any LOWER bound on length prune the TIME propagation — not the
/// independent distance-shortest metric, not crow-fly, not landmarks —
/// because a long fast path outside the length budget is often exactly what
/// proves that an inside candidate's short slow path is not time-optimal.
/// The bound has to come from the primary channel, which is what #613 does
/// below.
///
/// Saturating the length channel at `max_len + 1` to skip the second
/// channel's arc reads was tried and measured: 68 ms either way. The
/// primary channel's random access is the wall, not the secondary one.
///
/// # #613: the field IS bounded, in two passes, and the answer is identical
///
/// A length gate cannot produce the answer — but it can produce a BOUND, and
/// that bound does not have to be guessed, iterated towards, or read off a
/// new weight set.
///
/// * **Pass 1** runs the same 2-channel field with the gate on the LENGTH
///   channel ([`run_seeded_gated`], `GATE = 1`). Every node whose true
///   length-along-time is within `max_len` comes out with its EXACT label:
///   the lexicographic optimum is prefix-optimal and length only grows along
///   a path, so every node on such a node's own optimal path is itself
///   within the budget and none of that path's arcs is ever skipped. What
///   the pass may ALSO report is a node whose true length is over budget
///   wearing a corrupted label — and a corrupted label is always the cost of
///   some real path, hence an OVER-estimate of the time.
/// * So `T = max(time over pass 1's output)` is `>= max(time over the
///   admissible set)` — pass 1 reports every admissible node, exactly.
/// * **Pass 2** is then an ordinary time-bounded field at `T`: exact for
///   every node it settles, and it settles every admissible one. Filtering
///   it on length gives precisely the unbounded scan's answer, node for node
///   and label for label — this function's contract is unchanged, which is
///   what lets `gate_isodistance_truth` keep checking it against the matrix.
///
/// What the two passes buy is that BOTH are bounded: pass 1 explores a ball
/// on the length channel, pass 2 a ball on time, and the block-gated
/// downward scan skips the rest of the hierarchy in each. The whole-graph
/// scan is gone.
///
/// **Measured (Belgium car, Brussels, warm, REST wall):** 1 km 79.7 → 1.4 ms,
/// 2 km 81.0 → 3.4 ms, 5 km 95.1 → 20.7 ms, 10 km 129.2 → 76.2 ms, 20 km
/// 153.9 → 120.7 ms. `POST /isochrone/bulk` over 30 origins at 2 km: 258 →
/// 8.5 ms.
///
/// **What still costs, so nobody re-derives it.** `T` is set by the ANSWER's
/// own slowest node, not by pass 1's imprecision — the `isodistance two-pass
/// bound` log line prints `bound_s` beside `bound_tight_s`, the largest time
/// the answer actually contains, and they are equal or within 1-2 % at every
/// origin and budget measured. A 10 km isodistance from Brussels genuinely
/// contains a point 3669 s away, so pass 2 must cover a 3669 s ball and no
/// time bound can be tighter. Tightening `T` is not where the remaining
/// milliseconds are; a target-restricted (rPHAST) pass 2 over the hierarchy
/// closure of pass 1's candidate set is, and it is a bigger change than this
/// one. Above ~10 km the polygon stage is the majority of the wall anyway,
/// and it is shared with time isochrones.
///
/// The arrive mirror gets no such win and deliberately does not try — see
/// [`run_phast_reverse_seeded_2ch_by_len`].
pub fn run_phast_seeded_2ch_by_len(
    up_adj_flat: &UpAdjFlat,
    down_adj_flat: &DownAdjFlat,
    up_adj_flat_len: &UpAdjFlat,
    down_adj_flat_len: &DownAdjFlat,
    seeds: &[(u32, u32, u32)], // (rank, time_cost, len_cost)
    max_len: u32,
    mode: Mode,
) -> Vec<(u32, u32, u32)> {
    let flats = || {
        ScanFlats::with_len(
            up_adj_flat,
            down_adj_flat,
            up_adj_flat_len,
            down_adj_flat_len,
        )
    };

    // Pass 1: the completeness bound. Nothing here is served.
    let mut time_bound: Option<u32> = None;
    let mut pass1_nodes = 0usize;
    run_seeded_gated::<2, 1, Forward>(
        flats(),
        seeds.iter().map(|&(r, t, l)| (r, [t, l])),
        max_len,
        mode,
        |_rank, v| {
            pass1_nodes += 1;
            time_bound = Some(time_bound.map_or(v[0], |t| t.max(v[0])));
        },
    );
    // No node is within the budget — not even a seed. The unbounded scan
    // would have filtered every settled node away, so: nothing.
    let Some(time_bound) = time_bound else {
        return Vec::new();
    };

    // Pass 2: the field that is served, bounded where pass 1 proved it can
    // be, and exact inside that bound.
    let mut result: Vec<(u32, u32, u32)> = Vec::new();
    run_seeded::<2, Forward>(
        flats(),
        seeds.iter().map(|&(r, t, l)| (r, [t, l])),
        time_bound,
        mode,
        |rank, v| {
            if v[1] <= max_len {
                result.push((rank, v[0], v[1]));
            }
        },
    );
    // `bound` vs `bound_tight` is how loose pass 1's certificate was: the
    // tight one is the largest time the answer actually contains, the bound
    // is what pass 2 had to be run at because pass 1 could not tell the
    // admissible nodes from the over-estimated ones. Their ratio is the
    // headroom a target-restricted scan would recover.
    tracing::debug!(
        max_len = max_len,
        pass1_nodes = pass1_nodes,
        bound_s = time_bound,
        bound_tight_s = result.iter().map(|&(_, t, _)| t).max().unwrap_or(0),
        admissible = result.len(),
        "isodistance two-pass bound"
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

/// #612: the ARRIVE mirror of [`run_phast_seeded_2ch_by_len`] — `d(all →
/// target)` selected on the length channel. Same reasoning about why the
/// time sweep is unbounded.
///
/// **#613: and unlike the depart side, it stays that way, on purpose.** The
/// two-pass bound over there works because a FORWARD downward scan is
/// block-gated: bound the field and the scan skips the blocks it never
/// reaches. This one PULLs — for every rank, from its higher-rank
/// neighbours — because there is no reverse-UP adjacency to PUSH along, and
/// a PULL cannot propagate block activation. The loop therefore reads every
/// rank and every arc whatever the threshold is: a bound saves the `improve`
/// calls and nothing else. Running a first pass here to earn a bound that
/// buys nothing would simply pay the full scan twice.
///
/// That is not an isodistance problem — an ARRIVE time isochrone pays the
/// same full scan, at any threshold. The fix is a reverse-UP adjacency so
/// the arrive field can PUSH and be block-gated like the depart one, which
/// would speed up every arrive query, not just this one, and costs a new
/// per-mode flat (RSS). Out of scope here; measured and written up in the
/// #613 report.
pub fn run_phast_reverse_seeded_2ch_by_len(
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    up_adj_flat_len: &UpAdjFlat,
    down_rev_flat_len: &DownReverseAdjFlat,
    seeds: &[(u32, u32, u32)], // (rank, time_cost, len_cost)
    max_len: u32,
    mode: Mode,
) -> Vec<(u32, u32, u32)> {
    let mut result: Vec<(u32, u32, u32)> = Vec::new();
    run_seeded::<2, Reverse>(
        ScanFlats::with_len(
            down_rev_flat,
            up_adj_flat,
            down_rev_flat_len,
            up_adj_flat_len,
        ),
        seeds.iter().map(|&(r, t, l)| (r, [t, l])),
        u32::MAX,
        mode,
        |rank, v| {
            if v[1] <= max_len {
                result.push((rank, v[0], v[1]));
            }
        },
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

    /// #612: the tie-break has to apply to the SEED INITIALISATION too.
    /// Phantom endpoints hand the sweep several seeds, and two of them can
    /// land on one rank; the init used to adopt a later seed only when it was
    /// strictly FASTER, which kept the longer of two equal-time partials and
    /// propagated it to every node downstream — the one place `improve`'s
    /// rule was not applied.
    #[test]
    fn two_seeds_on_one_rank_keep_the_shorter_equal_time_partial() {
        // One edge 0→1; the answer at node 1 is seed(0) + (t=1, len=10).
        let up_t = up_flat(vec![0, 1, 1], vec![1], vec![1]);
        let up_l = up_flat(vec![0, 1, 1], vec![1], vec![10]);
        let dn_t = down_flat(vec![0, 0, 0], Vec::new(), Vec::new());
        let dn_l = down_flat(vec![0, 0, 0], Vec::new(), Vec::new());

        // Both orders of the same two equal-time seeds on rank 0 must give
        // the same answer: the SHORTER partial wins either way.
        for seeds in [
            [(0u32, 5u32, 900u32), (0u32, 5u32, 7u32)],
            [(0u32, 5u32, 7u32), (0u32, 5u32, 900u32)],
        ] {
            let out = run_phast_bounded_fast_seeded_2ch(
                &up_t,
                &dn_t,
                &up_l,
                &dn_l,
                &seeds,
                1000,
                Mode::from_u8(0),
            );
            let node1 = out
                .iter()
                .find(|(r, _, _)| *r == 1)
                .expect("node 1 settled");
            assert_eq!(node1.1, 6, "time is 5 (seed) + 1 (edge)");
            assert_eq!(
                node1.2, 17,
                "length must be 7 (the shorter equal-time seed) + 10, never \
                 900 + 10, whichever seed came first: {seeds:?}"
            );
        }
    }
}

#[cfg(test)]
mod isodistance_bound_tests {
    //! #613: the isodistance field is bounded in two passes, and the answer
    //! must be the one the unbounded whole-graph scan gave.
    //!
    //! The first pass gates on the CARRIED channel, which on its own is
    //! WRONG — that is the whole reason #612 ran unbounded. These tests pin
    //! both halves: that the wrongness is real and of exactly one kind (a
    //! false inclusion, never a missing node), and that the second pass
    //! removes it.
    use super::{
        Forward, ScanFlats, run_phast_bounded_fast_seeded_2ch, run_phast_seeded_2ch_by_len,
        run_seeded_gated,
    };
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

    /// The "long fast approach versus short slow approach" node, built to
    /// flip. Rank order 0 < 1 < 2 < 3, every arc UP:
    ///
    /// * `0→1` t=1  len=1000   the long FAST approach
    /// * `0→2` t=50 len=10     the short SLOW approach
    /// * `1→3` t=1  len=10
    /// * `2→3` t=1  len=10
    ///
    /// Node 3's time-shortest path is via node 1: t=2, and the length it
    /// accumulates ALONG THAT PATH is 1010. With a 100 m budget node 3 is
    /// therefore OUT — even though a 20 m path to it exists, because that is
    /// not the path the engine would drive.
    fn flip_case() -> (UpAdjFlat, DownAdjFlat, UpAdjFlat, DownAdjFlat) {
        let up_t = up_flat(vec![0, 2, 3, 4, 4], vec![1, 2, 3, 3], vec![1, 50, 1, 1]);
        let up_l = up_flat(
            vec![0, 2, 3, 4, 4],
            vec![1, 2, 3, 3],
            vec![1000, 10, 10, 10],
        );
        let dn_t = down_flat(vec![0, 0, 0, 0, 0], Vec::new(), Vec::new());
        let dn_l = down_flat(vec![0, 0, 0, 0, 0], Vec::new(), Vec::new());
        (up_t, dn_t, up_l, dn_l)
    }

    #[test]
    fn length_gated_pass_alone_would_serve_a_node_that_is_out_of_budget() {
        // Not a bug report — a lock. If this ever stops being true the
        // second pass has become dead weight and somebody will delete it.
        let (up_t, dn_t, up_l, dn_l) = flip_case();
        let mut got: Vec<(u32, u32, u32)> = Vec::new();
        run_seeded_gated::<2, 1, Forward>(
            ScanFlats::with_len(&up_t, &dn_t, &up_l, &dn_l),
            [(0u32, [0u32, 0u32])].into_iter(),
            100,
            Mode::from_u8(0),
            |rank, v| got.push((rank, v[0], v[1])),
        );
        let node3 = got.iter().find(|(r, _, _)| *r == 3);
        assert!(
            node3.is_some(),
            "the length-gated pass is expected to over-report node 3 — it \
             reaches it from the slow parent once the fast one is gated out"
        );
        let (_, t3, l3) = *node3.unwrap();
        assert_eq!((t3, l3), (51, 20), "and to over-estimate its TIME (51 > 2)");
    }

    #[test]
    fn two_pass_isodistance_excludes_the_flipped_node() {
        let (up_t, dn_t, up_l, dn_l) = flip_case();
        let out = run_phast_seeded_2ch_by_len(
            &up_t,
            &dn_t,
            &up_l,
            &dn_l,
            &[(0u32, 0u32, 0u32)],
            100,
            Mode::from_u8(0),
        );
        let ranks: Vec<u32> = out.iter().map(|&(r, _, _)| r).collect();
        assert_eq!(
            ranks,
            vec![0, 2],
            "node 3's length along its TIME-shortest path is 1010 > 100, and \
             node 1's is 1000 > 100: only the seed and node 2 are within \
             budget"
        );
    }

    /// A deterministic pseudo-random rank-structured graph, exercised at
    /// every budget: the two-pass answer must equal the unbounded scan's,
    /// rank for rank and label for label.
    #[test]
    fn two_pass_matches_the_unbounded_scan_over_random_graphs() {
        const N: usize = 48;
        let mut rng_state: u64 = 0x5DEE_CE66_D1CE_B00D;
        let mut next = move || {
            rng_state = rng_state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (rng_state >> 33) as u32
        };

        for case in 0..40 {
            // Rank-structured: an arc between i<j is UP out of i and DOWN
            // out of j, so both scans see the same graph.
            let mut arcs: Vec<(usize, usize, u32, u32)> = Vec::new();
            for i in 0..N {
                for j in (i + 1)..N {
                    if next() % 10 != 0 {
                        continue;
                    }
                    arcs.push((i, j, 1 + next() % 60, 1 + next() % 400));
                }
            }
            let mut up: Vec<Vec<(u32, u32, u32)>> = vec![Vec::new(); N];
            let mut dn: Vec<Vec<(u32, u32, u32)>> = vec![Vec::new(); N];
            for &(i, j, t, l) in &arcs {
                up[i].push((j as u32, t, l));
                dn[j].push((i as u32, t, l));
            }
            let flatten = |adj: &[Vec<(u32, u32, u32)>]| {
                let mut offsets = vec![0u64];
                let (mut tg, mut wt, mut wl) = (Vec::new(), Vec::new(), Vec::new());
                for row in adj {
                    for &(v, t, l) in row {
                        tg.push(v);
                        wt.push(t);
                        wl.push(l);
                    }
                    offsets.push(tg.len() as u64);
                }
                (offsets, tg, wt, wl)
            };
            let (uo, ut, uwt, uwl) = flatten(&up);
            let (do_, dt, dwt, dwl) = flatten(&dn);
            let up_t = up_flat(uo.clone(), ut.clone(), uwt);
            let up_l = up_flat(uo, ut, uwl);
            let dn_t = down_flat(do_.clone(), dt.clone(), dwt);
            let dn_l = down_flat(do_, dt, dwl);

            // Multi-seed, with per-channel partials, like a phantom centre.
            let seeds = [
                (0u32, next() % 20, next() % 50),
                ((next() as usize % N) as u32, next() % 20, next() % 50),
            ];

            for max_len in [0u32, 25, 100, 400, 1500, 100_000] {
                let want: Vec<(u32, u32, u32)> = run_phast_bounded_fast_seeded_2ch(
                    &up_t,
                    &dn_t,
                    &up_l,
                    &dn_l,
                    &seeds,
                    u32::MAX,
                    Mode::from_u8(0),
                )
                .into_iter()
                .filter(|&(_, _, l)| l <= max_len)
                .collect();
                let got = run_phast_seeded_2ch_by_len(
                    &up_t,
                    &dn_t,
                    &up_l,
                    &dn_l,
                    &seeds,
                    max_len,
                    Mode::from_u8(0),
                );
                assert_eq!(
                    got, want,
                    "case {case}, budget {max_len}: the bounded field must be \
                     the unbounded one filtered, not an approximation of it"
                );
            }
        }
    }
}
