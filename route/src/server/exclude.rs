//! Exclude / avoid: re-customize CCH weights to block specific edges.
//!
//! At startup, builds per-EBG-edge exclude flags (toll/ferry/motorway)
//! from way attributes. At query time, computes a fresh `CchWeights`
//! set with the flagged edges treated as INF.
//!
//! The recustomization is **incremental** (#240): start from the
//! build-time relaxed weights + middles, seed a queue with every CCH
//! base edge whose underlying OSM edge is flagged, propagate
//! recomputation to dependent edges via triangle dependencies, and
//! terminate when the queue is empty. Work is bounded by the size of
//! what the mask actually reaches, not by graph size — a 1 km polygon
//! touches a corner of Belgium, `exclude=motorway` touches the whole
//! long-distance hierarchy.
//!
//! The queue pops in increasing LEVEL order (#606) so every triangle
//! leg settles before the edge that reads it: one recomputation per
//! touched edge, and the same fixed point a from-scratch bottom-up
//! customization reaches. See `recustomize_weights_incremental` for the
//! algorithm and the dependency walk in `enqueue_dependents`.

/// Pack (weight, middle_rank) into a single u64 so the (weight, middle)
/// pair compares lexicographically as a unit: high 32 bits hold the
/// weight, low 32 bits hold the middle. `recompute_edge_weight` picks
/// the lex-smallest packed value over all candidate triangles, which
/// gives a deterministic (weight, middle) tuple even when several
/// middles produce the same weight.
///
/// Build-time customization (`customization.rs::triangle_relax_parallel`)
/// uses the same packing inside `AtomicU64::fetch_min`; serve-time
/// recustomization is single-threaded per call so no atomics here.
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

use crate::formats::way_attrs;
use crate::formats::{CchTopo, CchWeights, EbgNodes};
use crate::matrix::bucket_ch::{DownAdjFlat, DownReverseAdjFlat, UpAdjFlat};
use crate::model::types::class_bits;

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

/// #407: default LRU capacity for the per-mode exclude-weight cache.
///
/// The exclude mask is a `u8` with only three meaningful bits
/// (toll/ferry/motorway), so at most 8 distinct entries can ever exist
/// per mode. Each entry is **~1.5 GB of resident memory on Belgium**
/// (two `CchWeights` + six flat adjacencies sized to the CCH) — measured
/// as the server's RSS step across a cold computation, #606; the "5-8 GB"
/// this comment used to claim was an estimate, and wrong by 4x. An
/// unbounded cache still pins ~7 × 1.5 GB × n_modes of heap that never
/// releases, so the bound stays: cap 3 holds ~4.5 GB for a single mode,
/// and a miss costs one `compute_exclude_weights` — which is NOT cheap
/// (see `recustomize_weights`), so raise the cap rather than lower it if
/// a deployment actually uses several masks.
pub const DEFAULT_EXCLUDE_CACHE_CAP: usize = 3;

struct ExcludeCacheInner {
    // key = exclude_mask; value = (weights, last-touched generation).
    map: std::collections::HashMap<u8, (std::sync::Arc<ExcludeWeights>, u64)>,
    generation: u64,
    capacity: usize,
    hits: u64,
    misses: u64,
}

/// #407: bounded LRU for per-mode exclude weights, mirroring
/// [`super::avoid::AvoidWeightCache`]. Keyed on the raw `u8` exclude
/// mask — the cache lives inside `ModeData`, so the mode is implicit in
/// the key and the whole cache is dropped (and rebuilt) when #402
/// evicts the mode. Eviction is safe: an in-flight query holds its own
/// `Arc<ExcludeWeights>` clone, so a removed map entry only frees once
/// the last clone is released.
pub struct ExcludeWeightCache {
    inner: parking_lot::RwLock<ExcludeCacheInner>,
    /// #606 single flight: one lock per possible mask value (the mask is a
    /// u8 with three meaningful bits, so eight slots cover every key that
    /// can ever exist). A cold recustomization is seconds to a minute, and
    /// the realistic burst is the SAME mask arriving repeatedly — a client
    /// retrying after the 120 s `TimeoutLayer` gave up, or a fleet warming
    /// together. Without this each of them recomputes the same answer on its
    /// own blocking thread, which is how a rare option turns into a
    /// denial of service against ourselves. With it the first caller
    /// computes and the rest wait on its result.
    flight: [parking_lot::Mutex<()>; 256],
}

impl ExcludeWeightCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: parking_lot::RwLock::new(ExcludeCacheInner {
                map: std::collections::HashMap::with_capacity(capacity.max(1)),
                generation: 0,
                capacity: capacity.max(1),
                hits: 0,
                misses: 0,
            }),
            flight: [const { parking_lot::Mutex::new(()) }; 256],
        }
    }

    /// Hold the single-flight slot for `mask` (see [`ExcludeWeightCache`]).
    /// The caller MUST re-check the cache after acquiring it: the previous
    /// holder has usually just filled it.
    pub fn flight_guard(&self, mask: u8) -> parking_lot::MutexGuard<'_, ()> {
        self.flight[mask as usize].lock()
    }

    /// Return the cached entry for `mask`, bumping its LRU generation
    /// stamp. `None` on miss.
    pub fn get(&self, mask: u8) -> Option<std::sync::Arc<ExcludeWeights>> {
        // Fast path: read lock + presence check.
        if !self.inner.read().map.contains_key(&mask) {
            return None;
        }
        // Slow path: write lock to bump the LRU generation atomically.
        let mut inner = self.inner.write();
        let new_gen = inner.generation.wrapping_add(1);
        if let Some((entry, gen_stamp)) = inner.map.get_mut(&mask) {
            *gen_stamp = new_gen;
            let entry_clone = std::sync::Arc::clone(entry);
            inner.generation = new_gen;
            inner.hits += 1;
            return Some(entry_clone);
        }
        None
    }

    /// Insert `entry` for `mask`, evicting the least-recently-used entry
    /// first when at capacity.
    pub fn insert(&self, mask: u8, entry: std::sync::Arc<ExcludeWeights>) {
        let mut inner = self.inner.write();
        inner.misses += 1;
        if !inner.map.contains_key(&mask)
            && inner.map.len() >= inner.capacity
            && let Some(victim) = inner
                .map
                .iter()
                .min_by_key(|(_, (_, g))| *g)
                .map(|(k, _)| *k)
        {
            inner.map.remove(&victim);
        }
        inner.generation = inner.generation.wrapping_add(1);
        let gen_stamp = inner.generation;
        inner.map.insert(mask, (entry, gen_stamp));
    }

    /// (hits, misses, current size, capacity) — operational visibility.
    pub fn stats(&self) -> (u64, u64, usize, usize) {
        let inner = self.inner.read();
        (inner.hits, inner.misses, inner.map.len(), inner.capacity)
    }
}

impl Default for ExcludeWeightCache {
    fn default() -> Self {
        let cap = std::env::var("BUTTERFLY_EXCLUDE_CACHE_CAP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_EXCLUDE_CACHE_CAP);
        Self::new(cap)
    }
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

/// Compute time-only exclude weights (for P2P route queries).
///
/// Skips distance recustomization and flat adjacency builds. Uses the
/// incremental BFS recustomization (#240) — work is bounded by
/// polygon size rather than graph size. On Belgium, a 1 km rural
/// polygon takes ~780 ms instead of ~37 s.
pub fn compute_exclude_weights_time_only(
    topo: &CchTopo,
    base_time: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> CchWeights {
    let start = std::time::Instant::now();

    // #606: `recustomize_weights` picks the cheaper shape for this mask —
    // the incremental walk when the mask reaches little, the pipeline's
    // bottom-up + parallel relax when it reaches a whole road class.
    let time_weights = recustomize_weights(
        topo,
        base_time,
        edge_exclude_flags,
        exclude_mask,
        filtered_to_original,
    );

    tracing::info!(
        exclude_mask,
        elapsed_ms = start.elapsed().as_millis(),
        "computed exclude weights (time-only)"
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

    // Re-customize time and distance weights in parallel. #606:
    // `recustomize_weights` picks the shape per mask — the incremental walk
    // is bounded by the mask's reach, the from-scratch one by the graph but
    // uses every core.
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

/// Reverse DOWN adjacency: for each node m, the set of nodes x with a
/// DOWN edge x → m. Built once per recustomization and reused by the
/// BFS dependency walk.
struct ReverseDownAdj {
    offsets: Vec<u64>,
    sources: Vec<u32>,
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
    let mut insert = vec![0u64; n_nodes];

    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let m = topo.down_targets[i] as usize;
            let pos = (offsets[m] + insert[m]) as usize;
            sources[pos] = u as u32;
            insert[m] += 1;
        }
    }

    ReverseDownAdj { offsets, sources }
}

// ============================================================================
// #606 Two shapes, one fixed point — and the switch between them
// ============================================================================
//
// Both `recustomize_weights_incremental` and `recustomize_weights_from_scratch`
// solve the SAME system: every CCH edge's weight is the minimum of its own
// floor (a base edge's arc cost, INF when that arc is masked; INF for a
// shortcut, which owns no arc) and every two-hop through a lower-ranked apex.
// `both_recustomization_shapes_agree` asserts they return identical weights.
//
// They differ only in HOW they reach it, and that is a pure cost question:
//
//   * INCREMENTAL walks only the edges the mask can reach, one at a time, in
//     level order. Cost scales with the mask's reach, so a small avoid polygon
//     is ~1 s — but a whole road class reaches the entire long-distance
//     hierarchy and the serial walk then costs minutes.
//   * FROM SCRATCH is the pipeline's own step 8 — a cheap bottom-up pass over
//     the recorded middles, then the parallel triangle relaxation to a fixed
//     point. It always touches every edge, so it costs the same whatever the
//     mask is, but every core works.
//
// `SCRATCH_SEED_THRESHOLD` is where one overtakes the other, DERIVED from
// measurements on Belgium (car, 20-core, both metrics concurrent — the two
// points and the derivation are in CHANGELOG.md):
//
//   | mask                  | seeded arcs | incremental | from scratch |
//   |-----------------------|-------------|-------------|--------------|
//   | ~900 m avoid polygon  |         938 |    17.7 s   |     68.6 s   |
//   | `exclude=motorway`    |      20 835 |   228.1 s   |     59.1 s   |
//
// The from-scratch cost barely moves with the mask (59-69 s — a wider mask
// makes the relaxation converge in FEWER passes); the incremental cost grows
// with the seed count, 0.011-0.019 s per seed. The two therefore cross
// somewhere between 60/0.019 ≈ 3 200 and 69/0.011 ≈ 6 300 seeds, and the
// threshold takes the CONSERVATIVE end: at 3 500 seeds the incremental
// path's own worst case (~66 s) is still around the from-scratch cost, so
// **no mask can cost more than about 70 s**. That bound is the property that
// matters, because a synchronous computation inside an axum handler cannot
// be interrupted once it starts — the 120 s `TimeoutLayer` never gets polled
// (measured: a 228 s cold request returned 200, not 408). The common
// street-sized polygon still takes the incremental path and pays 17.7 s
// rather than 68.6 s.
//
// `BUTTERFLY_EXCLUDE_SHAPE=incremental|scratch` forces one shape (dev only —
// the point is to be able to measure and compare the two on the same server,
// never to guess which one ran).

/// Seeded base arcs at or above which the from-scratch shape is cheaper.
/// Derived, not fitted — see the table above.
pub const SCRATCH_SEED_THRESHOLD: usize = 3_500;

/// Which shape a recustomization took — logged, and what the shape tests
/// assert against so a switch regression cannot hide behind equal results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecustomizeShape {
    Incremental,
    FromScratch,
}

impl RecustomizeShape {
    fn as_str(self) -> &'static str {
        match self {
            Self::Incremental => "incremental",
            Self::FromScratch => "scratch",
        }
    }
}

/// The forced shape, if `BUTTERFLY_EXCLUDE_SHAPE` names one.
fn forced_shape() -> Option<RecustomizeShape> {
    match std::env::var("BUTTERFLY_EXCLUDE_SHAPE").ok()?.as_str() {
        "incremental" => Some(RecustomizeShape::Incremental),
        "scratch" => Some(RecustomizeShape::FromScratch),
        _ => None,
    }
}

/// How many CCH BASE edges the mask blocks outright — the seed count the
/// switch reads, and the only thing that distinguishes a street-sized polygon
/// from a whole road class.
pub fn count_seeded_edges(
    topo: &CchTopo,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> usize {
    use rayon::prelude::*;
    (0..topo.n_nodes as usize)
        .into_par_iter()
        .map(|source| {
            let mut n = 0usize;
            let (s, e) = (
                topo.up_offsets[source] as usize,
                topo.up_offsets[source + 1] as usize,
            );
            for idx in s..e {
                if !topo.up_is_shortcut.bit(idx)
                    && cch_base_edge_excluded(
                        topo.up_targets[idx] as usize,
                        topo,
                        edge_exclude_flags,
                        exclude_mask,
                        filtered_to_original,
                    )
                {
                    n += 1;
                }
            }
            let (s, e) = (
                topo.down_offsets[source] as usize,
                topo.down_offsets[source + 1] as usize,
            );
            for idx in s..e {
                if !topo.down_is_shortcut.bit(idx)
                    && cch_base_edge_excluded(
                        topo.down_targets[idx] as usize,
                        topo,
                        edge_exclude_flags,
                        exclude_mask,
                        filtered_to_original,
                    )
                {
                    n += 1;
                }
            }
            n
        })
        .sum()
}

/// Recustomize with whichever shape is cheaper for this mask (#606).
pub fn recustomize_weights(
    topo: &CchTopo,
    base_weights: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> CchWeights {
    let seeds = count_seeded_edges(topo, edge_exclude_flags, exclude_mask, filtered_to_original);
    let shape = forced_shape().unwrap_or(if seeds >= SCRATCH_SEED_THRESHOLD {
        RecustomizeShape::FromScratch
    } else {
        RecustomizeShape::Incremental
    });
    let start = std::time::Instant::now();
    let out = match shape {
        RecustomizeShape::Incremental => recustomize_weights_incremental(
            topo,
            base_weights,
            edge_exclude_flags,
            exclude_mask,
            filtered_to_original,
        ),
        RecustomizeShape::FromScratch => recustomize_weights_from_scratch(
            topo,
            base_weights,
            edge_exclude_flags,
            exclude_mask,
            filtered_to_original,
        ),
    };
    tracing::info!(
        shape = shape.as_str(),
        seeded_base_edges = seeds,
        threshold = SCRATCH_SEED_THRESHOLD,
        forced = forced_shape().is_some(),
        elapsed_ms = start.elapsed().as_millis(),
        "recustomized CCH weights"
    );
    out
}

/// The pipeline's own customization, run over the MASKED graph: bottom-up over
/// the recorded middles, then the parallel triangle relaxation to a fixed
/// point. Same primitives as step 8 — `customization::bottom_up_customize` and
/// `customization::triangle_relax_parallel` — so there is exactly ONE triangle
/// relaxation in the engine and the serve path cannot drift from the build.
///
/// The floor it starts each BASE edge from is the base edge's own weight, or
/// INF when the arc is masked; a shortcut has no floor, exactly as in the
/// incremental walk.
pub fn recustomize_weights_from_scratch(
    topo: &CchTopo,
    base_weights: &CchWeights,
    edge_exclude_flags: &[u8],
    exclude_mask: u8,
    filtered_to_original: &[u32],
) -> CchWeights {
    let sorted_down = crate::customization::sorted_down_indices(topo);
    let (up, down) = crate::customization::bottom_up_customize(topo, &sorted_down, |u, v| {
        if cch_base_edge_excluded(
            v,
            topo,
            edge_exclude_flags,
            exclude_mask,
            filtered_to_original,
        ) {
            return u32::MAX;
        }
        // `bottom_up_customize` calls this for a DOWN edge when rank(v) <
        // rank(u) and for an UP edge otherwise, so the rank order names the
        // array without an extra argument.
        if v > u {
            find_edge_index(u, v, &topo.up_offsets, &topo.up_targets)
                .map(|i| base_weights.up.get(i))
                .unwrap_or(u32::MAX)
        } else {
            find_edge_index(u, v, &topo.down_offsets, &topo.down_targets)
                .map(|i| base_weights.down.get(i))
                .unwrap_or(u32::MAX)
        }
    });
    drop(sorted_down);
    let rev_down = crate::customization::build_reverse_down_adj_for_relax(topo);
    let (up, down, up_middle, down_middle, _relaxations, _passes) =
        crate::customization::triangle_relax_parallel(topo, up, down, &rev_down, false);
    CchWeights {
        up: up.into(),
        down: down.into(),
        up_middle: up_middle.into(),
        down_middle: down_middle.into(),
    }
}

// ============================================================================
// #240 Incremental recustomization
// ============================================================================
//
// The incremental version starts from the BASE weights + base middles and
// only re-evaluates edges that depend, transitively, on a polygon-flagged
// base edge. Cost is O(|touched_shortcuts| × deg) rather than O(|edges|).
//
// Algorithm:
//   1. Initialise (up_weights, down_weights, up_middle, down_middle) to the
//      base build-time values — those are already triangle-relaxed for the
//      no-avoid graph.
//   2. Seed a queue with every CCH base edge whose underlying OSM edge
//      is in the polygon. For each, mark it as needing recomputation.
//   3. Pop edges in increasing LEVEL order (see below), recompute their
//      (weight, middle) by considering every triangle (x, m, y) where
//      x = edge.source and y = edge.target. If the result changed, write it
//      and enqueue every edge that uses this one as a triangle leg.
//   4. Terminate when the queue is empty.
//
// #606 — the two properties that make this exact, both of which the
// original BFS lacked:
//
//   a) A SHORTCUT is recomputed from INFINITY. A shortcut has no arc of its
//      own: it is *defined* as the best two-hop through a lower-ranked apex.
//      Seeding its recomputation with its BASE value (as the first version
//      did) turned `min(base, triangles)` into a floor no exclusion could
//      ever raise — a shortcut summarising a corridor through an excluded
//      edge kept the excluded corridor's weight, and, since nothing changed,
//      never enqueued its own dependents either, so the propagation died one
//      level above the seeds. On Belgium that made `exclude=motorway` move an
//      inter-city duration by under 4 % (long routes ride high-level
//      shortcuts almost exclusively) while a short hop, which uses more base
//      edges, moved by 58 %. `avoid_polygons` recustomizes through the same
//      function and had the same hole.
//
//   b) A BASE edge keeps its base value as its floor. `unpack_path` renders
//      a base CCH edge atomically — its weight IS the cost of that one EBG
//      arc, which removing OTHER arcs cannot change — so the floor is the
//      arc's own cost and is exactly right. (Build-time relaxation may lower
//      a base edge below its arc cost when a turn penalty on it exceeds a
//      two-hop around it; such an edge keeps a value a few seconds optimistic
//      here. It cannot put an excluded edge into a served route: unpack still
//      emits the direct arc.)
//
// LEVEL = min(rank(source), rank(target)). Every triangle leg of an edge
// runs through an apex m with rank(m) < min(rank(source), rank(target)), so a
// leg's level is STRICTLY below its closing edge's. Popping in increasing
// level order therefore settles every leg before the edge that reads it —
// one recomputation per touched edge, no re-visits, and the same fixed point
// a full bottom-up customization reaches. The FIFO it replaces re-visited an
// edge once per leg that moved, which (a) made much more expensive.
//
// COST, measured (Belgium, car, ≈5 M EBG nodes): a 1 km avoid polygon is ~1 s
// — it reaches almost nothing. `exclude=motorway` is **245 s**, because
// removing a whole road class reaches the entire long-distance hierarchy, and
// the walk is one edge at a time (the two metrics run concurrently via
// `rayon::join`; the levels themselves are sequential). The result caches per
// (mode, mask) and every later request is ~15 ms, and the cold call runs off
// the tokio worker (`avoid::off_runtime`). Parallelising WITHIN a level is
// sound — a level's edges are independent by the argument above — but a level
// holds ~13 edges on Belgium, too few to pay for the fan-out. The shape that
// would actually win on a class-wide mask is the build's: a cheap bottom-up
// pass over the recorded middles, then the parallel triangle relaxation to a
// fixed point (~35-40 s for the whole graph). It would cost every SMALL mask
// that same 40 s, so it belongs behind a seed-count switch, not in place of
// this walk.

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

impl EdgeRef {
    /// See the LEVEL note above: the lower of the two endpoint ranks.
    #[inline]
    fn level(&self) -> usize {
        self.source.min(self.target)
    }
}

/// One heap entry, ordered by [`EdgeRef::level`] ascending. The `(dir, idx)`
/// tie-break makes the pop order total, so the recustomization is
/// deterministic for a given input.
#[derive(Clone, Copy, Debug)]
struct QueueItem(EdgeRef);

impl QueueItem {
    #[inline]
    fn key(&self) -> (usize, u8, usize) {
        let dir = match self.0.dir {
            EdgeDir::Up => 0u8,
            EdgeDir::Down => 1u8,
        };
        (self.0.level(), dir, self.0.idx)
    }
}

impl PartialEq for QueueItem {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for QueueItem {}
impl PartialOrd for QueueItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for QueueItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reversed: `BinaryHeap` is a max-heap and we pop the lowest level.
        other.key().cmp(&self.key())
    }
}

/// The queue the recustomization walks: a min-heap on LEVEL.
type EdgeQueue = std::collections::BinaryHeap<QueueItem>;

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
    let mut up_weights = base_weights.up.iter().collect::<Vec<u32>>();
    let mut down_weights = base_weights.down.iter().collect::<Vec<u32>>();
    let mut up_middle = if base_weights.up_middle.len() == topo.up_targets.len() {
        base_weights.up_middle.to_vec()
    } else {
        topo.up_middle.to_vec_u32()
    };
    let mut down_middle = if base_weights.down_middle.len() == topo.down_targets.len() {
        base_weights.down_middle.to_vec()
    } else {
        topo.down_middle.to_vec_u32()
    };

    let mut queued_up = vec![false; topo.up_targets.len()];
    let mut queued_down = vec![false; topo.down_targets.len()];
    let mut queue: EdgeQueue = EdgeQueue::new();
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

    while let Some(QueueItem(edge)) = queue.pop() {
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
    queue: &mut EdgeQueue,
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
        queue.push(QueueItem(edge));
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

    // #606 — where the scan starts, and why.
    //
    // A SHORTCUT starts at INFINITY: it owns no arc, it only ever means
    // "the best two-hop through a lower-ranked apex", so the triangle scan
    // below IS its definition. Starting it at its BASE value instead made
    // the base value an unraisable floor, which is precisely how a corridor
    // through an excluded edge survived the exclusion.
    //
    // A BASE edge starts at its base value — the cost of the one EBG arc it
    // is, which excluding OTHER arcs cannot change — or at INFINITY when the
    // arc is itself excluded. The triangle scan can then only find it a
    // legal detour.
    let base_excluded = !is_shortcut
        && cch_base_edge_excluded(
            edge.target,
            topo,
            edge_exclude_flags,
            exclude_mask,
            filtered_to_original,
        );
    let mut best_weight = if is_shortcut || base_excluded {
        u32::MAX
    } else {
        match edge.dir {
            EdgeDir::Up => base_weights.up.get(edge.idx),
            EdgeDir::Down => base_weights.down.get(edge.idx),
        }
    };
    let mut best_middle = match edge.dir {
        EdgeDir::Up => topo.up_middle.get(edge.idx),
        EdgeDir::Down => topo.down_middle.get(edge.idx),
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
    queue: &mut EdgeQueue,
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
    queue: &mut EdgeQueue,
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
