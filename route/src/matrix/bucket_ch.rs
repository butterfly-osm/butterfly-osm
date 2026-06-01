//! Bucket-based Many-to-Many CH Algorithm
//!
//! This module implements the classic "bucket" algorithm for computing distance matrices
//! on Contraction Hierarchies. Unlike PHAST which computes one-to-ALL distances,
//! this algorithm efficiently computes N×M matrices by:
//!
//! 1. Forward phase: Run upward search from each source, storing (source_id, dist) in buckets
//! 2. Backward phase: Run backward search from each target, joining with buckets
//!
//! Complexity: O(N × upward_search + M × backward_search + bucket_joins)
//! Much faster than PHAST for sparse matrices (small N, M relative to graph size).
//!
//! ## Optimizations
//!
//! - **Flat reverse adjacency**: Stores (source, weight) directly, eliminating edge_idx indirection
//! - **4-ary heap**: Better cache locality than binary heap (4 children per node)
//! - **Bucket prefix-sum layout**: O(1) lookup instead of O(log n) binary search
//! - **Version-stamped distances**: Amortized O(1) per-search initialization

use crate::formats::{ArcCow, CchTopo, CchWeights};

// Thread-local `SearchState` scratch buffers for the parallel bucket M2M path.
//
// Rayon fans one closure invocation out per source/target. Allocating a
// fresh `SearchState` (two `Vec`s sized to `n_nodes` = ~2.4 M on Belgium,
// zero-filled = ~30 MB of memory traffic) inside each closure dominates
// small-matrix latency — the routing work itself only touches a few
// thousand nodes per search. We instead cache the state per rayon worker
// thread and call `SearchState::start_search()` (O(1) via version
// stamping) between invocations. Reinitialise lazily when `n_nodes`
// changes, so swapping graphs across calls stays safe.
// #409/#410: every bucket-M2M scratch arena is an `EvictableCell` so
// the idle-compactor reclaims it on ANY thread — the small-N `/table`
// fast paths run on the Tokio handler / spawn_blocking pool, which the
// old `rayon::broadcast`-based eviction could never reach. The cells
// own their `Option`/`Vec` interior, so an idle take() frees the whole
// arena; the next query rebuilds via the `with_or_init` initialiser.
use crate::server::evictable::EvictableCell;
thread_local! {
    static FORWARD_STATE: EvictableCell<SearchState> = const { EvictableCell::new() };
    static BACKWARD_STATE: EvictableCell<SearchState> = const { EvictableCell::new() };
    static FORWARD_BUCKET_ITEMS: EvictableCell<Vec<(u32, u32, u32)>> =
        const { EvictableCell::new() };
    // 2-channel (#372) thread-local state — kept separate so single-
    // channel callers don't pay the parallel-lat allocation.
    static FORWARD_STATE_LAT: EvictableCell<SearchState2> = const { EvictableCell::new() };
    static BACKWARD_STATE_LAT: EvictableCell<SearchState2> = const { EvictableCell::new() };
    static FORWARD_BUCKET_ITEMS_LAT: EvictableCell<Vec<(u32, u32, u32, u32)>> =
        const { EvictableCell::new() };
    // Sequential 2-channel engine (#395) — when small-N /table dispatches
    // to `table_bucket_full_flat_len_along_time`, reuse a single thread-
    // local SearchState2 instead of allocating ~80 MB per call. Tied to
    // the calling thread (the axum handler thread, not a rayon worker)
    // so it doesn't conflict with the parallel path's per-rayon-worker
    // FORWARD_STATE_LAT / BACKWARD_STATE_LAT.
    static SEQ_STATE_LAT: EvictableCell<SearchState2> = const { EvictableCell::new() };
    static SEQ_BUCKETS_LAT: EvictableCell<PrefixSumBuckets2> = const { EvictableCell::new() };
    static SEQ_BUCKET_ITEMS_LAT: EvictableCell<Vec<(u32, u32, u32, u32)>> =
        const { EvictableCell::new() };

    // Sequential engine for the small-N fast path (#129). At low cell
    // counts (~≤ 1000) rayon's thread-dispatch + work-stealing overhead
    // dwarfs the actual routing work, so we skip the parallel path
    // entirely and run sequentially in a single thread-cached engine.
    static SEQUENTIAL_ENGINE: EvictableCell<BucketM2MEngine> = const { EvictableCell::new() };
}

/// Cell-count threshold below which `table_bucket_parallel` dispatches
/// to the sequential thread-local engine instead of fanning rayon out.
///
/// Picked empirically (#129): the only size where the sequential path
/// beats the parallel path on Belgium is the 10×10 corner — at 25×25
/// (625 cells) parallel already wins by ~6× because there's enough
/// work to amortise rayon's thread-dispatch. We keep the threshold
/// tight (≤ 100 cells, i.e. ≤ 10×10) to avoid regressing 25×25 and up.
const SEQUENTIAL_FAST_PATH_CELL_THRESHOLD: usize = 100;

// =============================================================================
// FLAT ADJACENCY STRUCTURES - Pre-filtered INF edges
// =============================================================================

/// Flat forward adjacency for UP edges with embedded weights.
///
/// Filters out INF-weight edges at build time. `topo_edge_idx[i]` is a
/// back-reference to the original topo edge index — populated only for
/// flats that feed `CchQuery` (so `unpack_path` can recover the topo
/// edge from a parent pointer). Distance-metric flats and PHAST-only
/// flats leave it empty to keep memory down.
///
/// #306 PR 4: build a `WeightArray` from an owned `Vec<u32>` of
/// computed weights, narrowing to u16 when every value fits the
/// compact codec.
fn build_weight_array(
    weights_u32: Vec<u32>,
    width: crate::formats::WeightWidth,
) -> crate::formats::WeightArray {
    use crate::formats::{U24_SENTINEL, WeightArray, WeightWidth};
    match width {
        WeightWidth::U32 => WeightArray::from_vec_u32(weights_u32),
        WeightWidth::U24 => {
            // 3-byte LE storage; u32::MAX → U24_SENTINEL.
            let n = weights_u32.len();
            let mut bytes: Vec<u8> = Vec::with_capacity(n * 3);
            for &w in &weights_u32 {
                let v: u32 = if w == u32::MAX { U24_SENTINEL } else { w };
                let le = v.to_le_bytes();
                bytes.extend_from_slice(&le[..3]);
            }
            WeightArray::from_u24_bytes(bytes, n)
        }
        WeightWidth::U16 => {
            let v16: Vec<u16> = weights_u32
                .into_iter()
                .map(|w| {
                    if w == u32::MAX {
                        u16::MAX
                    } else {
                        // `WeightWidth::choose` only returns U16 when all
                        // finite values fit in u16 (and < u16::MAX), so
                        // the cast is lossless.
                        w as u16
                    }
                })
                .collect();
            WeightArray::from_vec_u16(v16)
        }
    }
}

/// Shared topology bytes for a `(mode, direction)` pair (#345).
///
/// Built once per direction, then referenced by every metric variant
/// (`time`, `dist`, ...) via `Arc<FlatTopo>`. Replaces the per-metric
/// duplication that the monolithic v4 flat format incurred — Belgium
/// bike `up_adj_flat.dist` was ~613 MB of redundant topo bytes already
/// present in `up_adj_flat.time`.
///
/// All fields are `ArcCow<T>` so the struct can be built fresh in
/// memory (tests, customize) or mmap-mapped from a `FlatTopo`
/// container section at server boot. The `Arc<Mmap>` clone keeps the
/// mapping alive as long as any view references it.
#[derive(Clone)]
pub struct FlatTopo {
    pub n_nodes: u32,
    pub offsets: ArcCow<u64>, // n_nodes + 1
    /// Targets (for UP / DOWN) or sources (for DOWN_REVERSE).
    /// Naming is direction-dependent; the field stays generic so
    /// downstream consumers don't care which side it is.
    pub targets: ArcCow<u32>,
    /// Back-reference to topo edge index per flat slot. Empty unless
    /// this flat feeds the routing hot path (`CchQuery::new` / the
    /// alternatives backend).
    pub topo_edge_idx: ArcCow<u32>,
}

/// Flat fields are `ArcCow<T>` so a single struct can either own its
/// arrays (legacy heap path: `UpAdjFlat::build`) or borrow them straight
/// from a live `Arc<Mmap>` (the #296 mmap path:
/// `UpAdjFlatFile::read_from_mmap_unverified`). The `Mmap` variant
/// holds an `Arc<Mmap>` clone so dropping the flat decrements the
/// mapping's strong count — no more leaked Arcs that pin the file in
/// RSS forever. All consumers index through the auto-deref to `&[u32]`
/// / `&[u64]` and never see the ArcCow wrapper.
#[derive(Clone)]
pub struct UpAdjFlat {
    pub offsets: ArcCow<u64>, // n_nodes + 1
    pub targets: ArcCow<u32>, // target node for edge
    /// Embedded weight per slot. `WeightArray` carries either u16 or
    /// u32 storage transparently — see `crate::formats::cch_weights`
    /// for the codec. Width is picked at build time based on the
    /// underlying `CchWeights` source (#306 PR 4).
    pub weights: crate::formats::WeightArray,
    /// Back-reference to topo edge index per flat slot. Empty unless
    /// this flat feeds the routing hot path (`CchQuery::new` / the
    /// alternatives backend) where parent pointers reference topo edges.
    pub topo_edge_idx: ArcCow<u32>,
}

impl UpAdjFlat {
    /// Build flat UP adjacency from topology and weights.
    /// `with_topo_idx` controls whether the back-reference is materialised.
    ///
    /// #306 PR 4: storage width for the flat's `weights` is **picked
    /// from the filtered set**, not copied from the input. INF entries
    /// are dropped during the build, so the flat's set is a strict
    /// subset of the cch_weights' set — `WeightWidth::choose` runs on
    /// the filtered values and may pick a tighter width than the
    /// input had (e.g. cch_weights at U32 but only finite values < u16
    /// survived). In practice on Belgium the chosen width matches
    /// cch_weights' width for the same direction.
    pub fn build_with(topo: &CchTopo, weights: &CchWeights, with_topo_idx: bool) -> Self {
        let n_nodes = topo.n_nodes as usize;

        // First pass: count valid edges per node
        let mut counts = vec![0usize; n_nodes];
        for (source, count) in counts.iter_mut().enumerate() {
            let start = topo.up_offsets[source] as usize;
            let end = topo.up_offsets[source + 1] as usize;
            for i in start..end {
                if weights.up.get(i) != u32::MAX {
                    *count += 1;
                }
            }
        }

        // Build offsets (prefix sum)
        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut offset = 0u64;
        for &count in &counts {
            offsets.push(offset);
            offset += count as u64;
        }
        offsets.push(offset);

        let total_edges = offset as usize;

        // Allocate arrays
        let mut targets = vec![0u32; total_edges];
        let mut flat_weights = vec![0u32; total_edges];
        let mut topo_edge_idx = if with_topo_idx {
            vec![0u32; total_edges]
        } else {
            Vec::new()
        };

        // Second pass: fill in edges (skip INF)
        counts.fill(0);
        for source in 0..n_nodes {
            let start = topo.up_offsets[source] as usize;
            let end = topo.up_offsets[source + 1] as usize;

            for i in start..end {
                let w = weights.up.get(i);
                if w == u32::MAX {
                    continue;
                }
                let target = topo.up_targets[i];
                let pos = offsets[source] as usize + counts[source];
                targets[pos] = target;
                flat_weights[pos] = w;
                if with_topo_idx {
                    topo_edge_idx[pos] = i as u32;
                }
                counts[source] += 1;
            }
        }

        // Embed at the same width as the source.
        let width = crate::formats::WeightWidth::choose(&flat_weights);
        let weights_arr = build_weight_array(flat_weights, width);

        Self {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets),
            weights: weights_arr,
            topo_edge_idx: ArcCow::from_vec(topo_edge_idx),
        }
    }

    /// Build flat UP adjacency without the topo back-reference.
    /// Backwards-compatible default for matrix / PHAST callers.
    pub fn build(topo: &CchTopo, weights: &CchWeights) -> Self {
        Self::build_with(topo, weights, false)
    }
}

/// Flat forward adjacency for DOWN edges with embedded weights.
///
/// Source-keyed mirror of `UpAdjFlat` but for DOWN. Required by the
/// PHAST forward-isochrone downward scan; it lets that scan read off
/// the flats so the underlying `cch_weights.down` mmap pages can be
/// `madvise(DONTNEED)`-ed at startup.
#[derive(Clone)]
pub struct DownAdjFlat {
    pub offsets: ArcCow<u64>,
    pub targets: ArcCow<u32>,
    pub weights: crate::formats::WeightArray,
}

impl DownAdjFlat {
    /// Build flat forward DOWN adjacency from topology and weights.
    pub fn build(topo: &CchTopo, weights: &CchWeights) -> Self {
        let n_nodes = topo.n_nodes as usize;

        let mut counts = vec![0usize; n_nodes];
        for (source, count) in counts.iter_mut().enumerate() {
            let start = topo.down_offsets[source] as usize;
            let end = topo.down_offsets[source + 1] as usize;
            for i in start..end {
                if weights.down.get(i) != u32::MAX {
                    *count += 1;
                }
            }
        }

        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut offset = 0u64;
        for &count in &counts {
            offsets.push(offset);
            offset += count as u64;
        }
        offsets.push(offset);

        let total_edges = offset as usize;
        let mut targets = vec![0u32; total_edges];
        let mut flat_weights = vec![0u32; total_edges];

        counts.fill(0);
        for source in 0..n_nodes {
            let start = topo.down_offsets[source] as usize;
            let end = topo.down_offsets[source + 1] as usize;
            for i in start..end {
                let w = weights.down.get(i);
                if w == u32::MAX {
                    continue;
                }
                let target = topo.down_targets[i];
                let pos = offsets[source] as usize + counts[source];
                targets[pos] = target;
                flat_weights[pos] = w;
                counts[source] += 1;
            }
        }

        let width = crate::formats::WeightWidth::choose(&flat_weights);
        let weights_arr = build_weight_array(flat_weights, width);

        Self {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets),
            weights: weights_arr,
        }
    }
}

/// Flat reverse adjacency for DOWN edges with embedded weights.
///
/// Target-keyed: `offsets[u]..offsets[u+1]` lists all DOWN edges x→u
/// that arrive at u. Used by the backward CCH search on the routing
/// hot path. `topo_edge_idx` is populated only when this flat feeds
/// `CchQuery` (so unpack can recover topo edges from parent pointers).
#[derive(Clone)]
pub struct DownReverseAdjFlat {
    pub offsets: ArcCow<u64>, // n_nodes + 1
    pub sources: ArcCow<u32>, // source node x for reverse edge
    /// Embedded weight per slot — see `UpAdjFlat::weights`.
    pub weights: crate::formats::WeightArray,
    /// Empty unless this flat feeds the routing hot path.
    pub topo_edge_idx: ArcCow<u32>,
}

impl DownReverseAdjFlat {
    /// Build flat reverse adjacency from topology and weights.
    /// `with_topo_idx` controls whether the back-reference is materialised.
    pub fn build_with(topo: &CchTopo, weights: &CchWeights, with_topo_idx: bool) -> Self {
        let n_nodes = topo.n_nodes as usize;

        // First pass: count incoming VALID edges per node (skip INF weights)
        let mut counts = vec![0usize; n_nodes];
        for source in 0..n_nodes {
            let start = topo.down_offsets[source] as usize;
            let end = topo.down_offsets[source + 1] as usize;
            for i in start..end {
                if weights.down.get(i) != u32::MAX {
                    let target = topo.down_targets[i] as usize;
                    counts[target] += 1;
                }
            }
        }

        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut offset = 0u64;
        for &count in &counts {
            offsets.push(offset);
            offset += count as u64;
        }
        offsets.push(offset);

        let total_edges = offset as usize;

        let mut sources = vec![0u32; total_edges];
        let mut flat_weights = vec![0u32; total_edges];
        let mut topo_edge_idx = if with_topo_idx {
            vec![0u32; total_edges]
        } else {
            Vec::new()
        };

        counts.fill(0);
        for source in 0..n_nodes {
            let start = topo.down_offsets[source] as usize;
            let end = topo.down_offsets[source + 1] as usize;
            for i in start..end {
                let w = weights.down.get(i);
                if w == u32::MAX {
                    continue;
                }
                let target = topo.down_targets[i] as usize;
                let pos = offsets[target] as usize + counts[target];
                sources[pos] = source as u32;
                flat_weights[pos] = w;
                if with_topo_idx {
                    topo_edge_idx[pos] = i as u32;
                }
                counts[target] += 1;
            }
        }

        let width = crate::formats::WeightWidth::choose(&flat_weights);
        let weights_arr = build_weight_array(flat_weights, width);

        Self {
            offsets: ArcCow::from_vec(offsets),
            sources: ArcCow::from_vec(sources),
            weights: weights_arr,
            topo_edge_idx: ArcCow::from_vec(topo_edge_idx),
        }
    }

    /// Build without back-references (matrix / PHAST callers).
    pub fn build(topo: &CchTopo, weights: &CchWeights) -> Self {
        Self::build_with(topo, weights, false)
    }
}

// =============================================================================
// SPLIT FORMAT — FlatTopo + FlatWeights (#345)
// =============================================================================
//
// New on-disk format that separates the (offsets + targets + topo_edge_idx)
// "topology" from the per-metric weights. Single FlatTopo section per
// (mode × direction); one FlatWeights section per (mode × direction × metric).
// The two sections together carry the same information as the v4
// `UpAdjFlatFile` / `DownAdjFlatFile` / `DownReverseAdjFlatFile` triplet
// but with the topology bytes shared across metric variants.
//
// FlatTopo layout (little-endian):
//
//   header (24 bytes):
//     magic        : u32   = 0x4F505446 ("FTPO" LE)
//     version      : u16   = 1
//     flags        : u16
//                          bits 0..=1: targets width (00=u32, 01=u16, 10=u24)
//                          bit 2     : offsets_u32 (1 if n_edges fits u32)
//                          bit 3     : has_topo_idx
//                          bits 4..=15: reserved (must be 0)
//     n_nodes      : u32
//     n_edges      : u64
//
//   body (each array padded to u64 boundary):
//     offsets       : (n_nodes + 1) × {4 or 8}
//     targets       : n_edges × {2, 3, or 4} bytes
//     topo_edge_idx : n_edges × u32         (IF has_topo_idx)
//
//   footer (16 bytes):
//     body_crc : u64
//     file_crc : u64
//
// FlatWeights layout (little-endian):
//
//   header (16 bytes):
//     magic        : u32   = 0x54475746 ("FWGT" LE)
//     version      : u16   = 1
//     flags        : u16
//                          bits 0..=1: weights width (00=u32, 01=u16, 10=u24)
//                          bits 2..=15: reserved (must be 0)
//     n_edges      : u64
//
//   body:
//     weights      : n_edges × {2, 3, or 4} bytes, padded to u64
//
//   footer (16 bytes):
//     body_crc : u64
//     file_crc : u64

const FLAT_TOPO_MAGIC: u32 = 0x4F505446; // "FTPO" LE
const FLAT_TOPO_VERSION: u16 = 1;
const FLAT_TOPO_HEADER_SIZE: usize = 24;
const FLAT_WEIGHTS_MAGIC: u32 = 0x54475746; // "FWGT" LE
const FLAT_WEIGHTS_VERSION: u16 = 1;
const FLAT_WEIGHTS_HEADER_SIZE: usize = 16;
const FLAT_SPLIT_FOOTER_SIZE: usize = 16;

const FLAT_TOPO_TARGETS_WIDTH_MASK: u16 = 0b0000_0000_0000_0011;
const FLAT_TOPO_OFFSETS_U32_BIT: u16 = 0b0000_0000_0000_0100;
const FLAT_TOPO_HAS_TOPO_IDX_BIT: u16 = 0b0000_0000_0000_1000;
const FLAT_TOPO_FLAGS_KNOWN: u16 = 0b0000_0000_0000_1111;

const FLAT_WEIGHTS_WIDTH_MASK: u16 = 0b0000_0000_0000_0011;
const FLAT_WEIGHTS_FLAGS_KNOWN: u16 = 0b0000_0000_0000_0011;

#[inline]
const fn pad_to_u64(n: usize) -> usize {
    (8 - (n & 7)) & 7
}

fn width_code_u16(w: crate::formats::WeightWidth) -> u16 {
    match w {
        crate::formats::WeightWidth::U32 => 0,
        crate::formats::WeightWidth::U16 => 1,
        crate::formats::WeightWidth::U24 => 2,
    }
}

fn width_from_code_u16(c: u16) -> anyhow::Result<crate::formats::WeightWidth> {
    match c {
        0 => Ok(crate::formats::WeightWidth::U32),
        1 => Ok(crate::formats::WeightWidth::U16),
        2 => Ok(crate::formats::WeightWidth::U24),
        _ => anyhow::bail!("unknown flat width code: {c}"),
    }
}

/// Pick the narrowest [`WeightWidth`] that fits every target/source rank
/// in `slice`. Same encoding the v4 monolithic flat already uses for
/// the targets array (#351).
fn pick_targets_width_split(slice: &[u32]) -> crate::formats::WeightWidth {
    let mut max_v: u32 = 0;
    for &v in slice {
        if v > max_v {
            max_v = v;
        }
    }
    if max_v < (u16::MAX as u32) {
        crate::formats::WeightWidth::U16
    } else if max_v < crate::formats::U24_SENTINEL {
        crate::formats::WeightWidth::U24
    } else {
        crate::formats::WeightWidth::U32
    }
}

fn encode_targets_to_bytes_split(slice: &[u32], width: crate::formats::WeightWidth) -> Vec<u8> {
    match width {
        crate::formats::WeightWidth::U32 => {
            let mut out = Vec::with_capacity(4 * slice.len());
            for &v in slice {
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        }
        crate::formats::WeightWidth::U16 => {
            let mut out = Vec::with_capacity(2 * slice.len());
            for &v in slice {
                debug_assert!(v < u16::MAX as u32);
                out.extend_from_slice(&(v as u16).to_le_bytes());
            }
            out
        }
        crate::formats::WeightWidth::U24 => {
            let mut out = Vec::with_capacity(3 * slice.len());
            for &v in slice {
                debug_assert!(v < crate::formats::U24_SENTINEL);
                out.push((v & 0xFF) as u8);
                out.push(((v >> 8) & 0xFF) as u8);
                out.push(((v >> 16) & 0xFF) as u8);
            }
            out
        }
    }
}

/// Serialise the topology half of a (mode × direction) flat. Output is
/// a complete section body (header + body + footer) ready to be
/// `append_bytes`'d into a container as `SectionKind::FlatTopo`.
pub fn encode_flat_topo_bytes(offsets: &[u64], targets: &[u32], topo_edge_idx: &[u32]) -> Vec<u8> {
    let n_nodes = offsets.len().saturating_sub(1);
    let n_edges = targets.len();
    let has_topo_idx = !topo_edge_idx.is_empty();
    debug_assert!(
        !has_topo_idx || topo_edge_idx.len() == n_edges,
        "topo_edge_idx must be empty or match n_edges"
    );

    let offsets_u32 = (n_edges as u64) < (u32::MAX as u64);
    let targets_width = pick_targets_width_split(targets);

    let mut flags: u16 = width_code_u16(targets_width);
    if offsets_u32 {
        flags |= FLAT_TOPO_OFFSETS_U32_BIT;
    }
    if has_topo_idx {
        flags |= FLAT_TOPO_HAS_TOPO_IDX_BIT;
    }
    debug_assert_eq!(
        flags & !FLAT_TOPO_FLAGS_KNOWN,
        0,
        "flat-topo flag pollution"
    );

    let offsets_entry_bytes = if offsets_u32 { 4 } else { 8 };
    let offsets_bytes_count = offsets.len() * offsets_entry_bytes;
    let offsets_pad = pad_to_u64(offsets_bytes_count);
    let targets_bytes_count = n_edges * targets_width.bytes_per_entry();
    let targets_pad = pad_to_u64(targets_bytes_count);
    let topo_idx_bytes_count = if has_topo_idx { 4 * n_edges } else { 0 };
    let topo_idx_pad = pad_to_u64(topo_idx_bytes_count);

    let body_size = offsets_bytes_count
        + offsets_pad
        + targets_bytes_count
        + targets_pad
        + topo_idx_bytes_count
        + topo_idx_pad;

    let mut out = Vec::with_capacity(FLAT_TOPO_HEADER_SIZE + body_size + FLAT_SPLIT_FOOTER_SIZE);

    // header
    out.extend_from_slice(&FLAT_TOPO_MAGIC.to_le_bytes());
    out.extend_from_slice(&FLAT_TOPO_VERSION.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&(n_nodes as u32).to_le_bytes());
    // reserved u32 pads header to u64 boundary so the offsets array
    // starts u64-aligned for `bytemuck::cast_slice::<u8, u64>` on the
    // mmap read path.
    out.extend_from_slice(&0u32.to_le_bytes());
    out.extend_from_slice(&(n_edges as u64).to_le_bytes());
    debug_assert_eq!(out.len(), FLAT_TOPO_HEADER_SIZE);

    // body
    if offsets_u32 {
        for &off in offsets {
            out.extend_from_slice(&(off as u32).to_le_bytes());
        }
    } else {
        for &off in offsets {
            out.extend_from_slice(&off.to_le_bytes());
        }
    }
    out.resize(out.len() + offsets_pad, 0);
    out.extend_from_slice(&encode_targets_to_bytes_split(targets, targets_width));
    out.resize(out.len() + targets_pad, 0);
    if has_topo_idx {
        for &t in topo_edge_idx {
            out.extend_from_slice(&t.to_le_bytes());
        }
        out.resize(out.len() + topo_idx_pad, 0);
    }
    debug_assert_eq!(out.len(), FLAT_TOPO_HEADER_SIZE + body_size);

    // footer
    let body_slice = &out[FLAT_TOPO_HEADER_SIZE..];
    let mut body_d = crate::formats::crc::Digest::new();
    body_d.update(body_slice);
    let body_crc = body_d.finalize();
    let mut file_d = crate::formats::crc::Digest::new();
    file_d.update(&out);
    let file_crc = file_d.finalize();
    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(&file_crc.to_le_bytes());

    out
}

/// Decoded FlatTopo (heap-owned). Production loaders will use a
/// mmap-backed variant later in this phase; this version is the
/// reference for round-trip tests and any path that already holds
/// owned bytes.
#[derive(Debug)]
pub struct DecodedFlatTopo {
    pub n_nodes: u32,
    pub offsets: Vec<u64>,
    pub targets: Vec<u32>,
    pub topo_edge_idx: Vec<u32>,
}

fn decode_targets_from_bytes_split(
    bytes: &[u8],
    n_edges: usize,
    width: crate::formats::WeightWidth,
) -> Vec<u32> {
    match width {
        crate::formats::WeightWidth::U32 => {
            bytemuck::cast_slice::<u8, u32>(&bytes[..4 * n_edges]).to_vec()
        }
        crate::formats::WeightWidth::U16 => {
            let s: &[u16] = bytemuck::cast_slice(&bytes[..2 * n_edges]);
            s.iter().map(|&v| v as u32).collect()
        }
        crate::formats::WeightWidth::U24 => {
            let mut out = Vec::with_capacity(n_edges);
            for i in 0..n_edges {
                let off = i * 3;
                let v = u32::from(bytes[off])
                    | (u32::from(bytes[off + 1]) << 8)
                    | (u32::from(bytes[off + 2]) << 16);
                out.push(v);
            }
            out
        }
    }
}

/// Parse a FlatTopo section from owned bytes. Verifies both CRCs.
pub fn decode_flat_topo_bytes(bytes: &[u8]) -> anyhow::Result<DecodedFlatTopo> {
    anyhow::ensure!(
        bytes.len() >= FLAT_TOPO_HEADER_SIZE + FLAT_SPLIT_FOOTER_SIZE,
        "flat-topo section too short: {} bytes",
        bytes.len()
    );

    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    anyhow::ensure!(
        magic == FLAT_TOPO_MAGIC,
        "flat-topo bad magic: 0x{magic:08X}"
    );
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    anyhow::ensure!(
        version == FLAT_TOPO_VERSION,
        "flat-topo bad version: {version}"
    );
    let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    anyhow::ensure!(
        flags & !FLAT_TOPO_FLAGS_KNOWN == 0,
        "flat-topo unknown flag bits: 0x{flags:04X}"
    );
    let targets_width = width_from_code_u16(flags & FLAT_TOPO_TARGETS_WIDTH_MASK)?;
    let offsets_u32 = flags & FLAT_TOPO_OFFSETS_U32_BIT != 0;
    let has_topo_idx = flags & FLAT_TOPO_HAS_TOPO_IDX_BIT != 0;
    let n_nodes = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    // bytes[12..16] reserved (must be zero)
    anyhow::ensure!(
        bytes[12] == 0 && bytes[13] == 0 && bytes[14] == 0 && bytes[15] == 0,
        "flat-topo reserved bytes 12..=15 must be zero"
    );
    let n_edges = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;

    let n_offsets = (n_nodes as usize) + 1;
    let offsets_entry_bytes = if offsets_u32 { 4 } else { 8 };
    let offsets_bytes_count = n_offsets * offsets_entry_bytes;
    let offsets_pad = pad_to_u64(offsets_bytes_count);
    let targets_bytes_count = n_edges * targets_width.bytes_per_entry();
    let targets_pad = pad_to_u64(targets_bytes_count);
    let topo_idx_bytes_count = if has_topo_idx { 4 * n_edges } else { 0 };
    let topo_idx_pad = pad_to_u64(topo_idx_bytes_count);

    let body_size = offsets_bytes_count
        + offsets_pad
        + targets_bytes_count
        + targets_pad
        + topo_idx_bytes_count
        + topo_idx_pad;
    let expected_total = FLAT_TOPO_HEADER_SIZE + body_size + FLAT_SPLIT_FOOTER_SIZE;
    anyhow::ensure!(
        bytes.len() == expected_total,
        "flat-topo size mismatch: got {}, expected {expected_total}",
        bytes.len()
    );

    let mut cur = FLAT_TOPO_HEADER_SIZE;
    let offsets: Vec<u64> = if offsets_u32 {
        let s: &[u32] = bytemuck::cast_slice(&bytes[cur..cur + offsets_bytes_count]);
        s.iter().map(|&v| v as u64).collect()
    } else {
        bytemuck::cast_slice::<u8, u64>(&bytes[cur..cur + offsets_bytes_count]).to_vec()
    };
    cur += offsets_bytes_count + offsets_pad;
    let targets = decode_targets_from_bytes_split(&bytes[cur..], n_edges, targets_width);
    cur += targets_bytes_count + targets_pad;
    let topo_edge_idx: Vec<u32> = if has_topo_idx {
        bytemuck::cast_slice::<u8, u32>(&bytes[cur..cur + topo_idx_bytes_count]).to_vec()
    } else {
        Vec::new()
    };

    // CRC check
    let body_slice = &bytes[FLAT_TOPO_HEADER_SIZE..FLAT_TOPO_HEADER_SIZE + body_size];
    let mut body_d = crate::formats::crc::Digest::new();
    body_d.update(body_slice);
    let computed_body = body_d.finalize();
    let stored_body = u64::from_le_bytes(
        bytes[expected_total - 16..expected_total - 8]
            .try_into()
            .unwrap(),
    );
    anyhow::ensure!(
        computed_body == stored_body,
        "flat-topo body CRC mismatch: computed {computed_body:#018x}, stored {stored_body:#018x}"
    );
    let mut file_d = crate::formats::crc::Digest::new();
    file_d.update(&bytes[..FLAT_TOPO_HEADER_SIZE + body_size]);
    let computed_file = file_d.finalize();
    let stored_file = u64::from_le_bytes(
        bytes[expected_total - 8..expected_total]
            .try_into()
            .unwrap(),
    );
    anyhow::ensure!(computed_file == stored_file, "flat-topo file CRC mismatch");

    Ok(DecodedFlatTopo {
        n_nodes,
        offsets,
        targets,
        topo_edge_idx,
    })
}

/// Serialise the weights half of a (mode × direction × metric) flat.
/// Output is a complete section body (header + body + footer) ready
/// for `SectionKind::FlatWeights`.
pub fn encode_flat_weights_bytes(weights: &crate::formats::WeightArray) -> Vec<u8> {
    let n_edges = weights.len();
    let width = weights.width();
    let flags: u16 = width_code_u16(width);
    debug_assert_eq!(flags & !FLAT_WEIGHTS_FLAGS_KNOWN, 0);

    let body_data_bytes = n_edges * width.bytes_per_entry();
    let body_pad = pad_to_u64(body_data_bytes);
    let body_size = body_data_bytes + body_pad;

    let mut out = Vec::with_capacity(FLAT_WEIGHTS_HEADER_SIZE + body_size + FLAT_SPLIT_FOOTER_SIZE);

    out.extend_from_slice(&FLAT_WEIGHTS_MAGIC.to_le_bytes());
    out.extend_from_slice(&FLAT_WEIGHTS_VERSION.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&(n_edges as u64).to_le_bytes());
    debug_assert_eq!(out.len(), FLAT_WEIGHTS_HEADER_SIZE);

    // body — encode through WeightArray::iter to handle u16/u24/u32
    // uniformly, going through the per-width sentinel that `WeightArray`
    // already implements.
    let mut body_bytes = Vec::with_capacity(body_data_bytes);
    match width {
        crate::formats::WeightWidth::U32 => {
            for v in weights.iter() {
                body_bytes.extend_from_slice(&v.to_le_bytes());
            }
        }
        crate::formats::WeightWidth::U16 => {
            for v in weights.iter() {
                let v16: u16 = if v == u32::MAX { u16::MAX } else { v as u16 };
                body_bytes.extend_from_slice(&v16.to_le_bytes());
            }
        }
        crate::formats::WeightWidth::U24 => {
            for v in weights.iter() {
                let packed: u32 = if v == u32::MAX {
                    crate::formats::U24_SENTINEL
                } else {
                    v
                };
                body_bytes.push((packed & 0xFF) as u8);
                body_bytes.push(((packed >> 8) & 0xFF) as u8);
                body_bytes.push(((packed >> 16) & 0xFF) as u8);
            }
        }
    }
    out.extend_from_slice(&body_bytes);
    out.resize(out.len() + body_pad, 0);
    debug_assert_eq!(out.len(), FLAT_WEIGHTS_HEADER_SIZE + body_size);

    let body_slice = &out[FLAT_WEIGHTS_HEADER_SIZE..];
    let mut body_d = crate::formats::crc::Digest::new();
    body_d.update(body_slice);
    let body_crc = body_d.finalize();
    let mut file_d = crate::formats::crc::Digest::new();
    file_d.update(&out);
    let file_crc = file_d.finalize();
    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(&file_crc.to_le_bytes());

    out
}

/// MMAP-backed parse of a FlatTopo section. Returns ArcCow views
/// directly into the supplied `Arc<Mmap>` so the topology bytes stay
/// in the page cache (zero-copy). The strong-count clone keeps the
/// mapping alive for the lifetime of the returned views.
///
/// #399 (2026-05-27): the production case (offsets u64, targets u32,
/// optional u32 topo_edge_idx) now truly zero-copies via
/// `ArcCow::from_mmap`. The u16/u24 targets paths still widen to
/// owned `Vec<u32>` since they need per-entry sentinel translation.
/// Pre-#399, the previous implementation always copied the body to
/// owned Vecs — that turned ~5+ GB of mmap-backed weight + topology
/// data into anon RSS on every Belgium boot.
pub fn decode_flat_topo_from_mmap(
    mmap: std::sync::Arc<memmap2::Mmap>,
    byte_offset: usize,
    byte_len: usize,
) -> anyhow::Result<(
    ArcCow<u64>, // offsets
    ArcCow<u32>, // targets
    ArcCow<u32>, // topo_edge_idx (empty if not present)
)> {
    anyhow::ensure!(
        byte_offset.saturating_add(byte_len) <= mmap.len(),
        "flat-topo section out of bounds"
    );
    anyhow::ensure!(
        byte_len >= FLAT_TOPO_HEADER_SIZE + FLAT_SPLIT_FOOTER_SIZE,
        "flat-topo section too short: {} bytes",
        byte_len
    );

    // Peek the header to determine widths + size up the body. CRC is
    // verified by the caller via the lazy_verify layer; we trust the
    // bytes here.
    let header = &mmap[byte_offset..byte_offset + FLAT_TOPO_HEADER_SIZE];
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    anyhow::ensure!(
        magic == FLAT_TOPO_MAGIC,
        "flat-topo bad magic: 0x{magic:08X}"
    );
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    anyhow::ensure!(
        version == FLAT_TOPO_VERSION,
        "flat-topo bad version: {version}"
    );
    let flags = u16::from_le_bytes(header[6..8].try_into().unwrap());
    anyhow::ensure!(
        flags & !FLAT_TOPO_FLAGS_KNOWN == 0,
        "flat-topo unknown flag bits: 0x{flags:04X}"
    );
    let targets_width = width_from_code_u16(flags & FLAT_TOPO_TARGETS_WIDTH_MASK)?;
    let offsets_u32 = flags & FLAT_TOPO_OFFSETS_U32_BIT != 0;
    let has_topo_idx = flags & FLAT_TOPO_HAS_TOPO_IDX_BIT != 0;
    let n_nodes = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let n_edges = u64::from_le_bytes(header[16..24].try_into().unwrap()) as usize;

    let n_offsets = (n_nodes as usize) + 1;
    let offsets_entry_bytes = if offsets_u32 { 4 } else { 8 };
    let offsets_bytes_count = n_offsets * offsets_entry_bytes;
    let offsets_pad = pad_to_u64(offsets_bytes_count);
    let targets_bytes_count = n_edges * targets_width.bytes_per_entry();
    let targets_pad = pad_to_u64(targets_bytes_count);
    let topo_idx_bytes_count = if has_topo_idx { 4 * n_edges } else { 0 };
    let topo_idx_pad = pad_to_u64(topo_idx_bytes_count);

    let body_size = offsets_bytes_count
        + offsets_pad
        + targets_bytes_count
        + targets_pad
        + topo_idx_bytes_count
        + topo_idx_pad;
    let expected_total = FLAT_TOPO_HEADER_SIZE + body_size + FLAT_SPLIT_FOOTER_SIZE;
    anyhow::ensure!(
        byte_len == expected_total,
        "flat-topo size mismatch: got {byte_len}, expected {expected_total}",
    );

    // Production fast path: offsets u64, targets u32. Each sub-array
    // is mmap-backed via ArcCow::from_mmap when the section crosses
    // the size threshold; below it we copy to Owned Vec to avoid the
    // per-access deref overhead. Pack guarantees 8-byte section
    // alignment, and the per-array offsets land on 4-byte boundaries.
    //
    // Threshold rationale (#399): an A/B at 1000×1000 showed the
    // mmap-backed path adding ~15% to wall time vs Owned. The savings
    // only justify the cost on big sections — bike's 250-550 MB flats
    // alone account for ~1.5 GB of the boot anon. Car/foot per-flat
    // are 35-155 MB and stay Owned.
    let mmap_back = body_size >= FLAT_SECTION_MMAP_BACK_THRESHOLD;
    if !offsets_u32 && targets_width == crate::formats::WeightWidth::U32 && mmap_back {
        let offsets_off = byte_offset + FLAT_TOPO_HEADER_SIZE;
        let targets_off = offsets_off + offsets_bytes_count + offsets_pad;
        let topo_idx_off = targets_off + targets_bytes_count + targets_pad;
        let offsets =
            ArcCow::<u64>::from_mmap(std::sync::Arc::clone(&mmap), offsets_off, n_offsets)?;
        let targets = ArcCow::<u32>::from_mmap(std::sync::Arc::clone(&mmap), targets_off, n_edges)?;
        let topo_edge_idx = if has_topo_idx {
            ArcCow::<u32>::from_mmap(std::sync::Arc::clone(&mmap), topo_idx_off, n_edges)?
        } else {
            ArcCow::from_vec(Vec::new())
        };
        return Ok((offsets, targets, topo_edge_idx));
    }

    // Fallback for u16/u24 targets or u32 offsets — these need
    // per-entry sentinel widening, so we keep the existing owned path.
    let bytes = &mmap[byte_offset..byte_offset + byte_len];
    let decoded = decode_flat_topo_bytes(bytes)?;
    Ok((
        ArcCow::from_vec(decoded.offsets),
        ArcCow::from_vec(decoded.targets),
        ArcCow::from_vec(decoded.topo_edge_idx),
    ))
}

/// MMAP-backed parse of a FlatWeights section. Returns the
/// [`WeightArray`] composed of mmap-backed bytes when the width is
/// fixed-stride (u16/u32/u24) — the zero-copy hot path.
///
/// #399 (2026-05-27): all three widths are now zero-copy. Previously
/// the function called `decode_flat_weights_bytes` which always
/// allocated `Vec<u32>` / `Vec<u16>` / `Vec<u8>` via `.to_vec()`,
/// converting ~5 GB of mmap-backed weight data into anon RSS on
/// Belgium boot. CRC verification is the caller's responsibility
/// (driven by `LazyContainer::verify_now`) — we trust the bytes.
pub fn decode_flat_weights_from_mmap(
    mmap: std::sync::Arc<memmap2::Mmap>,
    byte_offset: usize,
    byte_len: usize,
) -> anyhow::Result<crate::formats::WeightArray> {
    anyhow::ensure!(
        byte_offset.saturating_add(byte_len) <= mmap.len(),
        "flat-weights section out of bounds"
    );
    anyhow::ensure!(
        byte_len >= FLAT_WEIGHTS_HEADER_SIZE + FLAT_SPLIT_FOOTER_SIZE,
        "flat-weights section too short"
    );

    let header = &mmap[byte_offset..byte_offset + FLAT_WEIGHTS_HEADER_SIZE];
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    anyhow::ensure!(magic == FLAT_WEIGHTS_MAGIC, "flat-weights bad magic");
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    anyhow::ensure!(version == FLAT_WEIGHTS_VERSION, "flat-weights bad version");
    let flags = u16::from_le_bytes(header[6..8].try_into().unwrap());
    anyhow::ensure!(
        flags & !FLAT_WEIGHTS_FLAGS_KNOWN == 0,
        "flat-weights unknown flag bits"
    );
    let width = width_from_code_u16(flags & FLAT_WEIGHTS_WIDTH_MASK)?;
    let n_edges = u64::from_le_bytes(header[8..16].try_into().unwrap()) as usize;

    let body_data_bytes = n_edges * width.bytes_per_entry();
    let body_pad = pad_to_u64(body_data_bytes);
    let body_size = body_data_bytes + body_pad;
    let expected_total = FLAT_WEIGHTS_HEADER_SIZE + body_size + FLAT_SPLIT_FOOTER_SIZE;
    anyhow::ensure!(
        byte_len == expected_total,
        "flat-weights size mismatch: got {byte_len}, expected {expected_total}",
    );

    let body_off = byte_offset + FLAT_WEIGHTS_HEADER_SIZE;
    // Size-thresholded mmap (#399): match the FlatTopo threshold.
    if body_size >= FLAT_SECTION_MMAP_BACK_THRESHOLD {
        let arr = match width {
            crate::formats::WeightWidth::U32 => crate::formats::WeightArray::U32(
                ArcCow::<u32>::from_mmap(std::sync::Arc::clone(&mmap), body_off, n_edges)?,
            ),
            crate::formats::WeightWidth::U16 => crate::formats::WeightArray::U16(
                ArcCow::<u16>::from_mmap(std::sync::Arc::clone(&mmap), body_off, n_edges)?,
            ),
            crate::formats::WeightWidth::U24 => crate::formats::WeightArray::U24(
                ArcCow::<u8>::from_mmap(std::sync::Arc::clone(&mmap), body_off, n_edges * 3)?,
            ),
        };
        return Ok(arr);
    }
    // Small sections: fall through to the existing owned-Vec path.
    let bytes = &mmap[byte_offset..byte_offset + byte_len];
    decode_flat_weights_bytes(bytes)
}

/// Size-threshold (#399) for keeping a flat section mmap-backed vs
/// copying it to an owned `Vec` at boot. An A/B at 1000×1000 showed
/// the mmap path costing ~15% wall time per query (due to the
/// `ArcCow::Mmap` deref in the hot loops). Below the threshold we
/// pay the boot RAM (one Owned Vec per section) to keep the hot path
/// branch-free; above it the per-section RAM is large enough that
/// keeping the bytes file-backed dominates.
///
/// 128 MiB picked by inspection of Belgium per-mode flat sizes:
/// bike's 250-555 MB topo + 150 MB weights cross it; car/foot per-flat
/// (35-155 MB each) stay below.
const FLAT_SECTION_MMAP_BACK_THRESHOLD: usize = 128 * 1024 * 1024;

/// Parse a FlatWeights section from owned bytes. Verifies both CRCs.
pub fn decode_flat_weights_bytes(bytes: &[u8]) -> anyhow::Result<crate::formats::WeightArray> {
    anyhow::ensure!(
        bytes.len() >= FLAT_WEIGHTS_HEADER_SIZE + FLAT_SPLIT_FOOTER_SIZE,
        "flat-weights section too short"
    );
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    anyhow::ensure!(magic == FLAT_WEIGHTS_MAGIC, "flat-weights bad magic");
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    anyhow::ensure!(version == FLAT_WEIGHTS_VERSION, "flat-weights bad version");
    let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    anyhow::ensure!(
        flags & !FLAT_WEIGHTS_FLAGS_KNOWN == 0,
        "flat-weights unknown flag bits"
    );
    let width = width_from_code_u16(flags & FLAT_WEIGHTS_WIDTH_MASK)?;
    let n_edges = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;

    let body_data_bytes = n_edges * width.bytes_per_entry();
    let body_pad = pad_to_u64(body_data_bytes);
    let body_size = body_data_bytes + body_pad;
    let expected_total = FLAT_WEIGHTS_HEADER_SIZE + body_size + FLAT_SPLIT_FOOTER_SIZE;
    anyhow::ensure!(
        bytes.len() == expected_total,
        "flat-weights size mismatch: got {}, expected {expected_total}",
        bytes.len()
    );

    let body_slice = &bytes[FLAT_WEIGHTS_HEADER_SIZE..FLAT_WEIGHTS_HEADER_SIZE + body_size];
    let mut body_d = crate::formats::crc::Digest::new();
    body_d.update(body_slice);
    let computed_body = body_d.finalize();
    let stored_body = u64::from_le_bytes(
        bytes[expected_total - 16..expected_total - 8]
            .try_into()
            .unwrap(),
    );
    anyhow::ensure!(
        computed_body == stored_body,
        "flat-weights body CRC mismatch"
    );

    let body_data = &body_slice[..body_data_bytes];
    let weights = match width {
        crate::formats::WeightWidth::U32 => {
            let v: Vec<u32> = bytemuck::cast_slice::<u8, u32>(body_data).to_vec();
            crate::formats::WeightArray::from_vec_u32(v)
        }
        crate::formats::WeightWidth::U16 => {
            let v: Vec<u16> = bytemuck::cast_slice::<u8, u16>(body_data).to_vec();
            crate::formats::WeightArray::from_vec_u16(v)
        }
        crate::formats::WeightWidth::U24 => {
            crate::formats::WeightArray::from_u24_bytes(body_data.to_vec(), n_edges)
        }
    };
    Ok(weights)
}

// =============================================================================
// ON-DISK FORMAT FOR FLAT ADJACENCIES (#150)
// =============================================================================
//
// Each flat is serialised as a self-describing little-endian binary file
// with a CRC-checked body and full-file CRC, mirroring the cch_weights /
// cch_topo formats. The pack step writes one section per (mode × flat)
// into the butterfly.dat container; the server boot path mmaps the
// container and parses the section bytes as a `*View` whose slices borrow
// directly from the mapping.
//
// Layout (little-endian):
//
//   header (32 bytes):
//     magic        : u32   (kind-specific tag, see consts below)
//     version      : u16   = 1
//     has_topo_idx : u8    (0 or 1; only meaningful for UP/DOWN-REV)
//     _resv        : u8
//     n_nodes      : u64
//     n_edges      : u64
//     _resv2       : u64   (reserved; written 0)
//
//   body:
//     offsets        : (n_nodes + 1) × u64
//     targets/sources: n_edges × u32
//     weights        : n_edges × u32
//     topo_edge_idx  : (n_edges or 0) × u32     -- present iff has_topo_idx
//
//   footer (16 bytes):
//     body_crc : u64   (CRC-64 over the body section ONLY)
//     file_crc : u64   (CRC-64 over header || body)
//
// The container writer pads every section start to 8 bytes, so the file
// header (and therefore offsets) is always u64-aligned in memory. After
// the offsets array the cursor is `32 + 8 * (n_nodes + 1)`, still a
// multiple of 8, so the u32 arrays are at least 4-aligned for
// `bytemuck::cast_slice::<u32>`.

/// Version 1: weights always u32 on disk regardless of in-memory width.
/// Version 2 (#349): weights stored at native u16/u24/u32 width — the
/// width is encoded in header byte 7 (`width_code`). u16/u24 bodies are
/// padded to a 4-byte boundary so the following `topo_edge_idx` (still
/// u32) stays naturally aligned for `bytemuck::cast_slice` /
/// `ArcCow::<u32>::from_mmap`.
/// Version 3 (#350): header byte 7 bit 2 selects offset width
/// (0=u64, 1=u32). Writer picks u32 when every offset fits u32::MAX
/// (every Belgium-class region — n_edges_per_mode ~76 M ≪ 4 G). u32
/// offset bodies still land 4-aligned, but the targets/u32 arrays
/// that follow expect 4-alignment, NOT 8-alignment — when offsets
/// are u32 we shrink the body by `4*(n_nodes+1)` bytes which is
/// always a multiple of 4 so downstream alignment is preserved.
/// Version 4 (#351): header byte 7 bits 3..=4 select the **absolute**
/// targets array width on disk (00=u32, 01=u16, 10=u24). u24 is the
/// common case on Belgium (~76 M filtered nodes max, fits 24 bits at
/// 0..=16 M ranks). u16 only fits regions <= 65 535 filtered nodes
/// (small region pinning). Per codex's re-consult: NO rank-delta yet
/// — the absolute path lands first since the load+mask is ~1 ALU op
/// vs delta's 2-3, and the bench math says the lever is bandwidth-
/// neutral at best for hot-path matrix queries. Rank-delta is tracked
/// as a feature-flagged follow-up if the absolute version proves the
/// disk savings.
const ADJ_FLAT_VERSION: u16 = 4;
const ADJ_FLAT_HEADER_SIZE: usize = 32;
const ADJ_FLAT_FOOTER_SIZE: usize = 16;

/// Width code for the single weights array in this file, packed into
/// header byte 7. Each flat file carries one weights array (unlike
/// `cch.weights` which carries two — up and down — and uses both halves
/// of the same byte). Codes match the `cch.weights` v4 convention for
/// consistency: 0=u32, 1=u16, 2=u24.
///
/// Header byte 7 layout (v4, #351):
///   bits 0..=1  weights width code (#349)
///   bit 2       offsets_u32 flag (#350)
///   bits 3..=4  targets width code (#351)
///   bits 5..=7  reserved (must be 0)
const ADJ_FLAT_WIDTH_CODE_U32: u8 = 0;
const ADJ_FLAT_WIDTH_CODE_U16: u8 = 1;
const ADJ_FLAT_WIDTH_CODE_U24: u8 = 2;
const ADJ_FLAT_WIDTH_CODE_MASK: u8 = 0b0000_0011;
/// #350: bit 2 of header byte 7 set means offsets are u32-encoded on
/// disk; cleared means u64 (the v1/v2 default). Reader widens u32
/// offsets back to a u64 `ArcCow::Owned` Vec since the in-memory
/// representation stays `ArcCow<u64>` for hot-path simplicity.
const ADJ_FLAT_OFFSETS_U32_BIT: u8 = 0b0000_0100;
/// #351: bits 3..=4 of header byte 7 hold the targets width code.
/// Same encoding as the weights width code, shifted left by 3 bits.
/// 00=u32, 01=u16, 10=u24. The reader widens u16/u24 bytes back to
/// a `Vec<u32>` since the in-memory `flat.targets: ArcCow<u32>` shape
/// stays unchanged. mmap path still gets zero-copy for u32; u16/u24
/// allocate once at boot.
const ADJ_FLAT_TARGETS_CODE_SHIFT: u8 = 3;
const ADJ_FLAT_TARGETS_CODE_MASK: u8 = 0b0001_1000;

/// Magic for `UpAdjFlat` files. ASCII "UPAJ" (little-endian).
const UP_ADJ_FLAT_MAGIC: u32 = 0x4A415055;
/// Magic for `DownAdjFlat` files. ASCII "DAJF".
const DOWN_ADJ_FLAT_MAGIC: u32 = 0x464A4144;
/// Magic for `DownReverseAdjFlat` files. ASCII "DRJF".
const DOWN_REV_ADJ_FLAT_MAGIC: u32 = 0x464A5244;

fn width_code_of(w: crate::formats::WeightWidth) -> u8 {
    match w {
        crate::formats::WeightWidth::U32 => ADJ_FLAT_WIDTH_CODE_U32,
        crate::formats::WeightWidth::U16 => ADJ_FLAT_WIDTH_CODE_U16,
        crate::formats::WeightWidth::U24 => ADJ_FLAT_WIDTH_CODE_U24,
    }
}

/// Fields that go into the v4 flat-adjacency header. Grouped into a
/// struct so [`write_adj_flat_header`] takes a single argument
/// instead of an 8-tuple (clippy `too_many_arguments`).
struct AdjFlatHeader {
    magic: u32,
    has_topo_idx: bool,
    width: crate::formats::WeightWidth,
    offsets_u32: bool,
    targets_width: crate::formats::WeightWidth,
    n_nodes: u64,
    n_edges: u64,
}

fn write_adj_flat_header(out: &mut Vec<u8>, h: AdjFlatHeader) {
    let mut flags: u8 = width_code_of(h.width);
    if h.offsets_u32 {
        flags |= ADJ_FLAT_OFFSETS_U32_BIT;
    }
    flags |= width_code_of(h.targets_width) << ADJ_FLAT_TARGETS_CODE_SHIFT;
    out.extend_from_slice(&h.magic.to_le_bytes());
    out.extend_from_slice(&ADJ_FLAT_VERSION.to_le_bytes());
    out.push(if h.has_topo_idx { 1 } else { 0 });
    out.push(flags); // byte 7: see ADJ_FLAT_* constants above
    out.extend_from_slice(&h.n_nodes.to_le_bytes());
    out.extend_from_slice(&h.n_edges.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // _resv2
    debug_assert!(out.len().is_multiple_of(ADJ_FLAT_HEADER_SIZE));
}

fn parse_adj_flat_header(
    bytes: &[u8],
    expected_magic: u32,
) -> anyhow::Result<(
    bool,
    crate::formats::WeightWidth,
    bool,
    crate::formats::WeightWidth,
    usize,
    usize,
)> {
    anyhow::ensure!(
        bytes.len() >= ADJ_FLAT_HEADER_SIZE + ADJ_FLAT_FOOTER_SIZE,
        "adj-flat section too short: {} bytes",
        bytes.len()
    );
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    anyhow::ensure!(
        magic == expected_magic,
        "adj-flat magic mismatch: got 0x{:08X}, expected 0x{:08X}",
        magic,
        expected_magic
    );
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    anyhow::ensure!(
        version == ADJ_FLAT_VERSION,
        "adj-flat version {} unsupported (expected {}). Re-run pack to regenerate.",
        version,
        ADJ_FLAT_VERSION
    );
    let has_topo_idx = match bytes[6] {
        0 => false,
        1 => true,
        v => anyhow::bail!("adj-flat has_topo_idx byte invalid: {}", v),
    };
    // Reserved bits guard: byte 7 currently uses bits 0..=1 (weights
    // width code), bit 2 (offsets_u32 flag), and bits 3..=4 (targets
    // width code, #351). Bits 5..=7 are reserved and MUST be zero in
    // v4. Anything else means either future-format data we don't
    // understand or corruption — reject up front.
    const ADJ_FLAT_BYTE7_KNOWN_MASK: u8 =
        ADJ_FLAT_WIDTH_CODE_MASK | ADJ_FLAT_OFFSETS_U32_BIT | ADJ_FLAT_TARGETS_CODE_MASK;
    anyhow::ensure!(
        (bytes[7] & !ADJ_FLAT_BYTE7_KNOWN_MASK) == 0,
        "adj-flat header byte 7 has reserved bits set (0x{:02X}); refusing to load",
        bytes[7]
    );
    let width = match bytes[7] & ADJ_FLAT_WIDTH_CODE_MASK {
        ADJ_FLAT_WIDTH_CODE_U32 => crate::formats::WeightWidth::U32,
        ADJ_FLAT_WIDTH_CODE_U16 => crate::formats::WeightWidth::U16,
        ADJ_FLAT_WIDTH_CODE_U24 => crate::formats::WeightWidth::U24,
        v => anyhow::bail!(
            "adj-flat width code {} invalid (byte 7 = 0x{:02X})",
            v,
            bytes[7]
        ),
    };
    let offsets_u32 = (bytes[7] & ADJ_FLAT_OFFSETS_U32_BIT) != 0;
    let targets_width = match (bytes[7] & ADJ_FLAT_TARGETS_CODE_MASK) >> ADJ_FLAT_TARGETS_CODE_SHIFT
    {
        ADJ_FLAT_WIDTH_CODE_U32 => crate::formats::WeightWidth::U32,
        ADJ_FLAT_WIDTH_CODE_U16 => crate::formats::WeightWidth::U16,
        ADJ_FLAT_WIDTH_CODE_U24 => crate::formats::WeightWidth::U24,
        v => anyhow::bail!(
            "adj-flat targets width code {} invalid (byte 7 = 0x{:02X})",
            v,
            bytes[7]
        ),
    };
    let n_nodes = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let n_edges = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;
    Ok((
        has_topo_idx,
        width,
        offsets_u32,
        targets_width,
        n_nodes,
        n_edges,
    ))
}

fn body_layout(
    n_nodes: usize,
    n_edges: usize,
    has_topo_idx: bool,
    width: crate::formats::WeightWidth,
    offsets_u32: bool,
    targets_width: crate::formats::WeightWidth,
) -> (usize, usize, usize, usize, usize) {
    // Returns (offsets_off, targets_off, weights_off, topo_off, body_end)
    // All offsets are absolute byte offsets from the start of the file
    // (i.e. inclusive of the header). The container guarantees the
    // section starts u64-aligned, and our header is exactly 32 B, so
    // both 8-byte (u64) and 4-byte (u32) offsets land naturally
    // aligned. After the offsets array the cursor is still a multiple
    // of 4 (since `(n_nodes+1) * {4,8}` is always a multiple of 4),
    // so the following `targets` array stays aligned per its width.
    //
    // v2 (#349): weights body is `width.padded_body_bytes(n_edges)` —
    // u16/u24 bodies pad up to 4 B so the following `topo_edge_idx`
    // (still u32) stays 4-aligned for `bytemuck::cast_slice`.
    // v3 (#350): offsets body is `(n_nodes+1) * {4,8}` depending on
    // `offsets_u32`.
    // v4 (#351): targets body is `targets_width.padded_body_bytes(n_edges)`
    // — u16/u24 targets bodies pad up to 4 B so the following weights
    // body / topo_edge_idx (4-aligned) stays aligned.
    let offsets_off = ADJ_FLAT_HEADER_SIZE;
    let offsets_elem = if offsets_u32 { 4 } else { 8 };
    let targets_off = offsets_off + offsets_elem * (n_nodes + 1);
    let targets_bytes = targets_width.padded_body_bytes(n_edges);
    let weights_off = targets_off + targets_bytes;
    let weights_bytes = width.padded_body_bytes(n_edges);
    let topo_off = weights_off + weights_bytes;
    let body_end = if has_topo_idx {
        topo_off + 4 * n_edges
    } else {
        topo_off
    };
    (offsets_off, targets_off, weights_off, topo_off, body_end)
}

/// Choose u32 offsets when every offset value fits a u32. Belgium-class
/// regions have ~76 M edges max per mode → easily fits u32 (4 G).
#[inline]
fn pick_offsets_u32(offsets: &[u64]) -> bool {
    offsets
        .last()
        .map(|&v| v <= u32::MAX as u64)
        .unwrap_or(true)
}

/// #351: pick the narrowest width that losslessly stores every target.
/// Belgium-class regions have at most ~5 M filtered nodes; rank IDs sit
/// in `0..n_filtered_nodes`, so u24 (range `0..2^24 = 16.7 M`) covers
/// every Belgium / NL / DE / FR mode. u16 only fits regions with
/// <= 65 535 filtered nodes — not currently relevant for Europe but
/// kept for symmetry with the weights codec.
///
/// We pick **absolute** widths per codex's revised recommendation: the
/// rank-delta variant is deferred behind a feature flag pending bench
/// confirmation. The empty-array case is u32 (defensive default).
#[inline]
fn pick_targets_width(a32: &[u32]) -> crate::formats::WeightWidth {
    use crate::formats::WeightWidth;
    let max = a32.iter().copied().max().unwrap_or(0);
    if max < u16::MAX as u32 {
        WeightWidth::U16
    } else if max < crate::formats::U24_SENTINEL {
        WeightWidth::U24
    } else {
        WeightWidth::U32
    }
}

/// #350: decode the offsets array body from disk into a heap `Vec<u64>`.
/// When the file stored u32 offsets, widen on read; otherwise borrow the
/// u64 slice via `bytemuck::cast_slice` and copy into the Vec (legacy
/// byte-slice reader is owned-Vec semantics; mmap path has its own
/// helper that preserves zero-copy when offsets are u64).
fn read_offsets_vec(
    bytes: &[u8],
    offsets_off: usize,
    n_nodes: usize,
    offsets_u32: bool,
) -> Vec<u64> {
    let n = n_nodes + 1;
    if offsets_u32 {
        let body = &bytes[offsets_off..offsets_off + 4 * n];
        let view: &[u32] = bytemuck::cast_slice(body);
        view.iter().map(|&v| v as u64).collect()
    } else {
        let body = &bytes[offsets_off..offsets_off + 8 * n];
        let view: &[u64] = bytemuck::cast_slice(body);
        view.to_vec()
    }
}

/// #351: decode the targets array body from disk into a heap
/// `Vec<u32>`. When the file stored u32 targets, borrow via
/// `bytemuck::cast_slice` and copy. When u16/u24, widen on read. The
/// in-memory `flat.targets` type stays `ArcCow<u32>` for hot-path
/// simplicity (avoids touching ~174 call sites that do `.targets[i]`).
fn read_targets_vec(
    bytes: &[u8],
    targets_off: usize,
    n_edges: usize,
    targets_width: crate::formats::WeightWidth,
) -> Vec<u32> {
    use crate::formats::WeightWidth;
    let body_bytes = targets_width.bytes_per_entry() * n_edges;
    let body = &bytes[targets_off..targets_off + body_bytes];
    match targets_width {
        WeightWidth::U32 => bytemuck::cast_slice::<u8, u32>(body).to_vec(),
        WeightWidth::U16 => body
            .chunks_exact(2)
            .map(|c| u16::from_le_bytes([c[0], c[1]]) as u32)
            .collect(),
        WeightWidth::U24 => body
            .chunks_exact(3)
            .map(|c| u32::from(c[0]) | (u32::from(c[1]) << 8) | (u32::from(c[2]) << 16))
            .collect(),
    }
}

/// mmap-backed targets loader (#351). u32 keeps zero-copy semantics;
/// u16/u24 widen to a heap `Vec<u32>` once at load.
fn load_targets_arccow(
    mmap: &std::sync::Arc<memmap2::Mmap>,
    targets_abs: usize,
    n_edges: usize,
    targets_width: crate::formats::WeightWidth,
) -> anyhow::Result<ArcCow<u32>> {
    use crate::formats::WeightWidth;
    match targets_width {
        WeightWidth::U32 => {
            ArcCow::<u32>::from_mmap(std::sync::Arc::clone(mmap), targets_abs, n_edges)
        }
        WeightWidth::U16 => {
            let body = &mmap[targets_abs..targets_abs + 2 * n_edges];
            let view: &[u16] = bytemuck::cast_slice(body);
            Ok(ArcCow::from_vec(view.iter().map(|&v| v as u32).collect()))
        }
        WeightWidth::U24 => {
            let body = &mmap[targets_abs..targets_abs + 3 * n_edges];
            let widened: Vec<u32> = body
                .chunks_exact(3)
                .map(|c| u32::from(c[0]) | (u32::from(c[1]) << 8) | (u32::from(c[2]) << 16))
                .collect();
            Ok(ArcCow::from_vec(widened))
        }
    }
}

/// mmap-backed offsets loader (#350). When the file stored u64 offsets
/// we keep zero-copy semantics via `ArcCow::<u64>::from_mmap`. When the
/// file stored u32 offsets we widen to a heap `Vec<u64>` once, since
/// the in-memory `flat.offsets` type stays `ArcCow<u64>` for hot-path
/// simplicity — the offsets array is small (n_nodes+1 entries, ~38 MB
/// on Belgium) so the one-shot widen at boot is cheap vs the lifetime
/// of the loaded region.
fn load_offsets_arccow(
    mmap: &std::sync::Arc<memmap2::Mmap>,
    offsets_abs: usize,
    n_nodes: usize,
    offsets_u32: bool,
) -> anyhow::Result<ArcCow<u64>> {
    let n = n_nodes + 1;
    if offsets_u32 {
        let body = &mmap[offsets_abs..offsets_abs + 4 * n];
        let view: &[u32] = bytemuck::cast_slice(body);
        Ok(ArcCow::from_vec(view.iter().map(|&v| v as u64).collect()))
    } else {
        ArcCow::<u64>::from_mmap(std::sync::Arc::clone(mmap), offsets_abs, n)
    }
}

fn write_adj_flat_body_and_footer(
    out: &mut Vec<u8>,
    offsets: &[u64],
    a32: &[u32], // targets or sources (treated as targets for #351 width)
    weights: &crate::formats::WeightArray,
    topo_edge_idx: Option<&[u32]>,
    offsets_u32: bool,
    targets_width: crate::formats::WeightWidth,
) {
    debug_assert_eq!(out.len(), ADJ_FLAT_HEADER_SIZE);
    // v3 (#350): write offsets at u32 width when the writer picked it.
    // The `pick_offsets_u32()` guard at the call site verifies every
    // value fits u32; a `debug_assert!` here catches any future caller
    // that bypasses the guard (e.g. a forgotten branch).
    if offsets_u32 {
        debug_assert!(
            offsets.iter().all(|&v| v <= u32::MAX as u64),
            "writer asked for u32 offsets but at least one value > u32::MAX (caller bypassed pick_offsets_u32)"
        );
        let u32_offsets: Vec<u32> = offsets.iter().map(|&v| v as u32).collect();
        out.extend_from_slice(bytemuck::cast_slice(&u32_offsets));
    } else {
        out.extend_from_slice(bytemuck::cast_slice(offsets));
    }

    // v4 (#351): write targets at the picked width. Validated at the
    // call site via `pick_targets_width()`; the debug_assert catches
    // any future caller that bypasses the predicate.
    use crate::formats::WeightWidth;
    let n_edges = a32.len();
    match targets_width {
        WeightWidth::U32 => out.extend_from_slice(bytemuck::cast_slice(a32)),
        WeightWidth::U16 => {
            debug_assert!(
                a32.iter().all(|&v| v < u16::MAX as u32),
                "writer asked for u16 targets but at least one value >= u16::MAX (caller bypassed pick_targets_width)"
            );
            let u16_targets: Vec<u16> = a32.iter().map(|&v| v as u16).collect();
            out.extend_from_slice(bytemuck::cast_slice(&u16_targets));
        }
        WeightWidth::U24 => {
            debug_assert!(
                a32.iter().all(|&v| v < crate::formats::U24_SENTINEL),
                "writer asked for u24 targets but at least one value >= U24_SENTINEL"
            );
            for &v in a32 {
                let le = v.to_le_bytes();
                out.extend_from_slice(&le[..3]);
            }
        }
    }
    // Trailing pad for u16/u24 so the next array starts 4-B aligned.
    let pad_bytes =
        targets_width.padded_body_bytes(n_edges) - targets_width.bytes_per_entry() * n_edges;
    for _ in 0..pad_bytes {
        out.push(0);
    }

    // v2 (#349): weights body is written at the actual width — u16/u24
    // sentinel-encoded, u32 as-is via `cast_slice`. Pad with zero
    // bytes to a 4-B boundary so the following `topo_edge_idx` (still
    // u32) stays aligned for `bytemuck::cast_slice` /
    // `ArcCow::<u32>::from_mmap`.
    use crate::formats::WeightArray;
    let n_edges = a32.len();
    let width = weights.width();
    match weights {
        WeightArray::U32(arr) => {
            out.extend_from_slice(bytemuck::cast_slice(arr.as_slice()));
        }
        WeightArray::U16(arr) => {
            // The in-memory `ArcCow<u16>` already carries the
            // u32::MAX → u16::MAX sentinel mapping (applied at
            // build time via `WeightArray::from_vec_u32`). Bytes
            // round-trip verbatim through `get()`.
            let slice = arr.as_slice();
            debug_assert_eq!(slice.len(), n_edges);
            out.extend_from_slice(bytemuck::cast_slice(slice));
        }
        WeightArray::U24(bytes) => {
            // u24 stores 3 LE bytes per entry. The sentinel
            // `crate::formats::U24_SENTINEL` was baked in at build
            // time; the on-disk bytes round-trip verbatim and the
            // reader's `get()` widens U24_SENTINEL back to u32::MAX.
            let slice = bytes.as_slice();
            debug_assert_eq!(slice.len(), 3 * n_edges);
            out.extend_from_slice(slice);
        }
    }
    // Trailing pad so the next array starts 4-B aligned. u32 widths
    // never pad; u16 pads 0 or 2; u24 pads 0/1/2/3.
    let pad = width.padded_body_bytes(n_edges) - width.bytes_per_entry() * n_edges;
    for _ in 0..pad {
        out.push(0);
    }

    if let Some(t) = topo_edge_idx {
        out.extend_from_slice(bytemuck::cast_slice(t));
    }
    let body = &out[ADJ_FLAT_HEADER_SIZE..];
    let body_crc = super::super::formats::crc::checksum(body);
    let file_crc = {
        let mut d = super::super::formats::crc::Digest::new();
        d.update(&out[..]);
        d.finalize()
    };
    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(&file_crc.to_le_bytes());
}

fn verify_adj_flat_crcs(bytes: &[u8], body_end: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        bytes.len() == body_end + ADJ_FLAT_FOOTER_SIZE,
        "adj-flat trailing bytes: file_len={} body_end={}",
        bytes.len(),
        body_end
    );
    let body = &bytes[ADJ_FLAT_HEADER_SIZE..body_end];
    let computed_body = super::super::formats::crc::checksum(body);
    let stored_body = u64::from_le_bytes(bytes[body_end..body_end + 8].try_into().unwrap());
    anyhow::ensure!(
        computed_body == stored_body,
        "adj-flat body CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
        computed_body,
        stored_body
    );
    let computed_file = super::super::formats::crc::checksum(&bytes[..body_end]);
    let stored_file = u64::from_le_bytes(bytes[body_end + 8..body_end + 16].try_into().unwrap());
    anyhow::ensure!(
        computed_file == stored_file,
        "adj-flat file CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
        computed_file,
        stored_file
    );
    Ok(())
}

/// Serialiser / deserialiser for `UpAdjFlat`.
pub struct UpAdjFlatFile;

impl UpAdjFlatFile {
    /// Encode `flat` into the on-disk binary representation. Returns the
    /// owned byte vector ready to be appended to a container.
    pub fn encode(flat: &UpAdjFlat) -> Vec<u8> {
        let n_nodes = flat.offsets.len().saturating_sub(1);
        let n_edges = flat.weights.len();
        let has_topo_idx = !flat.topo_edge_idx.is_empty();
        if has_topo_idx {
            assert_eq!(
                flat.topo_edge_idx.len(),
                n_edges,
                "topo_edge_idx must have length n_edges"
            );
        }
        let width = flat.weights.width();
        let offsets_u32 = pick_offsets_u32(&flat.offsets);
        let targets_width = pick_targets_width(&flat.targets);
        let (_, _, _, _, body_end) = body_layout(
            n_nodes,
            n_edges,
            has_topo_idx,
            width,
            offsets_u32,
            targets_width,
        );
        let mut out = Vec::with_capacity(body_end + ADJ_FLAT_FOOTER_SIZE);
        write_adj_flat_header(
            &mut out,
            AdjFlatHeader {
                magic: UP_ADJ_FLAT_MAGIC,
                has_topo_idx,
                width,
                offsets_u32,
                targets_width,
                n_nodes: n_nodes as u64,
                n_edges: n_edges as u64,
            },
        );
        let topo: Option<&[u32]> = if has_topo_idx {
            Some(&flat.topo_edge_idx)
        } else {
            None
        };
        write_adj_flat_body_and_footer(
            &mut out,
            &flat.offsets,
            &flat.targets,
            &flat.weights,
            topo,
            offsets_u32,
            targets_width,
        );
        debug_assert_eq!(out.len(), body_end + ADJ_FLAT_FOOTER_SIZE);
        out
    }

    /// Legacy reader over a `'static` byte slice. Verifies both body
    /// and file CRCs before returning. Production loaders should use
    /// [`Self::read_from_mmap_unverified`] which keeps the `Arc<Mmap>`
    /// strong-count tied to the returned struct (no leak).
    ///
    /// Historically the returned slices were `Cow::Borrowed` into the
    /// `'static` input. After #296, this path copies into owned `Vec`s
    /// so the returned `UpAdjFlat` does not pin the input bytes; the
    /// production zero-copy lives on [`Self::read_from_mmap_unverified`].
    pub fn read_from_bytes(bytes: &'static [u8]) -> anyhow::Result<UpAdjFlat> {
        Self::read_from_bytes_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes`] but elides the per-format CRC
    /// walk over the body. Caller MUST guarantee the bytes have already
    /// been verified upstream (e.g. via the container's lazy CRC layer).
    pub fn read_from_bytes_unverified(bytes: &'static [u8]) -> anyhow::Result<UpAdjFlat> {
        Self::read_from_bytes_inner(bytes, false)
    }

    fn read_from_bytes_inner(bytes: &'static [u8], verify: bool) -> anyhow::Result<UpAdjFlat> {
        let (has_topo_idx, width, offsets_u32, targets_width, n_nodes, n_edges) =
            parse_adj_flat_header(bytes, UP_ADJ_FLAT_MAGIC)?;
        let (offsets_off, targets_off, weights_off, topo_off, body_end) = body_layout(
            n_nodes,
            n_edges,
            has_topo_idx,
            width,
            offsets_u32,
            targets_width,
        );
        anyhow::ensure!(
            bytes.len() == body_end + ADJ_FLAT_FOOTER_SIZE,
            "adj-flat size mismatch: got {}, expected {}",
            bytes.len(),
            body_end + ADJ_FLAT_FOOTER_SIZE
        );
        // Alignment guard — bytemuck would panic otherwise.
        anyhow::ensure!(
            (bytes.as_ptr() as usize).is_multiple_of(8),
            "adj-flat section bytes not 8-byte aligned (got pointer 0x{:x})",
            bytes.as_ptr() as usize
        );
        if verify {
            verify_adj_flat_crcs(bytes, body_end)?;
        }

        let offsets_vec: Vec<u64> = read_offsets_vec(bytes, offsets_off, n_nodes, offsets_u32);
        let targets_vec: Vec<u32> = read_targets_vec(bytes, targets_off, n_edges, targets_width);
        // v2 (#349): decode the weights body from its native width into
        // a Vec<u32> so the legacy byte-slice path returns the same
        // public shape it always did. Compact widths shrink the
        // on-disk bytes; the in-memory copy widens to u32 here, just
        // like v1.
        let weights_bytes = &bytes[weights_off..weights_off + width.bytes_per_entry() * n_edges];
        let weights_vec: Vec<u32> = match width {
            crate::formats::WeightWidth::U32 => {
                bytemuck::cast_slice::<u8, u32>(weights_bytes).to_vec()
            }
            crate::formats::WeightWidth::U16 => {
                crate::formats::cch_weights::decode_u16_to_u32_vec(weights_bytes)
            }
            crate::formats::WeightWidth::U24 => {
                crate::formats::cch_weights::decode_u24_to_u32_vec(weights_bytes)
            }
        };
        let topo_edge_idx: &[u32] = if has_topo_idx {
            bytemuck::cast_slice(&bytes[topo_off..topo_off + 4 * n_edges])
        } else {
            &[]
        };
        // Legacy zero-copy path: copy into owned `Vec`s so the on-disk
        // → in-memory shape matches the post-#296 `ArcCow<T>` field
        // type. The `Arc<Mmap>`-backed un-leak path is
        // [`Self::read_from_mmap_unverified`].
        Ok(UpAdjFlat {
            offsets: ArcCow::from_vec(offsets_vec),
            targets: ArcCow::from_vec(targets_vec),
            weights: crate::formats::WeightArray::from_vec_u32(weights_vec),
            topo_edge_idx: ArcCow::from_vec(topo_edge_idx.to_vec()),
        })
    }

    /// Production mmap-backed reader (#296). Holds an `Arc<Mmap>` clone
    /// for the returned flat's lifetime — when the flat drops, the
    /// strong count decreases. Once every clone drops, the `Mmap`
    /// drops, `munmap` fires, and the kernel reclaims the pages. This
    /// is the un-leak counterpart to [`Self::read_from_bytes`].
    ///
    /// `byte_offset` and `byte_len` are the position and length of the
    /// section within the container, as recorded in the directory
    /// entry. CRC walking is the caller's responsibility (typically
    /// driven through the lazy CRC layer before this call).
    pub fn read_from_mmap_unverified(
        mmap: std::sync::Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> anyhow::Result<UpAdjFlat> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "up_adj_flat section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let bytes = &mmap[byte_offset..byte_offset + byte_len];
        let (has_topo_idx, width, offsets_u32, targets_width, n_nodes, n_edges) =
            parse_adj_flat_header(bytes, UP_ADJ_FLAT_MAGIC)?;
        let (offsets_off, targets_off, weights_off, topo_off, body_end) = body_layout(
            n_nodes,
            n_edges,
            has_topo_idx,
            width,
            offsets_u32,
            targets_width,
        );
        anyhow::ensure!(
            byte_len == body_end + ADJ_FLAT_FOOTER_SIZE,
            "up_adj_flat size mismatch: got {}, expected {}",
            byte_len,
            body_end + ADJ_FLAT_FOOTER_SIZE
        );
        // Container-absolute byte offsets of each sub-array.
        let offsets_abs = byte_offset + offsets_off;
        let targets_abs = byte_offset + targets_off;
        let weights_abs = byte_offset + weights_off;
        let topo_abs = byte_offset + topo_off;

        let offsets = load_offsets_arccow(&mmap, offsets_abs, n_nodes, offsets_u32)?;
        let targets = load_targets_arccow(&mmap, targets_abs, n_edges, targets_width)?;
        let weights = crate::formats::cch_weights::decode_weight_array_mmap(
            &mmap,
            weights_abs,
            n_edges,
            width,
        )?;
        let topo_edge_idx = if has_topo_idx {
            ArcCow::<u32>::from_mmap(mmap, topo_abs, n_edges)?
        } else {
            ArcCow::from_vec(Vec::new())
        };
        Ok(UpAdjFlat {
            offsets,
            targets,
            weights,
            topo_edge_idx,
        })
    }
}

/// Serialiser / deserialiser for `DownAdjFlat`.
pub struct DownAdjFlatFile;

impl DownAdjFlatFile {
    pub fn encode(flat: &DownAdjFlat) -> Vec<u8> {
        let n_nodes = flat.offsets.len().saturating_sub(1);
        let n_edges = flat.weights.len();
        let width = flat.weights.width();
        let offsets_u32 = pick_offsets_u32(&flat.offsets);
        let targets_width = pick_targets_width(&flat.targets);
        // DownAdjFlat never carries topo_edge_idx.
        let (_, _, _, _, body_end) =
            body_layout(n_nodes, n_edges, false, width, offsets_u32, targets_width);
        let mut out = Vec::with_capacity(body_end + ADJ_FLAT_FOOTER_SIZE);
        write_adj_flat_header(
            &mut out,
            AdjFlatHeader {
                magic: DOWN_ADJ_FLAT_MAGIC,
                has_topo_idx: false,
                width,
                offsets_u32,
                targets_width,
                n_nodes: n_nodes as u64,
                n_edges: n_edges as u64,
            },
        );
        write_adj_flat_body_and_footer(
            &mut out,
            &flat.offsets,
            &flat.targets,
            &flat.weights,
            None,
            offsets_u32,
            targets_width,
        );
        out
    }

    pub fn read_from_bytes(bytes: &'static [u8]) -> anyhow::Result<DownAdjFlat> {
        Self::read_from_bytes_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes`] but elides the per-format CRC
    /// walk. Caller MUST guarantee the bytes are already verified.
    pub fn read_from_bytes_unverified(bytes: &'static [u8]) -> anyhow::Result<DownAdjFlat> {
        Self::read_from_bytes_inner(bytes, false)
    }

    fn read_from_bytes_inner(bytes: &'static [u8], verify: bool) -> anyhow::Result<DownAdjFlat> {
        let (has_topo_idx, width, offsets_u32, targets_width, n_nodes, n_edges) =
            parse_adj_flat_header(bytes, DOWN_ADJ_FLAT_MAGIC)?;
        anyhow::ensure!(
            !has_topo_idx,
            "DownAdjFlat must not carry topo_edge_idx (has_topo_idx=1)"
        );
        let (offsets_off, targets_off, weights_off, _, body_end) =
            body_layout(n_nodes, n_edges, false, width, offsets_u32, targets_width);
        anyhow::ensure!(
            bytes.len() == body_end + ADJ_FLAT_FOOTER_SIZE,
            "adj-flat size mismatch"
        );
        anyhow::ensure!(
            (bytes.as_ptr() as usize).is_multiple_of(8),
            "adj-flat section bytes not 8-byte aligned"
        );
        if verify {
            verify_adj_flat_crcs(bytes, body_end)?;
        }
        let offsets_vec: Vec<u64> = read_offsets_vec(bytes, offsets_off, n_nodes, offsets_u32);
        let targets_vec: Vec<u32> = read_targets_vec(bytes, targets_off, n_edges, targets_width);
        let weights_bytes = &bytes[weights_off..weights_off + width.bytes_per_entry() * n_edges];
        let weights_vec: Vec<u32> = match width {
            crate::formats::WeightWidth::U32 => {
                bytemuck::cast_slice::<u8, u32>(weights_bytes).to_vec()
            }
            crate::formats::WeightWidth::U16 => {
                crate::formats::cch_weights::decode_u16_to_u32_vec(weights_bytes)
            }
            crate::formats::WeightWidth::U24 => {
                crate::formats::cch_weights::decode_u24_to_u32_vec(weights_bytes)
            }
        };
        // Legacy zero-copy path now copies into owned Vecs (#296).
        Ok(DownAdjFlat {
            offsets: ArcCow::from_vec(offsets_vec),
            targets: ArcCow::from_vec(targets_vec),
            weights: crate::formats::WeightArray::from_vec_u32(weights_vec),
        })
    }

    /// Production mmap-backed reader (#296). See
    /// [`UpAdjFlatFile::read_from_mmap_unverified`] for the un-leak
    /// rationale; identical pattern.
    pub fn read_from_mmap_unverified(
        mmap: std::sync::Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> anyhow::Result<DownAdjFlat> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "down_adj_flat section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let bytes = &mmap[byte_offset..byte_offset + byte_len];
        let (has_topo_idx, width, offsets_u32, targets_width, n_nodes, n_edges) =
            parse_adj_flat_header(bytes, DOWN_ADJ_FLAT_MAGIC)?;
        anyhow::ensure!(
            !has_topo_idx,
            "DownAdjFlat must not carry topo_edge_idx (has_topo_idx=1)"
        );
        let (offsets_off, targets_off, weights_off, _, body_end) =
            body_layout(n_nodes, n_edges, false, width, offsets_u32, targets_width);
        anyhow::ensure!(
            byte_len == body_end + ADJ_FLAT_FOOTER_SIZE,
            "down_adj_flat size mismatch: got {}, expected {}",
            byte_len,
            body_end + ADJ_FLAT_FOOTER_SIZE
        );
        let offsets_abs = byte_offset + offsets_off;
        let targets_abs = byte_offset + targets_off;
        let weights_abs = byte_offset + weights_off;
        let offsets = load_offsets_arccow(&mmap, offsets_abs, n_nodes, offsets_u32)?;
        let targets = load_targets_arccow(&mmap, targets_abs, n_edges, targets_width)?;
        let weights = crate::formats::cch_weights::decode_weight_array_mmap(
            &mmap,
            weights_abs,
            n_edges,
            width,
        )?;
        Ok(DownAdjFlat {
            offsets,
            targets,
            weights,
        })
    }
}

/// Serialiser / deserialiser for `DownReverseAdjFlat`.
pub struct DownReverseAdjFlatFile;

impl DownReverseAdjFlatFile {
    pub fn encode(flat: &DownReverseAdjFlat) -> Vec<u8> {
        let n_nodes = flat.offsets.len().saturating_sub(1);
        let n_edges = flat.weights.len();
        let has_topo_idx = !flat.topo_edge_idx.is_empty();
        if has_topo_idx {
            assert_eq!(flat.topo_edge_idx.len(), n_edges);
        }
        let width = flat.weights.width();
        let offsets_u32 = pick_offsets_u32(&flat.offsets);
        let targets_width = pick_targets_width(&flat.sources);
        let (_, _, _, _, body_end) = body_layout(
            n_nodes,
            n_edges,
            has_topo_idx,
            width,
            offsets_u32,
            targets_width,
        );
        let mut out = Vec::with_capacity(body_end + ADJ_FLAT_FOOTER_SIZE);
        write_adj_flat_header(
            &mut out,
            AdjFlatHeader {
                magic: DOWN_REV_ADJ_FLAT_MAGIC,
                has_topo_idx,
                width,
                offsets_u32,
                targets_width,
                n_nodes: n_nodes as u64,
                n_edges: n_edges as u64,
            },
        );
        let topo: Option<&[u32]> = if has_topo_idx {
            Some(&flat.topo_edge_idx)
        } else {
            None
        };
        write_adj_flat_body_and_footer(
            &mut out,
            &flat.offsets,
            &flat.sources,
            &flat.weights,
            topo,
            offsets_u32,
            targets_width,
        );
        out
    }

    pub fn read_from_bytes(bytes: &'static [u8]) -> anyhow::Result<DownReverseAdjFlat> {
        Self::read_from_bytes_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes`] but elides the per-format CRC
    /// walk. Caller MUST guarantee the bytes are already verified.
    pub fn read_from_bytes_unverified(bytes: &'static [u8]) -> anyhow::Result<DownReverseAdjFlat> {
        Self::read_from_bytes_inner(bytes, false)
    }

    fn read_from_bytes_inner(
        bytes: &'static [u8],
        verify: bool,
    ) -> anyhow::Result<DownReverseAdjFlat> {
        let (has_topo_idx, width, offsets_u32, targets_width, n_nodes, n_edges) =
            parse_adj_flat_header(bytes, DOWN_REV_ADJ_FLAT_MAGIC)?;
        let (offsets_off, sources_off, weights_off, topo_off, body_end) = body_layout(
            n_nodes,
            n_edges,
            has_topo_idx,
            width,
            offsets_u32,
            targets_width,
        );
        anyhow::ensure!(
            bytes.len() == body_end + ADJ_FLAT_FOOTER_SIZE,
            "adj-flat size mismatch"
        );
        anyhow::ensure!(
            (bytes.as_ptr() as usize).is_multiple_of(8),
            "adj-flat section bytes not 8-byte aligned"
        );
        if verify {
            verify_adj_flat_crcs(bytes, body_end)?;
        }
        let offsets_vec: Vec<u64> = read_offsets_vec(bytes, offsets_off, n_nodes, offsets_u32);
        let sources_vec: Vec<u32> = read_targets_vec(bytes, sources_off, n_edges, targets_width);
        let weights_bytes = &bytes[weights_off..weights_off + width.bytes_per_entry() * n_edges];
        let weights_vec: Vec<u32> = match width {
            crate::formats::WeightWidth::U32 => {
                bytemuck::cast_slice::<u8, u32>(weights_bytes).to_vec()
            }
            crate::formats::WeightWidth::U16 => {
                crate::formats::cch_weights::decode_u16_to_u32_vec(weights_bytes)
            }
            crate::formats::WeightWidth::U24 => {
                crate::formats::cch_weights::decode_u24_to_u32_vec(weights_bytes)
            }
        };
        let topo_edge_idx: &[u32] = if has_topo_idx {
            bytemuck::cast_slice(&bytes[topo_off..topo_off + 4 * n_edges])
        } else {
            &[]
        };
        // Legacy zero-copy path now copies into owned Vecs (#296).
        Ok(DownReverseAdjFlat {
            offsets: ArcCow::from_vec(offsets_vec),
            sources: ArcCow::from_vec(sources_vec),
            weights: crate::formats::WeightArray::from_vec_u32(weights_vec),
            topo_edge_idx: ArcCow::from_vec(topo_edge_idx.to_vec()),
        })
    }

    /// Production mmap-backed reader (#296). See
    /// [`UpAdjFlatFile::read_from_mmap_unverified`] for the un-leak
    /// rationale; identical pattern.
    pub fn read_from_mmap_unverified(
        mmap: std::sync::Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> anyhow::Result<DownReverseAdjFlat> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "down_reverse_adj_flat section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let bytes = &mmap[byte_offset..byte_offset + byte_len];
        let (has_topo_idx, width, offsets_u32, targets_width, n_nodes, n_edges) =
            parse_adj_flat_header(bytes, DOWN_REV_ADJ_FLAT_MAGIC)?;
        let (offsets_off, sources_off, weights_off, topo_off, body_end) = body_layout(
            n_nodes,
            n_edges,
            has_topo_idx,
            width,
            offsets_u32,
            targets_width,
        );
        anyhow::ensure!(
            byte_len == body_end + ADJ_FLAT_FOOTER_SIZE,
            "down_reverse_adj_flat size mismatch: got {}, expected {}",
            byte_len,
            body_end + ADJ_FLAT_FOOTER_SIZE
        );
        let offsets_abs = byte_offset + offsets_off;
        let sources_abs = byte_offset + sources_off;
        let weights_abs = byte_offset + weights_off;
        let topo_abs = byte_offset + topo_off;
        let offsets = load_offsets_arccow(&mmap, offsets_abs, n_nodes, offsets_u32)?;
        let sources = load_targets_arccow(&mmap, sources_abs, n_edges, targets_width)?;
        let weights = crate::formats::cch_weights::decode_weight_array_mmap(
            &mmap,
            weights_abs,
            n_edges,
            width,
        )?;
        let topo_edge_idx = if has_topo_idx {
            ArcCow::<u32>::from_mmap(mmap, topo_abs, n_edges)?
        } else {
            ArcCow::from_vec(Vec::new())
        };
        Ok(DownReverseAdjFlat {
            offsets,
            sources,
            weights,
            topo_edge_idx,
        })
    }
}

// =============================================================================
// 4-ARY HEAP WITH DECREASE-KEY (OSRM-style)
// =============================================================================

const ARITY: usize = 4;
/// Single shared sentinel for "no live heap handle". Used both by
/// the matrix-side bucket Dijkstra (this module) and by the CCH
/// query-side `CchQueryState` (in `server/query.rs`). PR #317 review:
/// pulled up to `pub(crate)` so both call sites depend on the same
/// constant and can't drift.
pub(crate) const INVALID_HANDLE: u32 = u32::MAX;

/// 4-ary min-heap with decrease-key support
/// Mirrors OSRM's DAryHeap implementation
pub(crate) struct DAryHeap {
    /// Heap array: (weight, index into inserted_nodes)
    heap: Vec<(u32, u32)>,
}

impl DAryHeap {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            heap: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn size(&self) -> usize {
        self.heap.len()
    }

    /// Peek the minimum weight without popping.
    #[inline]
    pub(crate) fn peek_min_weight(&self) -> Option<u32> {
        self.heap.first().map(|(w, _)| *w)
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.heap.clear();
    }

    /// Insert new element and return its handle
    #[inline]
    pub(crate) fn push(&mut self, weight: u32, index: u32, handles: &mut [u32]) {
        let pos = self.heap.len();
        self.heap.push((weight, index));
        self.heapify_up(pos, handles);
    }

    /// Decrease key at given handle
    #[inline]
    pub(crate) fn decrease(&mut self, handle: u32, weight: u32, index: u32, handles: &mut [u32]) {
        let pos = handle as usize;
        debug_assert!(
            pos < self.heap.len(),
            "decrease: handle {} out of bounds (heap len {}), index/node {}",
            pos,
            self.heap.len(),
            index
        );
        self.heap[pos] = (weight, index);
        self.heapify_up(pos, handles);
    }

    /// Pop minimum element
    #[inline]
    pub(crate) fn pop(&mut self, handles: &mut [u32]) -> Option<(u32, u32)> {
        if self.heap.is_empty() {
            return None;
        }
        let result = self.heap[0];
        // #291 review fix: always clear the popped element's handle so
        // stale handle slots can't be mistaken for live ones on the next
        // push (which would call decrease on a dead position).
        handles[result.1 as usize] = INVALID_HANDLE;
        if self.heap.len() == 1 {
            self.heap.pop();
            return Some(result);
        }
        // Swap last element to front and heapify down
        let last_idx = self.heap.len() - 1;
        self.heap.swap(0, last_idx);
        // Update handle for element that moved to position 0
        handles[self.heap[0].1 as usize] = 0;
        self.heap.pop();
        if !self.heap.is_empty() {
            self.heapify_down(0, handles);
        }
        Some(result)
    }

    #[inline]
    fn parent(index: usize) -> usize {
        (index - 1) / ARITY
    }

    #[inline]
    fn kth_child(index: usize, k: usize) -> usize {
        ARITY * index + k + 1
    }

    #[inline]
    fn heapify_up(&mut self, mut pos: usize, handles: &mut [u32]) {
        let item = self.heap[pos];
        while pos > 0 {
            let parent_pos = Self::parent(pos);
            if item.0 >= self.heap[parent_pos].0 {
                break;
            }
            // Move parent down
            let parent_item = self.heap[parent_pos];
            self.heap[pos] = parent_item;
            handles[parent_item.1 as usize] = pos as u32;
            pos = parent_pos;
        }
        self.heap[pos] = item;
        handles[item.1 as usize] = pos as u32;
    }

    #[inline]
    fn heapify_down(&mut self, mut pos: usize, handles: &mut [u32]) {
        let item = self.heap[pos];
        let len = self.heap.len();
        loop {
            let first_child = Self::kth_child(pos, 0);
            if first_child >= len {
                break;
            }
            // Find minimum child
            let mut min_child = first_child;
            let mut min_weight = self.heap[first_child].0;
            for k in 1..ARITY {
                let child = Self::kth_child(pos, k);
                if child >= len {
                    break;
                }
                if self.heap[child].0 < min_weight {
                    min_child = child;
                    min_weight = self.heap[child].0;
                }
            }
            if item.0 <= min_weight {
                break;
            }
            // Move min child up
            let child_item = self.heap[min_child];
            self.heap[pos] = child_item;
            handles[child_item.1 as usize] = pos as u32;
            pos = min_child;
        }
        self.heap[pos] = item;
        handles[item.1 as usize] = pos as u32;
    }
}

// =============================================================================
// SEARCH STATE - OSRM-style with DecreaseKey
// =============================================================================

/// Entry tracking node state with version stamp
#[derive(Clone, Copy)]
#[repr(C)]
struct NodeEntry {
    dist: u32,
    version: u32,
}

/// Reusable search state with 4-ary heap and decrease-key
struct SearchState {
    /// Per-node state: distance + version
    entries: Vec<NodeEntry>,
    current_version: u32,
    /// 4-ary min-heap with decrease-key
    heap: DAryHeap,
    /// Handles array: node → position in heap (SINGLE source of truth for handles)
    /// INVALID_HANDLE means node is not in heap (never inserted or already settled)
    handles: Vec<u32>,
    /// Counters for profiling
    pushes: usize,
    pops: usize,
    stale_pops: usize, // Should always be 0 with decrease-key
    /// Early-stop bound (#415 / max_minutes): `pop()` returns `None`
    /// once the heap minimum exceeds this. `u32::MAX` = unbounded (no
    /// behavioural change). The min-heap pops in non-decreasing order, so
    /// once the minimum crosses the threshold every remaining entry does
    /// too — stopping here is exact for the reachable-within-threshold set.
    /// Set per-sweep by the bounded entry points; preserved across
    /// `start_search()` so it must be reset explicitly when reusing a
    /// thread-local state for an unbounded query.
    dist_threshold: u32,
}

impl SearchState {
    fn new(n_nodes: usize, heap_capacity: usize) -> Self {
        Self {
            entries: vec![
                NodeEntry {
                    dist: u32::MAX,
                    version: 0
                };
                n_nodes
            ],
            current_version: 0,
            heap: DAryHeap::new(heap_capacity),
            handles: vec![INVALID_HANDLE; n_nodes],
            pushes: 0,
            pops: 0,
            stale_pops: 0,
            dist_threshold: u32::MAX,
        }
    }

    #[inline]
    fn start_search(&mut self) {
        self.current_version = self.current_version.wrapping_add(1);
        if self.current_version == 0 {
            // Version overflow - reset all entries
            for e in &mut self.entries {
                e.dist = u32::MAX;
                e.version = 0;
            }
            // Also need to reset handles since we're starting fresh
            for h in &mut self.handles {
                *h = INVALID_HANDLE;
            }
            self.current_version = 1;
        }
        self.heap.clear();
    }

    /// Relax an edge: insert new or decrease-key existing
    #[inline]
    fn relax(&mut self, node: u32, dist: u32) -> bool {
        let e = &mut self.entries[node as usize];

        if e.version == self.current_version {
            // Node already seen this search
            if dist < e.dist {
                // Better path found - decrease key
                e.dist = dist;
                let handle = self.handles[node as usize];
                if handle != INVALID_HANDLE && (handle as usize) < self.heap.size() {
                    // Node is still in heap - decrease key
                    self.heap.decrease(handle, dist, node, &mut self.handles);
                    self.pushes += 1;
                }
                // Note: if handle == INVALID_HANDLE, node was already settled
                return true;
            }
            return false;
        }

        // First time seeing this node in current search
        // Reset handle to ensure no stale value is used
        self.handles[node as usize] = INVALID_HANDLE;
        e.dist = dist;
        e.version = self.current_version;
        self.heap.push(dist, node, &mut self.handles);
        self.pushes += 1;
        true
    }

    #[inline]
    fn pop(&mut self) -> Option<(u32, u32)> {
        if let Some((dist, node)) = self.heap.pop(&mut self.handles) {
            // Bounded early-stop: the popped node already had its handle
            // cleared by `DAryHeap::pop`, so returning `None` here cleanly
            // terminates the sweep. Every node with final distance
            // <= dist_threshold has already been settled (min-heap order).
            if dist > self.dist_threshold {
                return None;
            }
            self.pops += 1;
            // Mark as settled (handle becomes INVALID_HANDLE after pop in heapify_down)
            self.handles[node as usize] = INVALID_HANDLE;
            return Some((dist, node));
        }
        None
    }
}

// =============================================================================
// BUCKET LAYOUT - Prefix-sum for O(1) lookup with reusable buffers
// =============================================================================

/// Bucket item (8 bytes, aligned for fast access)
/// Used by backward_join_prefix for sequential version
#[derive(Clone, Copy)]
#[repr(C)]
struct BucketEntry {
    dist: u32,
    source_idx: u32,
}

/// Reusable prefix-sum bucket structure with version stamping
/// Uses Structure-of-Arrays (SoA) for better cache efficiency:
/// - dists: Vec<u32> - distances, contiguous
/// - source_indices: Vec<u32> - source indices, contiguous
///
/// This saves 2 bytes per entry (no padding) and improves cache utilization
struct PrefixSumBuckets {
    /// Count of items per node (stamped)
    counts: Vec<u32>,
    /// Version stamps for counts (avoid clearing)
    count_stamps: Vec<u32>,
    /// Current stamp for this build
    current_stamp: u32,
    /// Offsets into items array (n_nodes + 1)
    offsets: Vec<u32>,
    /// SoA: distances (4 bytes each)
    dists: Vec<u32>,
    /// SoA: source indices (4 bytes each)
    source_indices: Vec<u32>,
    /// AoS view exposed via [`get`] for callers that prefer
    /// `&[BucketEntry]` over the SoA `(dists, source_indices)` slices
    /// returned by [`get_range`].
    items: Vec<BucketEntry>,
    /// Temporary storage for nodes that have items (for offset building)
    active_nodes: Vec<u32>,
}

impl PrefixSumBuckets {
    fn new(n_nodes: usize) -> Self {
        Self {
            counts: vec![0; n_nodes],
            count_stamps: vec![0; n_nodes],
            current_stamp: 0,
            offsets: vec![0; n_nodes + 1],
            dists: Vec::new(),
            source_indices: Vec::new(),
            items: Vec::new(),
            active_nodes: Vec::new(),
        }
    }

    /// Build buckets from collected items - O(items) time, no per-node clearing
    /// Uses SoA layout for better cache efficiency
    fn build(&mut self, raw_items: &[(u32, u32, u32)]) {
        // Increment stamp (wrapping is fine, we compare equality)
        self.current_stamp = self.current_stamp.wrapping_add(1);
        if self.current_stamp == 0 {
            // Stamp overflow - must clear
            self.count_stamps.fill(0);
            self.current_stamp = 1;
        }

        self.active_nodes.clear();

        // First pass: count items per node (stamp-based, no clearing)
        for &(node, _, _) in raw_items {
            let n = node as usize;
            if self.count_stamps[n] != self.current_stamp {
                // First time seeing this node in this build
                self.count_stamps[n] = self.current_stamp;
                self.counts[n] = 0;
                self.active_nodes.push(node);
            }
            self.counts[n] += 1;
        }

        // Build offsets only for active nodes (sparse)
        let mut total = 0u32;
        for &node in &self.active_nodes {
            let n = node as usize;
            self.offsets[n] = total;
            total += self.counts[n];
        }

        // Resize SoA arrays if needed
        let total_items = total as usize;
        if self.dists.len() < total_items {
            self.dists.resize(total_items, 0);
            self.source_indices.resize(total_items, 0);
        }
        // Also keep legacy items for backward compatibility
        if self.items.len() < total_items {
            self.items.resize(
                total_items,
                BucketEntry {
                    dist: 0,
                    source_idx: 0,
                },
            );
        }

        // Reset counts for second pass (reuse as write cursors)
        for &node in &self.active_nodes {
            self.counts[node as usize] = 0;
        }

        // Second pass: place items in both SoA and AoS
        for &(node, source_idx, dist) in raw_items {
            let n = node as usize;
            let pos = (self.offsets[n] + self.counts[n]) as usize;
            self.dists[pos] = dist;
            self.source_indices[pos] = source_idx;
            self.items[pos] = BucketEntry { dist, source_idx };
            self.counts[n] += 1;
        }
    }

    /// Get bucket entries for a node (legacy AoS) - O(k) where k is bucket size
    #[inline]
    fn get(&self, node: u32) -> &[BucketEntry] {
        let n = node as usize;
        if self.count_stamps[n] != self.current_stamp {
            return &[];
        }
        let start = self.offsets[n] as usize;
        let len = self.counts[n] as usize;
        &self.items[start..start + len]
    }

    /// Get bucket data for a node using SoA layout
    /// Returns (start_idx, len) for indexing into dists/source_indices arrays
    #[inline]
    fn get_range(&self, node: u32) -> (usize, usize) {
        let n = node as usize;
        if self.count_stamps[n] != self.current_stamp {
            return (0, 0);
        }
        let start = self.offsets[n] as usize;
        let len = self.counts[n] as usize;
        (start, len)
    }

    fn total_items(&self) -> usize {
        self.active_nodes
            .iter()
            .map(|&n| self.counts[n as usize] as usize)
            .sum()
    }

    fn n_nodes_with_buckets(&self) -> usize {
        self.active_nodes.len()
    }
}

/// Sorted bucket layout with binary search (legacy, for comparison)
struct SortedBuckets {
    items: Vec<(u32, u32, u32)>, // (node, source_idx, dist)
}

impl SortedBuckets {
    fn build(mut items: Vec<(u32, u32, u32)>) -> Self {
        items.sort_unstable_by_key(|&(node, _, _)| node);
        Self { items }
    }

    #[inline]
    fn get(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let start = self.items.partition_point(|&(n, _, _)| n < node);
        let end = self.items.partition_point(|&(n, _, _)| n <= node);
        self.items[start..end].iter().map(|&(_, s, d)| (s, d))
    }

    fn total_items(&self) -> usize {
        self.items.len()
    }

    fn n_nodes_with_buckets(&self) -> usize {
        if self.items.is_empty() {
            return 0;
        }
        let mut count = 1;
        let mut prev = self.items[0].0;
        for &(n, _, _) in &self.items[1..] {
            if n != prev {
                count += 1;
                prev = n;
            }
        }
        count
    }

    /// Consume self and return the items buffer for reuse
    fn into_items(self) -> Vec<(u32, u32, u32)> {
        self.items
    }
}

// =============================================================================
// PUBLIC API
// =============================================================================

/// Statistics from bucket many-to-many computation
#[derive(Debug, Default, Clone)]
pub struct BucketM2MStats {
    pub n_sources: usize,
    pub n_targets: usize,
    pub forward_visited: usize,
    pub backward_visited: usize,
    pub bucket_items: usize,
    pub bucket_nodes: usize,
    pub join_operations: usize,
    pub skipped_joins: usize, // Bucket entries skipped due to bound-aware pruning
    pub forward_time_ms: u64,
    pub sort_time_ms: u64,
    pub backward_time_ms: u64,
    /// Total relaxations
    pub heap_pushes: usize,
    /// Total settlements (no stale with decrease-key)
    pub heap_pops: usize,
    /// Stale pops (always 0 with decrease-key heap)
    pub stale_pops: usize,
}

/// Compute many-to-many distance matrix using optimized bucket algorithm
///
/// Uses the correct directed-graph formulation:
///   d(s → t) = min over m: d(s → m) + d(m → t)
///
/// Optimizations:
/// - Flat reverse adjacency (no edge_idx indirection)
/// - 4-ary heap with decrease-key (no stale entries)
/// - Prefix-sum bucket layout (O(1) lookup)
/// - Version-stamped distances (O(1) per-search init)
pub fn table_bucket(
    topo: &CchTopo,
    weights: &CchWeights,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    // Build flat reverse adjacency with embedded weights
    let down_rev_flat = DownReverseAdjFlat::build(topo, weights);

    table_bucket_optimized(topo, weights, &down_rev_flat, sources, targets)
}

/// Optimized version using pre-built flat reverse adjacency
pub fn table_bucket_optimized(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    let n_nodes = topo.n_nodes as usize;
    let n_sources = sources.len();
    let n_targets = targets.len();

    let mut matrix = vec![u32::MAX; n_sources * n_targets];

    if n_sources == 0 || n_targets == 0 {
        return (matrix, BucketM2MStats::default());
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // Estimate for pre-allocation
    let avg_visited = (n_nodes / 400).clamp(500, 20000);

    // Single reusable search state
    let mut state = SearchState::new(n_nodes, avg_visited);

    // ========== PHASE 1: Forward searches from SOURCES (UP edges) ==========
    let forward_start = std::time::Instant::now();

    // Collect bucket items: (node, source_idx, dist)
    let mut bucket_items: Vec<(u32, u32, u32)> = Vec::with_capacity(n_sources * avg_visited);

    for (source_idx, &source) in sources.iter().enumerate() {
        if source as usize >= n_nodes {
            continue;
        }
        forward_fill_buckets_opt(
            topo,
            &weights.up,
            source_idx as u32,
            source,
            &mut state,
            &mut bucket_items,
        );
    }

    stats.forward_visited = bucket_items.len();
    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

    // ========== PHASE 2: Sort buckets for binary search ==========
    let sort_start = std::time::Instant::now();
    let buckets = SortedBuckets::build(bucket_items);
    stats.bucket_items = buckets.total_items();
    stats.bucket_nodes = buckets.n_nodes_with_buckets();
    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

    // ========== PHASE 3: Backward searches from TARGETS ==========
    let backward_start = std::time::Instant::now();

    for (target_idx, &target) in targets.iter().enumerate() {
        if target as usize >= n_nodes {
            continue;
        }

        let (visited, joins) = backward_join_opt(
            down_rev_flat,
            target,
            &buckets,
            &mut matrix,
            n_targets,
            target_idx,
            &mut state,
        );

        stats.backward_visited += visited;
        stats.join_operations += joins;
    }

    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    // Collect stats
    stats.heap_pushes = state.pushes;
    stats.heap_pops = state.pops;
    stats.stale_pops = state.stale_pops;

    (matrix, stats)
}

/// Reusable M2M engine to avoid per-call allocations
pub struct BucketM2MEngine {
    n_nodes: usize,
    state: SearchState,
    bucket_items: Vec<(u32, u32, u32)>,
}

impl BucketM2MEngine {
    /// Create a new engine for the given graph size
    pub fn new(n_nodes: usize) -> Self {
        let avg_visited = (n_nodes / 400).clamp(500, 20000);
        Self {
            n_nodes,
            state: SearchState::new(n_nodes, avg_visited),
            bucket_items: Vec::with_capacity(avg_visited * 100),
        }
    }

    /// Compute distance matrix using pre-allocated state
    pub fn compute(
        &mut self,
        topo: &CchTopo,
        weights: &CchWeights,
        down_rev_flat: &DownReverseAdjFlat,
        sources: &[u32],
        targets: &[u32],
    ) -> (Vec<u32>, BucketM2MStats) {
        let n_sources = sources.len();
        let n_targets = targets.len();

        let mut matrix = vec![u32::MAX; n_sources * n_targets];

        if n_sources == 0 || n_targets == 0 {
            return (matrix, BucketM2MStats::default());
        }

        let mut stats = BucketM2MStats {
            n_sources,
            n_targets,
            ..Default::default()
        };

        // Clear bucket items (reuse allocation)
        self.bucket_items.clear();

        // Reset counters for this computation
        self.state.pushes = 0;
        self.state.pops = 0;
        self.state.stale_pops = 0;

        // ========== PHASE 1: Forward searches from SOURCES (UP edges) ==========
        let forward_start = std::time::Instant::now();

        for (source_idx, &source) in sources.iter().enumerate() {
            if source as usize >= self.n_nodes {
                continue;
            }
            forward_fill_buckets_opt(
                topo,
                &weights.up,
                source_idx as u32,
                source,
                &mut self.state,
                &mut self.bucket_items,
            );
        }

        stats.forward_visited = self.bucket_items.len();
        stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

        // ========== PHASE 2: Sort buckets for binary search ==========
        let sort_start = std::time::Instant::now();
        let bucket_items = std::mem::take(&mut self.bucket_items);
        let buckets = SortedBuckets::build(bucket_items);
        stats.bucket_items = buckets.total_items();
        stats.bucket_nodes = buckets.n_nodes_with_buckets();
        stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

        // ========== PHASE 3: Backward searches from TARGETS ==========
        let backward_start = std::time::Instant::now();

        for (target_idx, &target) in targets.iter().enumerate() {
            if target as usize >= self.n_nodes {
                continue;
            }

            let (visited, joins) = backward_join_opt(
                down_rev_flat,
                target,
                &buckets,
                &mut matrix,
                n_targets,
                target_idx,
                &mut self.state,
            );

            stats.backward_visited += visited;
            stats.join_operations += joins;
        }

        stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

        // Restore bucket_items for reuse
        self.bucket_items = buckets.into_items();

        // Collect stats
        stats.heap_pushes = self.state.pushes;
        stats.heap_pops = self.state.pops;
        stats.stale_pops = self.state.stale_pops;

        (matrix, stats)
    }

    /// Compute using pre-built flat UP adjacency (no INF checks in forward loop)
    pub fn compute_flat(
        &mut self,
        up_adj_flat: &UpAdjFlat,
        down_rev_flat: &DownReverseAdjFlat,
        sources: &[u32],
        targets: &[u32],
        threshold: u32,
    ) -> (Vec<u32>, BucketM2MStats) {
        let n_sources = sources.len();
        let n_targets = targets.len();

        let mut matrix = vec![u32::MAX; n_sources * n_targets];

        if n_sources == 0 || n_targets == 0 {
            return (matrix, BucketM2MStats::default());
        }

        let mut stats = BucketM2MStats {
            n_sources,
            n_targets,
            ..Default::default()
        };

        // max_minutes bound: stop both forward and backward sweeps once the
        // time metric exceeds `threshold`. `u32::MAX` = unbounded.
        self.state.dist_threshold = threshold;

        // Clear bucket items (reuse allocation)
        self.bucket_items.clear();

        // Reset counters for this computation
        self.state.pushes = 0;
        self.state.pops = 0;
        self.state.stale_pops = 0;

        // ========== PHASE 1: Forward searches from SOURCES (UP edges, pre-filtered) ==========
        let forward_start = std::time::Instant::now();

        for (source_idx, &source) in sources.iter().enumerate() {
            if source as usize >= self.n_nodes {
                continue;
            }
            forward_fill_buckets_flat(
                up_adj_flat,
                source_idx as u32,
                source,
                &mut self.state,
                &mut self.bucket_items,
            );
        }

        stats.forward_visited = self.bucket_items.len();
        stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

        // ========== PHASE 2: Sort buckets for binary search ==========
        let sort_start = std::time::Instant::now();
        let bucket_items = std::mem::take(&mut self.bucket_items);
        let buckets = SortedBuckets::build(bucket_items);
        stats.bucket_items = buckets.total_items();
        stats.bucket_nodes = buckets.n_nodes_with_buckets();
        stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

        // ========== PHASE 3: Backward searches from TARGETS ==========
        let backward_start = std::time::Instant::now();

        for (target_idx, &target) in targets.iter().enumerate() {
            if target as usize >= self.n_nodes {
                continue;
            }

            let (visited, joins) = backward_join_opt(
                down_rev_flat,
                target,
                &buckets,
                &mut matrix,
                n_targets,
                target_idx,
                &mut self.state,
            );

            stats.backward_visited += visited;
            stats.join_operations += joins;
        }

        stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

        // Restore bucket_items for reuse
        self.bucket_items = buckets.into_items();

        // Collect stats
        stats.heap_pushes = self.state.pushes;
        stats.heap_pops = self.state.pops;
        stats.stale_pops = self.state.stale_pops;

        (matrix, stats)
    }
}

/// Fully optimized version using pre-built flat adjacencies for both directions
pub fn table_bucket_full_flat(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    let n_sources = sources.len();
    let n_targets = targets.len();

    let mut matrix = vec![u32::MAX; n_sources * n_targets];

    if n_sources == 0 || n_targets == 0 {
        return (matrix, BucketM2MStats::default());
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // Estimate for pre-allocation
    let avg_visited = (n_nodes / 400).clamp(500, 20000);

    // Single reusable search state
    let mut state = SearchState::new(n_nodes, avg_visited);

    // ========== PHASE 1: Forward searches from SOURCES (UP edges) ==========
    let forward_start = std::time::Instant::now();

    // Collect bucket items: (node, source_idx, dist)
    let mut bucket_items: Vec<(u32, u32, u32)> = Vec::with_capacity(n_sources * avg_visited);

    for (source_idx, &source) in sources.iter().enumerate() {
        if source as usize >= n_nodes {
            continue;
        }
        forward_fill_buckets_flat(
            up_adj_flat,
            source_idx as u32,
            source,
            &mut state,
            &mut bucket_items,
        );
    }

    stats.forward_visited = bucket_items.len();
    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

    // ========== PHASE 2: Build prefix-sum buckets (O(1) lookup) ==========
    let sort_start = std::time::Instant::now();
    let mut buckets = PrefixSumBuckets::new(n_nodes);
    buckets.build(&bucket_items);
    stats.bucket_items = buckets.total_items();
    stats.bucket_nodes = buckets.n_nodes_with_buckets();
    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

    // ========== PHASE 3: Backward searches from TARGETS ==========
    let backward_start = std::time::Instant::now();

    for (target_idx, &target) in targets.iter().enumerate() {
        if target as usize >= n_nodes {
            continue;
        }

        let (visited, joins) = backward_join_prefix(
            down_rev_flat,
            target,
            &buckets,
            &mut matrix,
            n_targets,
            target_idx,
            &mut state,
        );

        stats.backward_visited += visited;
        stats.join_operations += joins;
    }

    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    // Collect stats
    stats.heap_pushes = state.pushes;
    stats.heap_pops = state.pops;
    stats.stale_pops = state.stale_pops;

    (matrix, stats)
}

// =============================================================================
// SOURCE-BLOCK OPTIMIZED API (avoid repeated forward computation)
// =============================================================================

/// Precomputed forward buckets for a block of sources
/// Use `forward_build_buckets` to create, then `backward_join_with_buckets` for each target block
pub struct SourceBuckets {
    buckets: PrefixSumBuckets,
    n_sources: usize,
}

impl SourceBuckets {
    /// Get number of sources in this bucket set
    pub fn n_sources(&self) -> usize {
        self.n_sources
    }

    /// Get total bucket items (for stats)
    pub fn total_items(&self) -> usize {
        self.buckets.total_items()
    }
}

/// Forward phase only: compute buckets for a block of sources
/// Call ONCE per source block, then use `backward_join_with_buckets` for each target block
pub fn forward_build_buckets(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    sources: &[u32],
) -> SourceBuckets {
    forward_build_buckets_bounded(n_nodes, up_adj_flat, sources, u32::MAX)
}

/// Time-bounded [`forward_build_buckets`] (#415). Stops each source's forward
/// sweep once the up-distance exceeds `threshold`. `u32::MAX` = unbounded.
pub fn forward_build_buckets_bounded(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    sources: &[u32],
    threshold: u32,
) -> SourceBuckets {
    let n_sources = sources.len();

    if n_sources == 0 {
        return SourceBuckets {
            buckets: PrefixSumBuckets::new(n_nodes),
            n_sources: 0,
        };
    }

    // Estimate for pre-allocation
    let avg_visited = (n_nodes / 400).clamp(500, 20000);

    // Single reusable search state
    let mut state = SearchState::new(n_nodes, avg_visited);
    state.dist_threshold = threshold;

    // Collect bucket items: (node, source_idx, dist)
    let mut bucket_items: Vec<(u32, u32, u32)> = Vec::with_capacity(n_sources * avg_visited);

    for (source_idx, &source) in sources.iter().enumerate() {
        if source as usize >= n_nodes {
            continue;
        }
        forward_fill_buckets_flat(
            up_adj_flat,
            source_idx as u32,
            source,
            &mut state,
            &mut bucket_items,
        );
    }

    // Build prefix-sum buckets
    let mut buckets = PrefixSumBuckets::new(n_nodes);
    buckets.build(&bucket_items);

    SourceBuckets { buckets, n_sources }
}

/// Backward phase only: compute distances for targets using prebuilt source buckets
/// Returns a matrix of size n_sources × n_targets (row-major: matrix[src_idx * n_targets + tgt_idx])
pub fn backward_join_with_buckets(
    n_nodes: usize,
    down_rev_flat: &DownReverseAdjFlat,
    source_buckets: &SourceBuckets,
    targets: &[u32],
) -> Vec<u32> {
    backward_join_with_buckets_bounded(n_nodes, down_rev_flat, source_buckets, targets, u32::MAX)
}

/// Time-bounded [`backward_join_with_buckets`] (#415). Stops each target's
/// backward sweep once the down-distance exceeds `threshold`. Pairs whose
/// shortest time exceeds `threshold` are left as `u32::MAX` or a non-minimal
/// value > `threshold`; the caller must treat any cell > `threshold` as
/// out-of-bound. `u32::MAX` = unbounded.
pub fn backward_join_with_buckets_bounded(
    n_nodes: usize,
    down_rev_flat: &DownReverseAdjFlat,
    source_buckets: &SourceBuckets,
    targets: &[u32],
    threshold: u32,
) -> Vec<u32> {
    let n_sources = source_buckets.n_sources;
    let n_targets = targets.len();

    let mut matrix = vec![u32::MAX; n_sources * n_targets];

    if n_sources == 0 || n_targets == 0 {
        return matrix;
    }

    // Estimate for pre-allocation
    let avg_visited = (n_nodes / 400).clamp(500, 20000);
    let mut state = SearchState::new(n_nodes, avg_visited);
    state.dist_threshold = threshold;

    for (target_idx, &target) in targets.iter().enumerate() {
        if target as usize >= n_nodes {
            continue;
        }

        backward_join_prefix(
            down_rev_flat,
            target,
            &source_buckets.buckets,
            &mut matrix,
            n_targets,
            target_idx,
            &mut state,
        );
    }

    matrix
}

/// Forward search using flat UP adjacency (no INF check in hot loop).
///
/// #399: hoist `.as_slice()` once before the loop. ArcCow's Deref
/// otherwise dispatches a `match Owned/Mmap` on every index — fine for
/// `Owned` (single ptr fetch) but for `Mmap` includes `bytemuck::cast_slice`
/// per call, which the optimiser doesn't always inline through. Hoisting
/// to a raw `&[T]` once erases that cost in the inner loop.
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn forward_fill_buckets_flat(
    up_adj_flat: &UpAdjFlat,
    source_idx: u32,
    source: u32,
    state: &mut SearchState,
    bucket_items: &mut Vec<(u32, u32, u32)>,
) {
    state.start_search();
    state.relax(source, 0);

    let offsets = up_adj_flat.offsets.as_slice();
    let targets = up_adj_flat.targets.as_slice();
    let weights = &up_adj_flat.weights;

    while let Some((d, u)) = state.pop() {
        bucket_items.push((u, source_idx, d));

        // Relax UP edges (no INF check - pre-filtered)
        let start = offsets[u as usize] as usize;
        let end = offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = targets[i];
            let w = weights.get(i);
            let new_dist = d.saturating_add(w);
            state.relax(v, new_dist);
        }
    }
}

/// Forward search from source using UP edges, collecting bucket items.
/// Takes `&WeightArray` so the caller's `cch_weights.up` (which may be
/// `U16` or `U32` per #306 PR 2) passes through transparently — the
/// hot path stays one widening read per edge.
fn forward_fill_buckets_opt(
    topo: &CchTopo,
    weights_up: &crate::formats::WeightArray,
    source_idx: u32,
    source: u32,
    state: &mut SearchState,
    bucket_items: &mut Vec<(u32, u32, u32)>,
) {
    state.start_search();
    state.relax(source, 0);

    while let Some((d, u)) = state.pop() {
        bucket_items.push((u, source_idx, d));

        // Relax UP edges
        let start = topo.up_offsets[u as usize] as usize;
        let end = topo.up_offsets[u as usize + 1] as usize;

        for (slot, &v) in topo.up_targets[start..end].iter().enumerate() {
            let w = weights_up.get(start + slot);
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            state.relax(v, new_dist);
        }
    }
}

/// Backward search from target using flat reverse adjacency, joining with buckets
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn backward_join_opt(
    down_rev_flat: &DownReverseAdjFlat,
    target: u32,
    buckets: &SortedBuckets,
    matrix: &mut [u32],
    n_targets: usize,
    target_idx: usize,
    state: &mut SearchState,
) -> (usize, usize) {
    // (visited, joins)
    state.start_search();
    state.relax(target, 0);

    // #399: hoist mmap-backed slices once (see forward_fill_buckets_flat).
    let dr_offsets = down_rev_flat.offsets.as_slice();
    let dr_sources = down_rev_flat.sources.as_slice();
    let dr_weights = &down_rev_flat.weights;

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, u)) = state.pop() {
        visited += 1;

        // Binary search bucket lookup
        for (source_idx, bucket_dist) in buckets.get(u) {
            let total = bucket_dist.saturating_add(d);
            let cell = source_idx as usize * n_targets + target_idx;
            if total < matrix[cell] {
                matrix[cell] = total;
            }
            joins += 1;
        }

        // Relax reversed DOWN edges using flat adjacency (no edge_idx indirection!)
        let edge_start = dr_offsets[u as usize] as usize;
        let edge_end = dr_offsets[u as usize + 1] as usize;

        for i in edge_start..edge_end {
            let x = dr_sources[i];
            let w = dr_weights.get(i);
            let new_dist = d.saturating_add(w);
            state.relax(x, new_dist);
        }
    }

    (visited, joins)
}

/// Backward search from target using flat reverse adjacency, joining with prefix-sum buckets
/// O(1) bucket lookup instead of O(log n) binary search
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn backward_join_prefix(
    down_rev_flat: &DownReverseAdjFlat,
    target: u32,
    buckets: &PrefixSumBuckets,
    matrix: &mut [u32],
    n_targets: usize,
    target_idx: usize,
    state: &mut SearchState,
) -> (usize, usize) {
    state.start_search();
    state.relax(target, 0);

    // #399: hoist mmap-backed slices once.
    let dr_offsets = down_rev_flat.offsets.as_slice();
    let dr_sources = down_rev_flat.sources.as_slice();
    let dr_weights = &down_rev_flat.weights;

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, u)) = state.pop() {
        visited += 1;

        // O(1) prefix-sum bucket lookup (no binary search)
        let bucket_entries = buckets.get(u);
        for entry in bucket_entries {
            let cell = entry.source_idx as usize * n_targets + target_idx;

            // Bound-aware pruning: skip if current best can't be improved
            let current_best = matrix[cell];
            if current_best <= entry.dist {
                continue; // Already have path at least as good
            }

            let total = entry.dist.saturating_add(d);
            if total < current_best {
                matrix[cell] = total;
            }
            joins += 1;
        }

        // Relax reversed DOWN edges using flat adjacency (no edge_idx indirection!)
        let edge_start = dr_offsets[u as usize] as usize;
        let edge_end = dr_offsets[u as usize + 1] as usize;

        for i in edge_start..edge_end {
            let x = dr_sources[i];
            let w = dr_weights.get(i);
            let new_dist = d.saturating_add(w);
            state.relax(x, new_dist);
        }
    }

    (visited, joins)
}

// =============================================================================
// PARALLEL BUCKET M2M
// =============================================================================

use rayon::prelude::*;

/// Parallel bucket M2M computation
///
/// Dispatches to one of three strategies based on problem size:
///
/// 1. **Sequential fast path** (cells ≤ 100): the small-N corner where
///    rayon thread-dispatch overhead dwarfs routing work. See
///    `SEQUENTIAL_FAST_PATH_CELL_THRESHOLD`.
/// 2. **L3-aware source tiling** (#190): when the bucket working set
///    would blow out shared L3 (`pick_source_tile_size` returns
///    `Some(tile)`), iterate the source dimension in tiles so each
///    backward sweep stays L3-resident. Adds 4× backward sweeps for a
///    10k×10k query but each sweep walks a 4× smaller bucket array
///    out of L3 instead of DRAM, which net-wins on bandwidth-bound
///    machines.
/// 3. **Monolithic parallel** (default for 100 < N×M < L3 threshold):
///    the single-pass forward+backward shape that production tile
///    sizes already hit.
pub fn table_bucket_parallel(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    table_bucket_parallel_bounded(
        n_nodes,
        up_adj_flat,
        down_rev_flat,
        sources,
        targets,
        u32::MAX,
    )
}

/// Time-bounded variant of [`table_bucket_parallel`] (max_minutes, #415).
///
/// Stops every forward and backward sweep once the popped distance exceeds
/// `threshold` (in the metric of `up_adj_flat`/`down_rev_flat` — pass the
/// TIME flats and a seconds threshold). `u32::MAX` = unbounded (identical
/// behaviour to the wrapper above).
///
/// Exactness: for any pair whose shortest distance d(s→t) ≤ `threshold`, the
/// optimal CCH meeting node m* satisfies up(s→m*) ≤ d ≤ threshold and
/// down(m*→t) ≤ d ≤ threshold, so m* survives both bounded sweeps and the
/// join recovers d exactly. Pairs with d(s→t) > `threshold` may be returned
/// as `u32::MAX` or as a non-minimal value > `threshold`; callers MUST treat
/// any cell > `threshold` as out-of-bound (the /table handler nulls them).
pub fn table_bucket_parallel_bounded(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
    threshold: u32,
) -> (Vec<u32>, BucketM2MStats) {
    let n_sources = sources.len();
    let n_targets = targets.len();

    if n_sources == 0 || n_targets == 0 {
        return (
            vec![u32::MAX; n_sources * n_targets],
            BucketM2MStats::default(),
        );
    }

    // Small-N fast path (#129): at low cell counts rayon's thread-dispatch
    // and work-stealing overhead is larger than the actual routing work,
    // so we run sequentially in a thread-cached engine. The crossover
    // sits at ~1024 cells on Belgium — below that, OSRM CH's pure
    // sequential shape wins; above, parallel rayon already beats it.
    if n_sources * n_targets <= SEQUENTIAL_FAST_PATH_CELL_THRESHOLD {
        return SEQUENTIAL_ENGINE.with(|cell| {
            cell.with_or_init(
                || BucketM2MEngine::new(n_nodes),
                |engine| {
                    // Reinitialise on graph swap (e.g. switching data dirs
                    // across calls) or after the compactor freed and we
                    // rebuilt. Version-stamped state keeps calls O(1).
                    if engine.n_nodes != n_nodes {
                        *engine = BucketM2MEngine::new(n_nodes);
                    }
                    engine.compute_flat(up_adj_flat, down_rev_flat, sources, targets, threshold)
                },
            )
        });
    }

    // L3-aware source tiling (#190): for monolithic queries that would
    // blow shared L3, tile the source dimension so each backward sweep's
    // bucket array stays L3-resident. The threshold is data-driven
    // (cache size + per-source bucket fanout estimate).
    let avg_visited = (n_nodes / 400).clamp(500, 20_000);
    if let Some(tile) =
        crate::matrix::tile_geometry::pick_source_tile_size(n_sources, n_targets, avg_visited)
    {
        return table_bucket_parallel_l3_tiled(
            n_nodes,
            up_adj_flat,
            down_rev_flat,
            sources,
            targets,
            tile,
            threshold,
        );
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // ========== PHASE 1: Parallel forward searches from SOURCES ==========
    let forward_start = std::time::Instant::now();

    // Each source produces its own bucket items. Both the `SearchState`
    // and the per-call bucket `Vec` are cached in thread-local storage
    // so rayon workers amortise the ~30 MB scratch allocation across
    // every source they process, instead of paying it per iteration.
    let bucket_chunks: Vec<Vec<(u32, u32, u32)>> = sources
        .par_iter()
        .enumerate()
        .filter_map(|(source_idx, &source)| {
            if source as usize >= n_nodes {
                return None;
            }

            let avg_visited = (n_nodes / 400).clamp(500, 20000);

            FORWARD_STATE.with(|state_cell| {
                FORWARD_BUCKET_ITEMS.with(|items_cell| {
                    state_cell.with_or_init(
                        || SearchState::new(n_nodes, avg_visited),
                        |state| {
                            if state.entries.len() != n_nodes {
                                *state = SearchState::new(n_nodes, avg_visited);
                            }
                            state.dist_threshold = threshold;
                            items_cell.with_or_init(Vec::new, |items| {
                                items.clear();

                                forward_fill_buckets_flat(
                                    up_adj_flat,
                                    source_idx as u32,
                                    source,
                                    state,
                                    items,
                                );

                                // Hand the items out of the thread-local
                                // by swapping with an empty Vec; the next
                                // iteration on this worker rebuilds one.
                                Some(std::mem::take(items))
                            })
                        },
                    )
                })
            })
        })
        .collect();

    // Merge all bucket chunks
    let bucket_items: Vec<(u32, u32, u32)> = bucket_chunks.into_iter().flatten().collect();
    stats.forward_visited = bucket_items.len();
    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

    // ========== PHASE 2: Build prefix-sum buckets (O(1) lookup) ==========
    let sort_start = std::time::Instant::now();
    let mut buckets = PrefixSumBuckets::new(n_nodes);
    buckets.build(&bucket_items);
    stats.bucket_items = buckets.total_items();
    stats.bucket_nodes = buckets.n_nodes_with_buckets();
    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

    // ========== PHASE 3: Parallel backward searches from TARGETS ==========
    let backward_start = std::time::Instant::now();

    // Pre-allocate matrix
    let matrix: Vec<std::sync::atomic::AtomicU32> = (0..n_sources * n_targets)
        .map(|_| std::sync::atomic::AtomicU32::new(u32::MAX))
        .collect();

    // Parallel backward phase - each target can run independently.
    // Reuses a thread-local `SearchState` across every target the rayon
    // worker processes (see `FORWARD_STATE` comment above — same
    // rationale applies here, with an even tighter effect because the
    // backward search on CCH visits a few hundred nodes while the
    // allocation dwarfs that by four orders of magnitude).
    let (total_visited, total_joins): (usize, usize) = targets
        .par_iter()
        .enumerate()
        .filter(|&(_, target)| (*target as usize) < n_nodes)
        .map(|(target_idx, &target)| {
            let avg_visited = (n_nodes / 400).clamp(500, 20000);

            BACKWARD_STATE.with(|state_cell| {
                state_cell.with_or_init(
                    || SearchState::new(n_nodes, avg_visited),
                    |state| {
                        if state.entries.len() != n_nodes {
                            *state = SearchState::new(n_nodes, avg_visited);
                        }
                        state.dist_threshold = threshold;
                        backward_join_parallel_prefix(
                            down_rev_flat,
                            target,
                            &buckets,
                            &matrix,
                            n_targets,
                            target_idx,
                            state,
                        )
                    },
                )
            })
        })
        .reduce(|| (0, 0), |(v1, j1), (v2, j2)| (v1 + v2, j1 + j2));

    stats.backward_visited = total_visited;
    stats.join_operations = total_joins;
    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    // Convert atomic matrix to regular Vec
    let result_matrix: Vec<u32> = matrix.into_iter().map(|a| a.into_inner()).collect();

    (result_matrix, stats)
}

/// L3-aware source-tiled parallel bucket M2M (#190).
///
/// For monolithic queries (e.g. 10k×10k) the single-pass `PrefixSumBuckets`
/// working set is several hundred MB and blows out shared L3 — every
/// backward relax pulls bucket entries from DRAM. We tile the *source*
/// dimension into chunks of `src_tile_size` so each tile's `PrefixSumBuckets`
/// fits the L3 budget chosen by `tile_geometry::pick_source_tile_size`.
///
/// Per tile we still run the same forward+backward parallel shape — within
/// a tile, all rayon workers cooperate on a single set of buckets. Across
/// tiles we iterate sequentially: each tile's result rows are written to
/// disjoint slices of the output, so there's no cross-tile contention and
/// no atomic globals to merge.
///
/// Cost analysis vs. monolithic:
/// - Forward work: identical (`n_sources` searches total either way).
/// - Backward work: `n_tiles × n_targets` searches instead of `n_targets`,
///   but each search walks a `1/n_tiles`-sized bucket array — total joins
///   are unchanged. Memory bandwidth drops by `~n_tiles` because we're
///   now L3-resident on the bucket reads. Net: faster on DRAM-bound
///   workloads.
fn table_bucket_parallel_l3_tiled(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
    src_tile_size: usize,
    threshold: u32,
) -> (Vec<u32>, BucketM2MStats) {
    use std::sync::atomic::AtomicU32;

    let n_sources = sources.len();
    let n_targets = targets.len();

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // Single global result matrix written tile-by-tile (disjoint row slices).
    let result: Vec<AtomicU32> = (0..n_sources * n_targets)
        .map(|_| AtomicU32::new(u32::MAX))
        .collect();

    // Walk source tiles sequentially; within each tile, fan out to rayon.
    let mut src_start = 0usize;
    while src_start < n_sources {
        let src_end = (src_start + src_tile_size).min(n_sources);
        let tile_sources = &sources[src_start..src_end];

        // ===== PHASE 1: forward (parallel) — buckets for THIS tile's sources =====
        let forward_start = std::time::Instant::now();

        let bucket_chunks: Vec<Vec<(u32, u32, u32)>> = tile_sources
            .par_iter()
            .enumerate()
            .filter_map(|(local_src_idx, &source)| {
                if source as usize >= n_nodes {
                    return None;
                }
                let avg_visited = (n_nodes / 400).clamp(500, 20_000);

                FORWARD_STATE.with(|state_cell| {
                    FORWARD_BUCKET_ITEMS.with(|items_cell| {
                        state_cell.with_or_init(
                            || SearchState::new(n_nodes, avg_visited),
                            |state| {
                                if state.entries.len() != n_nodes {
                                    *state = SearchState::new(n_nodes, avg_visited);
                                }
                                state.dist_threshold = threshold;
                                items_cell.with_or_init(Vec::new, |items| {
                                    items.clear();

                                    // NOTE: `local_src_idx` so bucket entries
                                    // reference the position within the tile;
                                    // the output write uses the global row.
                                    forward_fill_buckets_flat(
                                        up_adj_flat,
                                        local_src_idx as u32,
                                        source,
                                        state,
                                        items,
                                    );

                                    Some(std::mem::take(items))
                                })
                            },
                        )
                    })
                })
            })
            .collect();

        let bucket_items: Vec<(u32, u32, u32)> = bucket_chunks.into_iter().flatten().collect();
        stats.forward_visited += bucket_items.len();
        stats.forward_time_ms += forward_start.elapsed().as_millis() as u64;

        // ===== PHASE 2: build buckets for THIS tile (sequential, fast) =====
        let sort_start = std::time::Instant::now();
        let mut buckets = PrefixSumBuckets::new(n_nodes);
        buckets.build(&bucket_items);
        stats.bucket_items += buckets.total_items();
        stats.bucket_nodes += buckets.n_nodes_with_buckets();
        stats.sort_time_ms += sort_start.elapsed().as_millis() as u64;

        // ===== PHASE 3: backward (parallel) over targets, writing this tile's rows =====
        let backward_start = std::time::Instant::now();

        let row_offset = src_start; // Each row's global index = row_offset + local_src_idx
        let result_ref = &result[..];

        let (tile_visited, tile_joins): (usize, usize) = targets
            .par_iter()
            .enumerate()
            .filter(|&(_, target)| (*target as usize) < n_nodes)
            .map(|(target_idx, &target)| {
                let avg_visited = (n_nodes / 400).clamp(500, 20_000);
                BACKWARD_STATE.with(|state_cell| {
                    state_cell.with_or_init(
                        || SearchState::new(n_nodes, avg_visited),
                        |state| {
                            if state.entries.len() != n_nodes {
                                *state = SearchState::new(n_nodes, avg_visited);
                            }
                            state.dist_threshold = threshold;
                            backward_join_tile(
                                down_rev_flat,
                                target,
                                &buckets,
                                result_ref,
                                n_targets,
                                target_idx,
                                row_offset,
                                state,
                            )
                        },
                    )
                })
            })
            .reduce(|| (0, 0), |(v1, j1), (v2, j2)| (v1 + v2, j1 + j2));

        stats.backward_visited += tile_visited;
        stats.join_operations += tile_joins;
        stats.backward_time_ms += backward_start.elapsed().as_millis() as u64;

        // Drop tile's bucket allocations before next tile so we don't pile up.
        drop(buckets);

        src_start = src_end;
    }

    let result_matrix: Vec<u32> = result.into_iter().map(|a| a.into_inner()).collect();
    (result_matrix, stats)
}

/// Backward join used by the L3-tiled path (#190).
///
/// Identical to `backward_join_parallel_prefix` except the result write
/// uses `row_offset + local_src_idx` so we land in the correct global row,
/// AND we issue software-prefetch hints on the random-access result-matrix
/// writes a few iterations ahead. Each `matrix[idx]` cell is at stride
/// `n_targets * 4` bytes from its predecessor (the bucket lists source
/// indices in arbitrary order), so the hardware prefetcher can't see the
/// pattern. A `T0` prefetch issued ~8 iterations ahead overlaps the DRAM
/// fetch with the current iteration's atomic load/store.
#[allow(clippy::too_many_arguments)] // mirrors backward_join_parallel_prefix; splitting would add a struct just for argument grouping
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn backward_join_tile(
    down_rev_flat: &DownReverseAdjFlat,
    target: u32,
    buckets: &PrefixSumBuckets,
    matrix: &[std::sync::atomic::AtomicU32],
    n_targets: usize,
    target_idx: usize,
    row_offset: usize,
    state: &mut SearchState,
) -> (usize, usize) {
    use std::sync::atomic::Ordering;

    state.start_search();
    state.relax(target, 0);

    // #399: hoist mmap-backed slices once.
    let dr_offsets = down_rev_flat.offsets.as_slice();
    let dr_sources = down_rev_flat.sources.as_slice();
    let dr_weights = &down_rev_flat.weights;

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, u)) = state.pop() {
        visited += 1;

        // O(1) prefix-sum bucket lookup using SoA layout.
        let (start, len) = buckets.get_range(u);
        if len > 0 {
            let dists = &buckets.dists[start..start + len];
            let source_indices = &buckets.source_indices[start..start + len];

            for i in 0..len {
                // Software prefetch of the result matrix cell `PF_DIST`
                // iterations ahead. Each write is at stride
                // `n_targets × 4` bytes from the prior write — a perfect
                // cache miss every time without prefetching. `T0` =
                // bring into all cache levels (we'll write to it next).
                if i + PREFETCH_DISTANCE < len {
                    let pf_src_idx = source_indices[i + PREFETCH_DISTANCE] as usize;
                    let pf_idx = (row_offset + pf_src_idx) * n_targets + target_idx;
                    prefetch_matrix_cell(matrix, pf_idx);
                }

                let entry_dist = dists[i];
                let source_idx = source_indices[i];
                // Tile-local source index → global row.
                let idx = (row_offset + source_idx as usize) * n_targets + target_idx;

                // Bound-aware pruning: skip if current best can't be improved.
                let current_best = matrix[idx].load(Ordering::Relaxed);
                if current_best <= entry_dist {
                    continue;
                }

                joins += 1;
                let total_dist = entry_dist.saturating_add(d);
                if total_dist < current_best {
                    matrix[idx].fetch_min(total_dist, Ordering::Relaxed);
                }
            }
        }

        // Relax DOWN-reverse edges. The hardware prefetcher handles the
        // sequential offsets/sources/weights reads, so no software
        // prefetch needed here.
        let edge_start = dr_offsets[u as usize] as usize;
        let edge_end = dr_offsets[u as usize + 1] as usize;

        for i in edge_start..edge_end {
            let x = dr_sources[i];
            let w = dr_weights.get(i);
            let new_dist = d.saturating_add(w);
            state.relax(x, new_dist);
        }
    }

    (visited, joins)
}

/// Distance (in iterations) at which we issue software prefetch hints for
/// random-access result-matrix writes. 8 covers ~1 DRAM round-trip on
/// modern x86_64 (~80–100 ns) given the per-iteration compute cost
/// (~10 ns: load, compare, optional fetch_min). Tuned empirically; the
/// curve is flat in [4..16].
const PREFETCH_DISTANCE: usize = 8;

/// Software-prefetch substitute: a pure-safe-Rust atomic load whose
/// result is fed to `core::hint::black_box`. The load brings the cache
/// line into L1 (`AtomicU32::load(Relaxed)` lowers to `mov` on x86_64
/// and `ldr` on aarch64) and `black_box` prevents the optimizer from
/// hoisting / eliminating it. Net effect on the inner loop: a few cycles
/// of issued load + 80 ns of overlapping DRAM fetch with the current
/// iteration's compute, exactly the win we'd get from `prefetcht0` —
/// without any `unsafe` block.
///
/// This approach was chosen over `core::arch::x86_64::_mm_prefetch`
/// because the project disallows `unsafe` Rust (see CLAUDE.md).
/// `_mm_prefetch` is `unsafe fn` even though it never dereferences,
/// because raw pointer arithmetic is involved at the call site.
#[inline(always)]
fn prefetch_matrix_cell(matrix: &[std::sync::atomic::AtomicU32], idx: usize) {
    if idx >= matrix.len() {
        return;
    }
    // Reading the atomic with `Relaxed` issues exactly one load —
    // identical to what a store would require to make progress, so
    // the cache line is pulled into L1 either way. `black_box`
    // prevents the optimizer from removing the load when LLVM
    // realizes we don't use the result.
    let v = matrix[idx].load(std::sync::atomic::Ordering::Relaxed);
    let _ = std::hint::black_box(v);
}

/// Backward join for parallel execution using PrefixSumBuckets (O(1) lookup)
/// With bound-aware pruning: skip joins where current best <= source distance
/// Uses SoA layout for better cache efficiency
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn backward_join_parallel_prefix(
    down_rev_flat: &DownReverseAdjFlat,
    target: u32,
    buckets: &PrefixSumBuckets,
    matrix: &[std::sync::atomic::AtomicU32],
    n_targets: usize,
    target_idx: usize,
    state: &mut SearchState,
) -> (usize, usize) {
    use std::sync::atomic::Ordering;

    state.start_search();
    state.relax(target, 0);

    // #399: hoist mmap-backed slices once.
    let dr_offsets = down_rev_flat.offsets.as_slice();
    let dr_sources = down_rev_flat.sources.as_slice();
    let dr_weights = &down_rev_flat.weights;

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, u)) = state.pop() {
        visited += 1;

        // O(1) prefix-sum bucket lookup using SoA layout
        let (start, len) = buckets.get_range(u);
        if len > 0 {
            // Access SoA arrays directly for better cache behavior
            let dists = &buckets.dists[start..start + len];
            let source_indices = &buckets.source_indices[start..start + len];

            // Prefetch is only beneficial when the result matrix is
            // larger than the per-thread L3 share — otherwise the
            // prefetched cache line is already hot and the issued
            // load just wastes a load-port slot. Threshold: matrix
            // bigger than 8 MiB (≈ ¼ of dev-host L3 / per-thread
            // share assuming all 20 cores active). Empirical: at
            // 1000×1000 (4 MiB) prefetch costs ~14% with no win;
            // at 5000×5000 (100 MiB) it would help, but #190's
            // dispatcher routes that to the L3-tiled path which
            // has its own prefetch logic.
            let result_bytes = matrix.len().saturating_mul(4);
            let prefetch_enabled = result_bytes >= 8 * 1024 * 1024;

            for i in 0..len {
                // Software-prefetch the matrix cell we'll touch in
                // `PREFETCH_DISTANCE` iterations. See the doc on
                // `prefetch_matrix_cell` for why we use a safe atomic
                // load + `black_box` instead of `_mm_prefetch`.
                if prefetch_enabled && i + PREFETCH_DISTANCE < len {
                    let pf_src_idx = source_indices[i + PREFETCH_DISTANCE] as usize;
                    let pf_idx = pf_src_idx * n_targets + target_idx;
                    prefetch_matrix_cell(matrix, pf_idx);
                }

                let entry_dist = dists[i];
                let source_idx = source_indices[i];
                let idx = source_idx as usize * n_targets + target_idx;

                // Bound-aware pruning: skip if current best can't be improved
                let current_best = matrix[idx].load(Ordering::Relaxed);
                if current_best <= entry_dist {
                    continue;
                }

                joins += 1;
                let total_dist = entry_dist.saturating_add(d);

                if total_dist < current_best {
                    matrix[idx].fetch_min(total_dist, Ordering::Relaxed);
                }
            }
        }

        // Relax DOWN-reverse edges
        let edge_start = dr_offsets[u as usize] as usize;
        let edge_end = dr_offsets[u as usize + 1] as usize;

        for i in edge_start..edge_end {
            let x = dr_sources[i];
            let w = dr_weights.get(i);
            let new_dist = d.saturating_add(w);
            state.relax(x, new_dist);
        }
    }

    (visited, joins)
}

// =============================================================================
// LEGACY API - For compatibility with existing code
// =============================================================================

pub struct BucketArena {
    items: Vec<(u32, u32, u32)>,
}

impl BucketArena {
    pub fn new(_n_nodes: usize, _n_sources: usize, _avg_visited_per_source: usize) -> Self {
        Self { items: Vec::new() }
    }

    #[inline]
    pub fn push(&mut self, node: u32, source_idx: u32, dist: u32) -> bool {
        self.items.push((node, source_idx, dist));
        true
    }

    #[inline]
    pub fn get(&self, _node: u32) -> &[(u32, u32)] {
        &[]
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn total_items(&self) -> usize {
        self.items.len()
    }
}

// =============================================================================
// 2-CHANNEL BUCKET-M2M (#372): time + length-along-time-shortest
// =============================================================================
//
// Mirrors the single-channel implementation above but propagates a second
// per-edge weight (length-along-time) alongside time during the SAME
// forward+backward Dijkstra. The path chosen is the time-shortest one
// (decisions driven entirely by `dist`); `lat` accumulates the distance
// along that path. The output is two matrices: time and lat.
//
// Why a separate fork rather than a generic 2-channel implementation:
//   - The hot path is a u32 heap + relax loop; adding optional second
//     channels via Option<&[u32]> branches in every iteration costs a
//     mispredict per relax that the static fork avoids.
//   - The bucket entries grow from 8 → 12 bytes; PrefixSumBuckets2 keeps
//     the SoA layout but with an extra `lats` array parallel to `dists`.
//   - Backward join chooses meeting node by MIN-TIME (not min-lat) and
//     reads lat at that meeting node — different from a generic
//     two-metric search.

/// Bucket entry (12 bytes) — like `BucketEntry` but carries a second u32
/// `lat` representing the length along the time-shortest path from the
/// source to this bucket's node.
#[derive(Clone, Copy)]
#[repr(C)]
struct Bucket2Entry {
    dist: u32,
    lat: u32,
    source_idx: u32,
}

/// Reusable prefix-sum bucket structure for the 2-channel variant.
/// SoA layout: `dists` + `lats` + `source_indices` as three parallel
/// `Vec<u32>` (12 bytes per entry, better cache utilisation than AoS).
struct PrefixSumBuckets2 {
    counts: Vec<u32>,
    count_stamps: Vec<u32>,
    current_stamp: u32,
    offsets: Vec<u32>,
    /// AoS layout — backward join reads one `Bucket2Entry` per join.
    /// (Tried a parallel SoA layout; the AoS read pattern was faster
    /// because the join touches all three fields per entry anyway.)
    items: Vec<Bucket2Entry>,
    active_nodes: Vec<u32>,
}

impl PrefixSumBuckets2 {
    fn new(n_nodes: usize) -> Self {
        Self {
            counts: vec![0; n_nodes],
            count_stamps: vec![0; n_nodes],
            current_stamp: 0,
            offsets: vec![0; n_nodes + 1],
            items: Vec::new(),
            active_nodes: Vec::new(),
        }
    }

    /// Build from raw items `(node, source_idx, dist, lat)`.
    fn build(&mut self, raw_items: &[(u32, u32, u32, u32)]) {
        self.current_stamp = self.current_stamp.wrapping_add(1);
        if self.current_stamp == 0 {
            self.count_stamps.fill(0);
            self.current_stamp = 1;
        }
        self.active_nodes.clear();

        for &(node, _, _, _) in raw_items {
            let n = node as usize;
            if self.count_stamps[n] != self.current_stamp {
                self.count_stamps[n] = self.current_stamp;
                self.counts[n] = 0;
                self.active_nodes.push(node);
            }
            self.counts[n] += 1;
        }

        let mut total = 0u32;
        for &node in &self.active_nodes {
            let n = node as usize;
            self.offsets[n] = total;
            total += self.counts[n];
        }

        let total_items = total as usize;
        if self.items.len() < total_items {
            self.items.resize(
                total_items,
                Bucket2Entry {
                    dist: 0,
                    lat: 0,
                    source_idx: 0,
                },
            );
        }

        if let Some(last) = self.offsets.last_mut() {
            *last = total;
        }

        // Per-node write cursor — reuse `counts` as cursor by zeroing
        // then re-counting, mirroring `PrefixSumBuckets::build`.
        for &node in &self.active_nodes {
            self.counts[node as usize] = 0;
        }
        for &(node, src_idx, dist, lat) in raw_items {
            let n = node as usize;
            let pos = (self.offsets[n] + self.counts[n]) as usize;
            self.items[pos] = Bucket2Entry {
                dist,
                lat,
                source_idx: src_idx,
            };
            self.counts[n] += 1;
        }
    }

    #[inline]
    fn get(&self, node: u32) -> &[Bucket2Entry] {
        let n = node as usize;
        if self.count_stamps[n] != self.current_stamp {
            return &[];
        }
        let start = self.offsets[n] as usize;
        let end = start + self.counts[n] as usize;
        &self.items[start..end]
    }
}

/// 2-channel search state — parallel to `SearchState` but with a `lats`
/// array storing the length-along-time-shortest accumulator. Reads of
/// `lats[node]` are valid only when `entries[node].version` matches
/// `current_version` (i.e. when `dist` is current).
struct SearchState2 {
    entries: Vec<NodeEntry>,
    lats: Vec<u32>,
    current_version: u32,
    heap: DAryHeap,
    handles: Vec<u32>,
    pushes: usize,
    pops: usize,
    /// Early-stop bound on the ordering metric (time). See
    /// [`SearchState::dist_threshold`]. `u32::MAX` = unbounded.
    dist_threshold: u32,
}

impl SearchState2 {
    fn new(n_nodes: usize, heap_capacity: usize) -> Self {
        Self {
            entries: vec![
                NodeEntry {
                    dist: u32::MAX,
                    version: 0,
                };
                n_nodes
            ],
            lats: vec![u32::MAX; n_nodes],
            current_version: 0,
            heap: DAryHeap::new(heap_capacity),
            handles: vec![INVALID_HANDLE; n_nodes],
            pushes: 0,
            pops: 0,
            dist_threshold: u32::MAX,
        }
    }

    #[inline]
    fn start_search(&mut self) {
        self.current_version = self.current_version.wrapping_add(1);
        if self.current_version == 0 {
            for e in &mut self.entries {
                e.dist = u32::MAX;
                e.version = 0;
            }
            for h in &mut self.handles {
                *h = INVALID_HANDLE;
            }
            self.current_version = 1;
        }
        self.heap.clear();
    }

    /// Relax with both `dist` (drives ordering) and `lat` (accumulates
    /// alongside). When `dist` improves, both fields are updated. Returns
    /// `true` if anything changed.
    #[inline]
    fn relax(&mut self, node: u32, dist: u32, lat: u32) -> bool {
        let e = &mut self.entries[node as usize];

        if e.version == self.current_version {
            if dist < e.dist {
                e.dist = dist;
                self.lats[node as usize] = lat;
                let handle = self.handles[node as usize];
                if handle != INVALID_HANDLE && (handle as usize) < self.heap.size() {
                    self.heap.decrease(handle, dist, node, &mut self.handles);
                    self.pushes += 1;
                }
                return true;
            }
            return false;
        }

        self.handles[node as usize] = INVALID_HANDLE;
        e.dist = dist;
        e.version = self.current_version;
        self.lats[node as usize] = lat;
        self.heap.push(dist, node, &mut self.handles);
        self.pushes += 1;
        true
    }

    /// Returns `(dist, lat, node)` for the next-min-dist node.
    #[inline]
    fn pop(&mut self) -> Option<(u32, u32, u32)> {
        if let Some((dist, node)) = self.heap.pop(&mut self.handles) {
            // Bounded early-stop on the time metric (`dist`). See
            // `SearchState::pop`.
            if dist > self.dist_threshold {
                return None;
            }
            self.pops += 1;
            self.handles[node as usize] = INVALID_HANDLE;
            let lat = self.lats[node as usize];
            return Some((dist, lat, node));
        }
        None
    }
}

/// Forward search using flat UP adjacency for BOTH metrics. `up_adj_flat`
/// and `up_adj_flat_len_along_time` share the CCH topology — index `i` addresses
/// the same edge in both. Reads time from the first, lat from the second.
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn forward_fill_buckets_flat_len_along_time(
    up_adj_flat: &UpAdjFlat,
    up_adj_flat_len_along_time: &UpAdjFlat,
    source_idx: u32,
    source: u32,
    state: &mut SearchState2,
    bucket_items: &mut Vec<(u32, u32, u32, u32)>,
) {
    state.start_search();
    state.relax(source, 0, 0);

    // #399: hoist mmap-backed slices once.
    let offsets = up_adj_flat.offsets.as_slice();
    let targets = up_adj_flat.targets.as_slice();
    let w_time_arr = &up_adj_flat.weights;
    let w_lat_arr = &up_adj_flat_len_along_time.weights;

    while let Some((d, l, u)) = state.pop() {
        bucket_items.push((u, source_idx, d, l));

        let start = offsets[u as usize] as usize;
        let end = offsets[u as usize + 1] as usize;
        for i in start..end {
            let v = targets[i];
            let w_time = w_time_arr.get(i);
            let w_lat = w_lat_arr.get(i);
            let new_dist = d.saturating_add(w_time);
            let new_lat = l.saturating_add(w_lat);
            state.relax(v, new_dist, new_lat);
        }
    }
}

/// Backward search from target — joins buckets at the meeting node and
/// writes BOTH the time-min and the lat-at-time-min into the output
/// matrices. The meeting node is chosen by min-TIME; lat is reported at
/// that node, not separately optimised.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn backward_join_prefix_len_along_time(
    down_rev_flat: &DownReverseAdjFlat,
    down_rev_flat_len_along_time: &DownReverseAdjFlat,
    target: u32,
    buckets: &PrefixSumBuckets2,
    time_matrix: &mut [u32],
    lat_matrix: &mut [u32],
    n_targets: usize,
    target_idx: usize,
    state: &mut SearchState2,
) -> (usize, usize) {
    state.start_search();
    state.relax(target, 0, 0);

    // #399: hoist mmap-backed slices once.
    let dr_offsets = down_rev_flat.offsets.as_slice();
    let dr_sources = down_rev_flat.sources.as_slice();
    let dr_w_time = &down_rev_flat.weights;
    let dr_w_lat = &down_rev_flat_len_along_time.weights;

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, l, u)) = state.pop() {
        visited += 1;

        let bucket_entries = buckets.get(u);
        for entry in bucket_entries {
            let cell = entry.source_idx as usize * n_targets + target_idx;

            let current_best_time = time_matrix[cell];
            // STRICT bound-prune (matches #385 parallel-path fix): at
            // equality `cur_time == entry.dist + 0` the per-edge (time,
            // lat) lex tie-break below can still improve the cell by
            // lowering lat without changing time. Using `<` (not `<=`)
            // keeps lex semantics consistent with the parallel solver.
            if current_best_time < entry.dist {
                continue;
            }
            let total_time = entry.dist.saturating_add(d);
            if total_time > current_best_time {
                continue;
            }
            let total_lat = entry.lat.saturating_add(l);
            if total_time < current_best_time || total_lat < lat_matrix[cell] {
                time_matrix[cell] = total_time;
                lat_matrix[cell] = total_lat;
                joins += 1;
            }
        }

        let edge_start = dr_offsets[u as usize] as usize;
        let edge_end = dr_offsets[u as usize + 1] as usize;
        for i in edge_start..edge_end {
            let x = dr_sources[i];
            let w_time = dr_w_time.get(i);
            let w_lat = dr_w_lat.get(i);
            let new_dist = d.saturating_add(w_time);
            let new_lat = l.saturating_add(w_lat);
            state.relax(x, new_dist, new_lat);
        }
    }

    (visited, joins)
}

/// 2-channel bucket-M2M (#372). Mirrors `table_bucket_full_flat` but
/// returns two matrices: time and length-along-time-shortest. The path
/// chosen at every cell is the time-shortest one; the lat number
/// belongs to that same path (no separate distance-shortest search).
///
/// `up_adj_flat` / `down_rev_flat` carry time weights; `_lat` variants
/// carry length-along-time weights with the SAME topology (built by
/// `state.rs` via `UpAdjFlat::build(&cch_topo, &cch_weights_len_along_time)`).
#[allow(clippy::too_many_arguments)]
pub fn table_bucket_full_flat_len_along_time(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    up_adj_flat_len_along_time: &UpAdjFlat,
    down_rev_flat_len_along_time: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
    threshold: u32,
) -> (Vec<u32>, Vec<u32>, BucketM2MStats) {
    let n_sources = sources.len();
    let n_targets = targets.len();

    let mut time_matrix = vec![u32::MAX; n_sources * n_targets];
    let mut lat_matrix = vec![u32::MAX; n_sources * n_targets];

    if n_sources == 0 || n_targets == 0 {
        return (time_matrix, lat_matrix, BucketM2MStats::default());
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    let avg_visited = (n_nodes / 400).clamp(500, 20000);

    // #395: thread-local SearchState2 + PrefixSumBuckets2 + bucket_items
    // pool — avoid ~80 MB malloc per call on the small-N fast path.
    // Re-initialise when n_nodes changes (operator switched regions
    // mid-process). `start_search()` resets via generation stamp.
    SEQ_STATE_LAT.with(|state_cell| {
        SEQ_BUCKETS_LAT.with(|buckets_cell| {
            SEQ_BUCKET_ITEMS_LAT.with(|items_cell| {
                state_cell.with_or_init(
                    || SearchState2::new(n_nodes, avg_visited),
                    |state| {
                        if state.entries.len() != n_nodes {
                            *state = SearchState2::new(n_nodes, avg_visited);
                        }
                        // Reset cumulative perf counters per call.
                        state.pushes = 0;
                        state.pops = 0;
                        state.dist_threshold = threshold;
                        buckets_cell.with_or_init(
                            || PrefixSumBuckets2::new(n_nodes),
                            |buckets| {
                                if buckets.counts.len() != n_nodes {
                                    *buckets = PrefixSumBuckets2::new(n_nodes);
                                }
                                items_cell.with_or_init(Vec::new, |bucket_items| {
                                    bucket_items.clear();
                                    bucket_items.reserve(n_sources * avg_visited);

                                    let forward_start = std::time::Instant::now();
                                    for (source_idx, &source) in sources.iter().enumerate() {
                                        if source as usize >= n_nodes {
                                            continue;
                                        }
                                        forward_fill_buckets_flat_len_along_time(
                                            up_adj_flat,
                                            up_adj_flat_len_along_time,
                                            source_idx as u32,
                                            source,
                                            state,
                                            bucket_items,
                                        );
                                    }
                                    stats.forward_visited = bucket_items.len();
                                    stats.forward_time_ms =
                                        forward_start.elapsed().as_millis() as u64;

                                    let sort_start = std::time::Instant::now();
                                    buckets.build(bucket_items);
                                    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

                                    let backward_start = std::time::Instant::now();
                                    for (target_idx, &target) in targets.iter().enumerate() {
                                        if target as usize >= n_nodes {
                                            continue;
                                        }
                                        let (visited, joins) = backward_join_prefix_len_along_time(
                                            down_rev_flat,
                                            down_rev_flat_len_along_time,
                                            target,
                                            buckets,
                                            &mut time_matrix,
                                            &mut lat_matrix,
                                            n_targets,
                                            target_idx,
                                            state,
                                        );
                                        stats.backward_visited += visited;
                                        stats.join_operations += joins;
                                    }
                                    stats.backward_time_ms =
                                        backward_start.elapsed().as_millis() as u64;

                                    stats.heap_pushes = state.pushes;
                                    stats.heap_pops = state.pops;
                                })
                            },
                        )
                    },
                )
            });
        });
    });

    (time_matrix, lat_matrix, stats)
}

/// Parallel 2-channel backward join. Writes into a single
/// `AtomicU64` matrix where each cell packs `(time << 32) | lat` —
/// `fetch_min` on this packed value keeps time and lat in lock-step
/// (smaller u64 = smaller time wins, and the lat tagged onto the
/// winning value rides along automatically).
#[allow(clippy::too_many_arguments)]
/// 2-channel backward join — target-owned local columns. Each target
/// writes its own row of (time, lat) into thread-local `time_col` and
/// `lat_col` `Vec<u32>` of size `n_sources`. No atomics needed because
/// every cell `(src, target)` is touched by exactly ONE target's
/// backward run. Replaces the AtomicU64 packed matrix at codex's
/// suggestion — eliminates the locked RMW that the bound-pruned CAS
/// loop still incurred on every successful update.
#[allow(clippy::needless_range_loop)] // weights.get(i) needs the index
fn backward_join_local_columns_len_along_time(
    down_rev_flat: &DownReverseAdjFlat,
    down_rev_flat_len_along_time: &DownReverseAdjFlat,
    target: u32,
    buckets: &PrefixSumBuckets2,
    time_col: &mut [u32],
    lat_col: &mut [u32],
    state: &mut SearchState2,
) -> (usize, usize) {
    state.start_search();
    state.relax(target, 0, 0);

    // #399: hoist mmap-backed slices once.
    let dr_offsets = down_rev_flat.offsets.as_slice();
    let dr_sources = down_rev_flat.sources.as_slice();
    let dr_w_time = &down_rev_flat.weights;
    let dr_w_lat = &down_rev_flat_len_along_time.weights;

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, l, u)) = state.pop() {
        visited += 1;

        let entries = buckets.get(u);
        for entry in entries {
            let src = entry.source_idx as usize;
            // Bound-aware pruning: skip only when the bucket's dist
            // ALONE is STRICTLY worse than the current best time. At
            // equality the per-edge (time, lat) lex tie-break below
            // can still improve the row by lowering lat without
            // changing time, so we cannot prune ties here.
            let cur_time = time_col[src];
            if cur_time < entry.dist {
                continue;
            }
            let total_time = entry.dist.saturating_add(d);
            if total_time > cur_time {
                continue;
            }
            let total_lat = entry.lat.saturating_add(l);
            // Lexicographic (time, lat) tie-break — matches the prior
            // packed-AtomicU64 `fetch_min` semantics: on equal time,
            // smaller lat wins. Keeps /table output deterministic
            // across runs and consistent with the pre-target-owned
            // implementation.
            if total_time < cur_time || total_lat < lat_col[src] {
                time_col[src] = total_time;
                lat_col[src] = total_lat;
                joins += 1;
            }
        }

        let edge_start = dr_offsets[u as usize] as usize;
        let edge_end = dr_offsets[u as usize + 1] as usize;
        for i in edge_start..edge_end {
            let x = dr_sources[i];
            let w_time = dr_w_time.get(i);
            let w_lat = dr_w_lat.get(i);
            let new_dist = d.saturating_add(w_time);
            let new_lat = l.saturating_add(w_lat);
            state.relax(x, new_dist, new_lat);
        }
    }

    (visited, joins)
}

/// Parallel 2-channel bucket-M2M. Mirrors `table_bucket_parallel` but
/// propagates length-along-time alongside time using thread-local
/// `SearchState2`. Backward phase writes into a packed `AtomicU64`
/// matrix so updates to (time, lat) stay atomic without per-cell
/// mutexes.
///
/// For small matrices the cost of forking rayon is higher than the
/// routing work itself; the caller dispatches to the sequential
/// `table_bucket_full_flat_len_along_time` below ~1024 cells.
#[allow(clippy::too_many_arguments)]
pub fn table_bucket_parallel_len_along_time(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    up_adj_flat_len_along_time: &UpAdjFlat,
    down_rev_flat_len_along_time: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, Vec<u32>, BucketM2MStats) {
    table_bucket_parallel_len_along_time_bounded(
        n_nodes,
        up_adj_flat,
        down_rev_flat,
        up_adj_flat_len_along_time,
        down_rev_flat_len_along_time,
        sources,
        targets,
        u32::MAX,
    )
}

/// Time-bounded 2-channel bucket-M2M (max_minutes, #415). `threshold` bounds
/// the TIME channel (the ordering metric); the length-along-time channel is
/// carried unbounded alongside. `u32::MAX` = unbounded. See
/// [`table_bucket_parallel_bounded`] for the exactness argument — it applies
/// identically here because the heap orders on time. The returned `time`
/// matrix is `u32::MAX` (or a value > threshold) for out-of-bound pairs, and
/// the paired `lat` cell for those pairs is meaningless — the /table handler
/// nulls both whenever the time cell exceeds the bound.
#[allow(clippy::too_many_arguments)]
pub fn table_bucket_parallel_len_along_time_bounded(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    up_adj_flat_len_along_time: &UpAdjFlat,
    down_rev_flat_len_along_time: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
    threshold: u32,
) -> (Vec<u32>, Vec<u32>, BucketM2MStats) {
    let n_sources = sources.len();
    let n_targets = targets.len();

    if n_sources == 0 || n_targets == 0 {
        return (
            vec![u32::MAX; n_sources * n_targets],
            vec![u32::MAX; n_sources * n_targets],
            BucketM2MStats::default(),
        );
    }

    // #395 small-N fast path: at low cell counts rayon fork/work-steal
    // overhead dwarfs the per-source Dijkstra work AND each rayon
    // worker takes a `RefCell::borrow_mut` on the thread-local state
    // per task. Below `SEQUENTIAL_FAST_PATH_CELL_THRESHOLD` we delegate
    // to the pooled sequential 2-channel engine — same Dijkstra work,
    // none of the dispatch overhead. Pool keeps the ~80 MB
    // SearchState2 hot across calls.
    if n_sources * n_targets <= SEQUENTIAL_FAST_PATH_CELL_THRESHOLD {
        return table_bucket_full_flat_len_along_time(
            n_nodes,
            up_adj_flat,
            down_rev_flat,
            up_adj_flat_len_along_time,
            down_rev_flat_len_along_time,
            sources,
            targets,
            threshold,
        );
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // ---- Forward phase: parallel per-source ----
    let forward_start = std::time::Instant::now();
    let bucket_chunks: Vec<Vec<(u32, u32, u32, u32)>> = sources
        .par_iter()
        .enumerate()
        .filter_map(|(source_idx, &source)| {
            if source as usize >= n_nodes {
                return None;
            }
            let avg_visited = (n_nodes / 400).clamp(500, 20_000);
            FORWARD_STATE_LAT.with(|state_cell| {
                FORWARD_BUCKET_ITEMS_LAT.with(|items_cell| {
                    state_cell.with_or_init(
                        || SearchState2::new(n_nodes, avg_visited),
                        |state| {
                            if state.entries.len() != n_nodes {
                                *state = SearchState2::new(n_nodes, avg_visited);
                            }
                            state.dist_threshold = threshold;
                            items_cell.with_or_init(Vec::new, |items| {
                                items.clear();
                                forward_fill_buckets_flat_len_along_time(
                                    up_adj_flat,
                                    up_adj_flat_len_along_time,
                                    source_idx as u32,
                                    source,
                                    state,
                                    items,
                                );
                                Some(std::mem::take(items))
                            })
                        },
                    )
                })
            })
        })
        .collect();
    let bucket_items: Vec<(u32, u32, u32, u32)> = bucket_chunks.into_iter().flatten().collect();
    stats.forward_visited = bucket_items.len();
    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

    // ---- Bucket build ----
    let sort_start = std::time::Instant::now();
    let mut buckets = PrefixSumBuckets2::new(n_nodes);
    buckets.build(&bucket_items);
    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

    // ---- Backward phase: target-owned local columns (no atomics) ----
    //
    // Each target's backward Dijkstra writes its own (n_sources)-sized
    // time + lat columns. Every cell (src, tgt) is touched by exactly
    // one target's backward run, so no cross-thread synchronisation
    // on writes is needed — plain branch+store on a local Vec<u32>
    // instead of locked CAS on a shared AtomicU64.
    //
    // After parallel collection we gather the columns into the final
    // row-major matrices.
    let backward_start = std::time::Instant::now();

    struct ColumnResult {
        target_idx: usize,
        time_col: Vec<u32>,
        lat_col: Vec<u32>,
        visited: usize,
        joins: usize,
    }
    let columns: Vec<ColumnResult> = targets
        .par_iter()
        .enumerate()
        .filter(|&(_, target)| (*target as usize) < n_nodes)
        .map(|(target_idx, &target)| {
            let avg_visited = (n_nodes / 400).clamp(500, 20_000);
            BACKWARD_STATE_LAT.with(|state_cell| {
                state_cell.with_or_init(
                    || SearchState2::new(n_nodes, avg_visited),
                    |state| {
                        if state.entries.len() != n_nodes {
                            *state = SearchState2::new(n_nodes, avg_visited);
                        }
                        state.dist_threshold = threshold;
                        let mut time_col = vec![u32::MAX; n_sources];
                        let mut lat_col = vec![u32::MAX; n_sources];
                        let (visited, joins) = backward_join_local_columns_len_along_time(
                            down_rev_flat,
                            down_rev_flat_len_along_time,
                            target,
                            &buckets,
                            &mut time_col,
                            &mut lat_col,
                            state,
                        );
                        ColumnResult {
                            target_idx,
                            time_col,
                            lat_col,
                            visited,
                            joins,
                        }
                    },
                )
            })
        })
        .collect();

    let mut total_visited = 0usize;
    let mut total_joins = 0usize;
    for c in &columns {
        total_visited += c.visited;
        total_joins += c.joins;
    }
    stats.backward_visited = total_visited;
    stats.join_operations = total_joins;
    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    // ---- Gather columns into row-major matrices ----
    //
    // Source-major outer loop, column inner loop: each iteration writes
    // a contiguous run of n_targets cells (one full row), so the row-
    // major output is touched in stride-1 order. This is significantly
    // friendlier on L1/L2 than the prior target-major iteration which
    // wrote each cell at stride `n_targets`.
    let mut time_matrix = vec![u32::MAX; n_sources * n_targets];
    let mut lat_matrix = vec![u32::MAX; n_sources * n_targets];
    for src in 0..n_sources {
        let row_off = src * n_targets;
        for c in &columns {
            time_matrix[row_off + c.target_idx] = c.time_col[src];
            lat_matrix[row_off + c.target_idx] = c.lat_col[src];
        }
    }

    (time_matrix, lat_matrix, stats)
}

#[cfg(test)]
mod step_a_tests {
    use super::*;
    use crate::formats::BitsetField;
    #[allow(unused_imports)]
    use crate::formats::CchTopo;
    use std::borrow::Cow;

    /// Build a small synthetic CCH with mixed original + shortcut edges
    /// and one INF entry, verify flat middles match the topo middles for
    /// every finite edge and that the topo_edge_idx back-reference is
    /// consistent.
    ///
    /// Topology: 4 nodes (rank 0..4).
    ///   UP edges:
    ///     0→2 (idx 0, original, w=10)
    ///     1→2 (idx 1, original, w=3)
    ///     2→3 (idx 2, shortcut via mid=2,  w=7)
    ///     2→3 (idx 3, INF — filtered out)
    ///   DOWN edges:
    ///     2→0 (idx 0, original, w=10)
    ///     2→1 (idx 1, original, w=3)
    ///     3→2 (idx 2, shortcut via mid=2,  w=7)
    ///     3→2 (idx 3, INF — filtered out)
    fn make_cch() -> (CchTopo, CchWeights) {
        let n_nodes = 4u32;
        let up_offsets: Vec<u64> = vec![0, 1, 2, 4, 4];
        let up_targets: Vec<u32> = vec![2, 2, 3, 3];
        let up_is_shortcut_bools = vec![false, false, true, true];
        let up_middle: Vec<u32> = vec![u32::MAX, u32::MAX, 2, 2];

        let down_offsets: Vec<u64> = vec![0, 0, 0, 2, 4];
        let down_targets: Vec<u32> = vec![0, 1, 2, 2];
        let down_is_shortcut_bools = vec![false, false, true, true];
        let down_middle: Vec<u32> = vec![u32::MAX, u32::MAX, 2, 2];

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 2,
            n_original_arcs: 2,
            inputs_sha: [0u8; 32],
            up_offsets: crate::formats::ArcCow::from_vec(up_offsets),
            up_targets: crate::formats::ArcCow::from_vec(up_targets),
            up_is_shortcut: BitsetField::from_bools(&up_is_shortcut_bools),
            up_middle: crate::formats::cch_weights::WeightArray::from_vec_u32(up_middle),
            down_offsets: crate::formats::ArcCow::from_vec(down_offsets),
            down_targets: crate::formats::ArcCow::from_vec(down_targets),
            down_is_shortcut: BitsetField::from_bools(&down_is_shortcut_bools),
            down_middle: crate::formats::cch_weights::WeightArray::from_vec_u32(down_middle),
            rank_to_filtered: crate::formats::ArcCow::from_vec(vec![0u32, 1, 2, 3]),
        };

        let weights = CchWeights {
            up: vec![10u32, 3, 7, u32::MAX].into(),
            down: vec![10u32, 3, 7, u32::MAX].into(),
            // empty relaxed middles → fall back to topo middles
            up_middle: vec![].into(),
            down_middle: vec![].into(),
        };

        (topo, weights)
    }

    #[test]
    fn up_adj_flat_with_topo_idx_back_reference() {
        let (topo, w) = make_cch();
        let flat = UpAdjFlat::build_with(&topo, &w, true);

        // 3 finite UP edges (idx 0,1,2 — idx 3 is INF and filtered)
        assert_eq!(flat.weights.len(), 3);
        assert_eq!(flat.topo_edge_idx.len(), 3);

        for (slot, &topo_idx) in flat.topo_edge_idx.iter().enumerate() {
            let i = topo_idx as usize;
            assert_eq!(flat.targets[slot], topo.up_targets[i]);
            assert_eq!(flat.weights.get(slot), w.up.get(i));
        }
    }

    #[test]
    fn up_adj_flat_default_skips_topo_idx() {
        let (topo, w) = make_cch();
        let flat = UpAdjFlat::build(&topo, &w);
        assert_eq!(flat.weights.len(), 3);
        assert!(
            flat.topo_edge_idx.is_empty(),
            "default build skips topo back-ref"
        );
    }

    #[test]
    fn down_adj_flat_targets_and_weights_filtered() {
        let (topo, w) = make_cch();
        let flat = DownAdjFlat::build(&topo, &w);

        // 3 finite DOWN edges (idx 0,1,2; idx 3 is INF)
        assert_eq!(flat.weights.len(), 3);

        // Walk the flat: each edge's target should appear with its
        // (non-INF) weight at some slot under the right source.
        for source in 0..topo.n_nodes as usize {
            let s_off = topo.down_offsets[source] as usize;
            let s_end = topo.down_offsets[source + 1] as usize;
            for i in s_off..s_end {
                let w_topo = w.down.get(i);
                if w_topo == u32::MAX {
                    continue;
                }
                let target = topo.down_targets[i];
                let f_off = flat.offsets[source] as usize;
                let f_end = flat.offsets[source + 1] as usize;
                let found = (f_off..f_end)
                    .any(|slot| flat.targets[slot] == target && flat.weights.get(slot) == w_topo);
                assert!(
                    found,
                    "edge {source}->{target} w={w_topo} missing in DownAdjFlat"
                );
            }
        }
    }

    #[test]
    fn down_rev_adj_flat_with_topo_idx() {
        let (topo, w) = make_cch();
        let flat = DownReverseAdjFlat::build_with(&topo, &w, true);

        assert_eq!(flat.weights.len(), 3);
        assert_eq!(flat.topo_edge_idx.len(), 3);

        for (slot, &topo_idx) in flat.topo_edge_idx.iter().enumerate() {
            let i = topo_idx as usize;
            assert_eq!(flat.weights.get(slot), w.down.get(i));
        }
    }

    #[test]
    fn down_rev_adj_flat_default_skips_topo_idx() {
        let (topo, w) = make_cch();
        let flat = DownReverseAdjFlat::build(&topo, &w);
        assert_eq!(flat.weights.len(), 3);
        assert!(flat.topo_edge_idx.is_empty());
    }

    // Suppress unused-import warning for `Cow` if no test uses it.
    #[test]
    fn _cow_alias_used() {
        let _: Cow<'static, [u32]> = Cow::Owned(vec![]);
    }

    // ----- #150 file format tests --------------------------------------

    /// Leak a buffer to `&'static [u8]` and align its start to 8 bytes
    /// so `read_from_bytes` can `bytemuck::cast_slice::<u64>` cleanly.
    /// The container writer guarantees this alignment in production; we
    /// reproduce it manually here because `Vec<u8>` has only 1-byte
    /// alignment.
    fn leak_aligned(bytes: Vec<u8>) -> &'static [u8] {
        // Allocate `Vec<u64>` (8-byte aligned) of the right capacity and
        // copy bytes into it, then reinterpret as &[u8].
        let n_u64 = bytes.len().div_ceil(8);
        let mut buf: Vec<u64> = vec![0u64; n_u64];
        // SAFETY: bytemuck::cast_slice_mut on a u64 vec gives a u8 view
        // that is exactly `n_u64 * 8` bytes long (>= bytes.len()).
        let view: &mut [u8] = bytemuck::cast_slice_mut(&mut buf[..]);
        view[..bytes.len()].copy_from_slice(&bytes);
        let leaked: &'static [u64] = Box::leak(buf.into_boxed_slice());
        let raw: &'static [u8] = bytemuck::cast_slice(leaked);
        // Trim to exactly the encoded length.
        &raw[..bytes.len()]
    }

    #[test]
    fn up_adj_flat_file_roundtrip_with_topo_idx() {
        let (topo, w) = make_cch();
        let flat = UpAdjFlat::build_with(&topo, &w, true);
        let encoded = UpAdjFlatFile::encode(&flat);
        let leaked = leak_aligned(encoded);
        let decoded = UpAdjFlatFile::read_from_bytes(leaked).expect("decode round-trip");
        assert_eq!(&*decoded.offsets, &*flat.offsets);
        assert_eq!(&*decoded.targets, &*flat.targets);
        assert_eq!(
            decoded.weights.iter().collect::<Vec<u32>>(),
            flat.weights.iter().collect::<Vec<u32>>()
        );
        assert_eq!(&*decoded.topo_edge_idx, &*flat.topo_edge_idx);
    }

    #[test]
    fn up_adj_flat_file_roundtrip_no_topo_idx() {
        let (topo, w) = make_cch();
        let flat = UpAdjFlat::build(&topo, &w);
        let encoded = UpAdjFlatFile::encode(&flat);
        let leaked = leak_aligned(encoded);
        let decoded = UpAdjFlatFile::read_from_bytes(leaked).expect("decode");
        assert_eq!(&*decoded.offsets, &*flat.offsets);
        assert_eq!(&*decoded.targets, &*flat.targets);
        assert_eq!(
            decoded.weights.iter().collect::<Vec<u32>>(),
            flat.weights.iter().collect::<Vec<u32>>()
        );
        assert!(decoded.topo_edge_idx.is_empty());
    }

    #[test]
    fn down_adj_flat_file_roundtrip() {
        let (topo, w) = make_cch();
        let flat = DownAdjFlat::build(&topo, &w);
        let encoded = DownAdjFlatFile::encode(&flat);
        let leaked = leak_aligned(encoded);
        let decoded = DownAdjFlatFile::read_from_bytes(leaked).expect("decode");
        assert_eq!(&*decoded.offsets, &*flat.offsets);
        assert_eq!(&*decoded.targets, &*flat.targets);
        assert_eq!(
            decoded.weights.iter().collect::<Vec<u32>>(),
            flat.weights.iter().collect::<Vec<u32>>()
        );
    }

    #[test]
    fn down_rev_adj_flat_file_roundtrip_with_topo_idx() {
        let (topo, w) = make_cch();
        let flat = DownReverseAdjFlat::build_with(&topo, &w, true);
        let encoded = DownReverseAdjFlatFile::encode(&flat);
        let leaked = leak_aligned(encoded);
        let decoded = DownReverseAdjFlatFile::read_from_bytes(leaked).expect("decode");
        assert_eq!(&*decoded.offsets, &*flat.offsets);
        assert_eq!(&*decoded.sources, &*flat.sources);
        assert_eq!(
            decoded.weights.iter().collect::<Vec<u32>>(),
            flat.weights.iter().collect::<Vec<u32>>()
        );
        assert_eq!(&*decoded.topo_edge_idx, &*flat.topo_edge_idx);
    }

    #[test]
    fn up_adj_flat_file_detects_corruption() {
        let (topo, w) = make_cch();
        let flat = UpAdjFlat::build(&topo, &w);
        let mut encoded = UpAdjFlatFile::encode(&flat);
        // Flip a byte in the body region.
        let body_off = ADJ_FLAT_HEADER_SIZE + 8; // somewhere in offsets array
        encoded[body_off] ^= 0xFF;
        let leaked = leak_aligned(encoded);
        let res = UpAdjFlatFile::read_from_bytes(leaked);
        assert!(res.is_err(), "corruption should fail CRC check");
        let msg = res.err().expect("expected error").to_string();
        assert!(msg.contains("CRC mismatch"), "unexpected error: {}", msg);
    }

    #[test]
    fn up_adj_flat_file_detects_misalignment() {
        let (topo, w) = make_cch();
        let flat = UpAdjFlat::build(&topo, &w);
        let encoded = UpAdjFlatFile::encode(&flat);
        // Build a 1-byte misaligned static slice by leaking a buffer with
        // a leading byte then offsetting into it.
        let mut padded = vec![0u64; encoded.len().div_ceil(8) + 1];
        let view: &mut [u8] = bytemuck::cast_slice_mut(&mut padded[..]);
        view[1..1 + encoded.len()].copy_from_slice(&encoded);
        let leaked: &'static [u64] = Box::leak(padded.into_boxed_slice());
        let raw: &'static [u8] = bytemuck::cast_slice(leaked);
        let misaligned: &'static [u8] = &raw[1..1 + encoded.len()];
        let res = UpAdjFlatFile::read_from_bytes(misaligned);
        assert!(res.is_err(), "misaligned input must be rejected");
        let msg = res.err().expect("expected error").to_string();
        assert!(
            msg.contains("not 8-byte aligned"),
            "unexpected error: {}",
            msg
        );
    }

    /// L3-tiled path (#190) must produce identical results to the
    /// monolithic parallel path. We force the tiled path with
    /// `src_tile_size=1` (each source becomes its own tile) on a small
    /// synthetic CCH and assert byte-for-byte parity with `table_bucket`.
    ///
    /// Uses a slightly bigger 6-node graph so we have multiple sources
    /// and targets and the join phase actually exercises the bucket
    /// machinery.
    fn make_cch_6() -> (CchTopo, CchWeights) {
        // 6 nodes (rank 0..5).
        // UP edges (mostly toward higher rank, simulating a CH):
        //   0→3 w=10, 0→4 w=20
        //   1→3 w=5,  1→5 w=15
        //   2→4 w=8,  2→5 w=12
        //   3→5 w=4
        //   4→5 w=3
        let n_nodes = 6u32;
        let up_offsets: Vec<u64> = vec![0, 2, 4, 6, 7, 8, 8];
        let up_targets: Vec<u32> = vec![3, 4, 3, 5, 4, 5, 5, 5];
        let up_is_shortcut_bools = vec![false; 8];
        let up_middle: Vec<u32> = vec![u32::MAX; 8];
        let up_w: Vec<u32> = vec![10, 20, 5, 15, 8, 12, 4, 3];

        // DOWN edges (reverse of UP):
        //   3→0 w=10, 3→1 w=5
        //   4→0 w=20, 4→2 w=8
        //   5→1 w=15, 5→2 w=12, 5→3 w=4, 5→4 w=3
        let down_offsets: Vec<u64> = vec![0, 0, 0, 0, 2, 4, 8];
        let down_targets: Vec<u32> = vec![0, 1, 0, 2, 1, 2, 3, 4];
        let down_is_shortcut_bools = vec![false; 8];
        let down_middle: Vec<u32> = vec![u32::MAX; 8];
        let down_w: Vec<u32> = vec![10, 5, 20, 8, 15, 12, 4, 3];

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: 8,
            inputs_sha: [0u8; 32],
            up_offsets: crate::formats::ArcCow::from_vec(up_offsets),
            up_targets: crate::formats::ArcCow::from_vec(up_targets),
            up_is_shortcut: BitsetField::from_bools(&up_is_shortcut_bools),
            up_middle: crate::formats::cch_weights::WeightArray::from_vec_u32(up_middle),
            down_offsets: crate::formats::ArcCow::from_vec(down_offsets),
            down_targets: crate::formats::ArcCow::from_vec(down_targets),
            down_is_shortcut: BitsetField::from_bools(&down_is_shortcut_bools),
            down_middle: crate::formats::cch_weights::WeightArray::from_vec_u32(down_middle),
            rank_to_filtered: crate::formats::ArcCow::from_vec((0..6).collect()),
        };

        let weights = CchWeights {
            up: up_w.into(),
            down: down_w.into(),
            up_middle: vec![].into(),
            down_middle: vec![].into(),
        };

        (topo, weights)
    }

    #[test]
    fn l3_tiled_path_matches_monolithic() {
        let (topo, w) = make_cch_6();
        let n_nodes = topo.n_nodes as usize;
        let up_adj = UpAdjFlat::build(&topo, &w);
        let down_rev = DownReverseAdjFlat::build(&topo, &w);

        // Pick sources/targets that produce non-trivial joins.
        let sources: Vec<u32> = vec![0, 1, 2, 0, 1, 2, 3, 4]; // 8 sources
        let targets: Vec<u32> = vec![3, 4, 5, 5, 4, 3]; // 6 targets

        // Monolithic reference (forces single-tile path because it's tiny).
        let (mono, _) = table_bucket_parallel(n_nodes, &up_adj, &down_rev, &sources, &targets);

        // L3-tiled with tile=1 (one source per tile — most aggressive split).
        let (tiled, _) = table_bucket_parallel_l3_tiled(
            n_nodes,
            &up_adj,
            &down_rev,
            &sources,
            &targets,
            1,
            u32::MAX,
        );
        assert_eq!(
            tiled, mono,
            "L3-tiled (tile=1) must match monolithic byte-for-byte"
        );

        // Also exercise tile=3 (forces non-uniform last tile).
        let (tiled3, _) = table_bucket_parallel_l3_tiled(
            n_nodes,
            &up_adj,
            &down_rev,
            &sources,
            &targets,
            3,
            u32::MAX,
        );
        assert_eq!(
            tiled3, mono,
            "L3-tiled (tile=3) must match monolithic byte-for-byte"
        );

        // And tile=8 (one tile, equivalent to monolithic shape).
        let (tiled8, _) = table_bucket_parallel_l3_tiled(
            n_nodes,
            &up_adj,
            &down_rev,
            &sources,
            &targets,
            8,
            u32::MAX,
        );
        assert_eq!(
            tiled8, mono,
            "L3-tiled (tile=8 = whole problem) must match monolithic"
        );
    }

    // ---- #349 flat-file v2 width round-trip tests --------------------
    //
    // Each test builds a small synthetic UpAdjFlat / DownAdjFlat /
    // DownReverseAdjFlat with weights at one specific WeightArray
    // variant, encodes via `*File::encode`, then decodes via
    // `read_from_bytes` and asserts:
    //   - n_edges / n_nodes preserved
    //   - the decoded WeightArray reads back the same `get(i)` for
    //     every slot
    //   - the on-disk header width code is what we asked for
    //   - the sentinel value (u32::MAX) round-trips through the
    //     widen-on-read mapping

    fn build_flat_with_width(width: crate::formats::WeightWidth) -> UpAdjFlat {
        use crate::formats::WeightArray;
        // 4 nodes, 3 edges, including a sentinel (u32::MAX) at slot 1.
        let offsets: Vec<u64> = vec![0, 1, 2, 3, 3];
        let targets: Vec<u32> = vec![1, 2, 0];
        // Pick representative finite weights per width:
        //   U16: small values plus u32::MAX sentinel
        //   U24: medium values plus u32::MAX sentinel
        //   U32: large values plus u32::MAX sentinel
        let w32: Vec<u32> = match width {
            crate::formats::WeightWidth::U16 => vec![10, u32::MAX, 65_000],
            crate::formats::WeightWidth::U24 => vec![100_000, u32::MAX, 10_000_000],
            crate::formats::WeightWidth::U32 => vec![1_000_000_000, u32::MAX, 4_000_000_000],
        };
        let weights = match width {
            crate::formats::WeightWidth::U16 => {
                let v16: Vec<u16> = w32
                    .iter()
                    .map(|&w| if w == u32::MAX { u16::MAX } else { w as u16 })
                    .collect();
                WeightArray::from_vec_u16(v16)
            }
            crate::formats::WeightWidth::U24 => {
                let mut bytes: Vec<u8> = Vec::with_capacity(3 * w32.len());
                for &w in &w32 {
                    let v: u32 = if w == u32::MAX {
                        crate::formats::U24_SENTINEL
                    } else {
                        w
                    };
                    let le = v.to_le_bytes();
                    bytes.extend_from_slice(&le[..3]);
                }
                WeightArray::from_u24_bytes(bytes, w32.len())
            }
            crate::formats::WeightWidth::U32 => WeightArray::from_vec_u32(w32.clone()),
        };
        // Smoke: in-memory get() returns the round-tripped values.
        assert_eq!(weights.get(0), w32[0]);
        assert_eq!(weights.get(1), u32::MAX);
        assert_eq!(weights.get(2), w32[2]);

        UpAdjFlat {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets),
            weights,
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        }
    }

    fn assert_width_code(bytes: &[u8], expected: u8) {
        // Header byte 7 = width code (low 2 bits) per #349.
        assert_eq!(
            bytes[7] & ADJ_FLAT_WIDTH_CODE_MASK,
            expected,
            "header width code byte mismatch"
        );
    }

    #[test]
    fn up_adj_flat_v2_round_trip_u32() {
        let flat = build_flat_with_width(crate::formats::WeightWidth::U32);
        let bytes = UpAdjFlatFile::encode(&flat);
        assert_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U32);
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        assert_eq!(back.weights.len(), 3);
        for i in 0..3 {
            assert_eq!(back.weights.get(i), flat.weights.get(i));
        }
    }

    #[test]
    fn up_adj_flat_v2_round_trip_u16() {
        let flat = build_flat_with_width(crate::formats::WeightWidth::U16);
        let bytes = UpAdjFlatFile::encode(&flat);
        assert_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U16);
        // n_edges=3 (odd) so u16 body is padded by 2 bytes — round-trip
        // proves the padding is correctly consumed by the reader.
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        assert_eq!(back.weights.len(), 3);
        assert_eq!(back.weights.get(0), 10);
        assert_eq!(
            back.weights.get(1),
            u32::MAX,
            "u16::MAX must widen back to u32::MAX"
        );
        assert_eq!(back.weights.get(2), 65_000);
    }

    #[test]
    fn up_adj_flat_v2_round_trip_u24() {
        let flat = build_flat_with_width(crate::formats::WeightWidth::U24);
        let bytes = UpAdjFlatFile::encode(&flat);
        assert_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U24);
        // n_edges=3 (so 9 bytes of u24 body, padded by 3 to 12).
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        assert_eq!(back.weights.len(), 3);
        assert_eq!(back.weights.get(0), 100_000);
        assert_eq!(
            back.weights.get(1),
            u32::MAX,
            "U24_SENTINEL must widen back to u32::MAX"
        );
        assert_eq!(back.weights.get(2), 10_000_000);
    }

    #[test]
    fn down_adj_flat_v2_round_trip_u16() {
        // DownAdjFlat builder doesn't have a public encode that accepts
        // raw vecs, so reuse the topology builder + override weights.
        use crate::formats::WeightArray;
        let (topo, w) = make_cch();
        let mut flat = DownAdjFlat::build(&topo, &w);
        // Replace weights with a u16-width array that holds the same
        // values to exercise the new write/read paths. Values are
        // small (the synth CCH uses 1..=20).
        let widened: Vec<u32> = (0..flat.weights.len() as u32)
            .map(|i| flat.weights.get(i as usize))
            .collect();
        let v16: Vec<u16> = widened
            .iter()
            .map(|&w| if w == u32::MAX { u16::MAX } else { w as u16 })
            .collect();
        flat.weights = WeightArray::from_vec_u16(v16);
        let bytes = DownAdjFlatFile::encode(&flat);
        assert_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U16);
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = DownAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        for i in 0..flat.weights.len() {
            assert_eq!(back.weights.get(i), flat.weights.get(i));
        }
    }

    #[test]
    fn up_adj_flat_v3_picks_u32_offsets_for_small_data() {
        // Synthetic flats have tiny offset counts, so `pick_offsets_u32`
        // selects u32. Header byte 7 bit 2 must be set; the round-trip
        // through `read_from_bytes` must reconstruct an identical
        // `flat.offsets` Vec.
        let flat = build_flat_with_width(crate::formats::WeightWidth::U32);
        let bytes = UpAdjFlatFile::encode(&flat);
        assert!(
            bytes[7] & ADJ_FLAT_OFFSETS_U32_BIT != 0,
            "small flats should pack u32 offsets (header byte 7 bit 2 set)"
        );
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        // offsets reconstructed byte-for-byte (widened from u32 → u64)
        assert_eq!(back.offsets.as_slice(), flat.offsets.as_slice());
    }

    #[test]
    fn up_adj_flat_v3_picks_u64_offsets_for_large_data() {
        // Forge a flat whose last offset overflows u32, forcing the
        // writer onto the u64 path. The body content is meaningless
        // (we just want the size selection to fire), so we sidestep
        // build_with by constructing the flat directly.
        use crate::formats::WeightArray;
        // 2 nodes, 1 edge. Offsets [0, u32::MAX as u64 + 1] — last
        // exceeds u32, so writer must pick u64.
        let offsets: Vec<u64> = vec![0, u32::MAX as u64 + 1];
        let targets: Vec<u32> = vec![0];
        let flat = UpAdjFlat {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets),
            weights: WeightArray::from_vec_u32(vec![42]),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        };
        let bytes = UpAdjFlatFile::encode(&flat);
        assert_eq!(
            bytes[7] & ADJ_FLAT_OFFSETS_U32_BIT,
            0,
            "large offsets must force the u64 path (header byte 7 bit 2 clear)"
        );
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        assert_eq!(back.offsets.as_slice(), flat.offsets.as_slice());
    }

    fn assert_targets_width_code(bytes: &[u8], expected: u8) {
        let code = (bytes[7] & ADJ_FLAT_TARGETS_CODE_MASK) >> ADJ_FLAT_TARGETS_CODE_SHIFT;
        assert_eq!(code, expected, "header byte 7 targets width code mismatch");
    }

    #[test]
    fn up_adj_flat_v4_picks_u16_targets_for_tiny_ranks() {
        // Synthetic flats have target values 0,1,2 — all fit u16.
        let flat = build_flat_with_width(crate::formats::WeightWidth::U32);
        let bytes = UpAdjFlatFile::encode(&flat);
        assert_targets_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U16);
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        assert_eq!(back.targets.as_slice(), flat.targets.as_slice());
    }

    #[test]
    fn up_adj_flat_v4_picks_u24_targets_for_million_range() {
        // Targets in the u24 band: u16::MAX+1 .. U24_SENTINEL-1.
        use crate::formats::WeightArray;
        let offsets: Vec<u64> = vec![0, 1, 2, 3, 3];
        let targets: Vec<u32> = vec![1_000_000, 5_000_000, 16_000_000];
        let flat = UpAdjFlat {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets.clone()),
            weights: WeightArray::from_vec_u32(vec![10, 20, 30]),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        };
        let bytes = UpAdjFlatFile::encode(&flat);
        assert_targets_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U24);
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        assert_eq!(back.targets.as_slice(), &targets[..]);
    }

    #[test]
    fn up_adj_flat_v4_picks_u32_targets_for_huge_ranks() {
        // Target at U24_SENTINEL forces u32.
        use crate::formats::WeightArray;
        let offsets: Vec<u64> = vec![0, 1, 2];
        let targets: Vec<u32> = vec![crate::formats::U24_SENTINEL, 100_000_000];
        let flat = UpAdjFlat {
            offsets: ArcCow::from_vec(offsets),
            targets: ArcCow::from_vec(targets.clone()),
            weights: WeightArray::from_vec_u32(vec![1, 2]),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        };
        let bytes = UpAdjFlatFile::encode(&flat);
        assert_targets_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U32);
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = UpAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        assert_eq!(back.targets.as_slice(), &targets[..]);
    }

    #[test]
    fn down_reverse_adj_flat_v2_round_trip_u24() {
        use crate::formats::{U24_SENTINEL, WeightArray};
        let (topo, w) = make_cch();
        let mut flat = DownReverseAdjFlat::build_with(&topo, &w, true);
        // Build a u24 weights view of the same values.
        let widened: Vec<u32> = (0..flat.weights.len() as u32)
            .map(|i| flat.weights.get(i as usize))
            .collect();
        let mut bytes24: Vec<u8> = Vec::with_capacity(3 * widened.len());
        for &w in &widened {
            let v: u32 = if w == u32::MAX { U24_SENTINEL } else { w };
            let le = v.to_le_bytes();
            bytes24.extend_from_slice(&le[..3]);
        }
        flat.weights = WeightArray::from_u24_bytes(bytes24, widened.len());
        let bytes = DownReverseAdjFlatFile::encode(&flat);
        assert_width_code(&bytes, ADJ_FLAT_WIDTH_CODE_U24);
        let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let back = DownReverseAdjFlatFile::read_from_bytes(static_bytes).unwrap();
        for i in 0..flat.weights.len() {
            assert_eq!(back.weights.get(i), flat.weights.get(i));
        }
    }

    // ---- #345 split format round-trip tests ----

    #[test]
    fn flat_topo_split_u16_targets_no_topo_idx_roundtrip() {
        let offsets: Vec<u64> = vec![0, 2, 3, 5, 6];
        let targets: Vec<u32> = vec![1, 3, 2, 0, 1, 3];
        let bytes = encode_flat_topo_bytes(&offsets, &targets, &[]);
        let decoded = decode_flat_topo_bytes(&bytes).unwrap();
        assert_eq!(decoded.n_nodes, 4);
        assert_eq!(decoded.offsets, offsets);
        assert_eq!(decoded.targets, targets);
        assert!(decoded.topo_edge_idx.is_empty());
    }

    #[test]
    fn flat_topo_split_u24_targets_with_topo_idx_roundtrip() {
        let offsets: Vec<u64> = vec![0, 1, 2, 3];
        let targets: Vec<u32> = vec![100_000, 1_000_000, 16_000_000];
        let topo_idx: Vec<u32> = vec![17, 23, 42];
        let bytes = encode_flat_topo_bytes(&offsets, &targets, &topo_idx);
        let decoded = decode_flat_topo_bytes(&bytes).unwrap();
        assert_eq!(decoded.targets, targets);
        assert_eq!(decoded.topo_edge_idx, topo_idx);
        let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        assert_eq!(flags & FLAT_TOPO_TARGETS_WIDTH_MASK, 2, "u24 code expected");
        assert!(flags & FLAT_TOPO_HAS_TOPO_IDX_BIT != 0, "has_topo_idx bit");
        assert!(flags & FLAT_TOPO_OFFSETS_U32_BIT != 0, "offsets_u32 fits");
    }

    #[test]
    fn flat_topo_split_u32_targets_when_u24_overflows() {
        let offsets: Vec<u64> = vec![0, 1];
        let targets: Vec<u32> = vec![crate::formats::U24_SENTINEL + 5];
        let bytes = encode_flat_topo_bytes(&offsets, &targets, &[]);
        let decoded = decode_flat_topo_bytes(&bytes).unwrap();
        assert_eq!(decoded.targets, targets);
        let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        assert_eq!(flags & FLAT_TOPO_TARGETS_WIDTH_MASK, 0, "u32 code");
    }

    #[test]
    fn flat_weights_u16_roundtrip() {
        use crate::formats::WeightArray;
        let w = WeightArray::from_vec_u16(vec![10, u16::MAX, 5, 0, 1234]);
        let bytes = encode_flat_weights_bytes(&w);
        let back = decode_flat_weights_bytes(&bytes).unwrap();
        assert_eq!(back.width(), crate::formats::WeightWidth::U16);
        assert_eq!(back.len(), 5);
        assert_eq!(back.get(0), 10);
        assert_eq!(back.get(1), u32::MAX);
        assert_eq!(back.get(2), 5);
        assert_eq!(back.get(3), 0);
        assert_eq!(back.get(4), 1234);
    }

    #[test]
    fn flat_weights_u24_roundtrip() {
        let w = crate::formats::WeightArray::from_u24_bytes(
            vec![
                0x10, 0x00, 0x00, // 0x000010 = 16
                0xFF, 0xFF, 0xFF, // U24_SENTINEL → u32::MAX
                0x00, 0x00, 0x01, // 0x010000 = 65536
            ],
            3,
        );
        let bytes = encode_flat_weights_bytes(&w);
        let back = decode_flat_weights_bytes(&bytes).unwrap();
        assert_eq!(back.width(), crate::formats::WeightWidth::U24);
        assert_eq!(back.get(0), 16);
        assert_eq!(back.get(1), u32::MAX);
        assert_eq!(back.get(2), 65536);
    }

    #[test]
    fn flat_weights_u32_roundtrip() {
        let w = crate::formats::WeightArray::from_vec_u32(vec![1, 2, 3, u32::MAX]);
        let bytes = encode_flat_weights_bytes(&w);
        let back = decode_flat_weights_bytes(&bytes).unwrap();
        assert_eq!(back.width(), crate::formats::WeightWidth::U32);
        assert_eq!(back.get(3), u32::MAX);
    }

    #[test]
    fn flat_topo_rejects_corrupted_crc() {
        let offsets: Vec<u64> = vec![0, 1];
        let targets: Vec<u32> = vec![42];
        let mut bytes = encode_flat_topo_bytes(&offsets, &targets, &[]);
        let n = bytes.len();
        bytes[n - 16] ^= 0xFF;
        let err = decode_flat_topo_bytes(&bytes).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("CRC"), "expected CRC error, got: {msg}");
    }

    #[test]
    fn flat_weights_rejects_corrupted_crc() {
        let w = crate::formats::WeightArray::from_vec_u32(vec![1, 2, 3]);
        let mut bytes = encode_flat_weights_bytes(&w);
        let n = bytes.len();
        bytes[n - 16] ^= 0xFF;
        let err = decode_flat_weights_bytes(&bytes).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("CRC"), "expected CRC error, got: {msg}");
    }
}

#[cfg(test)]
mod max_minutes_bound_tests {
    //! #415 max_minutes — exactness of the time-bounded bucket-M2M.
    //!
    //! Each test builds a deterministic "broom" graph: every leaf node has a
    //! single UP edge to one apex node, and a single reverse-down edge to the
    //! same apex. The bucket-M2M then computes, for leaf source `s` and leaf
    //! target `t`, exactly `d(s→t) = wu[s] + wd[t]` (and `0` on the diagonal),
    //! via the apex meeting node. Because there is exactly one path, an
    //! out-of-bound cell settles to `u32::MAX` — so we can assert the strict
    //! invariant: `bounded[c] == unbounded[c]` when `unbounded[c] ≤ threshold`,
    //! else `bounded[c] == u32::MAX`. This exercises the real `SearchState`
    //! early-stop through the sequential (≤100 cells) and monolithic (>100)
    //! dispatch paths, plus the 2-channel variant.
    use super::*;
    use crate::formats::{ArcCow, WeightArray};

    /// Build a broom with `n_leaf` leaves (ids 0..n_leaf) and one apex
    /// (id n_leaf). UP weights = (i+1)*10, reverse-down weights = (i+1).
    /// Returns (n_nodes, up, down, wu, wd).
    fn broom(n_leaf: usize) -> (usize, UpAdjFlat, DownReverseAdjFlat, Vec<u32>, Vec<u32>) {
        let apex = n_leaf as u32;
        let n_nodes = n_leaf + 1;
        let wu: Vec<u32> = (0..n_leaf).map(|i| (i as u32 + 1) * 10).collect();
        let wd: Vec<u32> = (0..n_leaf).map(|i| i as u32 + 1).collect();

        // CSR: each leaf has 1 edge to apex; apex has 0.
        let mut offsets: Vec<u64> = Vec::with_capacity(n_nodes + 1);
        for i in 0..=n_nodes {
            offsets.push(i.min(n_leaf) as u64);
        }
        let up_targets: Vec<u32> = vec![apex; n_leaf];
        let dn_sources: Vec<u32> = vec![apex; n_leaf];

        let up = UpAdjFlat {
            offsets: ArcCow::from_vec(offsets.clone()),
            targets: ArcCow::from_vec(up_targets),
            weights: WeightArray::from_vec_u32(wu.clone()),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        };
        let down = DownReverseAdjFlat {
            offsets: ArcCow::from_vec(offsets),
            sources: ArcCow::from_vec(dn_sources),
            weights: WeightArray::from_vec_u32(wd.clone()),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        };
        (n_nodes, up, down, wu, wd)
    }

    /// Expected single-channel time for cell (s, t).
    fn expect(wu: &[u32], wd: &[u32], s: usize, t: usize) -> u32 {
        if s == t { 0 } else { wu[s] + wd[t] }
    }

    fn assert_bounded_eq_filtered(
        n_nodes: usize,
        up: &UpAdjFlat,
        down: &DownReverseAdjFlat,
        wu: &[u32],
        wd: &[u32],
        threshold: u32,
    ) {
        let n = wu.len();
        let srcs: Vec<u32> = (0..n as u32).collect();
        let tgts: Vec<u32> = (0..n as u32).collect();

        let (unbounded, _) = table_bucket_parallel(n_nodes, up, down, &srcs, &tgts);
        let (bounded, _) =
            table_bucket_parallel_bounded(n_nodes, up, down, &srcs, &tgts, threshold);

        for s in 0..n {
            for t in 0..n {
                let c = s * n + t;
                // Ground truth (independent of the engine).
                assert_eq!(unbounded[c], expect(wu, wd, s, t), "unbounded ({s},{t})");
                if unbounded[c] <= threshold {
                    assert_eq!(
                        bounded[c], unbounded[c],
                        "in-bound cell ({s},{t}) must match exactly"
                    );
                } else {
                    // Engine-level invariant: a bounded cell may still hold a
                    // real value > threshold (both half-searches stayed within
                    // the bound but their sum exceeded it) or MAX — but NEVER a
                    // ≤threshold value, and NEVER an underestimate. The server
                    // post-filter nulls every > threshold cell, so the SERVED
                    // matrix is exactly the unbounded matrix filtered to
                    // ≤ threshold.
                    assert!(
                        bounded[c] > threshold,
                        "out-of-bound cell ({s},{t}) leaked a ≤threshold value: {}",
                        bounded[c]
                    );
                    assert!(
                        bounded[c] >= unbounded[c],
                        "bounded must never underestimate at ({s},{t})"
                    );
                }
            }
        }
    }

    #[test]
    fn bounded_equals_unbounded_filtered_sequential() {
        // 5×5 = 25 cells ≤ 100 → sequential BucketM2MEngine path.
        let (n_nodes, up, down, wu, wd) = broom(5);
        for &thr in &[0u32, 15, 25, 55, 1000] {
            assert_bounded_eq_filtered(n_nodes, &up, &down, &wu, &wd, thr);
        }
    }

    #[test]
    fn bounded_equals_unbounded_filtered_monolithic() {
        // 12×12 = 144 cells > 100 → monolithic parallel forward+backward.
        let (n_nodes, up, down, wu, wd) = broom(12);
        for &thr in &[0u32, 30, 75, 130, 100_000] {
            assert_bounded_eq_filtered(n_nodes, &up, &down, &wu, &wd, thr);
        }
    }

    #[test]
    fn unbounded_sentinel_is_byte_identical() {
        // threshold == u32::MAX must reproduce the unbounded matrix exactly.
        for n_leaf in [5usize, 12] {
            let (n_nodes, up, down, ..) = broom(n_leaf);
            let srcs: Vec<u32> = (0..n_leaf as u32).collect();
            let (a, _) = table_bucket_parallel(n_nodes, &up, &down, &srcs, &srcs);
            let (b, _) = table_bucket_parallel_bounded(n_nodes, &up, &down, &srcs, &srcs, u32::MAX);
            assert_eq!(
                a, b,
                "bounded(u32::MAX) must equal unbounded for n_leaf={n_leaf}"
            );
        }
    }

    #[test]
    fn bounded_2channel_time_bound_carries_length() {
        // 12×12 → 2-channel parallel path. Bound is on TIME; the length
        // channel (= 2× the time weights here) is carried alongside and must
        // match the unbounded length on in-bound cells, MAX otherwise.
        let (n_nodes, up, down, wu, wd) = broom(12);
        let up_lat = UpAdjFlat {
            offsets: up.offsets.clone(),
            targets: up.targets.clone(),
            weights: WeightArray::from_vec_u32(wu.iter().map(|w| w * 2).collect()),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        };
        let dn_lat = DownReverseAdjFlat {
            offsets: down.offsets.clone(),
            sources: down.sources.clone(),
            weights: WeightArray::from_vec_u32(wd.iter().map(|w| w * 2).collect()),
            topo_edge_idx: ArcCow::from_vec(Vec::new()),
        };
        let n = wu.len();
        let srcs: Vec<u32> = (0..n as u32).collect();
        let threshold = 75u32;

        let (u_time, u_lat, _) = table_bucket_parallel_len_along_time(
            n_nodes, &up, &down, &up_lat, &dn_lat, &srcs, &srcs,
        );
        let (b_time, b_lat, _) = table_bucket_parallel_len_along_time_bounded(
            n_nodes, &up, &down, &up_lat, &dn_lat, &srcs, &srcs, threshold,
        );

        for s in 0..n {
            for t in 0..n {
                let c = s * n + t;
                assert_eq!(
                    u_time[c],
                    expect(&wu, &wd, s, t),
                    "unbounded time ({s},{t})"
                );
                if u_time[c] <= threshold {
                    assert_eq!(b_time[c], u_time[c], "in-bound time ({s},{t})");
                    assert_eq!(b_lat[c], u_lat[c], "in-bound length ({s},{t})");
                } else {
                    // Out-of-bound: time > threshold (MAX or a real >threshold
                    // value). The server post-filter nulls both channels here.
                    assert!(
                        b_time[c] > threshold,
                        "out-of-bound time ({s},{t}) leaked: {}",
                        b_time[c]
                    );
                }
            }
        }
    }
}
