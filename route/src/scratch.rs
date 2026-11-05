//! The per-thread search scratch of the query engines, sized (#655).
//!
//! Below `server` so the engines' own tests can pin the constants without
//! importing the server layer (`matrix::layering`). See `server::mem`.

/// The per-thread scratch a query engine keeps, by engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scratch {
    /// Bucket many-to-many, one channel: forward + backward `SearchState`.
    BucketM2M,
    /// Bucket many-to-many, two channels (duration + distance):
    /// forward + backward `SearchState2`.
    BucketM2M2ch,
    /// Single-source PHAST with its length channel (`PhastState`).
    Phast,
    /// Predecessor-tree PHAST (`TreeScratch`, edges_batch / edges_flow).
    Tree,
    /// Bidirectional P2P query (`CchQueryState`, /route, route_batch).
    CchQuery,
}

impl Scratch {
    /// Bytes per graph node, from the struct layouts (pinned by the
    /// `scratch_size_tests` next to each struct).
    pub const fn bytes_per_node(self) -> u64 {
        match self {
            // NodeEntry {dist, version} 8 B + handle 4 B, ×2 states, plus
            // the heaps' capacity.
            Scratch::BucketM2M => 25,
            // NodeEntry 8 B + lat 4 B + handle 4 B, ×2 states, plus the
            // heaps' capacity.
            Scratch::BucketM2M2ch => 33,
            // dist 4 + version 4 + len 4, + pq capacity n/100 × 8 B.
            Scratch::Phast => 13,
            // dist, parent, generation, sel_gen, sel_pos, handles: 6 × 4 B,
            // plus the heap's capacity.
            Scratch::Tree => 25,
            // dist ×4 (16) + parent (u32,u32) ×2 (16) + gen ×2 (8) + handles ×2 (8),
            // plus the two heaps' capacity.
            Scratch::CchQuery => 50,
        }
    }
}

/// Bytes one lane of `kind` allocates for a graph of `n_nodes` nodes.
pub fn scratch_bytes(kind: Scratch, n_nodes: usize) -> u64 {
    kind.bytes_per_node() * n_nodes as u64
}

/// The largest per-lane state a PARALLEL request allocates (one per rayon
/// worker it reaches): the 2-channel bucket lane.
pub fn largest_parallel_lane_bytes(n_nodes: usize) -> u64 {
    scratch_bytes(Scratch::BucketM2M2ch, n_nodes)
}
