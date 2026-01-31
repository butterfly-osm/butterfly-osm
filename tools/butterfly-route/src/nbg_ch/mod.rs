//! Node-Based Contraction Hierarchy (NBG CH)
//!
//! Builds a contraction hierarchy on the node-based graph (NBG) for fast
//! shortest-path queries without turn costs.
//!
//! This is the first step towards the junction expansion approach:
//! 1. NBG CH provides fast node-to-node distances
//! 2. Junction expansion adds exact turn handling at query time
//!
//! Expected benefits over edge-based CCH:
//! - 1.9M nodes vs 5M edge-states (2.6x fewer)
//! - ~7 edges/node vs ~15 (2x fewer)
//! - Combined: ~5x less work per search

mod ordering;
mod contraction;
mod query;
mod validate;
mod lift_ordering;
mod turn_restrictions;

pub use ordering::{NbgNdOrdering, compute_nbg_ordering};
pub use contraction::{NbgChTopo, contract_nbg};
pub use query::{NbgChQuery, NbgBucketM2M, NbgM2MStats, FlatUpAdj, SearchState, SortedBuckets};
pub use validate::{dijkstra_nbg, validate_nbg_ch, validate_matrix, ValidationResult, ValidationError};
pub use lift_ordering::{LiftedEbgOrdering, lift_ordering_to_ebg};
pub use turn_restrictions::{TurnRestrictionIndex, NbgEdgeWayMap};

/// Statistics from NBG CH construction
#[derive(Debug, Clone)]
pub struct NbgChStats {
    pub n_nodes: u32,
    pub n_original_edges: u64,
    pub n_shortcuts: u64,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub max_rank: u32,
    pub ordering_time_ms: u64,
    pub contraction_time_ms: u64,
}
