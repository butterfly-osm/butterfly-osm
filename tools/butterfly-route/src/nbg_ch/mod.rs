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

mod contraction;
mod lift_ordering;
mod ordering;
mod query;
mod turn_restrictions;
mod validate;

pub use contraction::{contract_nbg, NbgChTopo};
pub use lift_ordering::{lift_ordering_to_ebg, LiftedEbgOrdering};
pub use ordering::{compute_nbg_ordering, NbgNdOrdering};
pub use query::{FlatUpAdj, NbgBucketM2M, NbgChQuery, NbgM2MStats, SearchState, SortedBuckets};
pub use turn_restrictions::{NbgEdgeWayMap, TurnRestrictionIndex};
pub use validate::{
    dijkstra_nbg, validate_matrix, validate_nbg_ch, ValidationError, ValidationResult,
};

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
