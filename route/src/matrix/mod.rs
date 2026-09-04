//! Bulk Matrix Computation
//!
//! This module provides two complementary algorithms for distance matrix computation:
//!
//! ## 1. Bucket-based Many-to-Many CH (`bucket_ch`)
//!
//! For **sparse matrices** (small N×M relative to graph size):
//! - Forward search from sources populates buckets
//! - Backward search from targets joins with buckets
//! - Complexity: O(N × up_search + M × down_search)
//! - Target: 50×50 < 100ms, matching OSRM performance
//!
//! ## 2. K-Lane Batched PHAST (`batched_phast`)
//!
//! For **dense queries** (isochrones, one-to-all, huge matrices):
//! - One-to-ALL distance computation
//! - K-lane batching amortizes memory access
//! - Best for streaming large matrices or isochrone computation
//!
//! ## Strategy Selection
//!
//! - **N×M ≤ 10,000**: Use bucket many-to-many (latency mode)
//! - **N×M > 10,000**: Use tiled PHAST streaming (throughput mode)
//! - **Isochrones**: Always use PHAST (need all reachable nodes)

pub mod arrow_stream;
pub mod batched_phast;
pub mod bucket_ch;
pub mod neighbors;
pub mod tile_geometry;

#[cfg(feature = "bench")]
pub use arrow_stream::ArrowMatrixWriter;

/// #529/#557: THE lexicographic "(time, then length) strictly better"
/// comparator — `true` iff `(t, l)` precedes `(best_t, best_l)`. One
/// definition shared by P2P meeting-node selection (`server::query`), the
/// bucket joins and the seeded PHAST evaluators, so every surface breaks an
/// equal-duration tie the same way and `/table` cannot disagree with
/// `/route` on the distance channel.
#[inline]
pub(crate) fn lex_better(t: u32, l: u32, best_t: u32, best_l: u32) -> bool {
    t < best_t || (t == best_t && l < best_l)
}
pub use arrow_stream::MatrixTile;
pub use batched_phast::{BatchedPhastEngine, BatchedPhastResult, BatchedPhastStats};
#[cfg(feature = "bench")]
pub use bucket_ch::{BucketArena, table_bucket, table_bucket_optimized};
pub use bucket_ch::{
    BucketM2MEngine,
    BucketM2MStats,
    // Data structures
    DownReverseAdjFlat,
    // #594: the plan a matrix call actually ran
    MatrixPlan,
    SourceBuckets,
    UpAdjFlat,
    backward_join_with_buckets,
    // Source-block optimized API (avoids repeated forward computation)
    forward_build_buckets,
    table_bucket_full_flat,
    table_bucket_parallel,
};
