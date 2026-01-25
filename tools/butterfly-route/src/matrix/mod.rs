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

pub mod batched_phast;
pub mod arrow_stream;
pub mod bucket_ch;

pub use batched_phast::{BatchedPhastEngine, BatchedPhastResult, BatchedPhastStats};
pub use arrow_stream::{MatrixTile, ArrowMatrixWriter};
pub use bucket_ch::{table_bucket, table_bucket_optimized, table_bucket_full_flat, table_bucket_parallel, DownReverseAdjFlat, UpAdjFlat, UpReverseAdjFlat, BucketArena, BucketM2MStats, BucketM2MEngine};
