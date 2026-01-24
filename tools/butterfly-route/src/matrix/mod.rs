//! Bulk Matrix Computation with K-Lane Batched PHAST
//!
//! This module implements high-performance distance matrix computation using
//! K-lane batched PHAST - the single most important optimization for bulk queries.
//!
//! ## Key Insight
//!
//! The downward scan in PHAST is memory-bound (80-87% cache miss rate).
//! By processing K sources in one downward pass, we amortize memory access cost:
//! - Each node is loaded from memory once
//! - We update K distance values per load
//! - Reduces O(N × #sources) memory access to O(N × #sources/K)
//!
//! ## Architecture
//!
//! 1. **K-Lane Upward Phase**: K parallel PQ-based Dijkstra searches
//! 2. **K-Lane Downward Phase**: Single linear scan updating K dist arrays
//! 3. **Tiled Output**: Stream results as Arrow IPC batches

pub mod batched_phast;
pub mod arrow_stream;

pub use batched_phast::{BatchedPhastEngine, BatchedPhastResult, BatchedPhastStats};
pub use arrow_stream::{MatrixTile, ArrowMatrixWriter};
