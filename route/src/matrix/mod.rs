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

#[cfg(test)]
mod layering {
    //! #569: the matrix engine must not import the HTTP server.
    //!
    //! It did: `matrix::bucket_ch` reached into
    //! `server::isochrone_handler` for the four seeded PHAST scans, so the
    //! query engine depended on a web handler module and the scans could only
    //! be built with the server compiled in. The scans now live in
    //! `range::phast_seeded` and the scratch-cell registry in
    //! `crate::evictable`; this test is what stops the inversion coming back
    //! by accident.
    //!
    //! Scope is `matrix/` alone — `range` still borrows `server::query`'s
    //! `HANDLE_NONE`, which is a separate cleanup.

    use std::collections::BTreeSet;
    use std::path::PathBuf;

    /// The module no `matrix/` source may path into.
    const UP: &str = "server";

    /// Upward imports, in the two spellings that reach [`UP`] from inside
    /// `matrix/`. Assembled at run time, never written out: a guard whose own
    /// needle is a literal flags itself. A doc comment may NAME the module;
    /// only a path does the damage, and a path needs one of these prefixes.
    fn forbidden() -> [String; 2] {
        [format!("crate::{UP}"), format!("super::{UP}")]
    }

    fn matrix_dir() -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/src/matrix"))
    }

    #[test]
    fn matrix_does_not_import_server() {
        let dir = matrix_dir();
        let mut scanned: BTreeSet<String> = BTreeSet::new();
        let mut offenders: Vec<String> = Vec::new();

        for entry in std::fs::read_dir(&dir).expect("matrix source directory is readable") {
            let path = entry.expect("readable dir entry").path();
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .expect("utf-8 file name")
                .to_string();
            let src = std::fs::read_to_string(&path).expect("matrix source is readable");
            for (i, line) in src.lines().enumerate() {
                if let Some(needle) = forbidden().iter().find(|n| line.contains(n.as_str())) {
                    offenders.push(format!("{name}:{}: {needle} — {}", i + 1, line.trim()));
                }
            }
            scanned.insert(name);
        }

        // A path typo or a moved module must FAIL the test, not silently
        // scan nothing: pin the files the guard is known to cover.
        assert!(
            scanned.contains("bucket_ch.rs") && scanned.contains("mod.rs"),
            "guard scanned {scanned:?} in {} — it is not looking at the \
             matrix module",
            dir.display()
        );

        assert!(
            offenders.is_empty(),
            "#569: the matrix engine imports the HTTP server — move the \
             shared code down (engine modules: `range`, `evictable`, \
             `formats`) instead of reaching up:\n{}",
            offenders.join("\n")
        );
    }
}
