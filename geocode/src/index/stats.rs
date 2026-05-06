//! Per-shard recall statistics, persisted as a JSON sidecar (#205).

use serde::{Deserialize, Serialize};

/// Summary stats consumed by `RecallBudget` to size top-K caps and
/// fanout limits adaptively per country.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardRecallStats {
    /// ISO-3166-1 alpha-2 country code (e.g. `BE`).
    pub country_iso2: String,
    /// Number of distinct keys in the FST.
    pub vocab_size: usize,
    /// Average key byte length (informational; useful when picking
    /// substring depth in the recall heuristic).
    pub avg_key_len: f64,
    /// Median posting-list size across all keys.
    pub p50_postings: u32,
    /// 95th-percentile posting-list size.
    pub p95_postings: u32,
    /// Total number of postings written to the payload file.
    pub total_postings: u64,
    /// Number of records in the source BFGS shard.
    pub record_count: usize,
}
