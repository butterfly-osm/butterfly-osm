//! Performance telemetry and metrics collection

use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Thread-safe telemetry collector
#[derive(Debug, Clone)]
pub struct Telemetry {
    inner: Arc<TelemetryInner>,
}

#[derive(Debug)]
struct TelemetryInner {
    // Phase timings
    node_ingest_ms: AtomicU64,
    way_remap_ms: AtomicU64,
    relation_pass_ms: AtomicU64,
    
    // RocksDB metrics
    put_count: AtomicU64,
    get_count: AtomicU64,
    multiget_count: AtomicU64,
    multiget_keys_total: AtomicU64,
    
    // Batching metrics
    ways_in_batch: AtomicU64,
    unique_node_ids_in_batch: AtomicU64,
    batch_count: AtomicU64,
    multiget_p95_ms: AtomicU64,
    
    // Cache metrics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    
    // Output metrics
    pbf_blocks_written: AtomicU64,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::new()
    }
}

impl Telemetry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(TelemetryInner {
                node_ingest_ms: AtomicU64::new(0),
                way_remap_ms: AtomicU64::new(0),
                relation_pass_ms: AtomicU64::new(0),
                put_count: AtomicU64::new(0),
                get_count: AtomicU64::new(0),
                multiget_count: AtomicU64::new(0),
                multiget_keys_total: AtomicU64::new(0),
                ways_in_batch: AtomicU64::new(0),
                unique_node_ids_in_batch: AtomicU64::new(0),
                batch_count: AtomicU64::new(0),
                multiget_p95_ms: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                pbf_blocks_written: AtomicU64::new(0),
            }),
        }
    }

    // Phase timing methods
    pub fn record_node_ingest_time(&self, duration: Duration) {
        self.inner.node_ingest_ms.store(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_way_remap_time(&self, duration: Duration) {
        self.inner.way_remap_ms.store(duration.as_millis() as u64, Ordering::Relaxed);
    }

    pub fn record_relation_pass_time(&self, duration: Duration) {
        self.inner.relation_pass_ms.store(duration.as_millis() as u64, Ordering::Relaxed);
    }

    // RocksDB metrics
    pub fn increment_puts(&self, count: u64) {
        self.inner.put_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_gets(&self, count: u64) {
        self.inner.get_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_multiget(&self, key_count: usize, duration: Duration) {
        self.inner.multiget_count.fetch_add(1, Ordering::Relaxed);
        self.inner.multiget_keys_total.fetch_add(key_count as u64, Ordering::Relaxed);
        
        // Simple p95 approximation - store the latest timing
        // In production, you'd want a proper histogram
        self.inner.multiget_p95_ms.store(duration.as_millis() as u64, Ordering::Relaxed);
    }

    // Batching metrics
    pub fn record_batch(&self, ways_count: usize, unique_nodes: usize) {
        self.inner.batch_count.fetch_add(1, Ordering::Relaxed);
        self.inner.ways_in_batch.fetch_add(ways_count as u64, Ordering::Relaxed);
        self.inner.unique_node_ids_in_batch.fetch_add(unique_nodes as u64, Ordering::Relaxed);
    }

    // Cache metrics
    pub fn record_cache_hit(&self) {
        self.inner.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.inner.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    
    /// Get cache hit ratio
    pub fn cache_hit_ratio(&self) -> f64 {
        let hits = self.inner.cache_hits.load(Ordering::Relaxed) as f64;
        let misses = self.inner.cache_misses.load(Ordering::Relaxed) as f64;
        let total = hits + misses;
        if total > 0.0 {
            hits / total
        } else {
            0.0
        }
    }
    
    /// Get average MultiGet latency in milliseconds
    pub fn avg_multiget_latency_ms(&self) -> f64 {
        let total_ms = self.inner.multiget_p95_ms.load(Ordering::Relaxed) as f64;
        // Note: using p95 as proxy for average since we simplified the implementation
        total_ms
    }

    // Output metrics
    pub fn increment_pbf_blocks(&self, count: u64) {
        self.inner.pbf_blocks_written.fetch_add(count, Ordering::Relaxed);
    }

    // Statistics getters
    pub fn get_stats(&self) -> TelemetryStats {
        TelemetryStats {
            node_ingest_ms: self.inner.node_ingest_ms.load(Ordering::Relaxed),
            way_remap_ms: self.inner.way_remap_ms.load(Ordering::Relaxed),
            relation_pass_ms: self.inner.relation_pass_ms.load(Ordering::Relaxed),
            put_count: self.inner.put_count.load(Ordering::Relaxed),
            get_count: self.inner.get_count.load(Ordering::Relaxed),
            multiget_count: self.inner.multiget_count.load(Ordering::Relaxed),
            multiget_keys_total: self.inner.multiget_keys_total.load(Ordering::Relaxed),
            ways_in_batch: self.inner.ways_in_batch.load(Ordering::Relaxed),
            unique_node_ids_in_batch: self.inner.unique_node_ids_in_batch.load(Ordering::Relaxed),
            batch_count: self.inner.batch_count.load(Ordering::Relaxed),
            multiget_p95_ms: self.inner.multiget_p95_ms.load(Ordering::Relaxed),
            cache_hits: self.inner.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.inner.cache_misses.load(Ordering::Relaxed),
            pbf_blocks_written: self.inner.pbf_blocks_written.load(Ordering::Relaxed),
        }
    }

    pub fn print_stats(&self) {
        let stats = self.get_stats();
        
        log::info!("=== Performance Telemetry ===");
        log::info!("Phase Timings:");
        log::info!("  Node ingest: {}ms", stats.node_ingest_ms);
        log::info!("  Way remap: {}ms", stats.way_remap_ms);
        log::info!("  Relation pass: {}ms", stats.relation_pass_ms);
        log::info!("  Total: {}ms", stats.node_ingest_ms + stats.way_remap_ms + stats.relation_pass_ms);
        
        log::info!("RocksDB Metrics:");
        log::info!("  Puts: {}", stats.put_count);
        log::info!("  Gets: {}", stats.get_count);
        log::info!("  MultiGets: {} ({} keys total)", stats.multiget_count, stats.multiget_keys_total);
        log::info!("  MultiGet P95: {}ms", stats.multiget_p95_ms);
        
        if stats.batch_count > 0 {
            log::info!("Batching Metrics:");
            log::info!("  Batches: {}", stats.batch_count);
            log::info!("  Avg ways/batch: {}", stats.ways_in_batch / stats.batch_count);
            log::info!("  Avg unique nodes/batch: {}", stats.unique_node_ids_in_batch / stats.batch_count);
        }
        
        if stats.cache_hits + stats.cache_misses > 0 {
            let total_lookups = stats.cache_hits + stats.cache_misses;
            let hit_ratio = (stats.cache_hits as f64 / total_lookups as f64) * 100.0;
            log::info!("Cache Metrics:");
            log::info!("  Hits: {} ({:.1}%)", stats.cache_hits, hit_ratio);
            log::info!("  Misses: {}", stats.cache_misses);
        }
        
        log::info!("Output:");
        log::info!("  PBF blocks written: {}", stats.pbf_blocks_written);
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryStats {
    pub node_ingest_ms: u64,
    pub way_remap_ms: u64,
    pub relation_pass_ms: u64,
    pub put_count: u64,
    pub get_count: u64,
    pub multiget_count: u64,
    pub multiget_keys_total: u64,
    pub ways_in_batch: u64,
    pub unique_node_ids_in_batch: u64,
    pub batch_count: u64,
    pub multiget_p95_ms: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub pbf_blocks_written: u64,
}

/// Timer helper for measuring durations
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for Timer {
    fn default() -> Self {
        Self::new()
    }
}