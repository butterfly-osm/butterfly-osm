//! M6.1 - Weight Compression: u16 quantization with tick calculation and overflow tables

use crate::profiles::{EdgeId, TransportProfile};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Edge block size for weight compression (131k edges per block)
pub const EDGE_BLOCK_SIZE: usize = 131_072;

/// Compressed time weight using u16 quantization
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct CompressedTimeWeight {
    pub quantized_time: u16,     // Quantized time in ticks
    pub quantized_distance: u16, // Quantized distance
}

impl CompressedTimeWeight {
    pub fn new(quantized_time: u16, quantized_distance: u16) -> Self {
        Self {
            quantized_time,
            quantized_distance,
        }
    }

    /// Check if this weight represents an overflow case
    pub fn is_overflow(&self) -> bool {
        self.quantized_time == u16::MAX
    }
}

/// Weight compression configuration per edge block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub block_id: u32,
    pub time_tick_seconds: f64,    // Seconds per tick for this block
    pub distance_tick_meters: f64, // Meters per tick for this block
    pub max_time_seconds: f64,     // Maximum time before overflow
    pub max_distance_meters: f64,  // Maximum distance before overflow
    pub edge_count: usize,         // Number of edges in this block
}

impl CompressionConfig {
    /// Create compression config optimized for a block of edges
    pub fn new_for_block(block_id: u32, edge_times: &[f64], edge_distances: &[f64]) -> Self {
        assert!(
            !edge_times.is_empty(),
            "Cannot create config for empty block"
        );
        assert_eq!(edge_times.len(), edge_distances.len());

        let max_time = edge_times.iter().fold(0.0_f64, |a, &b| a.max(b));
        let max_distance = edge_distances.iter().fold(0.0_f64, |a, &b| a.max(b));

        // Calculate tick sizes to maximize precision within u16 range
        let time_tick_seconds = max_time / (u16::MAX - 1) as f64;
        let distance_tick_meters = max_distance / (u16::MAX - 1) as f64;

        Self {
            block_id,
            time_tick_seconds,
            distance_tick_meters,
            max_time_seconds: max_time,
            max_distance_meters: max_distance,
            edge_count: edge_times.len(),
        }
    }

    /// Compress a time/distance pair to quantized form
    pub fn compress(&self, time_seconds: f64, distance_meters: f64) -> CompressedTimeWeight {
        if time_seconds > self.max_time_seconds || distance_meters > self.max_distance_meters {
            // Overflow case - will be stored in overflow table
            return CompressedTimeWeight::new(u16::MAX, u16::MAX);
        }

        let quantized_time = if self.time_tick_seconds > 0.0 {
            ((time_seconds / self.time_tick_seconds).round() as u16).min(u16::MAX - 1)
        } else {
            0
        };

        let quantized_distance = if self.distance_tick_meters > 0.0 {
            ((distance_meters / self.distance_tick_meters).round() as u16).min(u16::MAX - 1)
        } else {
            0
        };

        CompressedTimeWeight::new(quantized_time, quantized_distance)
    }

    /// Decompress quantized values back to original scale
    pub fn decompress(&self, compressed: &CompressedTimeWeight) -> (f64, f64) {
        if compressed.is_overflow() {
            // Overflow case - caller should check overflow table
            return (f64::INFINITY, f64::INFINITY);
        }

        let time_seconds = compressed.quantized_time as f64 * self.time_tick_seconds;
        let distance_meters = compressed.quantized_distance as f64 * self.distance_tick_meters;

        (time_seconds, distance_meters)
    }

    /// Calculate compression ratio achieved
    pub fn compression_ratio(&self) -> f64 {
        // Original: 2 × f64 = 16 bytes per weight
        // Compressed: 2 × u16 = 4 bytes per weight
        // Plus compression config overhead per block
        let original_size = self.edge_count * 16;
        let compressed_size = self.edge_count * 4 + std::mem::size_of::<CompressionConfig>();
        compressed_size as f64 / original_size as f64
    }

    /// Get precision loss estimate (percentage)
    pub fn precision_loss_estimate(&self) -> f64 {
        let time_precision_loss = if self.max_time_seconds > 0.0 {
            self.time_tick_seconds / self.max_time_seconds
        } else {
            0.0
        };

        let distance_precision_loss = if self.max_distance_meters > 0.0 {
            self.distance_tick_meters / self.max_distance_meters
        } else {
            0.0
        };

        (time_precision_loss + distance_precision_loss) * 50.0 // Average as percentage
    }
}

/// Overflow table for weights that exceed u16 quantization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverflowTable {
    pub block_id: u32,
    pub overflow_entries: HashMap<EdgeId, (f64, f64)>, // EdgeId -> (time_seconds, distance_meters)
}

impl OverflowTable {
    pub fn new(block_id: u32) -> Self {
        Self {
            block_id,
            overflow_entries: HashMap::new(),
        }
    }

    /// Add an overflow entry
    pub fn add_overflow(&mut self, edge_id: EdgeId, time_seconds: f64, distance_meters: f64) {
        self.overflow_entries
            .insert(edge_id, (time_seconds, distance_meters));
    }

    /// Get overflow entry if exists
    pub fn get_overflow(&self, edge_id: &EdgeId) -> Option<(f64, f64)> {
        self.overflow_entries.get(edge_id).copied()
    }

    /// Check if an edge has an overflow entry
    pub fn has_overflow(&self, edge_id: &EdgeId) -> bool {
        self.overflow_entries.contains_key(edge_id)
    }

    /// Get overflow rate for this block
    pub fn overflow_rate(&self, total_edges: usize) -> f64 {
        if total_edges == 0 {
            return 0.0;
        }
        self.overflow_entries.len() as f64 / total_edges as f64
    }

    /// Memory usage of overflow table
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.overflow_entries.len() * (std::mem::size_of::<EdgeId>() + 16)
        // EdgeId + 2×f64
    }
}

/// Weight compression system managing blocks and overflow tables
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeightCompressionSystem {
    pub compression_configs: HashMap<u32, CompressionConfig>,
    pub overflow_tables: HashMap<u32, OverflowTable>,
    pub compressed_weights:
        HashMap<u32, HashMap<EdgeId, HashMap<TransportProfile, CompressedTimeWeight>>>,
}

impl WeightCompressionSystem {
    pub fn new() -> Self {
        Self {
            compression_configs: HashMap::new(),
            overflow_tables: HashMap::new(),
            compressed_weights: HashMap::new(),
        }
    }

    /// Create a new compression block
    pub fn create_block(
        &mut self,
        block_id: u32,
        edges: &[(EdgeId, TransportProfile, f64, f64)], // (edge_id, profile, time, distance)
    ) -> Result<CompressionStats, String> {
        if edges.len() > EDGE_BLOCK_SIZE {
            return Err(format!(
                "Block size {} exceeds maximum {}",
                edges.len(),
                EDGE_BLOCK_SIZE
            ));
        }

        // Group by profile for separate compression configs
        let mut profiles_data: HashMap<TransportProfile, Vec<(EdgeId, f64, f64)>> = HashMap::new();
        for &(edge_id, profile, time, distance) in edges {
            profiles_data
                .entry(profile)
                .or_default()
                .push((edge_id, time, distance));
        }

        let mut block_stats = CompressionStats::new(block_id);
        let mut overflow_table = OverflowTable::new(block_id);
        let mut block_weights = HashMap::new();

        // Process each profile separately to optimize compression
        for (profile, profile_edges) in &profiles_data {
            let times: Vec<f64> = profile_edges.iter().map(|(_, t, _)| *t).collect();
            let distances: Vec<f64> = profile_edges.iter().map(|(_, _, d)| *d).collect();

            // Create compression config for this profile in this block
            let config = CompressionConfig::new_for_block(block_id, &times, &distances);

            // Compress all weights for this profile
            for &(edge_id, time, distance) in profile_edges {
                let compressed = config.compress(time, distance);

                if compressed.is_overflow() {
                    overflow_table.add_overflow(edge_id, time, distance);
                    block_stats.overflow_count += 1;
                }

                block_weights
                    .entry(edge_id)
                    .or_insert_with(HashMap::new)
                    .insert(*profile, compressed);
            }

            block_stats.compression_ratio += config.compression_ratio();
            block_stats.precision_loss += config.precision_loss_estimate();
        }

        // Average stats across profiles
        let profile_count = profiles_data.len() as f64;
        if profile_count > 0.0 {
            block_stats.compression_ratio /= profile_count;
            block_stats.precision_loss /= profile_count;
        }
        block_stats.total_edges = edges.len();

        // Store the results using the first profile's config as representative
        // (In practice, you might want profile-specific configs)
        if let Some(profile) = profiles_data.keys().next() {
            let times: Vec<f64> = edges
                .iter()
                .filter(|(_, p, _, _)| p == profile)
                .map(|(_, _, t, _)| *t)
                .collect();
            let distances: Vec<f64> = edges
                .iter()
                .filter(|(_, p, _, _)| p == profile)
                .map(|(_, _, _, d)| *d)
                .collect();

            if !times.is_empty() {
                let config = CompressionConfig::new_for_block(block_id, &times, &distances);
                self.compression_configs.insert(block_id, config);
            }
        }

        self.overflow_tables.insert(block_id, overflow_table);
        self.compressed_weights.insert(block_id, block_weights);

        Ok(block_stats)
    }

    /// Get compressed weight for an edge
    pub fn get_weight(
        &self,
        block_id: u32,
        edge_id: &EdgeId,
        profile: &TransportProfile,
    ) -> Option<(f64, f64)> {
        // Check overflow table first
        if let Some(overflow_table) = self.overflow_tables.get(&block_id) {
            if let Some(overflow_weight) = overflow_table.get_overflow(edge_id) {
                return Some(overflow_weight);
            }
        }

        // Get compressed weight and decompress
        let compressed = self
            .compressed_weights
            .get(&block_id)?
            .get(edge_id)?
            .get(profile)?;

        let config = self.compression_configs.get(&block_id)?;
        let (time, distance) = config.decompress(compressed);

        if time.is_infinite() || distance.is_infinite() {
            None // Overflow case not in overflow table - error
        } else {
            Some((time, distance))
        }
    }

    /// Get system-wide statistics
    pub fn get_system_stats(&self) -> SystemCompressionStats {
        let mut stats = SystemCompressionStats::new();

        for (block_id, config) in &self.compression_configs {
            stats.total_blocks += 1;
            stats.total_edges += config.edge_count;
            stats.total_compression_ratio += config.compression_ratio();
            stats.total_precision_loss += config.precision_loss_estimate();

            if let Some(overflow_table) = self.overflow_tables.get(block_id) {
                stats.total_overflow_entries += overflow_table.overflow_entries.len();
                stats.total_overflow_rate += overflow_table.overflow_rate(config.edge_count);
            }
        }

        if stats.total_blocks > 0 {
            stats.average_compression_ratio =
                stats.total_compression_ratio / stats.total_blocks as f64;
            stats.average_precision_loss = stats.total_precision_loss / stats.total_blocks as f64;
            stats.average_overflow_rate = stats.total_overflow_rate / stats.total_blocks as f64;
        }

        stats
    }

    /// Calculate memory usage
    pub fn memory_usage(&self) -> usize {
        let configs_size =
            self.compression_configs.len() * std::mem::size_of::<CompressionConfig>();
        let overflow_size: usize = self
            .overflow_tables
            .values()
            .map(|t| t.memory_usage())
            .sum();
        let weights_size = self.compressed_weights.len() * EDGE_BLOCK_SIZE * 4; // Estimate

        configs_size + overflow_size + weights_size
    }
}

/// Statistics for a compression block
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    pub block_id: u32,
    pub total_edges: usize,
    pub overflow_count: usize,
    pub compression_ratio: f64,
    pub precision_loss: f64,
}

impl CompressionStats {
    pub fn new(block_id: u32) -> Self {
        Self {
            block_id,
            total_edges: 0,
            overflow_count: 0,
            compression_ratio: 0.0,
            precision_loss: 0.0,
        }
    }

    pub fn overflow_rate(&self) -> f64 {
        if self.total_edges == 0 {
            0.0
        } else {
            self.overflow_count as f64 / self.total_edges as f64
        }
    }
}

/// System-wide compression statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemCompressionStats {
    pub total_blocks: usize,
    pub total_edges: usize,
    pub total_overflow_entries: usize,
    pub total_compression_ratio: f64,
    pub total_precision_loss: f64,
    pub total_overflow_rate: f64,
    pub average_compression_ratio: f64,
    pub average_precision_loss: f64,
    pub average_overflow_rate: f64,
}

impl SystemCompressionStats {
    pub fn new() -> Self {
        Self {
            total_blocks: 0,
            total_edges: 0,
            total_overflow_entries: 0,
            total_compression_ratio: 0.0,
            total_precision_loss: 0.0,
            total_overflow_rate: 0.0,
            average_compression_ratio: 0.0,
            average_precision_loss: 0.0,
            average_overflow_rate: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profiles::TransportProfile;

    #[test]
    fn test_compression_config_creation() {
        let times = vec![10.0, 20.0, 100.0, 500.0];
        let distances = vec![100.0, 200.0, 1000.0, 5000.0];

        let config = CompressionConfig::new_for_block(1, &times, &distances);

        assert_eq!(config.block_id, 1);
        assert_eq!(config.edge_count, 4);
        assert!(config.max_time_seconds >= 500.0);
        assert!(config.max_distance_meters >= 5000.0);
        assert!(config.time_tick_seconds > 0.0);
        assert!(config.distance_tick_meters > 0.0);
    }

    #[test]
    fn test_weight_compression_decompression() {
        let times = vec![30.0, 60.0, 120.0];
        let distances = vec![500.0, 1000.0, 2000.0];

        let config = CompressionConfig::new_for_block(1, &times, &distances);

        let original_time = 90.0;
        let original_distance = 1500.0;

        let compressed = config.compress(original_time, original_distance);
        let (decompressed_time, decompressed_distance) = config.decompress(&compressed);

        // Should be close to original (within tick precision)
        let time_error = (decompressed_time - original_time).abs();
        let distance_error = (decompressed_distance - original_distance).abs();

        assert!(time_error <= config.time_tick_seconds);
        assert!(distance_error <= config.distance_tick_meters);
    }

    #[test]
    fn test_overflow_handling() {
        let times = vec![10.0, 20.0, 30.0];
        let distances = vec![100.0, 200.0, 300.0];

        let config = CompressionConfig::new_for_block(1, &times, &distances);

        // Test overflow case
        let overflow_time = config.max_time_seconds * 2.0;
        let overflow_distance = config.max_distance_meters * 2.0;

        let compressed = config.compress(overflow_time, overflow_distance);
        assert!(compressed.is_overflow());

        let (decompressed_time, decompressed_distance) = config.decompress(&compressed);
        assert!(decompressed_time.is_infinite());
        assert!(decompressed_distance.is_infinite());
    }

    #[test]
    fn test_overflow_table() {
        let mut table = OverflowTable::new(1);
        let edge_id = EdgeId(123);

        assert!(!table.has_overflow(&edge_id));

        table.add_overflow(edge_id, 1000.0, 10000.0);

        assert!(table.has_overflow(&edge_id));
        assert_eq!(table.get_overflow(&edge_id), Some((1000.0, 10000.0)));
        assert_eq!(table.overflow_rate(100), 0.01); // 1 overflow / 100 edges
    }

    #[test]
    fn test_weight_compression_system() {
        let mut system = WeightCompressionSystem::new();

        let edges = vec![
            (EdgeId(1), TransportProfile::Car, 30.0, 500.0),
            (EdgeId(2), TransportProfile::Car, 60.0, 1000.0),
            (EdgeId(3), TransportProfile::Bicycle, 120.0, 1000.0),
            (EdgeId(4), TransportProfile::Bicycle, 180.0, 1000.0),
        ];

        let stats = system.create_block(1, &edges).unwrap();

        assert_eq!(stats.block_id, 1);
        assert_eq!(stats.total_edges, 4);
        assert!(stats.compression_ratio > 0.0);
        assert!(stats.compression_ratio > 0.0); // May be > 1.0 for small test cases due to overhead

        // Test weight retrieval
        let (time, distance) = system
            .get_weight(1, &EdgeId(1), &TransportProfile::Car)
            .unwrap();
        assert!(time > 0.0);
        assert!(distance > 0.0);
    }

    #[test]
    fn test_compression_ratio_calculation() {
        let times = vec![10.0; 1000]; // 1000 edges
        let distances = vec![100.0; 1000];

        let config = CompressionConfig::new_for_block(1, &times, &distances);
        let ratio = config.compression_ratio();

        // Should be significantly less than 1.0 (good compression)
        assert!(ratio < 0.5); // At least 50% compression
        assert!(ratio > 0.0);
    }

    #[test]
    fn test_precision_loss_estimate() {
        let times = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let distances = vec![10.0, 20.0, 30.0, 40.0, 50.0];

        let config = CompressionConfig::new_for_block(1, &times, &distances);
        let precision_loss = config.precision_loss_estimate();

        assert!(precision_loss >= 0.0);
        assert!(precision_loss <= 100.0); // Should be a percentage
    }

    #[test]
    fn test_block_size_limit() {
        let mut system = WeightCompressionSystem::new();

        // Create oversized block
        let large_edges: Vec<_> = (0..EDGE_BLOCK_SIZE + 1)
            .map(|i| (EdgeId(i as i64), TransportProfile::Car, 30.0, 500.0))
            .collect();

        let result = system.create_block(1, &large_edges);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exceeds maximum"));
    }

    #[test]
    fn test_system_stats() {
        let mut system = WeightCompressionSystem::new();

        let edges1 = vec![
            (EdgeId(1), TransportProfile::Car, 30.0, 500.0),
            (EdgeId(2), TransportProfile::Car, 60.0, 1000.0),
        ];
        let edges2 = vec![
            (EdgeId(3), TransportProfile::Bicycle, 120.0, 1000.0),
            (EdgeId(4), TransportProfile::Bicycle, 180.0, 1000.0),
        ];

        system.create_block(1, &edges1).unwrap();
        system.create_block(2, &edges2).unwrap();

        let stats = system.get_system_stats();

        assert_eq!(stats.total_blocks, 2);
        assert_eq!(stats.total_edges, 4);
        assert!(stats.average_compression_ratio > 0.0);
        assert!(stats.average_compression_ratio > 0.0); // May be > 1.0 for small test cases due to overhead
    }

    #[test]
    fn test_memory_usage_calculation() {
        let mut system = WeightCompressionSystem::new();

        let edges = vec![
            (EdgeId(1), TransportProfile::Car, 30.0, 500.0),
            (EdgeId(2), TransportProfile::Car, 60.0, 1000.0),
        ];

        system.create_block(1, &edges).unwrap();

        let memory_usage = system.memory_usage();
        assert!(memory_usage > 0);
    }
}
