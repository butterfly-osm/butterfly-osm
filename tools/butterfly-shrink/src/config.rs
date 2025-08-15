//! Configuration management for butterfly-shrink

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Highway presets for different routing profiles
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Preset {
    Car,
    Bike,
    Foot,
}

impl Preset {
    pub fn highway_tags(&self) -> HashSet<String> {
        let tags = match self {
            Preset::Car => vec![
                // Main road hierarchy
                "motorway",
                "motorway_link", 
                "trunk",
                "trunk_link",
                "primary",
                "primary_link",
                "secondary", 
                "secondary_link",
                "tertiary",
                "tertiary_link",
                "unclassified",
                "residential",
                "living_street",
                "service",
                // Special road types accessible to cars
                "road",          // Generic road when classification unknown
                "escape",        // Escape lanes
                "busway",        // If cars allowed
                // Under construction but potentially accessible
                "construction", // Construction roads that may be passable
            ],
            Preset::Bike => {
                let tags = vec![
                    // All car-accessible roads
                    "motorway", "motorway_link", "trunk", "trunk_link",
                    "primary", "primary_link", "secondary", "secondary_link", 
                    "tertiary", "tertiary_link", "unclassified", "residential",
                    "living_street", "service", "road", "construction",
                    // Bike-specific infrastructure
                    "cycleway",      // Dedicated bike lanes
                    "track",         // Unpaved tracks often accessible to bikes
                    "path",          // Multi-use paths
                    "bridleway",     // Often allow bikes
                    "bus_guideway",  // Some allow bikes
                    "pedestrian",    // Pedestrian areas where bikes may be allowed
                    "escape",        // Emergency lanes
                    "busway",        // Bus lanes that may allow bikes
                ];
                tags
            }
            Preset::Foot => {
                vec![
                    // All previously accessible ways
                    "motorway", "motorway_link", "trunk", "trunk_link",
                    "primary", "primary_link", "secondary", "secondary_link",
                    "tertiary", "tertiary_link", "unclassified", "residential", 
                    "living_street", "service", "road", "construction",
                    "cycleway", "track", "path", "bridleway", "bus_guideway",
                    "pedestrian", "escape", "busway",
                    // Pedestrian-specific infrastructure
                    "footway",       // Dedicated footpaths
                    "steps",         // Stairs
                    "corridor",      // Indoor corridors
                    "via_ferrata",   // Mountain climbing paths
                    // Lifecycle states
                    "proposed",      // Proposed ways that may be walkable
                ]
            }
        };
        
        tags.into_iter()
            .map(String::from)
            .collect()
    }
}

/// YAML configuration file format
#[derive(Debug, Deserialize, Serialize)]
pub struct YamlConfig {
    pub version: u32,
    pub grid_size_m: Option<f64>,
    pub highway_tags: Option<HighwayConfig>,
    pub restrictions: Option<RestrictionConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct HighwayConfig {
    pub include: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RestrictionConfig {
    pub keep_turn_restrictions: bool,
}

/// Runtime configuration
#[derive(Debug, Clone)]
pub struct Config {
    pub grid_size_m: f64,
    pub highway_tags: HashSet<String>,
    pub keep_turn_restrictions: bool,
    pub num_workers: usize,
    
    // RocksDB configuration
    pub db_cache_mb: usize,               // RocksDB block cache size
    pub direct_io: bool,
    pub compact_after_nodes: bool,        // Whether to compact at phase boundary
    
    // Batching configuration
    pub batch_ways: usize,                // Max ways per batch
    pub batch_memory_limit_mb: usize,     // Max memory per batch
    pub batch_unique_nodes: usize,        // Max unique node IDs per batch
    
    // LRU cache configuration
    pub cache_mb: usize,                  // In-memory LRU cache size in MB
    pub lru_cache_size: usize,            // Number of entries (calculated from cache_mb)
    
    // I/O configuration
    pub multiget_readahead_mb: usize,     // Readahead for MultiGet operations
    pub zstd_level: u32,                  // Zstd compression level (1-22)
    pub pbf_block_size_kb: usize,         // PBF output block size
    
    // Tile bucketing (optional)
    pub enable_tile_bucketing: bool,      // Enable spatial tile bucketing for ways
    pub tile_grid_degrees: f64,           // Tile size in degrees (e.g., 0.1)
    pub max_tiles_in_memory: usize,       // Max tiles to keep in memory
    
    // Autotuning
    pub enable_autotuning: bool,          // Enable batch size autotuning
    pub autotune_interval: usize,         // Batches between autotune checks
}

impl Default for Config {
    fn default() -> Self {
        let cache_mb = 128; // Default 128MB for LRU cache
        let entry_size = 24; // ~24 bytes per entry (8 byte key + 8 byte value + overhead)
        let lru_cache_size = (cache_mb * 1024 * 1024) / entry_size;
        
        Self {
            grid_size_m: 5.0,
            highway_tags: Preset::Car.highway_tags(),
            keep_turn_restrictions: true,
            num_workers: num_cpus::get().min(8),
            
            // RocksDB configuration
            db_cache_mb: 128,                     // 128MB RocksDB block cache
            direct_io: true,                      // Use direct I/O for better control
            compact_after_nodes: true,            // Always compact at phase boundary
            
            // Batching configuration
            batch_ways: 50_000,                   // 50k ways per batch
            batch_memory_limit_mb: 150,           // 150MB max memory per batch
            batch_unique_nodes: 1_500_000,        // 1.5M unique nodes max
            
            // LRU cache configuration
            cache_mb,                              // 128MB LRU cache
            lru_cache_size,                        // Calculated from cache_mb
            
            // I/O optimization
            multiget_readahead_mb: 4,             // 4MB readahead for MultiGet
            zstd_level: 6,                         // Zstd level 6 (good balance)
            pbf_block_size_kb: 256,               // 256KB blocks - good balance for most sizes
            
            // Tile bucketing disabled by default
            enable_tile_bucketing: false,
            tile_grid_degrees: 0.1,               // 0.1° grid (~11km at equator)
            max_tiles_in_memory: 32,              // Keep up to 32 tiles
            
            // Autotuning enabled by default
            enable_autotuning: true,
            autotune_interval: 10,                // Check every 10 batches
        }
    }
}

impl Config {
    pub fn from_yaml(yaml: &YamlConfig) -> Self {
        let mut config = Self::default();
        
        if let Some(grid_size) = yaml.grid_size_m {
            config.grid_size_m = grid_size;
        }
        
        if let Some(highway_config) = &yaml.highway_tags {
            config.highway_tags = highway_config.include.iter().cloned().collect();
        }
        
        if let Some(restriction_config) = &yaml.restrictions {
            config.keep_turn_restrictions = restriction_config.keep_turn_restrictions;
        }
        
        config
    }
    
    pub fn apply_preset(&mut self, preset: Preset) {
        self.highway_tags = preset.highway_tags();
    }
    
    /// Set cache size in MB and recalculate entries
    pub fn set_cache_mb(&mut self, cache_mb: usize) {
        self.cache_mb = cache_mb;
        let entry_size = 24; // ~24 bytes per entry
        self.lru_cache_size = (cache_mb * 1024 * 1024) / entry_size;
    }
    
    /// Validate configuration and print warnings
    pub fn validate(&self) {
        // Warn about small grid sizes with large inputs
        if self.grid_size_m <= 1.0 {
            log::warn!("Grid size {}m is very small. Consider using 5m for continent-scale data.", self.grid_size_m);
        }
        
        // Check memory constraints
        let total_memory = self.cache_mb + self.db_cache_mb + self.batch_memory_limit_mb;
        if total_memory > 500 {
            log::info!("Total memory usage: {}MB (cache: {}MB, db: {}MB, batch: {}MB)",
                total_memory, self.cache_mb, self.db_cache_mb, self.batch_memory_limit_mb);
        }
        
        // Validate compression level
        if self.zstd_level > 22 {
            log::warn!("Zstd level {} is invalid (max 22), using 22", self.zstd_level);
        }
    }
}