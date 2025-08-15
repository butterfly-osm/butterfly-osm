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
    pub rocksdb_cache_mb: usize,
    pub direct_io: bool,
    // Batching configuration
    pub batch_size: usize,
    pub batch_memory_limit_mb: usize,
    pub lru_cache_size: usize,
    // I/O configuration
    pub multiget_readahead_mb: usize,
    pub pbf_compression_level: u32,
    pub pbf_block_size_kb: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            grid_size_m: 5.0,
            highway_tags: Preset::Car.highway_tags(),
            keep_turn_restrictions: true,
            num_workers: num_cpus::get().min(8),
            rocksdb_cache_mb: 128,
            direct_io: false,
            // Optimal batch configuration based on testing
            batch_size: 50_000,                   // 50k ways per batch
            batch_memory_limit_mb: 150,           // 150MB max memory usage per batch
            lru_cache_size: 100_000,              // 100k node mappings in cache
            // I/O optimization settings
            multiget_readahead_mb: 4,             // 4MB readahead for MultiGet
            pbf_compression_level: 6,             // Default zlib compression
            pbf_block_size_kb: 64,                // 64KB PBF blocks
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
}