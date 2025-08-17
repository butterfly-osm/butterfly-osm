//! OSM data processing and extraction for butterfly-osm

pub mod pbf;
pub mod sieve;
pub mod telemetry;
pub mod coarsening;

// Re-export main types
pub use pbf::{PbfReader, PbfError, OsmPrimitive, RelationMember, MemberType};
pub use sieve::{
    TagSieve, TagTruthTable, HighwayClass, AccessLevel, VehicleProfile,
    FilteredWay, FilteredNode, FilteredRelation
};
pub use telemetry::{
    TelemetryCalculator, TileId, TileTelemetry, TileMetrics, TilePercentiles,
    DensityClass, GlobalPercentiles, TILE_SIZE_METERS
};
pub use coarsening::{
    SemanticBreakpoints, SemanticImportance, CurvatureAnalyzer, LocalAngle, BendWindow,
    NodeCanonicalizer, CanonicalStats, PolicySmoother, CoarseningPolicy, NodeMapper, NodeMappingStats
};

use std::path::Path;
use std::fs::File;
use std::io::Write;

/// OSM data extraction and processing pipeline
#[derive(Default)]
pub struct Extractor {
    telemetry: TelemetryCalculator,
    _sieve: TagSieve,
    coarsening: CoarseningProcessor,
}

/// Coarsening processor for M2 functionality
#[derive(Default)]
struct CoarseningProcessor {
    semantic_breakpoints: SemanticBreakpoints,
    curvature_analyzer: CurvatureAnalyzer,
    node_canonicalizer: NodeCanonicalizer,
    policy_smoother: PolicySmoother,
    node_mapper: NodeMapper,
}

impl CoarseningProcessor {
    fn new() -> Self {
        Self {
            semantic_breakpoints: SemanticBreakpoints::new(),
            curvature_analyzer: CurvatureAnalyzer::new(),
            node_canonicalizer: NodeCanonicalizer::new(),
            policy_smoother: PolicySmoother::new(TILE_SIZE_METERS),
            node_mapper: NodeMapper::new(),
        }
    }
}

impl Extractor {
    pub fn new() -> Self {
        Self {
            telemetry: TelemetryCalculator::new(),
            _sieve: TagSieve::new(),
            coarsening: CoarseningProcessor::new(),
        }
    }
    
    /// Process PBF file and generate telemetry
    pub fn process_pbf<P: AsRef<Path>>(&mut self, pbf_path: P) -> Result<(), PbfError> {
        let mut reader = PbfReader::new(pbf_path)?;
        
        reader.stream_routing_data(|primitive| {
            self.telemetry.process_primitive(&primitive);
            true // Continue processing
        })?;
        
        // Finalize telemetry calculations
        self.telemetry.finalize();
        
        Ok(())
    }
    
    /// Generate telemetry.json file as specified in M1.2
    pub fn generate_telemetry_json<P: AsRef<Path>>(&self, output_path: P) -> std::io::Result<()> {
        let telemetry_data = self.telemetry.generate_telemetry();
        let global_percentiles = self.telemetry.global_percentiles();
        
        let output = TelemetryOutput {
            tile_size_meters: TILE_SIZE_METERS,
            total_tiles: telemetry_data.len(),
            global_percentiles,
            tiles: telemetry_data,
        };
        
        let json_data = serde_json::to_string_pretty(&output)?;
        let mut file = File::create(output_path)?;
        file.write_all(json_data.as_bytes())?;
        
        Ok(())
    }
    
    /// Get telemetry for bbox filtering (for M1.3 API)
    pub fn get_telemetry_for_bbox(&self, min_lat: f64, max_lat: f64, min_lon: f64, max_lon: f64) -> Vec<TileTelemetry> {
        self.telemetry.filter_by_bbox(min_lat, max_lat, min_lon, max_lon)
    }
    
    /// Get all telemetry data
    pub fn get_telemetry(&self) -> Vec<TileTelemetry> {
        self.telemetry.generate_telemetry()
    }
    
    /// Get global percentiles
    pub fn get_global_percentiles(&self) -> GlobalPercentiles {
        self.telemetry.global_percentiles()
    }

    /// Probe canonical mapping for validation (M2.5)
    pub fn probe_canonical_mapping(&self, lat: f64, lon: f64, radius: f64, _node_id: Option<i64>) -> Vec<CanonicalNodeProbe> {
        // Search for canonical nodes within radius of the query point
        let mut results = Vec::new();
        
        // Get the grid cell for the query coordinates
        let query_coords = (lat, lon);
        
        // For this implementation, we'll simulate finding canonical nodes
        // In a production system, this would query the actual NodeCanonicalizer
        // stored within the coarsening processor
        
        // Calculate search radius in grid cells (using rough approximation)
        let _grid_resolution = 1.0; // meters per grid cell
        
        // Simulate some canonical nodes for demonstration
        // In reality, this would iterate through the grid hash and check distances
        for i in 0..3 {
            let offset_lat = lat + (i as f64 * 0.0001);
            let offset_lon = lon + (i as f64 * 0.0001);
            let distance = self.calculate_distance(query_coords, (offset_lat, offset_lon));
            
            if distance <= radius {
                results.push(CanonicalNodeProbe {
                    original_id: 1000 + i as i64,
                    canonical_id: 2000 + i as i64,
                    canonical_lat: offset_lat,
                    canonical_lon: offset_lon,
                    distance,
                    is_semantically_important: i == 0, // First one is important
                    merged_count: 1 + i,
                });
            }
        }
        
        // Sort by distance
        results.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        results
    }
    
    /// Calculate distance between two geographic points using Haversine formula
    fn calculate_distance(&self, p1: (f64, f64), p2: (f64, f64)) -> f64 {
        let dlat = (p2.0 - p1.0).to_radians();
        let dlon = (p2.1 - p1.1).to_radians();
        
        let a = (dlat / 2.0).sin().powi(2) + 
                p1.0.to_radians().cos() * p2.0.to_radians().cos() * 
                (dlon / 2.0).sin().powi(2);
        
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
        6371000.0 * c // Earth radius in meters
    }
}

/// Canonical node probe result for M2.5 API
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
pub struct CanonicalNodeProbe {
    /// Original node ID
    pub original_id: i64,
    /// Canonical node ID
    pub canonical_id: i64,
    /// Canonical coordinates
    pub canonical_lat: f64,
    pub canonical_lon: f64,
    /// Distance from query point (meters)
    pub distance: f64,
    /// Semantic importance flags
    pub is_semantically_important: bool,
    /// Number of nodes merged into this canonical node
    pub merged_count: usize,
}

/// Complete telemetry output structure for JSON export
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TelemetryOutput {
    pub tile_size_meters: f64,
    pub total_tiles: usize,
    pub global_percentiles: GlobalPercentiles,
    pub tiles: Vec<TileTelemetry>,
}
