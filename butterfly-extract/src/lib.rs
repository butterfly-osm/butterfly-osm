//! OSM data processing and extraction for butterfly-osm

pub mod pbf;
pub mod sieve;
pub mod telemetry;

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

use std::path::Path;
use std::fs::File;
use std::io::Write;

/// OSM data extraction and processing pipeline
#[derive(Default)]
pub struct Extractor {
    telemetry: TelemetryCalculator,
    _sieve: TagSieve,
}

impl Extractor {
    pub fn new() -> Self {
        Self {
            telemetry: TelemetryCalculator::new(),
            _sieve: TagSieve::new(),
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
}

/// Complete telemetry output structure for JSON export
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TelemetryOutput {
    pub tile_size_meters: f64,
    pub total_tiles: usize,
    pub global_percentiles: GlobalPercentiles,
    pub tiles: Vec<TileTelemetry>,
}
