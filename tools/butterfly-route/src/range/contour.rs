//! Isochrone Contour Types and Export
//!
//! Shared types used across isochrone geometry pipeline:
//! - `ContourResult`: outer ring + holes polygon
//! - `ContourStats`: generation statistics
//! - `export_contour_geojson`: write polygon to GeoJSON file

use anyhow::Result;

/// Contour polygon result
#[derive(Debug)]
pub struct ContourResult {
    /// Outer ring coordinates (lon, lat pairs)
    pub outer_ring: Vec<(f64, f64)>,
    /// Hole rings (if any)
    pub holes: Vec<Vec<(f64, f64)>>,
    /// Statistics
    pub stats: ContourStats,
}

#[derive(Debug, Default)]
pub struct ContourStats {
    pub input_segments: usize,
    pub grid_cols: usize,
    pub grid_rows: usize,
    pub filled_cells: usize,
    pub contour_vertices_before_simplify: usize,
    pub contour_vertices_after_simplify: usize,
    pub elapsed_ms: u64,
}

/// Export contour to GeoJSON
pub fn export_contour_geojson(result: &ContourResult, output_path: &std::path::Path) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(output_path)?;

    write!(
        file,
        r#"{{"type": "Feature", "geometry": {{"type": "Polygon", "coordinates": [["#
    )?;

    for (i, &(lon, lat)) in result.outer_ring.iter().enumerate() {
        if i > 0 {
            write!(file, ",")?;
        }
        write!(file, "[{:.7}, {:.7}]", lon, lat)?;
    }

    // Close the ring
    if let Some(&(lon, lat)) = result.outer_ring.first() {
        write!(file, ",[{:.7}, {:.7}]", lon, lat)?;
    }

    writeln!(
        file,
        r#"]]}}, "properties": {{"vertices": {}, "cells": {}, "segments": {}}}}}"#,
        result.stats.contour_vertices_after_simplify,
        result.stats.filled_cells,
        result.stats.input_segments
    )?;

    Ok(())
}
