//! Isochrone Contour Types and Export
//!
//! Shared types used across isochrone geometry pipeline:
//! - `ContourPolygon`: the tracer/topology view of one polygon
//! - `ContourResult`: the SERVED contour — one simple ring
//! - `ContourStats`: generation statistics
//! - `export_contour_geojson`: write polygon to GeoJSON file

use anyhow::Result;

/// One polygon of a contour: an outer ring plus its holes. Coordinates are
/// whatever space the producer works in (cell units inside the sparse
/// tracer, WGS84 `(lon, lat)` once emitted).
#[derive(Debug, Clone, Default)]
pub struct ContourPolygon {
    pub outer: Vec<(f64, f64)>,
    pub holes: Vec<Vec<(f64, f64)>>,
}

/// The contour as it is SERVED: ONE simple ring.
///
/// An isochrone (or a catchment lasso) is one simple polygon by definition
/// (#535/#542, product rule 2026-09-03) — never holed, never a MultiPolygon.
/// Since #570 that rule is this type: there is no second component to drop
/// and no hole to fill downstream, so `/isochrone`, `/isochrone/bulk`, the
/// Flight `isochrone` action and the catchment hulls cannot disagree about
/// what "the polygon" is.
#[derive(Debug, Default)]
pub struct ContourResult {
    /// Outer ring coordinates (lon, lat pairs), open — the encoders close it.
    pub ring: Vec<(f64, f64)>,
    /// Statistics
    pub stats: ContourStats,
}

impl ContourResult {
    /// The served contour of a traced topology: its primary polygon's outer
    /// ring. The tracer emits at most one polygon and never a hole (#570),
    /// so this is a projection onto the served surface, not a filter.
    pub fn from_topology(topology: impl IntoIterator<Item = ContourPolygon>) -> Self {
        Self {
            ring: topology
                .into_iter()
                .next()
                .map(|p| p.outer)
                .unwrap_or_default(),
            stats: ContourStats::default(),
        }
    }
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

/// Export contour to GeoJSON (a Polygon, always — #570)
pub fn export_contour_geojson(result: &ContourResult, output_path: &std::path::Path) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(output_path)?;

    // One closed ring: a Polygon, always (#570).
    let mut ring = String::with_capacity(result.ring.len() * 24 + 24);
    ring.push('[');
    for (i, &(lon, lat)) in result.ring.iter().enumerate() {
        if i > 0 {
            ring.push(',');
        }
        ring.push_str(&format!("[{:.7}, {:.7}]", lon, lat));
    }
    if let Some(&(lon, lat)) = result.ring.first() {
        ring.push_str(&format!(",[{:.7}, {:.7}]", lon, lat));
    }
    ring.push(']');

    write!(
        file,
        r#"{{"type": "Feature", "geometry": {{"type": "Polygon", "coordinates": [{}]}}"#,
        ring
    )?;

    writeln!(
        file,
        r#", "properties": {{"vertices": {}, "cells": {}, "segments": {}}}}}"#,
        result.stats.contour_vertices_after_simplify,
        result.stats.filled_cells,
        result.stats.input_segments
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::range::wkb_stream::encode_polygon_wkb;

    /// The server-side topology type still *permits* several polygons with
    /// holes; the SERVED contour is the primary polygon's outer ring alone,
    /// and nothing else can reach the encoder (#570).
    #[test]
    fn from_topology_serves_only_the_primary_ring() {
        let primary = ContourPolygon {
            outer: vec![(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)],
            holes: vec![vec![(0.5, 0.5), (1.5, 0.5), (1.5, 1.5), (0.5, 1.5)]],
        };
        let detached = ContourPolygon {
            outer: vec![(10.0, 10.0), (11.0, 10.0), (11.0, 11.0)],
            holes: vec![],
        };
        let served = ContourResult::from_topology(vec![primary.clone(), detached]);
        assert_eq!(served.ring, primary.outer, "the primary component's ring");

        let wkb = encode_polygon_wkb(&served).expect("non-empty contour encodes");
        assert_eq!(
            u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]]),
            3,
            "Polygon (3), never MultiPolygon (6)"
        );
        assert_eq!(
            u32::from_le_bytes([wkb[5], wkb[6], wkb[7], wkb[8]]),
            1,
            "exactly one ring: no hole may reach the encoder"
        );

        assert!(ContourResult::from_topology(vec![]).ring.is_empty());
    }

    #[test]
    fn export_contour_geojson_writes_one_closed_polygon() {
        let served = ContourResult::from_topology(vec![ContourPolygon {
            outer: vec![(4.0, 50.0), (4.1, 50.0), (4.1, 50.1), (4.0, 50.1)],
            holes: vec![vec![(4.02, 50.02), (4.08, 50.02), (4.08, 50.08)]],
        }]);
        let path = std::env::temp_dir().join(format!(
            "butterfly_contour_{}_{}.geojson",
            std::process::id(),
            line!()
        ));
        export_contour_geojson(&served, &path).expect("write");
        let json = std::fs::read_to_string(&path).expect("read back");
        let _ = std::fs::remove_file(&path);

        assert!(json.contains(r#""type": "Polygon""#), "{json}");
        assert!(!json.contains("MultiPolygon"), "{json}");
        // One ring only, and it is closed (first vertex repeated last).
        assert_eq!(json.matches("[[").count(), 1, "a single ring: {json}");
        assert_eq!(json.matches("]]").count(), 1, "a single ring: {json}");
        assert_eq!(
            json.matches("[4.0000000, 50.0000000]").count(),
            2,
            "ring must be closed: {json}"
        );
    }
}
