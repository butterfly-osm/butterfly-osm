//! Isochrone Contour Types and Export
//!
//! Shared types used across isochrone geometry pipeline:
//! - `ContourResult`: outer ring + holes polygon
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

/// Contour polygon result.
///
/// `outer_ring` + `holes` describe the PRIMARY polygon — the component that
/// contains the query origin (#497), or the largest one. Any further
/// reachable components (a village reached over a fast road whose
/// surroundings do not raster-connect to the main blob) live in `extra`;
/// together they form a MultiPolygon. Before 2026-09-03 both `holes` and the
/// extra components were silently dropped, so unreachable pockets were
/// filled in and detached reach vanished from the drawn shape.
#[derive(Debug, Default)]
pub struct ContourResult {
    /// Outer ring coordinates (lon, lat pairs)
    pub outer_ring: Vec<(f64, f64)>,
    /// Hole rings (if any)
    pub holes: Vec<Vec<(f64, f64)>>,
    /// Additional disconnected components (MultiPolygon parts after the primary).
    pub extra: Vec<ContourPolygon>,
    /// Statistics
    pub stats: ContourStats,
}

impl ContourResult {
    /// Build from an ordered polygon list (primary first), no stats.
    pub fn from_polygons(mut polygons: Vec<ContourPolygon>) -> Self {
        if polygons.is_empty() {
            return Self::default();
        }
        let first = polygons.remove(0);
        Self {
            outer_ring: first.outer,
            holes: first.holes,
            extra: polygons,
            stats: ContourStats::default(),
        }
    }

    /// Every polygon of the result, primary first.
    pub fn polygons(&self) -> impl Iterator<Item = ContourPolygon> + '_ {
        std::iter::once(ContourPolygon {
            outer: self.outer_ring.clone(),
            holes: self.holes.clone(),
        })
        .chain(self.extra.iter().cloned())
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

/// Export contour to GeoJSON
pub fn export_contour_geojson(result: &ContourResult, output_path: &std::path::Path) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(output_path)?;

    // Closed rings, every polygon (primary + extra), holes included.
    let ring_json = |ring: &[(f64, f64)]| -> String {
        let mut s = String::with_capacity(ring.len() * 24 + 24);
        s.push('[');
        for (i, &(lon, lat)) in ring.iter().enumerate() {
            if i > 0 {
                s.push(',');
            }
            s.push_str(&format!("[{:.7}, {:.7}]", lon, lat));
        }
        if let Some(&(lon, lat)) = ring.first() {
            s.push_str(&format!(",[{:.7}, {:.7}]", lon, lat));
        }
        s.push(']');
        s
    };
    let poly_json = |p: &ContourPolygon| -> String {
        let mut s = String::from("[");
        s.push_str(&ring_json(&p.outer));
        for h in &p.holes {
            s.push(',');
            s.push_str(&ring_json(h));
        }
        s.push(']');
        s
    };
    let polys: Vec<ContourPolygon> = result.polygons().collect();
    if polys.len() == 1 {
        write!(
            file,
            r#"{{"type": "Feature", "geometry": {{"type": "Polygon", "coordinates": {}}}"#,
            poly_json(&polys[0])
        )?;
    } else {
        let parts: Vec<String> = polys.iter().map(poly_json).collect();
        write!(
            file,
            r#"{{"type": "Feature", "geometry": {{"type": "MultiPolygon", "coordinates": [{}]}}"#,
            parts.join(",")
        )?;
    }

    writeln!(
        file,
        r#", "properties": {{"vertices": {}, "cells": {}, "segments": {}}}}}"#,
        result.stats.contour_vertices_after_simplify,
        result.stats.filled_cells,
        result.stats.input_segments
    )?;

    Ok(())
}
