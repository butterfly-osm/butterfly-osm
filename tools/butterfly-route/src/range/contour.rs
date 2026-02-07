//! Grid-based Isochrone Contour Generation
//!
//! Pipeline:
//! 1. Project reachable segments to metric (Web Mercator)
//! 2. Build boolean raster grid
//! 3. Stamp road segments onto grid (line rasterization)
//! 4. Morphological closing (dilate then erode) to fill gaps
//! 5. Marching squares to extract contour
//! 6. Simplify polygon (Douglas-Peucker)

use anyhow::Result;

use super::frontier::ReachableSegment;

/// Grid cell size in meters
#[derive(Debug, Clone, Copy)]
pub struct GridConfig {
    /// Cell size in meters
    pub cell_size_m: f64,
    /// Simplification tolerance in meters
    pub simplify_tolerance_m: f64,
    /// Morphological closing iterations (0 = no closing)
    pub closing_iterations: usize,
}

impl GridConfig {
    pub fn for_car() -> Self {
        Self { cell_size_m: 100.0, simplify_tolerance_m: 75.0, closing_iterations: 1 }
    }

    pub fn for_bike() -> Self {
        Self { cell_size_m: 50.0, simplify_tolerance_m: 50.0, closing_iterations: 1 }
    }

    pub fn for_foot() -> Self {
        Self { cell_size_m: 25.0, simplify_tolerance_m: 25.0, closing_iterations: 1 }
    }
}

/// A point in Web Mercator meters
#[derive(Debug, Clone, Copy)]
struct MercatorPoint {
    x: f64,
    y: f64,
}

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

/// Convert WGS84 to Web Mercator
fn to_mercator(lat: f64, lon: f64) -> MercatorPoint {
    const EARTH_RADIUS: f64 = 6378137.0;
    let x = lon.to_radians() * EARTH_RADIUS;
    let y = ((std::f64::consts::PI / 4.0) + (lat.to_radians() / 2.0)).tan().ln() * EARTH_RADIUS;
    MercatorPoint { x, y }
}

/// Convert Web Mercator to WGS84
fn from_mercator(x: f64, y: f64) -> (f64, f64) {
    const EARTH_RADIUS: f64 = 6378137.0;
    let lon = (x / EARTH_RADIUS).to_degrees();
    let lat = (2.0 * (y / EARTH_RADIUS).exp().atan() - std::f64::consts::PI / 2.0).to_degrees();
    (lon, lat)
}

/// Generate isochrone contour from reachable road segments
pub fn generate_contour(
    segments: &[ReachableSegment],
    config: &GridConfig,
) -> Result<ContourResult> {
    let start = std::time::Instant::now();
    let mut stats = ContourStats { input_segments: segments.len(), ..Default::default() };

    if segments.is_empty() {
        return Ok(ContourResult {
            outer_ring: vec![],
            holes: vec![],
            stats,
        });
    }

    // Step 1: Project all segment points to Mercator and find bounding box
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    let mercator_segments: Vec<Vec<MercatorPoint>> = segments
        .iter()
        .map(|seg| {
            seg.points
                .iter()
                .map(|&(lat_fxp, lon_fxp)| {
                    let lat = lat_fxp as f64 / 1e7;
                    let lon = lon_fxp as f64 / 1e7;
                    let pt = to_mercator(lat, lon);
                    min_x = min_x.min(pt.x);
                    max_x = max_x.max(pt.x);
                    min_y = min_y.min(pt.y);
                    max_y = max_y.max(pt.y);
                    pt
                })
                .collect()
        })
        .collect();

    // Add margin (5 cells on each side to ensure border is exterior after closing)
    let margin = config.cell_size_m * 5.0;
    min_x -= margin;
    max_x += margin;
    min_y -= margin;
    max_y += margin;

    // Step 2: Create raster grid
    let n_cols = ((max_x - min_x) / config.cell_size_m).ceil() as usize + 1;
    let n_rows = ((max_y - min_y) / config.cell_size_m).ceil() as usize + 1;
    stats.grid_cols = n_cols;
    stats.grid_rows = n_rows;

    let mut raster = vec![false; n_cols * n_rows];

    // Step 3: Stamp segments onto grid using line rasterization
    for seg in &mercator_segments {
        for window in seg.windows(2) {
            let p0 = &window[0];
            let p1 = &window[1];
            stamp_line(&mut raster, n_cols, n_rows, min_x, min_y, config.cell_size_m, p0, p1);
        }
        // Also stamp individual points (for single-point segments)
        for pt in seg {
            let col = ((pt.x - min_x) / config.cell_size_m).floor() as i32;
            let row = ((pt.y - min_y) / config.cell_size_m).floor() as i32;
            if col >= 0 && row >= 0 && (col as usize) < n_cols && (row as usize) < n_rows {
                raster[row as usize * n_cols + col as usize] = true;
            }
        }
    }

    // Step 4: Morphological operations to create filled regions from road segments
    // Roads are linear features - we need significant dilation to connect them into areas
    // Use asymmetric closing: many dilations, fewer erosions
    let mut closed = raster.clone();
    let dilation_rounds = config.closing_iterations.max(3); // At least 3 dilations
    let erosion_rounds = config.closing_iterations;

    for _ in 0..dilation_rounds {
        closed = dilate(&closed, n_cols, n_rows);
    }
    for _ in 0..erosion_rounds {
        closed = erode(&closed, n_cols, n_rows);
    }

    stats.filled_cells = closed.iter().filter(|&&b| b).count();

    // Step 5: Marching squares to extract contour
    let contour = marching_squares(&closed, n_cols, n_rows);
    stats.contour_vertices_before_simplify = contour.len();

    if contour.is_empty() {
        return Ok(ContourResult {
            outer_ring: vec![],
            holes: vec![],
            stats,
        });
    }

    // Step 6: Convert grid coordinates back to WGS84
    let mut wgs84_contour: Vec<(f64, f64)> = contour
        .iter()
        .map(|&(col, row)| {
            let x = min_x + col * config.cell_size_m;
            let y = min_y + row * config.cell_size_m;
            from_mercator(x, y)
        })
        .collect();

    // Step 7: Simplify polygon
    let tolerance_deg = config.simplify_tolerance_m / 111000.0; // Rough conversion
    wgs84_contour = douglas_peucker(&wgs84_contour, tolerance_deg);
    stats.contour_vertices_after_simplify = wgs84_contour.len();

    stats.elapsed_ms = start.elapsed().as_millis() as u64;

    Ok(ContourResult {
        outer_ring: wgs84_contour,
        holes: vec![],
        stats,
    })
}

/// Stamp a line segment onto the raster grid using Bresenham's algorithm
#[allow(clippy::too_many_arguments)]
fn stamp_line(
    raster: &mut [bool],
    n_cols: usize,
    n_rows: usize,
    min_x: f64,
    min_y: f64,
    cell_size: f64,
    p0: &MercatorPoint,
    p1: &MercatorPoint,
) {
    let col0 = ((p0.x - min_x) / cell_size).floor() as i32;
    let row0 = ((p0.y - min_y) / cell_size).floor() as i32;
    let col1 = ((p1.x - min_x) / cell_size).floor() as i32;
    let row1 = ((p1.y - min_y) / cell_size).floor() as i32;

    // Bresenham's line algorithm
    let dx = (col1 - col0).abs();
    let dy = (row1 - row0).abs();
    let sx = if col0 < col1 { 1 } else { -1 };
    let sy = if row0 < row1 { 1 } else { -1 };
    let mut err = dx - dy;

    let mut col = col0;
    let mut row = row0;

    loop {
        // Mark current cell
        if col >= 0 && row >= 0 && (col as usize) < n_cols && (row as usize) < n_rows {
            raster[row as usize * n_cols + col as usize] = true;
        }

        if col == col1 && row == row1 {
            break;
        }

        let e2 = 2 * err;
        if e2 > -dy {
            err -= dy;
            col += sx;
        }
        if e2 < dx {
            err += dx;
            row += sy;
        }
    }
}

/// Morphological dilation (grow by 1 cell)
fn dilate(raster: &[bool], n_cols: usize, n_rows: usize) -> Vec<bool> {
    let mut result = vec![false; n_cols * n_rows];

    for row in 0..n_rows {
        for col in 0..n_cols {
            let idx = row * n_cols + col;
            if raster[idx] {
                // Mark 3x3 neighborhood
                for dr in -1i32..=1 {
                    for dc in -1i32..=1 {
                        let nr = row as i32 + dr;
                        let nc = col as i32 + dc;
                        if nr >= 0 && nc >= 0 && (nr as usize) < n_rows && (nc as usize) < n_cols {
                            result[nr as usize * n_cols + nc as usize] = true;
                        }
                    }
                }
            }
        }
    }

    result
}

/// Morphological erosion (shrink by 1 cell)
fn erode(raster: &[bool], n_cols: usize, n_rows: usize) -> Vec<bool> {
    let mut result = vec![false; n_cols * n_rows];

    for row in 0..n_rows {
        for col in 0..n_cols {
            let idx = row * n_cols + col;
            if !raster[idx] {
                continue;
            }

            // Check if all neighbors are also set
            let mut all_neighbors_set = true;
            for dr in -1i32..=1 {
                for dc in -1i32..=1 {
                    let nr = row as i32 + dr;
                    let nc = col as i32 + dc;
                    if nr >= 0 && nc >= 0 && (nr as usize) < n_rows && (nc as usize) < n_cols {
                        if !raster[nr as usize * n_cols + nc as usize] {
                            all_neighbors_set = false;
                            break;
                        }
                    } else {
                        // Edge of grid counts as not set
                        all_neighbors_set = false;
                        break;
                    }
                }
                if !all_neighbors_set {
                    break;
                }
            }

            result[idx] = all_neighbors_set;
        }
    }

    result
}

/// Marching squares algorithm to extract OUTER contour from binary raster
fn marching_squares(raster: &[bool], n_cols: usize, n_rows: usize) -> Vec<(f64, f64)> {
    if n_cols < 2 || n_rows < 2 {
        return vec![];
    }

    // First, flood fill from corners to mark exterior cells
    // This ensures we find the OUTER boundary, not internal holes
    let mut exterior = vec![false; n_cols * n_rows];
    let mut stack: Vec<(usize, usize)> = Vec::new();

    // Start from all border cells that are not filled
    for col in 0..n_cols {
        if !raster[col] { stack.push((0, col)); }
        if !raster[(n_rows - 1) * n_cols + col] { stack.push((n_rows - 1, col)); }
    }
    for row in 0..n_rows {
        if !raster[row * n_cols] { stack.push((row, 0)); }
        if !raster[row * n_cols + n_cols - 1] { stack.push((row, n_cols - 1)); }
    }

    // Flood fill exterior
    while let Some((r, c)) = stack.pop() {
        let idx = r * n_cols + c;
        if exterior[idx] || raster[idx] {
            continue;
        }
        exterior[idx] = true;

        if r > 0 { stack.push((r - 1, c)); }
        if r + 1 < n_rows { stack.push((r + 1, c)); }
        if c > 0 { stack.push((r, c - 1)); }
        if c + 1 < n_cols { stack.push((r, c + 1)); }
    }

    // Create interior raster: filled OR not-exterior (to close internal holes)
    let interior: Vec<bool> = (0..n_cols * n_rows)
        .map(|i| raster[i] || !exterior[i])
        .collect();

    // Find a starting boundary cell on the OUTER boundary
    // Scan from outside inward to find where exterior meets interior
    let mut start_col = None;
    let mut start_row = None;
    let mut start_case = 0u8;

    'outer: for row in 0..n_rows - 1 {
        for col in 0..n_cols - 1 {
            let case = get_case(&interior, n_cols, col, row);
            // Only consider boundary cells that touch exterior
            if case != 0 && case != 15 {
                // Verify at least one corner is actually exterior
                let has_exterior = exterior[row * n_cols + col]
                    || exterior[row * n_cols + col + 1]
                    || exterior[(row + 1) * n_cols + col + 1]
                    || exterior[(row + 1) * n_cols + col];
                if has_exterior {
                    start_col = Some(col);
                    start_row = Some(row);
                    start_case = case;
                    break 'outer;
                }
            }
        }
    }

    let (start_col, start_row) = match (start_col, start_row) {
        (Some(c), Some(r)) => (c, r),
        _ => return vec![],
    };

    // Track visited edges to avoid infinite loops
    let mut visited = std::collections::HashSet::new();
    let mut contour = Vec::new();

    let mut col = start_col;
    let mut row = start_row;
    let mut entry_dir = determine_entry_direction(start_case);

    let max_iterations = n_cols * n_rows * 4;
    let mut iterations = 0;

    loop {
        iterations += 1;
        if iterations > max_iterations {
            break;
        }

        let case = get_case(&interior, n_cols, col, row);
        if case == 0 || case == 15 {
            break;
        }

        // Get edge crossing for this case
        let (edge_point, exit_dir) = get_edge_crossing(case, col, row, entry_dir);

        // Check if we've visited this edge
        let edge_key = (col, row, entry_dir);
        if visited.contains(&edge_key) {
            break;
        }
        visited.insert(edge_key);

        contour.push(edge_point);

        // Move to next cell
        let (next_col, next_row, next_entry) = match exit_dir {
            0 => (col + 1, row, 2),           // right -> enter from left
            1 => (col, row + 1, 3),           // up -> enter from bottom
            2 => (col.wrapping_sub(1), row, 0), // left -> enter from right
            3 => (col, row.wrapping_sub(1), 1), // down -> enter from top
            _ => break,
        };

        // Bounds check
        if next_col >= n_cols - 1 || next_row >= n_rows - 1 {
            break;
        }

        // Check if we're back at start with same entry
        if next_col == start_col && next_row == start_row && next_entry == determine_entry_direction(start_case) {
            break;
        }

        col = next_col;
        row = next_row;
        entry_dir = next_entry;
    }

    contour
}

/// Get marching squares case (0-15) for a 2x2 cell
/// Bits: 0=bottom-left, 1=bottom-right, 2=top-right, 3=top-left
fn get_case(raster: &[bool], n_cols: usize, col: usize, row: usize) -> u8 {
    let mut case = 0u8;
    if raster[row * n_cols + col] { case |= 1; }           // bottom-left
    if raster[row * n_cols + col + 1] { case |= 2; }       // bottom-right
    if raster[(row + 1) * n_cols + col + 1] { case |= 4; } // top-right
    if raster[(row + 1) * n_cols + col] { case |= 8; }     // top-left
    case
}

/// Determine initial entry direction based on case
fn determine_entry_direction(case: u8) -> u8 {
    match case {
        1 | 3 | 7 => 0,  // Enter from right
        2 | 6 | 14 => 1, // Enter from top
        4 | 12 | 13 => 2, // Enter from left
        8 | 9 | 11 => 3, // Enter from bottom
        5 => 0,  // Saddle point, enter from right
        10 => 1, // Saddle point, enter from top
        _ => 0,
    }
}

/// Get edge crossing point and exit direction for a marching squares case
/// Returns (edge_midpoint, exit_direction)
/// Directions: 0=right, 1=up, 2=left, 3=down
fn get_edge_crossing(case: u8, col: usize, row: usize, entry_dir: u8) -> ((f64, f64), u8) {
    let c = col as f64;
    let r = row as f64;

    // Standard marching squares edge crossings
    // Each case defines which edges are crossed and in what order
    match case {
        // Single corner cases
        1 => ((c, r + 0.5), 3),           // bottom-left only: left edge -> down
        2 => ((c + 0.5, r), 0),           // bottom-right only: bottom edge -> right
        4 => ((c + 1.0, r + 0.5), 1),     // top-right only: right edge -> up
        8 => ((c + 0.5, r + 1.0), 2),     // top-left only: top edge -> left

        // Two adjacent corners
        3 => ((c + 1.0, r + 0.5), 0),     // bottom: right edge -> right
        6 => ((c + 0.5, r + 1.0), 1),     // right side: top edge -> up
        12 => ((c, r + 0.5), 2),          // top: left edge -> left
        9 => ((c + 0.5, r), 3),           // left side: bottom edge -> down

        // Three corners (one missing)
        7 => ((c + 0.5, r + 1.0), 1),     // missing top-left: top edge -> up
        11 => ((c + 1.0, r + 0.5), 0),    // missing top-right: right edge -> right
        13 => ((c + 0.5, r), 3),          // missing bottom-right: bottom edge -> down
        14 => ((c, r + 0.5), 2),          // missing bottom-left: left edge -> left

        // Saddle points - direction depends on entry
        5 => {
            if entry_dir == 0 || entry_dir == 2 {
                ((c + 0.5, r), 3)         // horizontal through: bottom -> down
            } else {
                ((c + 0.5, r + 1.0), 1)   // vertical through: top -> up
            }
        }
        10 => {
            if entry_dir == 1 || entry_dir == 3 {
                ((c, r + 0.5), 2)         // vertical through: left -> left
            } else {
                ((c + 1.0, r + 0.5), 0)   // horizontal through: right -> right
            }
        }

        // Empty or full - shouldn't happen
        _ => ((c + 0.5, r + 0.5), entry_dir),
    }
}

/// Douglas-Peucker line simplification
fn douglas_peucker(points: &[(f64, f64)], tolerance: f64) -> Vec<(f64, f64)> {
    if points.len() < 3 {
        return points.to_vec();
    }

    // Find the point with the maximum distance from the line between first and last
    let (first, last) = (points[0], points[points.len() - 1]);
    let mut max_dist = 0.0;
    let mut max_idx = 0;

    for (i, &point) in points.iter().enumerate().skip(1).take(points.len() - 2) {
        let dist = perpendicular_distance(point, first, last);
        if dist > max_dist {
            max_dist = dist;
            max_idx = i;
        }
    }

    // If max distance is greater than tolerance, recursively simplify
    if max_dist > tolerance {
        let mut result1 = douglas_peucker(&points[..=max_idx], tolerance);
        let result2 = douglas_peucker(&points[max_idx..], tolerance);

        result1.pop(); // Remove duplicate point
        result1.extend(result2);
        result1
    } else {
        vec![first, last]
    }
}

/// Calculate perpendicular distance from a point to a line
fn perpendicular_distance(point: (f64, f64), line_start: (f64, f64), line_end: (f64, f64)) -> f64 {
    let dx = line_end.0 - line_start.0;
    let dy = line_end.1 - line_start.1;

    let len_sq = dx * dx + dy * dy;
    if len_sq == 0.0 {
        return ((point.0 - line_start.0).powi(2) + (point.1 - line_start.1).powi(2)).sqrt();
    }

    let t = ((point.0 - line_start.0) * dx + (point.1 - line_start.1) * dy) / len_sq;
    let t = t.clamp(0.0, 1.0);

    let proj_x = line_start.0 + t * dx;
    let proj_y = line_start.1 + t * dy;

    ((point.0 - proj_x).powi(2) + (point.1 - proj_y).powi(2)).sqrt()
}

/// Export contour to GeoJSON
pub fn export_contour_geojson(result: &ContourResult, output_path: &std::path::Path) -> Result<()> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(output_path)?;

    write!(file, r#"{{"type": "Feature", "geometry": {{"type": "Polygon", "coordinates": [["#)?;

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

    writeln!(file, r#"]]}}, "properties": {{"vertices": {}, "cells": {}, "segments": {}}}}}"#,
             result.stats.contour_vertices_after_simplify,
             result.stats.filled_cells,
             result.stats.input_segments)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mercator_roundtrip() {
        let lat = 50.85;
        let lon = 4.35;
        let merc = to_mercator(lat, lon);
        let (lon2, lat2) = from_mercator(merc.x, merc.y);
        assert!((lat - lat2).abs() < 0.0001);
        assert!((lon - lon2).abs() < 0.0001);
    }

    #[test]
    fn test_douglas_peucker() {
        let points = vec![
            (0.0, 0.0),
            (0.1, 0.01),
            (0.2, 0.0),
            (0.3, 0.02),
            (1.0, 0.0),
        ];
        let simplified = douglas_peucker(&points, 0.05);
        assert!(simplified.len() < points.len());
    }

    #[test]
    fn test_bresenham_line() {
        let mut raster = vec![false; 10 * 10];
        let p0 = MercatorPoint { x: 0.0, y: 0.0 };
        let p1 = MercatorPoint { x: 4.0, y: 4.0 };
        stamp_line(&mut raster, 10, 10, 0.0, 0.0, 1.0, &p0, &p1);

        // Check diagonal is stamped
        assert!(raster[0]); // (0, 0)
        assert!(raster[11]); // (1, 1)
        assert!(raster[22]); // (2, 2)
    }
}
