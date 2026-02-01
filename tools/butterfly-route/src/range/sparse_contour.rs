//! Sparse tile-based contour generation
//!
//! Key insight: cost should be O(frontier_complexity), not O(bbox_area).
//!
//! Approach:
//! 1. Use sparse tile map instead of dense raster
//! 2. Only allocate tiles that contain stamped segments
//! 3. Run morphology only on active tiles + their neighbors
//! 4. Run marching squares per tile with seam stitching

use std::collections::{HashMap, HashSet};
use anyhow::Result;

use super::frontier::ReachableSegment;

/// Tile size in cells (64x64 = 4096 bits = 512 bytes per tile)
const TILE_SIZE: usize = 64;

/// Tile coordinate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TileCoord {
    tx: i32,
    ty: i32,
}

/// A single tile's bitmap (TILE_SIZE x TILE_SIZE)
struct TileBitmap {
    bits: Vec<u64>, // TILE_SIZE rows, each row is a u64 (64 bits)
}

impl TileBitmap {
    fn new() -> Self {
        Self {
            bits: vec![0u64; TILE_SIZE],
        }
    }

    #[inline]
    fn get(&self, local_col: usize, local_row: usize) -> bool {
        debug_assert!(local_col < TILE_SIZE && local_row < TILE_SIZE);
        (self.bits[local_row] >> local_col) & 1 != 0
    }

    #[inline]
    fn set(&mut self, local_col: usize, local_row: usize) {
        debug_assert!(local_col < TILE_SIZE && local_row < TILE_SIZE);
        self.bits[local_row] |= 1u64 << local_col;
    }

    fn is_empty(&self) -> bool {
        self.bits.iter().all(|&row| row == 0)
    }

    fn count_set(&self) -> usize {
        self.bits.iter().map(|row| row.count_ones() as usize).sum()
    }
}

/// Sparse tile map
struct SparseTileMap {
    tiles: HashMap<TileCoord, TileBitmap>,
    cell_size_m: f64,
    origin_x: f64, // Mercator X of global (0,0)
    origin_y: f64, // Mercator Y of global (0,0)
}

impl SparseTileMap {
    fn new(cell_size_m: f64, origin_x: f64, origin_y: f64) -> Self {
        Self {
            tiles: HashMap::new(),
            cell_size_m,
            origin_x,
            origin_y,
        }
    }

    /// Convert Mercator coordinates to global cell coordinates
    #[inline]
    fn mercator_to_cell(&self, x: f64, y: f64) -> (i32, i32) {
        let col = ((x - self.origin_x) / self.cell_size_m).floor() as i32;
        let row = ((y - self.origin_y) / self.cell_size_m).floor() as i32;
        (col, row)
    }

    /// Convert global cell to tile coord + local offset
    #[inline]
    fn cell_to_tile(&self, col: i32, row: i32) -> (TileCoord, usize, usize) {
        let tx = col.div_euclid(TILE_SIZE as i32);
        let ty = row.div_euclid(TILE_SIZE as i32);
        let local_col = col.rem_euclid(TILE_SIZE as i32) as usize;
        let local_row = row.rem_euclid(TILE_SIZE as i32) as usize;
        (TileCoord { tx, ty }, local_col, local_row)
    }

    /// Set a cell (creates tile if needed)
    fn set_cell(&mut self, col: i32, row: i32) {
        let (tile_coord, local_col, local_row) = self.cell_to_tile(col, row);
        let tile = self.tiles.entry(tile_coord).or_insert_with(TileBitmap::new);
        tile.set(local_col, local_row);
    }

    /// Get a cell (returns false for non-existent tiles)
    fn get_cell(&self, col: i32, row: i32) -> bool {
        let (tile_coord, local_col, local_row) = self.cell_to_tile(col, row);
        self.tiles.get(&tile_coord)
            .map(|tile| tile.get(local_col, local_row))
            .unwrap_or(false)
    }

    /// Stamp a line segment using Bresenham's algorithm
    fn stamp_line(&mut self, x0: f64, y0: f64, x1: f64, y1: f64) {
        let (col0, row0) = self.mercator_to_cell(x0, y0);
        let (col1, row1) = self.mercator_to_cell(x1, y1);

        // Bresenham's line algorithm
        let dx = (col1 - col0).abs();
        let dy = (row1 - row0).abs();
        let sx = if col0 < col1 { 1 } else { -1 };
        let sy = if row0 < row1 { 1 } else { -1 };
        let mut err = dx - dy;

        let mut col = col0;
        let mut row = row0;

        loop {
            self.set_cell(col, row);

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

    /// Get all active tile coordinates
    fn active_tiles(&self) -> Vec<TileCoord> {
        self.tiles.keys().copied().collect()
    }

    /// Get active tiles plus their 8-neighbors (for morphology halo)
    fn active_tiles_with_halo(&self) -> HashSet<TileCoord> {
        let mut result = HashSet::new();
        for &coord in self.tiles.keys() {
            for dy in -1..=1 {
                for dx in -1..=1 {
                    result.insert(TileCoord {
                        tx: coord.tx + dx,
                        ty: coord.ty + dy,
                    });
                }
            }
        }
        result
    }

    /// Count total set cells across all tiles
    fn count_set_cells(&self) -> usize {
        self.tiles.values().map(|t| t.count_set()).sum()
    }
}

/// Empty tile constant - all zeros
const EMPTY_TILE: [u64; TILE_SIZE] = [0u64; TILE_SIZE];

/// Get a tile's bits as a slice, or empty slice if not present
#[inline]
fn get_tile_bits<'a>(map: &'a SparseTileMap, coord: TileCoord) -> &'a [u64; TILE_SIZE] {
    map.tiles.get(&coord)
        .map(|t| t.bits.as_slice().try_into().unwrap())
        .unwrap_or(&EMPTY_TILE)
}

/// Dilate the sparse tile map using bitwise operations
///
/// For each cell, output is set if ANY of its 9 neighbors (including self) is set.
/// Using bitwise ops: for each row, dilate = row | (row << 1) | (row >> 1)
///                                        | row_above | row_below (and their shifts)
fn dilate_sparse(map: &SparseTileMap) -> SparseTileMap {
    let mut result = SparseTileMap::new(map.cell_size_m, map.origin_x, map.origin_y);

    // Get tiles that might have output (active tiles + their neighbors)
    let tiles_to_process = map.active_tiles_with_halo();

    for coord in tiles_to_process {
        // Cache all 9 tile references (one HashMap lookup each)
        let center = get_tile_bits(map, coord);
        let above = get_tile_bits(map, TileCoord { tx: coord.tx, ty: coord.ty - 1 });
        let below = get_tile_bits(map, TileCoord { tx: coord.tx, ty: coord.ty + 1 });
        let left = get_tile_bits(map, TileCoord { tx: coord.tx - 1, ty: coord.ty });
        let right = get_tile_bits(map, TileCoord { tx: coord.tx + 1, ty: coord.ty });
        let above_left = get_tile_bits(map, TileCoord { tx: coord.tx - 1, ty: coord.ty - 1 });
        let above_right = get_tile_bits(map, TileCoord { tx: coord.tx + 1, ty: coord.ty - 1 });
        let below_left = get_tile_bits(map, TileCoord { tx: coord.tx - 1, ty: coord.ty + 1 });
        let below_right = get_tile_bits(map, TileCoord { tx: coord.tx + 1, ty: coord.ty + 1 });

        let mut new_tile = TileBitmap::new();

        for local_row in 0..TILE_SIZE {
            // Get current row and edge bits from left/right neighbors
            let cur = center[local_row];
            let left_bit = (left[local_row] >> 63) & 1;
            let right_bit = (right[local_row] & 1) << 63;
            let cur_h = cur | (cur << 1) | (cur >> 1) | left_bit | right_bit;

            // Get row above with edge bits
            let (above_row, above_left_row, above_right_row) = if local_row == 0 {
                (above[TILE_SIZE - 1], above_left[TILE_SIZE - 1], above_right[TILE_SIZE - 1])
            } else {
                (center[local_row - 1], left[local_row - 1], right[local_row - 1])
            };
            let above_left_bit = (above_left_row >> 63) & 1;
            let above_right_bit = (above_right_row & 1) << 63;
            let above_h = above_row | (above_row << 1) | (above_row >> 1) | above_left_bit | above_right_bit;

            // Get row below with edge bits
            let (below_row, below_left_row, below_right_row) = if local_row == TILE_SIZE - 1 {
                (below[0], below_left[0], below_right[0])
            } else {
                (center[local_row + 1], left[local_row + 1], right[local_row + 1])
            };
            let below_left_bit = (below_left_row >> 63) & 1;
            let below_right_bit = (below_right_row & 1) << 63;
            let below_h = below_row | (below_row << 1) | (below_row >> 1) | below_left_bit | below_right_bit;

            // Final dilation: OR all three rows
            new_tile.bits[local_row] = cur_h | above_h | below_h;
        }

        if !new_tile.is_empty() {
            result.tiles.insert(coord, new_tile);
        }
    }

    result
}

/// Erode the sparse tile map using bitwise operations
///
/// For each cell, output is set only if ALL 9 neighbors (including self) are set.
/// Edge cells adjacent to non-existent tiles will erode (treated as 0).
fn erode_sparse(map: &SparseTileMap) -> SparseTileMap {
    let mut result = SparseTileMap::new(map.cell_size_m, map.origin_x, map.origin_y);

    for &coord in map.tiles.keys() {
        // Cache all 9 tile references (one HashMap lookup each)
        let center = get_tile_bits(map, coord);
        let above = get_tile_bits(map, TileCoord { tx: coord.tx, ty: coord.ty - 1 });
        let below = get_tile_bits(map, TileCoord { tx: coord.tx, ty: coord.ty + 1 });
        let left = get_tile_bits(map, TileCoord { tx: coord.tx - 1, ty: coord.ty });
        let right = get_tile_bits(map, TileCoord { tx: coord.tx + 1, ty: coord.ty });
        let above_left = get_tile_bits(map, TileCoord { tx: coord.tx - 1, ty: coord.ty - 1 });
        let above_right = get_tile_bits(map, TileCoord { tx: coord.tx + 1, ty: coord.ty - 1 });
        let below_left = get_tile_bits(map, TileCoord { tx: coord.tx - 1, ty: coord.ty + 1 });
        let below_right = get_tile_bits(map, TileCoord { tx: coord.tx + 1, ty: coord.ty + 1 });

        let mut new_tile = TileBitmap::new();

        for local_row in 0..TILE_SIZE {
            let cur = center[local_row];
            if cur == 0 {
                continue; // Early exit: nothing to erode
            }

            // Build horizontal erosion for current row
            let left_bit = (left[local_row] >> 63) & 1;
            let right_bit = (right[local_row] & 1) << 63;
            let cur_h = cur & ((cur << 1) | left_bit) & ((cur >> 1) | right_bit);

            // Build horizontal erosion for row above
            let (above_row, above_left_row, above_right_row) = if local_row == 0 {
                (above[TILE_SIZE - 1], above_left[TILE_SIZE - 1], above_right[TILE_SIZE - 1])
            } else {
                (center[local_row - 1], left[local_row - 1], right[local_row - 1])
            };
            let above_left_bit = (above_left_row >> 63) & 1;
            let above_right_bit = (above_right_row & 1) << 63;
            let above_h = above_row & ((above_row << 1) | above_left_bit) & ((above_row >> 1) | above_right_bit);

            // Build horizontal erosion for row below
            let (below_row, below_left_row, below_right_row) = if local_row == TILE_SIZE - 1 {
                (below[0], below_left[0], below_right[0])
            } else {
                (center[local_row + 1], left[local_row + 1], right[local_row + 1])
            };
            let below_left_bit = (below_left_row >> 63) & 1;
            let below_right_bit = (below_right_row & 1) << 63;
            let below_h = below_row & ((below_row << 1) | below_left_bit) & ((below_row >> 1) | below_right_bit);

            // Final erosion: AND all three horizontally-eroded rows
            new_tile.bits[local_row] = cur_h & above_h & below_h;
        }

        if !new_tile.is_empty() {
            result.tiles.insert(coord, new_tile);
        }
    }

    result
}

/// Mercator projection
struct MercatorPoint {
    x: f64,
    y: f64,
}

const EARTH_RADIUS: f64 = 6_378_137.0;

fn to_mercator(lat: f64, lon: f64) -> MercatorPoint {
    let x = lon.to_radians() * EARTH_RADIUS;
    let y = ((lat.to_radians() / 2.0 + std::f64::consts::PI / 4.0).tan()).ln() * EARTH_RADIUS;
    MercatorPoint { x, y }
}

fn from_mercator(x: f64, y: f64) -> (f64, f64) {
    let lon = (x / EARTH_RADIUS).to_degrees();
    let lat = (2.0 * (y / EARTH_RADIUS).exp().atan() - std::f64::consts::PI / 2.0).to_degrees();
    (lon, lat)
}

/// Configuration for sparse contour generation
#[derive(Debug, Clone)]
pub struct SparseContourConfig {
    pub cell_size_m: f64,
    pub dilation_rounds: usize,
    pub erosion_rounds: usize,
    pub simplify_tolerance_m: f64,
}

impl SparseContourConfig {
    /// Default car config (30m cells - better accuracy)
    pub fn for_car() -> Self {
        Self {
            cell_size_m: 30.0,
            dilation_rounds: 2,  // Reduced to avoid over-expansion
            erosion_rounds: 1,
            simplify_tolerance_m: 30.0,
        }
    }

    /// Default bike config (40m cells)
    pub fn for_bike() -> Self {
        Self {
            cell_size_m: 40.0,
            dilation_rounds: 3,
            erosion_rounds: 1,
            simplify_tolerance_m: 40.0,
        }
    }

    /// Default foot config (25m cells)
    pub fn for_foot() -> Self {
        Self {
            cell_size_m: 25.0,
            dilation_rounds: 3,
            erosion_rounds: 1,
            simplify_tolerance_m: 25.0,
        }
    }

    /// High-detail car config - more vertices, comparable to Valhalla (25m cells, ~2000+ vertices)
    pub fn for_car_hd() -> Self {
        Self {
            cell_size_m: 25.0,
            dilation_rounds: 2,
            erosion_rounds: 1,
            simplify_tolerance_m: 50.0, // Match Valhalla default generalize
        }
    }

    /// High-detail bike config (15m cells)
    pub fn for_bike_hd() -> Self {
        Self {
            cell_size_m: 15.0,
            dilation_rounds: 2,
            erosion_rounds: 1,
            simplify_tolerance_m: 25.0,
        }
    }

    /// High-detail foot config (10m cells)
    pub fn for_foot_hd() -> Self {
        Self {
            cell_size_m: 10.0,
            dilation_rounds: 2,
            erosion_rounds: 1,
            simplify_tolerance_m: 15.0,
        }
    }

    /// Custom configuration
    pub fn custom(cell_size_m: f64, simplify_tolerance_m: f64) -> Self {
        Self {
            cell_size_m,
            dilation_rounds: 2,
            erosion_rounds: 1,
            simplify_tolerance_m,
        }
    }

    /// Custom configuration with explicit morphology control
    pub fn custom_full(
        cell_size_m: f64,
        dilation_rounds: usize,
        erosion_rounds: usize,
        simplify_tolerance_m: f64,
    ) -> Self {
        Self {
            cell_size_m,
            dilation_rounds,
            erosion_rounds,
            simplify_tolerance_m,
        }
    }

    /// No morphology - raw stamped segments only (for debugging)
    pub fn no_morphology(cell_size_m: f64) -> Self {
        Self {
            cell_size_m,
            dilation_rounds: 0,
            erosion_rounds: 0,
            simplify_tolerance_m: 0.0,
        }
    }
}

/// Statistics from sparse contour generation
#[derive(Debug, Default)]
pub struct SparseContourStats {
    pub input_segments: usize,
    pub active_tiles: usize,
    pub active_tiles_after_morphology: usize,
    pub total_cells_set: usize,
    pub contour_vertices_before_simplify: usize,
    pub contour_vertices_after_simplify: usize,
    pub stamp_time_us: u64,
    pub morphology_time_us: u64,
    pub contour_time_us: u64,
    pub simplify_time_us: u64,
}

/// Result of sparse contour generation
pub struct SparseContourResult {
    pub outer_ring: Vec<(f64, f64)>, // WGS84 (lon, lat) pairs
    pub holes: Vec<Vec<(f64, f64)>>,
    pub stats: SparseContourStats,
}

/// Generate contour using sparse tile-based approach
pub fn generate_sparse_contour(
    segments: &[ReachableSegment],
    config: &SparseContourConfig,
) -> Result<SparseContourResult> {
    let mut stats = SparseContourStats::default();
    stats.input_segments = segments.len();

    if segments.is_empty() {
        return Ok(SparseContourResult {
            outer_ring: vec![],
            holes: vec![],
            stats,
        });
    }

    // Step 1: Find bounding box and project to Mercator
    let stamp_start = std::time::Instant::now();

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

    // Add margin
    let margin = config.cell_size_m * 3.0;
    min_x -= margin;
    min_y -= margin;

    // Step 2: Create sparse tile map and stamp segments
    let mut tile_map = SparseTileMap::new(config.cell_size_m, min_x, min_y);

    for seg in &mercator_segments {
        for window in seg.windows(2) {
            tile_map.stamp_line(window[0].x, window[0].y, window[1].x, window[1].y);
        }
        // Stamp individual points
        for pt in seg {
            let (col, row) = tile_map.mercator_to_cell(pt.x, pt.y);
            tile_map.set_cell(col, row);
        }
    }

    stats.active_tiles = tile_map.tiles.len();
    stats.stamp_time_us = stamp_start.elapsed().as_micros() as u64;

    // Step 3: Morphological operations (sparse)
    let morph_start = std::time::Instant::now();

    let mut closed = tile_map;
    for _ in 0..config.dilation_rounds {
        closed = dilate_sparse(&closed);
    }
    for _ in 0..config.erosion_rounds {
        closed = erode_sparse(&closed);
    }

    stats.active_tiles_after_morphology = closed.tiles.len();
    stats.total_cells_set = closed.count_set_cells();
    stats.morphology_time_us = morph_start.elapsed().as_micros() as u64;

    // Step 4: Extract contour using marching squares on sparse tiles
    let contour_start = std::time::Instant::now();
    let contour = extract_contour_sparse(&closed);
    stats.contour_vertices_before_simplify = contour.len();
    stats.contour_time_us = contour_start.elapsed().as_micros() as u64;

    if contour.is_empty() {
        return Ok(SparseContourResult {
            outer_ring: vec![],
            holes: vec![],
            stats,
        });
    }

    // Step 5: Convert to WGS84 and simplify
    let simplify_start = std::time::Instant::now();

    let mut wgs84_contour: Vec<(f64, f64)> = contour
        .iter()
        .map(|&(col, row)| {
            let x = min_x + col * config.cell_size_m;
            let y = min_y + row * config.cell_size_m;
            from_mercator(x, y)
        })
        .collect();

    let tolerance_deg = config.simplify_tolerance_m / 111000.0;
    wgs84_contour = douglas_peucker(&wgs84_contour, tolerance_deg);
    stats.contour_vertices_after_simplify = wgs84_contour.len();
    stats.simplify_time_us = simplify_start.elapsed().as_micros() as u64;

    Ok(SparseContourResult {
        outer_ring: wgs84_contour,
        holes: vec![],
        stats,
    })
}

/// Extract contour from sparse tile map using Moore-neighbor boundary tracing
///
/// This is O(perimeter), not O(area) - no densification needed.
/// We trace the boundary between filled and empty cells directly on the sparse tile map.
///
/// For maps with multiple disconnected components, we trace ALL boundaries and return
/// the LARGEST one (by vertex count). This handles cases where roads reach far-away
/// areas without connecting to intermediate regions.
fn extract_contour_sparse(map: &SparseTileMap) -> Vec<(f64, f64)> {
    if map.tiles.is_empty() {
        return vec![];
    }

    // Track which cells have been visited (as part of a boundary)
    let mut visited_edges: HashSet<(i32, i32, u8)> = HashSet::new();
    let mut all_contours: Vec<Vec<(f64, f64)>> = Vec::new();

    // Find all boundary starts and trace each component
    let boundary_starts = find_all_boundary_starts(map);

    for (start_col, start_row, start_edge) in boundary_starts {
        // Skip if this edge was already traced
        if visited_edges.contains(&(start_col, start_row, start_edge)) {
            continue;
        }

        // Trace this boundary
        let contour = trace_boundary_edges_with_visited(
            map,
            start_col,
            start_row,
            start_edge,
            &mut visited_edges
        );

        if contour.len() >= 3 {
            all_contours.push(contour);
        }
    }

    // Return the largest contour (by vertex count)
    all_contours.into_iter()
        .max_by_key(|c| c.len())
        .unwrap_or_default()
}

/// Find ALL boundary cells that could start a trace (for multi-component maps)
/// Returns Vec of (col, row, edge) for each potential starting point
fn find_all_boundary_starts(map: &SparseTileMap) -> Vec<(i32, i32, u8)> {
    let mut starts = Vec::new();

    for (&coord, tile) in &map.tiles {
        let base_col = coord.tx * TILE_SIZE as i32;
        let base_row = coord.ty * TILE_SIZE as i32;

        for local_row in 0..TILE_SIZE {
            let row_bits = tile.bits[local_row];
            if row_bits == 0 {
                continue;
            }

            for local_col in 0..TILE_SIZE {
                if (row_bits >> local_col) & 1 == 0 {
                    continue;
                }

                let col = base_col + local_col as i32;
                let row = base_row + local_row as i32;

                // Check for boundary edges - add one start per boundary cell
                // We only need one edge per cell to start tracing
                if !map.get_cell(col, row - 1) {
                    starts.push((col, row, 0)); // North edge
                } else if !map.get_cell(col - 1, row) {
                    starts.push((col, row, 3)); // West edge
                } else if !map.get_cell(col + 1, row) {
                    starts.push((col, row, 1)); // East edge
                } else if !map.get_cell(col, row + 1) {
                    starts.push((col, row, 2)); // South edge
                }
            }
        }
    }

    starts
}

/// Trace boundary edges, marking visited edges to avoid re-tracing the same component
fn trace_boundary_edges_with_visited(
    map: &SparseTileMap,
    start_col: i32,
    start_row: i32,
    start_edge: u8,
    visited: &mut HashSet<(i32, i32, u8)>,
) -> Vec<(f64, f64)> {
    let mut contour = Vec::new();

    let mut col = start_col;
    let mut row = start_row;
    let mut edge = start_edge;

    // Maximum iterations = perimeter bound
    let max_iter = map.tiles.len() * TILE_SIZE * TILE_SIZE * 4;
    let mut iter = 0;

    loop {
        iter += 1;
        if iter > max_iter {
            break;
        }

        // Mark this edge as visited
        visited.insert((col, row, edge));

        // Emit the corner vertex at the START of this edge (clockwise direction)
        // Edge 0 (North): top-left corner (col, row)
        // Edge 1 (East): top-right corner (col+1, row)
        // Edge 2 (South): bottom-right corner (col+1, row+1)
        // Edge 3 (West): bottom-left corner (col, row+1)
        let (vx, vy) = match edge {
            0 => (col as f64, row as f64),
            1 => (col as f64 + 1.0, row as f64),
            2 => (col as f64 + 1.0, row as f64 + 1.0),
            _ => (col as f64, row as f64 + 1.0),
        };
        contour.push((vx, vy));

        // Find next boundary edge
        let (next_col, next_row, next_edge) = next_boundary_edge(map, col, row, edge);

        // Check if we're back at start
        if next_col == start_col && next_row == start_row && next_edge == start_edge {
            break;
        }

        col = next_col;
        row = next_row;
        edge = next_edge;
    }

    contour
}

/// Trace boundary edges clockwise, emitting corner vertices
/// Edge encoding: 0=North, 1=East, 2=South, 3=West
/// Walking clockwise means filled cells are on our right
fn trace_boundary_edges(
    map: &SparseTileMap,
    start_col: i32,
    start_row: i32,
    start_edge: u8,
) -> Vec<(f64, f64)> {
    let mut contour = Vec::new();

    let mut col = start_col;
    let mut row = start_row;
    let mut edge = start_edge;

    // Maximum iterations = perimeter bound (shouldn't exceed 4 * n_cells)
    let max_iter = map.count_set_cells() * 4 + 1000;
    let mut iter = 0;

    loop {
        iter += 1;
        if iter > max_iter {
            break; // Safety valve
        }

        // Emit the corner vertex at the START of this edge (clockwise direction)
        // Edge 0 (North): top-left corner (col, row)
        // Edge 1 (East): top-right corner (col+1, row)
        // Edge 2 (South): bottom-right corner (col+1, row+1)
        // Edge 3 (West): bottom-left corner (col, row+1)
        let (vx, vy) = match edge {
            0 => (col as f64, row as f64),
            1 => (col as f64 + 1.0, row as f64),
            2 => (col as f64 + 1.0, row as f64 + 1.0),
            _ => (col as f64, row as f64 + 1.0),
        };
        contour.push((vx, vy));

        // Determine next edge by checking the cell we'd enter and the corner we're at
        // We're walking clockwise with filled on our right
        //
        // At each corner, we have 3 choices:
        // 1. Turn right (filled cell ahead-right) -> continue on same cell, next edge CW
        // 2. Go straight (filled ahead, empty ahead-right) -> cross to next cell
        // 3. Turn left (empty ahead) -> stay at corner, turn CCW on same cell

        let (next_col, next_row, next_edge) = next_boundary_edge(map, col, row, edge);

        // Check if we're back at start
        if next_col == start_col && next_row == start_row && next_edge == start_edge {
            break;
        }

        col = next_col;
        row = next_row;
        edge = next_edge;
    }

    contour
}

/// Determine the next boundary edge when walking clockwise
/// Returns (next_col, next_row, next_edge)
fn next_boundary_edge(map: &SparseTileMap, col: i32, row: i32, edge: u8) -> (i32, i32, u8) {
    // We're at cell (col, row), on edge `edge`, walking clockwise (filled on right)
    // At the end vertex of this edge, check what's ahead:
    //
    // For edge 0 (North, walking East):
    //   - end vertex is (col+1, row)
    //   - ahead is cell (col+1, row-1) [above-right]
    //   - right is cell (col+1, row) [right]
    //
    // Rules:
    //   1. If cell ahead-right is filled: turn right (convex corner)
    //   2. If cell ahead is filled: go straight (continue boundary)
    //   3. Otherwise: turn left (concave corner)

    match edge {
        0 => { // North edge, walking East toward (col+1, row)
            let ahead_right = map.get_cell(col + 1, row - 1); // NE cell
            let ahead = map.get_cell(col + 1, row);           // E cell

            if ahead_right {
                // Turn right: go to NE cell's West edge
                (col + 1, row - 1, 3)
            } else if ahead {
                // Straight: go to E cell's North edge
                (col + 1, row, 0)
            } else {
                // Turn left: stay on this cell, go to East edge
                (col, row, 1)
            }
        }
        1 => { // East edge, walking South toward (col+1, row+1)
            let ahead_right = map.get_cell(col + 1, row + 1); // SE cell
            let ahead = map.get_cell(col, row + 1);           // S cell

            if ahead_right {
                // Turn right: go to SE cell's North edge
                (col + 1, row + 1, 0)
            } else if ahead {
                // Straight: go to S cell's East edge
                (col, row + 1, 1)
            } else {
                // Turn left: stay on this cell, go to South edge
                (col, row, 2)
            }
        }
        2 => { // South edge, walking West toward (col, row+1)
            let ahead_right = map.get_cell(col - 1, row + 1); // SW cell
            let ahead = map.get_cell(col - 1, row);           // W cell

            if ahead_right {
                // Turn right: go to SW cell's East edge
                (col - 1, row + 1, 1)
            } else if ahead {
                // Straight: go to W cell's South edge
                (col - 1, row, 2)
            } else {
                // Turn left: stay on this cell, go to West edge
                (col, row, 3)
            }
        }
        _ => { // West edge (3), walking North toward (col, row)
            let ahead_right = map.get_cell(col - 1, row - 1); // NW cell
            let ahead = map.get_cell(col, row - 1);           // N cell

            if ahead_right {
                // Turn right: go to NW cell's South edge
                (col - 1, row - 1, 2)
            } else if ahead {
                // Straight: go to N cell's West edge
                (col, row - 1, 3)
            } else {
                // Turn left: stay on this cell, go to North edge
                (col, row, 0)
            }
        }
    }
}

/// Marching squares on dense array with flood fill for exterior
fn marching_squares_dense(
    raster: &[bool],
    n_cols: usize,
    n_rows: usize,
    offset_col: f64,
    offset_row: f64,
) -> Vec<(f64, f64)> {
    if raster.is_empty() || n_cols == 0 || n_rows == 0 {
        return vec![];
    }

    // Mark exterior cells via flood fill from corners
    let mut exterior = vec![false; n_cols * n_rows];
    let mut stack = Vec::with_capacity(1024);

    // Start flood fill from all border cells that are not set
    for col in 0..n_cols {
        if !raster[col] { stack.push((col, 0)); }
        if !raster[(n_rows - 1) * n_cols + col] { stack.push((col, n_rows - 1)); }
    }
    for row in 0..n_rows {
        if !raster[row * n_cols] { stack.push((0, row)); }
        if !raster[row * n_cols + n_cols - 1] { stack.push((n_cols - 1, row)); }
    }

    while let Some((col, row)) = stack.pop() {
        let idx = row * n_cols + col;
        if exterior[idx] || raster[idx] {
            continue;
        }
        exterior[idx] = true;

        if col > 0 { stack.push((col - 1, row)); }
        if col + 1 < n_cols { stack.push((col + 1, row)); }
        if row > 0 { stack.push((col, row - 1)); }
        if row + 1 < n_rows { stack.push((col, row + 1)); }
    }

    // Find a starting edge (transition from exterior to interior)
    let mut start = None;
    'outer: for row in 0..n_rows.saturating_sub(1) {
        for col in 0..n_cols.saturating_sub(1) {
            let idx = row * n_cols + col;
            // Look for a cell that's exterior with interior neighbor to the right
            if exterior[idx] && col + 1 < n_cols && !exterior[idx + 1] && raster[idx + 1] {
                start = Some((col, row, 0)); // 0 = coming from left
                break 'outer;
            }
        }
    }

    let Some((start_col, start_row, start_dir)) = start else {
        return vec![];
    };

    // Trace contour
    let mut contour = Vec::new();
    let mut col = start_col;
    let mut row = start_row;
    let mut dir = start_dir;
    let mut iterations = 0;
    let max_iterations = n_cols * n_rows * 4;

    loop {
        iterations += 1;
        if iterations > max_iterations {
            break; // Safety valve
        }

        // Get 2x2 cell configuration
        let tl = if row > 0 && col > 0 { raster[(row - 1) * n_cols + col - 1] && !exterior[(row - 1) * n_cols + col - 1] } else { false };
        let tr = if row > 0 && col < n_cols { raster[(row - 1) * n_cols + col] && !exterior[(row - 1) * n_cols + col] } else { false };
        let bl = if row < n_rows && col > 0 { raster[row * n_cols + col - 1] && !exterior[row * n_cols + col - 1] } else { false };
        let br = if row < n_rows && col < n_cols { raster[row * n_cols + col] && !exterior[row * n_cols + col] } else { false };

        let case = (tl as u8) | ((tr as u8) << 1) | ((bl as u8) << 2) | ((br as u8) << 3);

        // Add vertex
        contour.push((offset_col + col as f64, offset_row + row as f64));

        // Determine next direction based on marching squares case
        let next_dir = match (case, dir) {
            // Standard marching squares transitions
            (1, _) | (14, _) => 3,  // up
            (2, _) | (13, _) => 0,  // right
            (3, _) | (12, _) => 0,  // right
            (4, _) | (11, _) => 3,  // up
            (6, 0) | (9, 2) => 3,   // saddle: prefer up
            (6, _) | (9, _) => 1,   // saddle: prefer down
            (7, _) | (8, _) => 3,   // up
            (5, _) | (10, _) => dir, // straight through
            _ => {
                // Move based on current direction
                match dir {
                    0 => 1, // right -> down
                    1 => 2, // down -> left
                    2 => 3, // left -> up
                    _ => 0, // up -> right
                }
            }
        };

        // Move to next cell
        match next_dir {
            0 => col += 1, // right
            1 => row += 1, // down
            2 => col = col.saturating_sub(1), // left
            _ => row = row.saturating_sub(1), // up
        }
        dir = next_dir;

        // Check if we're back at start
        if col == start_col && row == start_row && dir == start_dir && contour.len() > 2 {
            break;
        }
    }

    contour
}

/// Douglas-Peucker line simplification
fn douglas_peucker(points: &[(f64, f64)], tolerance: f64) -> Vec<(f64, f64)> {
    if points.len() <= 2 {
        return points.to_vec();
    }

    let mut max_dist = 0.0;
    let mut max_idx = 0;
    let start = points[0];
    let end = points[points.len() - 1];

    for (i, &point) in points.iter().enumerate().skip(1).take(points.len() - 2) {
        let dist = perpendicular_distance(point, start, end);
        if dist > max_dist {
            max_dist = dist;
            max_idx = i;
        }
    }

    if max_dist > tolerance {
        let mut left = douglas_peucker(&points[..=max_idx], tolerance);
        let right = douglas_peucker(&points[max_idx..], tolerance);
        left.pop();
        left.extend(right);
        left
    } else {
        vec![start, end]
    }
}

fn perpendicular_distance(point: (f64, f64), start: (f64, f64), end: (f64, f64)) -> f64 {
    let dx = end.0 - start.0;
    let dy = end.1 - start.1;
    let len_sq = dx * dx + dy * dy;

    if len_sq < 1e-12 {
        let pdx = point.0 - start.0;
        let pdy = point.1 - start.1;
        return (pdx * pdx + pdy * pdy).sqrt();
    }

    let t = ((point.0 - start.0) * dx + (point.1 - start.1) * dy) / len_sq;
    let t = t.clamp(0.0, 1.0);

    let proj_x = start.0 + t * dx;
    let proj_y = start.1 + t * dy;

    let pdx = point.0 - proj_x;
    let pdy = point.1 - proj_y;
    (pdx * pdx + pdy * pdy).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tile_bitmap() {
        let mut tile = TileBitmap::new();
        assert!(!tile.get(0, 0));
        tile.set(0, 0);
        assert!(tile.get(0, 0));
        assert!(!tile.get(1, 0));

        tile.set(63, 63);
        assert!(tile.get(63, 63));
        assert_eq!(tile.count_set(), 2);
    }

    #[test]
    fn test_sparse_tile_map() {
        let mut map = SparseTileMap::new(1.0, 0.0, 0.0);

        // Set cells across multiple tiles
        map.set_cell(0, 0);
        map.set_cell(64, 0);  // Next tile
        map.set_cell(0, 64);  // Different tile

        assert!(map.get_cell(0, 0));
        assert!(map.get_cell(64, 0));
        assert!(map.get_cell(0, 64));
        assert!(!map.get_cell(1, 1));

        assert_eq!(map.tiles.len(), 3);
    }

    #[test]
    fn test_cell_to_tile() {
        let map = SparseTileMap::new(1.0, 0.0, 0.0);

        let (coord, lc, lr) = map.cell_to_tile(0, 0);
        assert_eq!(coord, TileCoord { tx: 0, ty: 0 });
        assert_eq!((lc, lr), (0, 0));

        let (coord, lc, lr) = map.cell_to_tile(63, 63);
        assert_eq!(coord, TileCoord { tx: 0, ty: 0 });
        assert_eq!((lc, lr), (63, 63));

        let (coord, lc, lr) = map.cell_to_tile(64, 64);
        assert_eq!(coord, TileCoord { tx: 1, ty: 1 });
        assert_eq!((lc, lr), (0, 0));

        // Negative coordinates
        let (coord, lc, lr) = map.cell_to_tile(-1, -1);
        assert_eq!(coord, TileCoord { tx: -1, ty: -1 });
        assert_eq!((lc, lr), (63, 63));
    }
}
