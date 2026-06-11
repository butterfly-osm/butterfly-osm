//! Sparse tile-based contour generation
//!
//! Key insight: cost should be O(frontier_complexity), not O(bbox_area).
//!
//! Approach:
//! 1. Use sparse tile map instead of dense raster
//! 2. Only allocate tiles that contain stamped segments
//! 3. Run morphology only on active tiles + their neighbors
//! 4. Run marching squares per tile with seam stitching

use anyhow::Result;
use std::collections::{HashMap, HashSet};

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
        self.tiles
            .get(&tile_coord)
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
fn get_tile_bits(map: &SparseTileMap, coord: TileCoord) -> &[u64; TILE_SIZE] {
    map.tiles
        .get(&coord)
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
        let above = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx,
                ty: coord.ty - 1,
            },
        );
        let below = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx,
                ty: coord.ty + 1,
            },
        );
        let left = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx - 1,
                ty: coord.ty,
            },
        );
        let right = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx + 1,
                ty: coord.ty,
            },
        );
        let above_left = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx - 1,
                ty: coord.ty - 1,
            },
        );
        let above_right = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx + 1,
                ty: coord.ty - 1,
            },
        );
        let below_left = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx - 1,
                ty: coord.ty + 1,
            },
        );
        let below_right = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx + 1,
                ty: coord.ty + 1,
            },
        );

        let mut new_tile = TileBitmap::new();

        for local_row in 0..TILE_SIZE {
            // Get current row and edge bits from left/right neighbors
            let cur = center[local_row];
            let left_bit = (left[local_row] >> 63) & 1;
            let right_bit = (right[local_row] & 1) << 63;
            let cur_h = cur | (cur << 1) | (cur >> 1) | left_bit | right_bit;

            // Get row above with edge bits
            let (above_row, above_left_row, above_right_row) = if local_row == 0 {
                (
                    above[TILE_SIZE - 1],
                    above_left[TILE_SIZE - 1],
                    above_right[TILE_SIZE - 1],
                )
            } else {
                (
                    center[local_row - 1],
                    left[local_row - 1],
                    right[local_row - 1],
                )
            };
            let above_left_bit = (above_left_row >> 63) & 1;
            let above_right_bit = (above_right_row & 1) << 63;
            let above_h =
                above_row | (above_row << 1) | (above_row >> 1) | above_left_bit | above_right_bit;

            // Get row below with edge bits
            let (below_row, below_left_row, below_right_row) = if local_row == TILE_SIZE - 1 {
                (below[0], below_left[0], below_right[0])
            } else {
                (
                    center[local_row + 1],
                    left[local_row + 1],
                    right[local_row + 1],
                )
            };
            let below_left_bit = (below_left_row >> 63) & 1;
            let below_right_bit = (below_right_row & 1) << 63;
            let below_h =
                below_row | (below_row << 1) | (below_row >> 1) | below_left_bit | below_right_bit;

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
        let above = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx,
                ty: coord.ty - 1,
            },
        );
        let below = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx,
                ty: coord.ty + 1,
            },
        );
        let left = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx - 1,
                ty: coord.ty,
            },
        );
        let right = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx + 1,
                ty: coord.ty,
            },
        );
        let above_left = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx - 1,
                ty: coord.ty - 1,
            },
        );
        let above_right = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx + 1,
                ty: coord.ty - 1,
            },
        );
        let below_left = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx - 1,
                ty: coord.ty + 1,
            },
        );
        let below_right = get_tile_bits(
            map,
            TileCoord {
                tx: coord.tx + 1,
                ty: coord.ty + 1,
            },
        );

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
                (
                    above[TILE_SIZE - 1],
                    above_left[TILE_SIZE - 1],
                    above_right[TILE_SIZE - 1],
                )
            } else {
                (
                    center[local_row - 1],
                    left[local_row - 1],
                    right[local_row - 1],
                )
            };
            let above_left_bit = (above_left_row >> 63) & 1;
            let above_right_bit = (above_right_row & 1) << 63;
            let above_h = above_row
                & ((above_row << 1) | above_left_bit)
                & ((above_row >> 1) | above_right_bit);

            // Build horizontal erosion for row below
            let (below_row, below_left_row, below_right_row) = if local_row == TILE_SIZE - 1 {
                (below[0], below_left[0], below_right[0])
            } else {
                (
                    center[local_row + 1],
                    left[local_row + 1],
                    right[local_row + 1],
                )
            };
            let below_left_bit = (below_left_row >> 63) & 1;
            let below_right_bit = (below_right_row & 1) << 63;
            let below_h = below_row
                & ((below_row << 1) | below_left_bit)
                & ((below_row >> 1) | below_right_bit);

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
    /// Cell size in GROUND meters. The pipeline converts it to Web-Mercator
    /// meters internally via the cos(lat) correction (#431 rank 3), so a
    /// 30 m config really is ~30 m on the ground at any latitude.
    pub cell_size_m: f64,
    pub dilation_rounds: usize,
    pub erosion_rounds: usize,
    pub simplify_tolerance_m: f64,
}

impl SparseContourConfig {
    /// Default car config (30m cells - better accuracy)
    ///
    /// #431: the closing is BALANCED (erosion_rounds == dilation_rounds) —
    /// a gap-filling closing must erode exactly what it dilated, or every
    /// un-eroded round leaves a net +1-cell uniform outward grow that is
    /// never reclaimed (~+19 m/side for car at the 300 s tier). Gap
    /// bridging strength is set by the dilation count alone; closing is
    /// extensive (result ⊇ original stamped set), so erosion can thin a
    /// dilation-created bridge but never disconnect original geometry —
    /// confirmed empirically on Belgium urban + rural origins (#431).
    pub fn for_car() -> Self {
        Self {
            cell_size_m: 30.0,
            dilation_rounds: 2,
            erosion_rounds: 2,
            simplify_tolerance_m: 30.0,
        }
    }

    /// Default bike config (40m cells)
    pub fn for_bike() -> Self {
        Self {
            cell_size_m: 40.0,
            dilation_rounds: 3,
            erosion_rounds: 3,
            simplify_tolerance_m: 40.0,
        }
    }

    /// Default foot config (25m cells)
    pub fn for_foot() -> Self {
        Self {
            cell_size_m: 25.0,
            dilation_rounds: 3,
            erosion_rounds: 3,
            simplify_tolerance_m: 25.0,
        }
    }

    /// Select config by mode name string
    pub fn for_mode_name(name: &str) -> Self {
        match name {
            "car" => Self::for_car(),
            "bike" => Self::for_bike(),
            "foot" => Self::for_foot(),
            _ => Self::for_car(), // fallback for unknown modes
        }
    }

    /// Select config by mode name and threshold (in seconds, post-#297).
    ///
    /// For large thresholds the reachable set is enormous, so we use coarser
    /// cells and stronger simplification to keep the contour pipeline fast
    /// without visible quality loss (the polygon covers hundreds of km).
    ///
    /// Threshold tiers:
    /// -  <=600 s  (10 min): base cell size (finest detail)
    /// -  <=1800 s (30 min): 2x base cell size
    /// -  <=3600 s (60 min): 4x base cell size
    /// -  >3600 s         : 6.67x base cell size
    pub fn for_mode_name_with_threshold(name: &str, threshold_s: u32) -> Self {
        let base = Self::for_mode_name(name);

        let (cell_mult, simplify_mult) = if threshold_s <= 600 {
            (1.0, 1.0)
        } else if threshold_s <= 1800 {
            (2.0, 2.0)
        } else if threshold_s <= 3600 {
            (4.0, 4.0)
        } else {
            (6.67, 8.0)
        };

        Self {
            cell_size_m: base.cell_size_m * cell_mult,
            dilation_rounds: base.dilation_rounds,
            erosion_rounds: base.erosion_rounds,
            simplify_tolerance_m: base.simplify_tolerance_m * simplify_mult,
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
    let mut stats = SparseContourStats {
        input_segments: segments.len(),
        ..Default::default()
    };

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
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;

    let mercator_segments: Vec<Vec<MercatorPoint>> = segments
        .iter()
        .map(|seg| {
            seg.points
                .iter()
                .map(|&(lat_fxp, lon_fxp)| {
                    let lat = lat_fxp as f64 / 1e7;
                    let lon = lon_fxp as f64 / 1e7;
                    min_lat = min_lat.min(lat);
                    max_lat = max_lat.max(lat);
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

    // #431 rank 3 (cos(lat) ground-sizing) was REJECTED BY VALIDATION:
    // sizing cells as true ground meters coarsened Belgium's grid from the
    // de-facto ~19 m (Mercator-inflated "30 m") to a real 30 m, and the
    // empirically tuned morphology (dilation rounds, simplify tolerance)
    // grew the polygon ~+1.7% with MORE consistency violations (5→11
    // inside on the seeded urban+rural A/B). The Mercator-meter sizing is
    // therefore kept INTENTIONALLY — `cell_size_m` is "Mercator meters at
    // the region's latitude", i.e. finer ground cells at higher latitude,
    // which the accuracy budget was tuned around. Revisit only together
    // with a morphology re-tune (#431 thread has the A/B numbers).
    let cell_size_merc = config.cell_size_m;

    // Add margin
    let margin = cell_size_merc * 3.0;
    min_x -= margin;
    min_y -= margin;

    // Step 2: Create sparse tile map and stamp segments
    let mut tile_map = SparseTileMap::new(cell_size_merc, min_x, min_y);

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
            let x = min_x + col * cell_size_merc;
            let y = min_y + row * cell_size_merc;
            from_mercator(x, y)
        })
        .collect();

    let tolerance_deg = config.simplify_tolerance_m / 111000.0;
    wgs84_contour = douglas_peucker(&wgs84_contour, tolerance_deg);
    stats.contour_vertices_after_simplify = wgs84_contour.len();
    stats.simplify_time_us = simplify_start.elapsed().as_micros() as u64;

    tracing::debug!(
        input_segments = stats.input_segments,
        active_tiles = stats.active_tiles,
        tiles_after_morph = stats.active_tiles_after_morphology,
        cells_set = stats.total_cells_set,
        stamp_us = stats.stamp_time_us,
        morphology_us = stats.morphology_time_us,
        contour_us = stats.contour_time_us,
        simplify_us = stats.simplify_time_us,
        verts_before = stats.contour_vertices_before_simplify,
        verts_after = stats.contour_vertices_after_simplify,
        cell_size_m = config.cell_size_m,
        cell_size_merc_m = cell_size_merc,
        simplify_tolerance_m = config.simplify_tolerance_m,
        "sparse contour pipeline timing"
    );

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

    // Find all boundary starts and trace each component.
    //
    // Sort for determinism (#431): the start list is collected in HashMap
    // iteration order, which is process-random. The ring's starting vertex
    // (its rotation) feeds Douglas-Peucker — which pins the first/last
    // vertex — so an unsorted start list made the simplified polygon vary
    // across identical runs.
    let mut boundary_starts = find_all_boundary_starts(map);
    boundary_starts.sort_unstable();

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
            &mut visited_edges,
        );

        if contour.len() >= 3 {
            all_contours.push(contour);
        }
    }

    // Return the largest contour (by vertex count).
    all_contours
        .into_iter()
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

        // Emit the CENTER of the boundary cell (#431 rank 2).
        //
        // Cells are floor-quantisations of the stamped road geometry: a
        // point anywhere in [col, col+1) lands in cell `col`. The previous
        // code emitted the cell's OUTER corners, placing the ring at the far
        // edge of every boundary cell — a systematic +0.5-cell MEAN outward
        // bias on straight runs (worst +1 cell, plus an extra sqrt(2)/2-cell
        // spike on convex corners). The center (col+0.5, row+0.5) is the
        // unbiased inverse of the floor snap (floor + 0.5 == round to
        // nearest), so the ring passes through the expected position of the
        // outermost stamped geometry instead of half a cell beyond it.
        //
        // All four edges of a cell share one center, so consecutive edges on
        // the same cell would emit duplicates — dedup as we go. Thin 1-cell
        // appendages collapse to zero-width out-and-back runs; downstream
        // consumers already handle self-touching rings, and any half-cell
        // debias necessarily collapses sub-cell-wide features.
        let v = (col as f64 + 0.5, row as f64 + 0.5);
        if contour.last() != Some(&v) {
            contour.push(v);
        }

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

    // The walk can close back onto the start cell, re-emitting its center as
    // the final vertex — trim it so the ring stays open here (closure and
    // CCW orientation are applied downstream by the handlers).
    while contour.len() > 1 && contour.last() == contour.first() {
        contour.pop();
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
        0 => {
            // North edge, walking East toward (col+1, row)
            let ahead_right = map.get_cell(col + 1, row - 1); // NE cell
            let ahead = map.get_cell(col + 1, row); // E cell

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
        1 => {
            // East edge, walking South toward (col+1, row+1)
            let ahead_right = map.get_cell(col + 1, row + 1); // SE cell
            let ahead = map.get_cell(col, row + 1); // S cell

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
        2 => {
            // South edge, walking West toward (col, row+1)
            let ahead_right = map.get_cell(col - 1, row + 1); // SW cell
            let ahead = map.get_cell(col - 1, row); // W cell

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
        _ => {
            // West edge (3), walking North toward (col, row)
            let ahead_right = map.get_cell(col - 1, row - 1); // NW cell
            let ahead = map.get_cell(col, row - 1); // N cell

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
        map.set_cell(64, 0); // Next tile
        map.set_cell(0, 64); // Different tile

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

    // ==================================================================
    // Adaptive cell size / simplification tests
    // ==================================================================

    #[test]
    fn test_adaptive_cell_size_small_threshold() {
        // 300 s = 5 min → should use base config (1x multiplier).
        let base = SparseContourConfig::for_mode_name("car");
        let adaptive = SparseContourConfig::for_mode_name_with_threshold("car", 300);
        assert!(
            (adaptive.cell_size_m - base.cell_size_m).abs() < 0.01,
            "small threshold should use base cell size: got {} expected {}",
            adaptive.cell_size_m,
            base.cell_size_m
        );
        assert!(
            (adaptive.simplify_tolerance_m - base.simplify_tolerance_m).abs() < 0.01,
            "small threshold should use base simplify tolerance"
        );
    }

    #[test]
    fn test_adaptive_cell_size_medium_threshold() {
        // 1800 s = 30 min → 2x multiplier.
        let base = SparseContourConfig::for_mode_name("car");
        let adaptive = SparseContourConfig::for_mode_name_with_threshold("car", 1800);
        assert!(
            (adaptive.cell_size_m - base.cell_size_m * 2.0).abs() < 0.01,
            "30-min threshold should use 2x cell size: got {} expected {}",
            adaptive.cell_size_m,
            base.cell_size_m * 2.0
        );
        assert!(
            (adaptive.simplify_tolerance_m - base.simplify_tolerance_m * 2.0).abs() < 0.01,
            "30-min threshold should use 2x simplify tolerance"
        );
    }

    #[test]
    fn test_adaptive_cell_size_large_threshold() {
        // 3600 s = 60 min → 4x multiplier.
        let base = SparseContourConfig::for_mode_name("car");
        let adaptive = SparseContourConfig::for_mode_name_with_threshold("car", 3600);
        assert!(
            (adaptive.cell_size_m - base.cell_size_m * 4.0).abs() < 0.01,
            "60-min threshold should use 4x cell size: got {} expected {}",
            adaptive.cell_size_m,
            base.cell_size_m * 4.0
        );
        assert!(
            (adaptive.simplify_tolerance_m - base.simplify_tolerance_m * 4.0).abs() < 0.01,
            "60-min threshold should use 4x simplify tolerance"
        );
    }

    #[test]
    fn test_adaptive_cell_size_huge_threshold() {
        // 7200 s = 120 min → 6.67x multiplier.
        let base = SparseContourConfig::for_mode_name("car");
        let adaptive = SparseContourConfig::for_mode_name_with_threshold("car", 7200);
        assert!(
            adaptive.cell_size_m > base.cell_size_m * 6.0,
            "120-min threshold should use >6x cell size: got {} base {}",
            adaptive.cell_size_m,
            base.cell_size_m
        );
        assert!(
            adaptive.simplify_tolerance_m > base.simplify_tolerance_m * 7.0,
            "120-min threshold should use >7x simplify tolerance: got {} base {}",
            adaptive.simplify_tolerance_m,
            base.simplify_tolerance_m
        );
    }

    #[test]
    fn test_simplification_scales_with_threshold() {
        // Verify that simplification tolerance monotonically increases with threshold.
        // Values now in seconds (post-#297).
        let thresholds_s = [100u32, 600, 1200, 3600, 7200];
        let mut prev_tolerance = 0.0f64;
        for &t in &thresholds_s {
            let config = SparseContourConfig::for_mode_name_with_threshold("car", t);
            assert!(
                config.simplify_tolerance_m >= prev_tolerance,
                "simplify_tolerance_m should be monotonically increasing: \
                 at threshold_s={} got {}, prev was {}",
                t,
                config.simplify_tolerance_m,
                prev_tolerance
            );
            prev_tolerance = config.simplify_tolerance_m;
        }
    }

    #[test]
    fn test_adaptive_preserves_morphology_rounds() {
        // Adaptive scaling should not change dilation/erosion rounds.
        let base = SparseContourConfig::for_mode_name("car");
        for &t_s in &[300u32, 1800, 3600, 7200] {
            let adaptive = SparseContourConfig::for_mode_name_with_threshold("car", t_s);
            assert_eq!(
                adaptive.dilation_rounds, base.dilation_rounds,
                "dilation_rounds should be preserved at threshold_s={}",
                t_s
            );
            assert_eq!(
                adaptive.erosion_rounds, base.erosion_rounds,
                "erosion_rounds should be preserved at threshold_s={}",
                t_s
            );
        }
    }

    #[test]
    fn test_adaptive_works_for_all_modes() {
        // Should work for car, bike, foot, and unknown modes without panicking.
        for mode in &["car", "bike", "foot", "truck", "bus"] {
            for &t_s in &[100u32, 1800, 3600, 7200] {
                let config = SparseContourConfig::for_mode_name_with_threshold(mode, t_s);
                assert!(
                    config.cell_size_m > 0.0,
                    "cell_size_m must be positive for mode={} threshold_s={}",
                    mode,
                    t_s
                );
                assert!(
                    config.simplify_tolerance_m >= 0.0,
                    "simplify_tolerance_m must be non-negative for mode={} threshold_s={}",
                    mode,
                    t_s
                );
            }
        }
    }

    #[test]
    fn test_adaptive_boundary_at_exactly_600s() {
        // 600 s = 10 min → last tier using 1x multiplier.
        let base = SparseContourConfig::for_mode_name("car");
        let adaptive = SparseContourConfig::for_mode_name_with_threshold("car", 600);
        assert!(
            (adaptive.cell_size_m - base.cell_size_m).abs() < 0.01,
            "exactly 600 s should use base cell size"
        );
    }

    #[test]
    fn test_adaptive_boundary_at_601s() {
        // 601 s → 2x multiplier (> 600 s tier).
        let base = SparseContourConfig::for_mode_name("car");
        let adaptive = SparseContourConfig::for_mode_name_with_threshold("car", 601);
        assert!(
            (adaptive.cell_size_m - base.cell_size_m * 2.0).abs() < 0.01,
            "601 s should use 2x cell size: got {} expected {}",
            adaptive.cell_size_m,
            base.cell_size_m * 2.0
        );
    }

    // ==================================================================
    // #431 rank 2: cell-center vertex emission (sub-cell debias)
    // ==================================================================

    fn map_with_cells(cells: &[(i32, i32)]) -> SparseTileMap {
        let mut map = SparseTileMap::new(1.0, 0.0, 0.0);
        for &(col, row) in cells {
            map.set_cell(col, row);
        }
        map
    }

    /// Quantise a ring vertex to half-cell units for exact comparison
    /// (centers are exact .5 multiples, representable in f64).
    fn half_cells(ring: &[(f64, f64)]) -> Vec<(i64, i64)> {
        ring.iter()
            .map(|&(x, y)| ((x * 2.0).round() as i64, (y * 2.0).round() as i64))
            .collect()
    }

    #[test]
    fn test_center_emission_square_block() {
        // 3x3 block of cells: the ring must pass through the centers of the
        // 8 perimeter cells, in boundary-walk order — NOT through the outer
        // corners (which would span [0,3] instead of [0.5,2.5]).
        let cells: Vec<(i32, i32)> = (0..3)
            .flat_map(|row| (0..3).map(move |col| (col, row)))
            .collect();
        let ring = extract_contour_sparse(&map_with_cells(&cells));

        let got: HashSet<(i64, i64)> = half_cells(&ring).into_iter().collect();
        let expected: HashSet<(i64, i64)> = [
            (1, 1),
            (3, 1),
            (5, 1),
            (5, 3),
            (5, 5),
            (3, 5),
            (1, 5),
            (1, 3),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            got, expected,
            "ring must be exactly the 8 perimeter-cell centers, got {ring:?}"
        );
        assert_eq!(ring.len(), 8, "no duplicate vertices expected");
    }

    #[test]
    fn test_center_emission_no_half_cell_overshoot() {
        // The old corner emission put the ring bbox at [0, 3]x[0, 3] for a
        // 3x3 block — +0.5 cell beyond the expected position of the stamped
        // geometry on every side. Center emission must give [0.5, 2.5].
        let cells: Vec<(i32, i32)> = (0..3)
            .flat_map(|row| (0..3).map(move |col| (col, row)))
            .collect();
        let ring = extract_contour_sparse(&map_with_cells(&cells));
        assert!(!ring.is_empty());

        let min_x = ring.iter().map(|p| p.0).fold(f64::INFINITY, f64::min);
        let max_x = ring.iter().map(|p| p.0).fold(f64::NEG_INFINITY, f64::max);
        let min_y = ring.iter().map(|p| p.1).fold(f64::INFINITY, f64::min);
        let max_y = ring.iter().map(|p| p.1).fold(f64::NEG_INFINITY, f64::max);

        assert!((min_x - 0.5).abs() < 1e-12, "min_x: {min_x}");
        assert!((max_x - 2.5).abs() < 1e-12, "max_x: {max_x}");
        assert!((min_y - 0.5).abs() < 1e-12, "min_y: {min_y}");
        assert!((max_y - 2.5).abs() < 1e-12, "max_y: {max_y}");
    }

    #[test]
    fn test_center_emission_l_shape_dedup() {
        // L-shape: three cells. Each cell contributes several boundary
        // edges, but all edges of one cell share its center — the ring must
        // be exactly the 3 distinct centers with no consecutive duplicates
        // and no trailing repeat of the first vertex (closure happens
        // downstream).
        let ring = extract_contour_sparse(&map_with_cells(&[(0, 0), (1, 0), (0, 1)]));
        let q = half_cells(&ring);

        assert_eq!(q.len(), 3, "expected 3 distinct centers, got {ring:?}");
        let got: HashSet<(i64, i64)> = q.iter().copied().collect();
        let expected: HashSet<(i64, i64)> = [(1, 1), (3, 1), (1, 3)].into_iter().collect();
        assert_eq!(got, expected);

        for w in q.windows(2) {
            assert_ne!(w[0], w[1], "consecutive duplicate vertex in {ring:?}");
        }
        assert_ne!(
            q.first(),
            q.last(),
            "ring must not be pre-closed (closure is applied downstream)"
        );
    }

    #[test]
    fn test_extract_contour_deterministic() {
        // Two components (the larger one wins) — repeated extraction over
        // freshly built maps must yield byte-identical rings. Pre-#431 the
        // trace start came from HashMap iteration order, so the ring's
        // rotation (and therefore the Douglas-Peucker output) was
        // process-random.
        let mut cells: Vec<(i32, i32)> = (0..5)
            .flat_map(|row| (0..5).map(move |col| (col, row)))
            .collect();
        // Far-away smaller component, in different tiles.
        cells.extend((200..203).flat_map(|row| (200..203).map(move |col| (col, row))));

        let reference = extract_contour_sparse(&map_with_cells(&cells));
        assert!(!reference.is_empty());
        for _ in 0..5 {
            let again = extract_contour_sparse(&map_with_cells(&cells));
            assert_eq!(
                half_cells(&again),
                half_cells(&reference),
                "extract_contour_sparse must be deterministic"
            );
        }
    }

    #[test]
    fn test_largest_component_wins() {
        // 5x5 block + far 2x2 block: the returned ring must outline the 5x5.
        let mut cells: Vec<(i32, i32)> = (0..5)
            .flat_map(|row| (0..5).map(move |col| (col, row)))
            .collect();
        cells.extend([(300, 300), (301, 300), (300, 301), (301, 301)]);

        let ring = extract_contour_sparse(&map_with_cells(&cells));
        assert!(!ring.is_empty());
        for &(x, y) in &ring {
            assert!(
                (0.0..=5.0).contains(&x) && (0.0..=5.0).contains(&y),
                "vertex ({x}, {y}) outside the largest component"
            );
        }
    }

    // ==================================================================
    // #431 rank 3: cos(lat) ground-meter cell sizing
    // ==================================================================

    /// Build a single-segment `ReachableSegment` between two WGS84 points.
    fn segment(lat0: f64, lon0: f64, lat1: f64, lon1: f64) -> ReachableSegment {
        ReachableSegment {
            points: vec![
                ((lat0 * 1e7) as i32, (lon0 * 1e7) as i32),
                ((lat1 * 1e7) as i32, (lon1 * 1e7) as i32),
            ],
        }
    }

    /// Ground meters per degree of longitude at the given latitude.
    fn ground_m_per_lon_deg(lat: f64) -> f64 {
        EARTH_RADIUS * std::f64::consts::PI / 180.0 * lat.to_radians().cos()
    }

    #[test]
    fn test_e2e_ring_debiased_within_half_cell_of_input() {
        // Full pipeline: a 2-row blob of stamped segments at 50.8°N. The
        // ring's east-west extent must track the input extent to within
        // ±0.51 ground cells. Corner emission overshot by up to +1 cell
        // (here the segment length is k + 0.1 cells, so the overshoot would
        // be +0.9 cells ≈ +27 m — a deterministic failure pre-#431).
        let lat = 50.8_f64;
        let cell_m = 30.0;
        // cell_size_m is MERCATOR meters (rank 3 rejected by validation, see
        // generate_sparse_contour) — a cell's GROUND footprint at this
        // latitude is cell_m * cos(lat) ≈ 18.96 m. The blob and the debias
        // band are expressed in that footprint.
        let ground_cell = cell_m * lat.to_radians().cos();
        let m_per_deg = ground_m_per_lon_deg(lat);
        let lon0 = 4.0;
        // 10.1 cells long: the max point sits 0.1 cells into its column.
        let lon1 = lon0 + 10.1 * ground_cell / m_per_deg;
        // Two rows 1.5 ground-cells apart -> two adjacent grid rows. The
        // grid origin sits 3 cells below the y-min input point, so the
        // y-min point lands EXACTLY on a cell boundary and its row would
        // floor unstably (3.0 vs 2.999...). Anchor the blob with a point a
        // quarter cell further south so both long rows sit mid-cell
        // (rows 3.25 and 4.75 -> 3 and 4, robust to float jitter).
        let dlat = 1.5 * ground_cell / 111_320.0;
        let anchor_lat = lat - 0.25 * ground_cell / 111_320.0;

        let config = SparseContourConfig::no_morphology(cell_m);
        let segments = vec![
            segment(anchor_lat, lon0, anchor_lat, lon0),
            segment(lat, lon0, lat, lon1),
            segment(lat + dlat, lon0, lat + dlat, lon1),
        ];
        let result = generate_sparse_contour(&segments, &config).unwrap();
        assert!(
            result.outer_ring.len() >= 4,
            "expected a 2-row rectangle ring, got {:?}",
            result.outer_ring
        );

        let ring_max_lon = result
            .outer_ring
            .iter()
            .map(|p| p.0)
            .fold(f64::NEG_INFINITY, f64::max);
        let ring_min_lon = result
            .outer_ring
            .iter()
            .map(|p| p.0)
            .fold(f64::INFINITY, f64::min);

        let half_cell_deg = 0.51 * ground_cell / m_per_deg;
        assert!(
            ring_max_lon <= lon1 + half_cell_deg,
            "east edge overshoots: ring {ring_max_lon} vs input {lon1} \
             (+{:.1} m, corner emission would give ~+27 m)",
            (ring_max_lon - lon1) * m_per_deg
        );
        assert!(
            ring_max_lon >= lon1 - half_cell_deg,
            "east edge over-shrunk: ring {ring_max_lon} vs input {lon1} \
             ({:.1} m)",
            (ring_max_lon - lon1) * m_per_deg
        );
        assert!(
            ring_min_lon >= lon0 - half_cell_deg && ring_min_lon <= lon0 + half_cell_deg,
            "west edge out of band: ring {ring_min_lon} vs input {lon0} \
             ({:.1} m)",
            (ring_min_lon - lon0) * m_per_deg
        );
    }

    #[test]
    fn test_mercator_round_trip() {
        let (lat, lon) = (50.8503, 4.3517);
        let pt = to_mercator(lat, lon);
        let (lon2, lat2) = from_mercator(pt.x, pt.y);
        assert!((lat - lat2).abs() < 1e-9, "lat: {lat} vs {lat2}");
        assert!((lon - lon2).abs() < 1e-9, "lon: {lon} vs {lon2}");
    }
}
