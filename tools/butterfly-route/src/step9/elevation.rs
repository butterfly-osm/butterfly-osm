//! SRTM elevation data loading and lookup.
//!
//! Supports SRTM1 (1 arc-second, 3601x3601) and SRTM3 (3 arc-second, 1201x1201)
//! .hgt tiles. Provides bilinear interpolation for sub-pixel accuracy.
//!
//! # File Format
//!
//! Each .hgt file covers 1 degree x 1 degree of lat/lon.
//! Filename encodes the SW corner: `N50E004.hgt` covers lat 50-51, lon 4-5.
//! Data is row-major, big-endian signed 16-bit integers.
//! Row 0 = northernmost row, column 0 = westernmost column.
//! Special value -32768 means void/no data.

use std::collections::HashMap;
use std::fs;
use std::io::{self, Read};
use std::path::Path;

/// Void/no-data sentinel in SRTM tiles.
const SRTM_VOID: i16 = -32768;

/// A single SRTM tile covering 1 degree x 1 degree of lat/lon.
pub struct SrtmTile {
    /// SW corner latitude (signed, negative for southern hemisphere)
    lat_sw: i16,
    /// SW corner longitude (signed, negative for western hemisphere)
    lon_sw: i16,
    /// Number of samples per side (1201 for SRTM3, 3601 for SRTM1)
    samples_per_side: u16,
    /// Row-major elevation data in meters. Row 0 is northernmost.
    data: Vec<i16>,
}

impl SrtmTile {
    /// Create a new tile from raw data (used in tests).
    pub fn new(lat_sw: i16, lon_sw: i16, samples_per_side: u16, data: Vec<i16>) -> Self {
        assert_eq!(
            data.len(),
            (samples_per_side as usize) * (samples_per_side as usize),
            "Data length must be samples_per_side^2"
        );
        Self {
            lat_sw,
            lon_sw,
            samples_per_side,
            data,
        }
    }

    /// Get the raw elevation value at integer row/col without interpolation.
    /// Returns None if the value is void (-32768) or indices are out of range.
    fn get_raw(&self, row: usize, col: usize) -> Option<i16> {
        let n = self.samples_per_side as usize;
        if row >= n || col >= n {
            return None;
        }
        let val = self.data[row * n + col];
        if val == SRTM_VOID {
            return None;
        }
        Some(val)
    }

    /// Get the bilinearly interpolated elevation at a given (lat, lon).
    /// Returns None if the point is outside this tile or any of the four
    /// surrounding samples is void.
    fn interpolate(&self, lat: f64, lon: f64) -> Option<f64> {
        let n = self.samples_per_side as usize;
        let n_intervals = n - 1; // number of intervals between samples

        // Fractional position within the tile.
        // lat_sw is the southern edge, lat_sw + 1 is the northern edge.
        // Row 0 = north, row (n-1) = south.
        let frac_lat = lat - self.lat_sw as f64; // 0.0 at south, 1.0 at north
        let frac_lon = lon - self.lon_sw as f64; // 0.0 at west, 1.0 at east

        // Check bounds: the tile covers [lat_sw, lat_sw+1] x [lon_sw, lon_sw+1]
        if frac_lat < 0.0 || frac_lat > 1.0 || frac_lon < 0.0 || frac_lon > 1.0 {
            return None;
        }

        // Convert to row/col coordinates (floating point).
        // Row increases southward: row 0 at north (lat_sw + 1), row (n-1) at south (lat_sw).
        let row_f = (1.0 - frac_lat) * n_intervals as f64;
        let col_f = frac_lon * n_intervals as f64;

        // Clamp to valid range to handle exact boundary values
        let row_f = row_f.clamp(0.0, (n_intervals) as f64);
        let col_f = col_f.clamp(0.0, (n_intervals) as f64);

        // Integer indices of the top-left corner of the interpolation cell
        let row0 = row_f.floor() as usize;
        let col0 = col_f.floor() as usize;

        // Handle exact edge case: if we are exactly on the last row/col,
        // step back one so we have a valid 2x2 cell.
        let row0 = if row0 >= n_intervals {
            n_intervals - 1
        } else {
            row0
        };
        let col0 = if col0 >= n_intervals {
            n_intervals - 1
        } else {
            col0
        };

        let row1 = row0 + 1;
        let col1 = col0 + 1;

        // Get four surrounding elevation values
        let v00 = self.get_raw(row0, col0)? as f64; // top-left
        let v01 = self.get_raw(row0, col1)? as f64; // top-right
        let v10 = self.get_raw(row1, col0)? as f64; // bottom-left
        let v11 = self.get_raw(row1, col1)? as f64; // bottom-right

        // Fractional position within the cell
        let dr = row_f - row0 as f64;
        let dc = col_f - col0 as f64;

        // Bilinear interpolation
        let top = v00 + (v01 - v00) * dc;
        let bot = v10 + (v11 - v10) * dc;
        let val = top + (bot - top) * dr;

        Some(val)
    }
}

/// Parse an SRTM .hgt filename into (lat_sw, lon_sw).
///
/// Expected format: `N50E004.hgt`, `S12W077.hgt`, etc.
/// The letter before the latitude is N (positive) or S (negative).
/// The letter before the longitude is E (positive) or W (negative).
///
/// Returns `None` if the filename does not match the expected pattern.
pub fn parse_hgt_filename(filename: &str) -> Option<(i16, i16)> {
    // Strip .hgt extension if present
    let stem = filename.strip_suffix(".hgt").unwrap_or(filename);

    if stem.len() != 7 {
        return None;
    }

    let bytes = stem.as_bytes();

    let lat_sign = match bytes[0] {
        b'N' | b'n' => 1i16,
        b'S' | b's' => -1i16,
        _ => return None,
    };

    let lat_digits = &stem[1..3];
    let lat_val: i16 = lat_digits.parse().ok()?;

    let lon_sign = match bytes[3] {
        b'E' | b'e' => 1i16,
        b'W' | b'w' => -1i16,
        _ => return None,
    };

    let lon_digits = &stem[4..7];
    let lon_val: i16 = lon_digits.parse().ok()?;

    Some((lat_sign * lat_val, lon_sign * lon_val))
}

/// Load a single SRTM .hgt tile from a file path.
///
/// Automatically detects SRTM1 vs SRTM3 from file size:
/// - SRTM1: 3601 * 3601 * 2 = 25,934,402 bytes
/// - SRTM3: 1201 * 1201 * 2 = 2,884,802 bytes
fn load_tile_from_file(path: &Path) -> io::Result<SrtmTile> {
    let filename = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Invalid filename"))?;

    let (lat_sw, lon_sw) = parse_hgt_filename(filename).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("Cannot parse SRTM filename: {}", filename),
        )
    })?;

    let mut file = fs::File::open(path)?;
    let metadata = file.metadata()?;
    let file_size = metadata.len();

    let samples_per_side: u16 = match file_size {
        25_934_402 => 3601, // SRTM1
        2_884_802 => 1201,  // SRTM3
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Unexpected .hgt file size: {} bytes (expected 25934402 for SRTM1 or 2884802 for SRTM3)",
                    file_size
                ),
            ));
        }
    };

    let n_samples = (samples_per_side as usize) * (samples_per_side as usize);
    let mut buf = vec![0u8; n_samples * 2];
    file.read_exact(&mut buf)?;

    // Convert big-endian i16 values
    let data: Vec<i16> = buf
        .chunks_exact(2)
        .map(|pair| i16::from_be_bytes([pair[0], pair[1]]))
        .collect();

    Ok(SrtmTile {
        lat_sw,
        lon_sw,
        samples_per_side,
        data,
    })
}

/// Collection of SRTM tiles for elevation lookup.
///
/// After loading, this struct is read-only and safe to share across threads
/// (`Send + Sync` is automatically derived since all fields are owned data).
pub struct ElevationData {
    /// Tiles indexed by (lat_sw, lon_sw) of their SW corner.
    tiles: HashMap<(i16, i16), SrtmTile>,
}

impl ElevationData {
    /// Load all .hgt files from a directory.
    ///
    /// Scans the directory (non-recursively) for files ending in `.hgt`,
    /// parses each one, and stores it indexed by its SW corner coordinates.
    ///
    /// Returns an `ElevationData` even if no tiles are found (it will just
    /// return `None` for all lookups).
    pub fn load_from_dir(dir: &Path) -> io::Result<Self> {
        let mut tiles = HashMap::new();

        let entries = fs::read_dir(dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext.eq_ignore_ascii_case("hgt") {
                        let tile = load_tile_from_file(&path)?;
                        println!(
                            "  Loaded SRTM tile: {}x{} samples at ({}, {})",
                            tile.samples_per_side,
                            tile.samples_per_side,
                            tile.lat_sw,
                            tile.lon_sw
                        );
                        tiles.insert((tile.lat_sw, tile.lon_sw), tile);
                    }
                }
            }
        }

        Ok(Self { tiles })
    }

    /// Create an empty ElevationData with no tiles (for testing or when no data is available).
    pub fn empty() -> Self {
        Self {
            tiles: HashMap::new(),
        }
    }

    /// Create from a list of pre-built tiles (for testing).
    pub fn from_tiles(tile_list: Vec<SrtmTile>) -> Self {
        let mut tiles = HashMap::new();
        for tile in tile_list {
            tiles.insert((tile.lat_sw, tile.lon_sw), tile);
        }
        Self { tiles }
    }

    /// Return how many tiles are loaded.
    pub fn tile_count(&self) -> usize {
        self.tiles.len()
    }

    /// Get elevation at a point using bilinear interpolation.
    ///
    /// Returns `None` if:
    /// - No tile is loaded for the given coordinates
    /// - The point falls in a void area (ocean, missing data)
    /// - Any of the four surrounding samples is void
    pub fn elevation_at(&self, lat: f64, lon: f64) -> Option<f64> {
        // Determine which tile this point belongs to.
        // The tile with SW corner (floor(lat), floor(lon)) contains this point.
        // However, when lat or lon is exactly an integer, it sits on the
        // boundary between two tiles. floor(51.0) = 51, but tile (50, _)
        // covers lat [50, 51] inclusive. We try the primary tile first,
        // then fall back to the tile one step south/west if the point
        // lies exactly on the north/east edge.
        let tile_lat = lat.floor() as i16;
        let tile_lon = lon.floor() as i16;

        // Try the primary tile first
        if let Some(tile) = self.tiles.get(&(tile_lat, tile_lon)) {
            if let Some(elev) = tile.interpolate(lat, lon) {
                return Some(elev);
            }
        }

        // If lat is exactly on a degree boundary, try the tile to the south
        // (which covers up to lat_sw + 1 inclusive)
        let try_lat_south = lat == lat.floor() && lat == tile_lat as f64;
        let try_lon_west = lon == lon.floor() && lon == tile_lon as f64;

        if try_lat_south {
            if let Some(tile) = self.tiles.get(&(tile_lat - 1, tile_lon)) {
                if let Some(elev) = tile.interpolate(lat, lon) {
                    return Some(elev);
                }
            }
        }

        if try_lon_west {
            if let Some(tile) = self.tiles.get(&(tile_lat, tile_lon - 1)) {
                if let Some(elev) = tile.interpolate(lat, lon) {
                    return Some(elev);
                }
            }
        }

        // If both lat and lon are on boundaries, try the diagonal tile (SW)
        if try_lat_south && try_lon_west {
            if let Some(tile) = self.tiles.get(&(tile_lat - 1, tile_lon - 1)) {
                if let Some(elev) = tile.interpolate(lat, lon) {
                    return Some(elev);
                }
            }
        }

        None
    }

    /// Get elevations for multiple points (batch).
    ///
    /// Each coordinate is `[lat, lon]`.
    /// Returns a vector of the same length, with `None` for points outside
    /// loaded tiles or in void areas.
    pub fn elevations_batch(&self, coords: &[[f64; 2]]) -> Vec<Option<f64>> {
        coords
            .iter()
            .map(|&[lat, lon]| self.elevation_at(lat, lon))
            .collect()
    }

    /// Get an elevation profile along a path, sampling at regular intervals.
    ///
    /// `path` is a slice of `[lat, lon]` waypoints defining a polyline.
    /// `interval_m` is the desired sampling distance in meters along the path.
    ///
    /// Returns elevation points sampled at approximately `interval_m` spacing,
    /// plus the start and end points of each segment. Points where elevation
    /// data is unavailable are skipped.
    pub fn elevation_profile(
        &self,
        path: &[[f64; 2]],
        interval_m: f64,
    ) -> Vec<ElevationPoint> {
        if path.is_empty() {
            return Vec::new();
        }
        if interval_m <= 0.0 {
            return Vec::new();
        }

        let mut result = Vec::new();
        let mut cumulative_dist = 0.0;

        // Always sample the start point
        if let Some(elev) = self.elevation_at(path[0][0], path[0][1]) {
            result.push(ElevationPoint {
                lat: path[0][0],
                lon: path[0][1],
                elevation: elev,
                distance_m: 0.0,
            });
        }

        let mut residual = 0.0; // distance remaining from last sample to next interval

        for i in 1..path.len() {
            let lat0 = path[i - 1][0];
            let lon0 = path[i - 1][1];
            let lat1 = path[i][0];
            let lon1 = path[i][1];

            let seg_len = haversine_distance(lat0, lon0, lat1, lon1);
            if seg_len < 1e-9 {
                cumulative_dist += seg_len;
                continue;
            }

            // Walk along this segment at interval_m spacing
            let mut along = interval_m - residual;
            while along < seg_len {
                let frac = along / seg_len;
                let lat = lat0 + (lat1 - lat0) * frac;
                let lon = lon0 + (lon1 - lon0) * frac;
                let dist = cumulative_dist + along;

                if let Some(elev) = self.elevation_at(lat, lon) {
                    result.push(ElevationPoint {
                        lat,
                        lon,
                        elevation: elev,
                        distance_m: dist,
                    });
                }

                along += interval_m;
            }

            residual = seg_len - (along - interval_m);
            cumulative_dist += seg_len;
        }

        // Always sample the end point (if path has more than one point)
        if path.len() > 1 {
            let last = path[path.len() - 1];
            if let Some(elev) = self.elevation_at(last[0], last[1]) {
                // Avoid duplicate if the last interval sample landed exactly on the endpoint
                let dominated = result
                    .last()
                    .map(|p| (p.distance_m - cumulative_dist).abs() < 0.01)
                    .unwrap_or(false);
                if !dominated {
                    result.push(ElevationPoint {
                        lat: last[0],
                        lon: last[1],
                        elevation: elev,
                        distance_m: cumulative_dist,
                    });
                }
            }
        }

        result
    }

    /// Check if we have tile coverage for a given bounding box.
    ///
    /// Returns true only if every 1x1 degree tile needed to cover the
    /// bounding box is loaded.
    pub fn has_coverage(
        &self,
        min_lat: f64,
        min_lon: f64,
        max_lat: f64,
        max_lon: f64,
    ) -> bool {
        let lat_start = min_lat.floor() as i16;
        let lat_end = max_lat.ceil() as i16;
        let lon_start = min_lon.floor() as i16;
        let lon_end = max_lon.ceil() as i16;

        // ceil(x) - 1 gives the highest tile SW-corner needed.
        // For example, max_lat=50.5 => ceil=51 => need tile 50.
        // max_lat=51.0 => ceil=51 => need tile 50 (tile 50 covers up to 51 inclusive).
        let lat_end = lat_end - 1;
        let lon_end = lon_end - 1;

        for lat in lat_start..=lat_end {
            for lon in lon_start..=lon_end {
                if !self.tiles.contains_key(&(lat, lon)) {
                    return false;
                }
            }
        }
        true
    }
}

/// A point along an elevation profile.
#[derive(Debug, Clone)]
pub struct ElevationPoint {
    /// Latitude in degrees
    pub lat: f64,
    /// Longitude in degrees
    pub lon: f64,
    /// Elevation in meters above sea level
    pub elevation: f64,
    /// Cumulative distance from the start of the path, in meters
    pub distance_m: f64,
}

// ============ Height Endpoint Types ============

use serde::{Deserialize, Serialize};

/// Request for the GET /height endpoint.
///
/// Coordinates are passed as a pipe-separated string of "lon,lat" pairs
/// (matching Valhalla convention).
///
/// Example: `?coordinates=4.3517,50.8503|4.4017,50.8603`
#[derive(Debug, Deserialize)]
pub struct HeightRequest {
    /// Pipe-separated coordinate pairs: "lon,lat|lon,lat|..."
    pub coordinates: String,
}

/// Response from the /height endpoint.
#[derive(Debug, Serialize)]
pub struct HeightResponse {
    /// One result per input coordinate, in order.
    pub heights: Vec<HeightResult>,
}

/// Elevation result for a single coordinate.
#[derive(Debug, Serialize)]
pub struct HeightResult {
    /// The input coordinate as [lon, lat].
    pub location: [f64; 2],
    /// Elevation in meters above sea level, or null if no data.
    pub elevation: Option<f64>,
}

/// Parse a coordinate string in "lon,lat|lon,lat|..." format.
///
/// Returns a vec of (lon, lat) pairs, or an error string describing the problem.
pub fn parse_coordinates(input: &str) -> Result<Vec<(f64, f64)>, String> {
    let input = input.trim();
    if input.is_empty() {
        return Err("coordinates parameter is empty".to_string());
    }

    let mut result = Vec::new();
    for (i, pair_str) in input.split('|').enumerate() {
        let parts: Vec<&str> = pair_str.split(',').collect();
        if parts.len() != 2 {
            return Err(format!(
                "coordinate {} ('{}') must have exactly 2 values (lon,lat) separated by comma",
                i, pair_str
            ));
        }
        let lon: f64 = parts[0].trim().parse().map_err(|_| {
            format!(
                "coordinate {} ('{}') has invalid longitude: '{}'",
                i, pair_str, parts[0]
            )
        })?;
        let lat: f64 = parts[1].trim().parse().map_err(|_| {
            format!(
                "coordinate {} ('{}') has invalid latitude: '{}'",
                i, pair_str, parts[1]
            )
        })?;

        // Basic range validation
        if !(-90.0..=90.0).contains(&lat) {
            return Err(format!(
                "coordinate {} has latitude {} outside valid range [-90, 90]",
                i, lat
            ));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(format!(
                "coordinate {} has longitude {} outside valid range [-180, 180]",
                i, lon
            ));
        }

        result.push((lon, lat));
    }

    Ok(result)
}

/// Handle a height request against loaded elevation data.
///
/// This is a pure function suitable for calling from an Axum handler.
/// Returns `Ok(HeightResponse)` on success, or `Err(String)` with an error message.
pub fn handle_height_request(
    elevation: &ElevationData,
    req: &HeightRequest,
) -> Result<HeightResponse, String> {
    let coords = parse_coordinates(&req.coordinates)?;

    if coords.len() > 10_000 {
        return Err(format!(
            "Too many coordinates: {} (maximum 10000)",
            coords.len()
        ));
    }

    let heights: Vec<HeightResult> = coords
        .iter()
        .map(|&(lon, lat)| {
            let elevation = elevation.elevation_at(lat, lon);
            HeightResult {
                location: [lon, lat],
                elevation,
            }
        })
        .collect();

    Ok(HeightResponse { heights })
}

// ============ Utility Functions ============

/// Haversine distance between two points in meters.
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;

    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();

    let a =
        (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

// ============ Tests ============

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: create a small synthetic tile with known elevation values.
    // For a 3x3 tile (samples_per_side = 3), we have 2 intervals.
    // Row 0 = north (lat_sw + 1), Row 2 = south (lat_sw).
    // Col 0 = west (lon_sw), Col 2 = east (lon_sw + 1).
    fn make_3x3_tile(lat_sw: i16, lon_sw: i16) -> SrtmTile {
        // Layout (geographic):
        //
        //   NW=100  N=200  NE=300    (row 0, lat = lat_sw + 1)
        //   W=400   C=500  E=600     (row 1, lat = lat_sw + 0.5)
        //   SW=700  S=800  SE=900    (row 2, lat = lat_sw)
        //
        #[rustfmt::skip]
        let data = vec![
            100, 200, 300,
            400, 500, 600,
            700, 800, 900,
        ];
        SrtmTile::new(lat_sw, lon_sw, 3, data)
    }

    #[test]
    fn test_parse_hgt_filename() {
        // Standard northern/eastern hemisphere
        assert_eq!(parse_hgt_filename("N50E004.hgt"), Some((50, 4)));
        // Southern/western hemisphere
        assert_eq!(parse_hgt_filename("S12W077.hgt"), Some((-12, -77)));
        // Zero cases
        assert_eq!(parse_hgt_filename("N00E000.hgt"), Some((0, 0)));
        assert_eq!(parse_hgt_filename("S00W000.hgt"), Some((0, 0)));
        // Lowercase
        assert_eq!(parse_hgt_filename("n50e004.hgt"), Some((50, 4)));
        // Without extension
        assert_eq!(parse_hgt_filename("N50E004"), Some((50, 4)));
        // Invalid cases
        assert_eq!(parse_hgt_filename("X50E004.hgt"), None);
        assert_eq!(parse_hgt_filename("N50X004.hgt"), None);
        assert_eq!(parse_hgt_filename("short.hgt"), None);
        assert_eq!(parse_hgt_filename(""), None);
        assert_eq!(parse_hgt_filename("NXXEYYY.hgt"), None);
    }

    #[test]
    fn test_bilinear_interpolation() {
        let tile = make_3x3_tile(50, 4);
        let elev = ElevationData::from_tiles(vec![tile]);

        // Center of the tile (lat=50.5, lon=4.5) should be 500
        // because it's at row 1, col 1 exactly.
        let center = elev.elevation_at(50.5, 4.5).unwrap();
        assert!(
            (center - 500.0).abs() < 1e-6,
            "Center should be 500, got {}",
            center
        );

        // Midpoint between NW (100) and N (200) at row=0, col=0.5:
        // lat=51.0, lon=4.25
        // row_f = (1.0 - 1.0) * 2 = 0.0, col_f = 0.25 * 2 = 0.5
        // top = 100 + (200-100)*0.5 = 150, bot = 400 + (500-400)*0.5 = 450
        // val = 150 + (450-150)*0.0 = 150
        let nw_n = elev.elevation_at(51.0, 4.25).unwrap();
        assert!(
            (nw_n - 150.0).abs() < 1e-6,
            "NW-N midpoint should be 150, got {}",
            nw_n
        );

        // Midpoint between C (500) and E (600) at row=1, col=1.5:
        // lat=50.5, lon=4.75
        // row_f = (1.0-0.5)*2 = 1.0, col_f = 0.75*2 = 1.5
        // top = 200 + (300-200)*0.5 = 250, bot = 500 + (600-500)*0.5 = 550
        // val = 250 + (550-250)*0.0... wait, row_f=1.0, so row0=1, dr=0
        // Actually: row0=1, col0=1, dr=1.0-1.0=0.0, dc=1.5-1.0=0.5
        // v00=500(row1,col1), v01=600(row1,col2), v10=800(row2,col1), v11=900(row2,col2)
        // top = 500 + (600-500)*0.5 = 550, bot = 800 + (900-800)*0.5 = 850
        // val = 550 + (850-550)*0.0 = 550
        let c_e = elev.elevation_at(50.5, 4.75).unwrap();
        assert!(
            (c_e - 550.0).abs() < 1e-6,
            "C-E midpoint should be 550, got {}",
            c_e
        );

        // Truly interior point: lat=50.75, lon=4.25
        // frac_lat = 0.75, frac_lon = 0.25
        // row_f = (1.0 - 0.75)*2 = 0.5, col_f = 0.25*2 = 0.5
        // row0=0, col0=0, dr=0.5, dc=0.5
        // v00=100(r0c0), v01=200(r0c1), v10=400(r1c0), v11=500(r1c1)
        // top = 100 + (200-100)*0.5 = 150
        // bot = 400 + (500-400)*0.5 = 450
        // val = 150 + (450-150)*0.5 = 300
        let interior = elev.elevation_at(50.75, 4.25).unwrap();
        assert!(
            (interior - 300.0).abs() < 1e-6,
            "Interior (50.75, 4.25) should be 300, got {}",
            interior
        );
    }

    #[test]
    fn test_elevation_at_corners() {
        let tile = make_3x3_tile(50, 4);
        let elev = ElevationData::from_tiles(vec![tile]);

        // SW corner: lat=50, lon=4 => row=2, col=0 => 700
        let sw = elev.elevation_at(50.0, 4.0).unwrap();
        assert!(
            (sw - 700.0).abs() < 1e-6,
            "SW corner should be 700, got {}",
            sw
        );

        // NW corner: lat=51, lon=4 => row=0, col=0 => 100
        let nw = elev.elevation_at(51.0, 4.0).unwrap();
        assert!(
            (nw - 100.0).abs() < 1e-6,
            "NW corner should be 100, got {}",
            nw
        );

        // NE corner: lat=51, lon=5 => row=0, col=2 => 300
        let ne = elev.elevation_at(51.0, 5.0).unwrap();
        assert!(
            (ne - 300.0).abs() < 1e-6,
            "NE corner should be 300, got {}",
            ne
        );

        // SE corner: lat=50, lon=5 => row=2, col=2 => 900
        let se = elev.elevation_at(50.0, 5.0).unwrap();
        assert!(
            (se - 900.0).abs() < 1e-6,
            "SE corner should be 900, got {}",
            se
        );
    }

    #[test]
    fn test_void_handling() {
        // Create a tile with some void values
        #[rustfmt::skip]
        let data = vec![
            100,       200,       300,
            400,       SRTM_VOID, 600,
            700,       800,       900,
        ];
        let tile = SrtmTile::new(50, 4, 3, data);
        let elev = ElevationData::from_tiles(vec![tile]);

        // Exact void position (center): should be None
        let center = elev.elevation_at(50.5, 4.5);
        assert!(center.is_none(), "Void cell should return None");

        // Interpolation involving the void cell should also be None
        // lat=50.75, lon=4.25 uses cells (0,0)=100, (0,1)=200, (1,0)=400, (1,1)=VOID
        let near_void = elev.elevation_at(50.75, 4.25);
        assert!(
            near_void.is_none(),
            "Interpolation touching void cell should return None"
        );

        // Far corner away from void: lat=50.0, lon=4.0 (SW corner = 700)
        // This only uses the cell at (2,0) directly, but at exact corner we
        // still reference a 2x2 cell. For (50.0, 4.0):
        // row_f = (1.0 - 0.0)*2 = 2.0 => clamped to row0=1
        // col_f = 0.0*2 = 0.0 => col0=0
        // cells: (1,0)=400, (1,1)=VOID, (2,0)=700, (2,1)=800
        // => touches void, returns None
        let far_corner = elev.elevation_at(50.0, 4.0);
        assert!(
            far_corner.is_none(),
            "SW corner interpolation touches void center, should be None"
        );

        // A point that does NOT touch the void: NW area
        // lat=50.9, lon=4.0 => row_f = 0.1*2 = 0.2, col_f = 0.0
        // row0=0, col0=0 => cells (0,0)=100, (0,1)=200, (1,0)=400, (1,1)=VOID
        // touches void => None
        let nw_area = elev.elevation_at(50.9, 4.0);
        assert!(
            nw_area.is_none(),
            "NW area touching void should be None"
        );

        // Point entirely in the SE quadrant: lat=50.1, lon=4.6
        // row_f = (1.0-0.1)*2 = 1.8, col_f = 0.6*2 = 1.2
        // row0=1, col0=1 => cells (1,1)=VOID => None
        let se_area = elev.elevation_at(50.1, 4.6);
        assert!(se_area.is_none(), "SE area touching void should be None");

        // Now test with a tile that has NO voids: the NE corner area should work
        let data2 = vec![100, 200, 300, 400, 500, 600, 700, 800, 900];
        let tile2 = SrtmTile::new(50, 4, 3, data2);
        let elev2 = ElevationData::from_tiles(vec![tile2]);
        // Verify a non-void tile does return values everywhere
        assert!(
            elev2.elevation_at(50.5, 4.5).is_some(),
            "Non-void tile center should have data"
        );
    }

    #[test]
    fn test_batch_elevations() {
        let tile = make_3x3_tile(50, 4);
        let elev = ElevationData::from_tiles(vec![tile]);

        let coords = [
            [50.5, 4.5],   // inside tile: center = 500
            [50.0, 4.0],   // SW corner = 700
            [60.0, 4.0],   // outside tile (no tile at lat 60)
            [50.5, 10.0],  // outside tile (no tile at lon 10)
            [51.0, 5.0],   // NE corner = 300
        ];

        let results = elev.elevations_batch(&coords);
        assert_eq!(results.len(), 5);

        // Center: 500
        assert!((results[0].unwrap() - 500.0).abs() < 1e-6);
        // SW corner: 700
        assert!((results[1].unwrap() - 700.0).abs() < 1e-6);
        // Outside: None
        assert!(results[2].is_none());
        assert!(results[3].is_none());
        // NE corner: 300
        assert!((results[4].unwrap() - 300.0).abs() < 1e-6);
    }

    #[test]
    fn test_tile_boundary() {
        // Two adjacent tiles: (50, 4) and (50, 5)
        // They share the column at lon=5: tile1's east edge = tile2's west edge.
        let tile1 = make_3x3_tile(50, 4);

        // Second tile: different values
        #[rustfmt::skip]
        let data2 = vec![
            1000, 1100, 1200,
            1300, 1400, 1500,
            1600, 1700, 1800,
        ];
        let tile2 = SrtmTile::new(50, 5, 3, data2);
        let elev = ElevationData::from_tiles(vec![tile1, tile2]);

        // Point exactly at lon=5, lat=50.5 is on the boundary.
        // This falls in tile (50, 5) because floor(5.0) = 5.
        // Actually floor(5.0) = 5, so it looks up tile (50, 5).
        // In tile2: lat=50.5 => frac_lat=0.5, row_f = 0.5*2 = 1.0 => row0=1
        // lon=5.0 => frac_lon=0.0, col_f = 0.0 => col0=0
        // cells (1,0)=1300, (1,1)=1400, (2,0)=1600, (2,1)=1700
        // dr=0.0, dc=0.0 => val = 1300
        let boundary = elev.elevation_at(50.5, 5.0).unwrap();
        assert!(
            (boundary - 1300.0).abs() < 1e-6,
            "Boundary point at lon=5 should be 1300 from tile2, got {}",
            boundary
        );

        // Just barely inside tile1: lon=4.999...
        // floor(4.999) = 4, so tile1
        // frac_lon = 0.999, col_f = 0.999*2 = 1.998
        // col0 = 1 (since 1.998 < 2), col1 = 2
        // lat=50.5 => row_f=1.0, row0=1, dr=0.0
        // cells (1,1)=500, (1,2)=600
        // dc = 1.998 - 1.0 = 0.998
        // top = 500 + (600-500)*0.998 = 599.8
        let just_inside = elev.elevation_at(50.5, 4.999).unwrap();
        assert!(
            (just_inside - 599.8).abs() < 0.5,
            "Just inside tile1 at lon=4.999 should be ~599.8, got {}",
            just_inside
        );

        // Test vertical boundary: lat=51.0 is on the north edge of tile (50, 4).
        // floor(51.0) = 51, so the primary lookup tries tile (51, 4) which
        // doesn't exist. The fallback tries tile (50, 4) which covers
        // lat [50, 51] inclusive. The value at (51.0, 4.5) in tile (50, 4):
        // row_f = (1.0 - 1.0)*2 = 0.0, col_f = 0.5*2 = 1.0
        // row0=0, col0=1, dr=0, dc=0 => v00 = data[0*3+1] = 200
        let north_boundary = elev.elevation_at(51.0, 4.5);
        assert!(
            north_boundary.is_some(),
            "lat=51.0 should resolve via fallback to tile (50,4)"
        );
        assert!(
            (north_boundary.unwrap() - 200.0).abs() < 1e-6,
            "North boundary at (51.0, 4.5) should be 200 (N sample), got {}",
            north_boundary.unwrap()
        );

        // A point truly outside all tiles: lat=52.0, only tiles with lat_sw=50
        let outside = elev.elevation_at(52.0, 4.5);
        assert!(
            outside.is_none(),
            "lat=52.0 should be outside all loaded tiles"
        );
    }

    #[test]
    fn test_elevation_profile() {
        // Use a 3x3 tile for a known elevation surface
        let tile = make_3x3_tile(50, 4);
        let elev = ElevationData::from_tiles(vec![tile]);

        // Path from SW corner to NE corner
        let path = [[50.0, 4.0], [51.0, 5.0]];

        // The diagonal distance is roughly 157 km (1 degree lat + 1 degree lon)
        // Use a large interval so we get just a few points
        let profile = elev.elevation_profile(&path, 50_000.0);

        // Should have at least start and end points (if they have valid elevation)
        assert!(
            profile.len() >= 2,
            "Profile should have at least 2 points, got {}",
            profile.len()
        );

        // First point should be at distance 0
        assert!(
            (profile[0].distance_m).abs() < 1e-6,
            "First point should be at distance 0"
        );

        // Last point should be at the total path distance
        let last = profile.last().unwrap();
        assert!(
            last.distance_m > 100_000.0,
            "Total distance should be > 100km"
        );

        // Distances should be monotonically increasing
        for i in 1..profile.len() {
            assert!(
                profile[i].distance_m > profile[i - 1].distance_m,
                "Distances must be monotonically increasing"
            );
        }

        // Empty path
        let empty = elev.elevation_profile(&[], 100.0);
        assert!(empty.is_empty());

        // Single point path
        let single = elev.elevation_profile(&[[50.5, 4.5]], 100.0);
        assert_eq!(single.len(), 1);
        assert!((single[0].elevation - 500.0).abs() < 1e-6);
    }

    #[test]
    fn test_has_coverage() {
        let tile1 = make_3x3_tile(50, 4);
        let tile2 = make_3x3_tile(50, 5);
        let elev = ElevationData::from_tiles(vec![tile1, tile2]);

        // Fully covered bbox
        assert!(elev.has_coverage(50.1, 4.1, 50.9, 5.9));

        // Partially covered: needs tile at (50, 6) which is missing
        assert!(!elev.has_coverage(50.1, 4.1, 50.9, 6.5));

        // Needs tile at (51, 4) which is missing
        assert!(!elev.has_coverage(50.1, 4.1, 51.5, 4.9));

        // Completely outside
        assert!(!elev.has_coverage(60.0, 10.0, 61.0, 11.0));
    }

    #[test]
    fn test_parse_coordinates() {
        // Valid
        let coords = parse_coordinates("4.3517,50.8503|4.4017,50.8603").unwrap();
        assert_eq!(coords.len(), 2);
        assert!((coords[0].0 - 4.3517).abs() < 1e-9);
        assert!((coords[0].1 - 50.8503).abs() < 1e-9);
        assert!((coords[1].0 - 4.4017).abs() < 1e-9);
        assert!((coords[1].1 - 50.8603).abs() < 1e-9);

        // Single coordinate
        let single = parse_coordinates("4.0,50.0").unwrap();
        assert_eq!(single.len(), 1);

        // Empty string
        assert!(parse_coordinates("").is_err());

        // Missing component
        assert!(parse_coordinates("4.0").is_err());

        // Invalid number
        assert!(parse_coordinates("abc,50.0").is_err());

        // Out of range latitude
        assert!(parse_coordinates("4.0,91.0").is_err());

        // Out of range longitude
        assert!(parse_coordinates("181.0,50.0").is_err());

        // Negative coordinates (valid)
        let neg = parse_coordinates("-77.0,-12.0").unwrap();
        assert!((neg[0].0 - (-77.0)).abs() < 1e-9);
        assert!((neg[0].1 - (-12.0)).abs() < 1e-9);
    }

    #[test]
    fn test_handle_height_request() {
        let tile = make_3x3_tile(50, 4);
        let elev = ElevationData::from_tiles(vec![tile]);

        let req = HeightRequest {
            coordinates: "4.5,50.5|4.0,50.0|10.0,60.0".to_string(),
        };

        let resp = handle_height_request(&elev, &req).unwrap();
        assert_eq!(resp.heights.len(), 3);

        // Center
        assert!((resp.heights[0].location[0] - 4.5).abs() < 1e-9);
        assert!((resp.heights[0].location[1] - 50.5).abs() < 1e-9);
        assert!((resp.heights[0].elevation.unwrap() - 500.0).abs() < 1e-6);

        // SW corner
        assert!(resp.heights[1].elevation.is_some());

        // Outside
        assert!(resp.heights[2].elevation.is_none());
    }

    #[test]
    fn test_empty_elevation_data() {
        let elev = ElevationData::empty();

        assert_eq!(elev.tile_count(), 0);
        assert!(elev.elevation_at(50.5, 4.5).is_none());
        assert!(!elev.has_coverage(50.0, 4.0, 51.0, 5.0));
    }

    #[test]
    fn test_haversine_sanity() {
        // Brussels (50.8503, 4.3517) to Antwerp (51.2194, 4.4025)
        // Should be roughly 41 km
        let dist = haversine_distance(50.8503, 4.3517, 51.2194, 4.4025);
        assert!(
            (dist - 41_100.0).abs() < 2000.0,
            "Brussels-Antwerp should be ~41km, got {}m",
            dist
        );

        // Same point: distance = 0
        let zero = haversine_distance(50.0, 4.0, 50.0, 4.0);
        assert!(zero.abs() < 1e-6);
    }
}
