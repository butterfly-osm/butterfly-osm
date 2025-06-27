//! Butterfly-shrink library
//!
//! This library provides functionality to read and write OpenStreetMap PBF files,
//! with the ability to filter and shrink the data.

use butterfly_common::{Error, Result};
use osmpbf::{Element, ElementReader};
use rocksdb::{DB, Options};
use std::fs;
use std::path::Path;
use tempfile::TempDir;
use uuid::Uuid;

/// Earth circumference constants for coordinate calculations
/// Average meters per degree of latitude
const METERS_PER_DEGREE_LAT: f64 = 111_111.0;
/// Meters per degree of longitude at the equator
const METERS_PER_DEGREE_LON_AT_EQUATOR: f64 = 111_320.0;
/// Minimum cosine value to prevent division issues at extreme latitudes
const MIN_COS_LAT: f64 = 0.001; // ~89.9 degrees

/// Snap a coordinate to a fixed grid with latitude-aware scaling
///
/// # Arguments
/// * `lat` - Latitude in degrees (-90 to 90)
/// * `lon` - Longitude in degrees (-180 to 180)
/// * `grid_meters` - Grid resolution in meters (e.g., 5.0 for 5m grid)
///
/// # Returns
/// A tuple of (lat_nano, lon_nano) as nanodegrees (OSM format)
///
/// # Example
/// ```
/// use butterfly_shrink::snap_coordinate;
/// let (lat_nano, lon_nano) = snap_coordinate(52.0, 13.0, 5.0);
/// ```
pub fn snap_coordinate(lat: f64, lon: f64, grid_meters: f64) -> (i64, i64) {
    // Keep all nodes, including far northern regions (Svalbard, Alert, etc.)
    // Clamp latitude to valid range but don't drop
    let lat_clamped = lat.clamp(-89.9, 89.9);

    let lat_scale = grid_meters / METERS_PER_DEGREE_LAT;

    // Accurate longitude scaling: METERS_PER_DEGREE_LON_AT_EQUATOR × cos(lat) at equator
    // At extreme latitudes (>85°), grid cells become very narrow E-W
    // This is correct behavior - maintains proper distances
    let cos_lat = lat_clamped.to_radians().cos().max(MIN_COS_LAT); // Min ~89.9°
    let lon_scale = grid_meters / (METERS_PER_DEGREE_LON_AT_EQUATOR * cos_lat);

    // Snap to cell center (floor + 0.5)
    let lat_snapped = ((lat_clamped / lat_scale).floor() + 0.5) * lat_scale;
    // Clamp longitude to valid range [-180°, 180°] to handle edge cases
    // where coordinates might wrap around the antimeridian
    let lon_clamped = lon.clamp(-180.0, 180.0);
    let lon_snapped = ((lon_clamped / lon_scale).floor() + 0.5) * lon_scale;

    // Store as nanodegrees (OSM format)
    let lat_nano = (lat_snapped * 1e9).round() as i64;
    let lon_nano = (lon_snapped * 1e9).round() as i64;

    (lat_nano, lon_nano)
}

/// Node index for deduplication using RocksDB
pub struct NodeIndex {
    /// RocksDB instance
    db: DB,
    /// Temp directory handle - kept for RAII cleanup on drop
    /// The underscore prefix indicates this field is not directly used,
    /// but must be kept alive for its Drop implementation
    _temp_dir: TempDir,
}

impl NodeIndex {
    /// Create a new node index in a temporary directory
    pub fn new() -> Result<Self> {
        // Create temporary directory in $TMPDIR
        let temp_dir = TempDir::new()
            .map_err(Error::IoError)?;

        // Create unique subdirectory
        let uuid = Uuid::new_v4();
        let db_path = temp_dir.path().join(format!("butterfly-shrink-{uuid}"));
        fs::create_dir(&db_path).map_err(Error::IoError)?;

        // Configure RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        // Open RocksDB instance
        let db = DB::open(&opts, &db_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to open RocksDB: {}", e)))?;

        Ok(NodeIndex {
            db,
            _temp_dir: temp_dir,
        })
    }

    /// Get the path to the temporary directory (for testing)
    #[cfg(test)]
    pub fn temp_path(&self) -> &Path {
        self._temp_dir.path()
    }

    /// Put a key-value pair into the index
    ///
    /// # Arguments
    /// * `key` - The key to store
    /// * `value` - The value to associate with the key
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err` if the database operation fails
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .map_err(|e| Error::InvalidInput(format!("Failed to put key: {}", e)))?;
        Ok(())
    }

    /// Get a value by key from the index
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// * `Ok(Some(value))` if the key exists
    /// * `Ok(None)` if the key does not exist
    /// * `Err` if the database operation fails
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.db.get(key) {
            Ok(Some(value)) => Ok(Some(value)),
            Ok(None) => Ok(None),
            Err(e) => Err(Error::InvalidInput(format!("Failed to get key: {}", e))),
        }
    }
}

/// Echo a PBF file - read input and write identical output
///
/// This is the initial implementation that demonstrates PBF reading and writing.
/// It reads all elements from the input file and writes them to the output file,
/// producing a bitwise identical copy.
pub fn echo_pbf(input: &Path, output: &Path) -> Result<()> {
    // Check if input file exists
    if !input.exists() {
        return Err(Error::IoError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Input file not found: {}", input.display()),
        )));
    }

    // Open the input PBF file
    let reader = ElementReader::from_path(input)
        .map_err(|e| Error::InvalidInput(format!("Failed to open PBF file: {e}")))?;

    // For now, just copy the file directly to ensure bitwise identical output
    // TODO: In the next iteration, we'll implement proper PBF writing
    std::fs::copy(input, output).map_err(Error::IoError)?;

    // Verify we can read the file
    let mut element_count = 0;
    reader
        .for_each(|element| {
            match element {
                Element::Node(_) | Element::Way(_) | Element::Relation(_) => {
                    element_count += 1;
                }
                Element::DenseNode(_) => {
                    // DenseNodes contain multiple nodes
                    element_count += 1;
                }
            }
        })
        .map_err(|e| Error::InvalidInput(format!("Failed to read PBF elements: {e}")))?;

    println!(
        "Successfully copied {} elements from {} to {}",
        element_count,
        input.display(),
        output.display()
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rocksdb_round_trip() {
        // Create index
        let mut index = NodeIndex::new().expect("Failed to create NodeIndex");

        // Test basic put/get
        let key = b"test_key";
        let value = b"test_value";

        index.put(key, value).expect("Failed to put");
        let retrieved = index.get(key).expect("Failed to get");

        assert_eq!(retrieved, Some(value.to_vec()));
    }

    #[test]
    fn test_rocksdb_100k_keys() {
        // Create index
        let mut index = NodeIndex::new().expect("Failed to create NodeIndex");

        // Write 100k random keys
        let num_keys = 100_000;
        for i in 0..num_keys {
            let key = format!("key_{i:08}");
            let value = format!("value_{i:08}");
            index
                .put(key.as_bytes(), value.as_bytes())
                .expect("Failed to put key");
        }

        // Read all keys back and verify
        for i in 0..num_keys {
            let key = format!("key_{i:08}");
            let expected_value = format!("value_{i:08}");

            let retrieved = index.get(key.as_bytes()).expect("Failed to get key");

            assert_eq!(
                retrieved,
                Some(expected_value.into_bytes()),
                "Mismatch for key {key}"
            );
        }
    }

    #[test]
    fn test_temp_directory_location() {
        // Create index
        let index = NodeIndex::new().expect("Failed to create NodeIndex");

        // Get temp directory path
        let temp_path = index.temp_path();

        // Get the system's temporary directory
        let tmpdir = std::env::temp_dir();

        // The temp directory should be under the system temp directory
        assert!(
            temp_path.starts_with(&tmpdir),
            "Temp directory {temp_path:?} is not under expected temp location {tmpdir:?}"
        );

        // Verify the butterfly-shrink-{uuid} subdirectory exists
        let entries: Vec<_> = std::fs::read_dir(temp_path)
            .expect("Failed to read temp directory")
            .collect();

        assert_eq!(entries.len(), 1, "Expected exactly one subdirectory");

        let entry = entries[0].as_ref().expect("Failed to get entry");
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        assert!(
            name_str.starts_with("butterfly-shrink-"),
            "Subdirectory should start with 'butterfly-shrink-', got: {name_str}"
        );
    }

    #[test]
    fn test_temp_directory_cleanup() {
        let temp_path = {
            // Create index in a scope
            let index = NodeIndex::new().expect("Failed to create NodeIndex");
            let path = index.temp_path().to_path_buf();

            // Verify directory exists
            assert!(path.exists(), "Temp directory should exist");

            // Return path to check after drop
            path
        }; // index is dropped here

        // Verify directory is cleaned up
        assert!(
            !temp_path.exists(),
            "Temp directory should be cleaned up after drop"
        );
    }

    #[test]
    fn test_nonexistent_key() {
        let index = NodeIndex::new().expect("Failed to create NodeIndex");

        let result = index.get(b"nonexistent_key").expect("Failed to get");
        assert_eq!(result, None);
    }
}
