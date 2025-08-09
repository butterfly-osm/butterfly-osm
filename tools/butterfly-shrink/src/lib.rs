//! Butterfly-shrink library
//!
//! This library provides functionality to read and write OpenStreetMap PBF files,
//! with the ability to filter and shrink the data.

use butterfly_common::{Error, Result};
use osmpbf::{Element, ElementReader};
use std::path::Path;

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

    let lat_scale = grid_meters / 111_111.0;

    // Accurate longitude scaling: 111_320m × cos(lat) at equator
    // At extreme latitudes (>85°), grid cells become very narrow E-W
    // This is correct behavior - maintains proper distances
    let cos_lat = lat_clamped.to_radians().cos().max(0.001); // Min ~89.9°
    let lon_scale = grid_meters / (111_320.0 * cos_lat);

    // Snap to cell center (floor + 0.5)
    let lat_snapped = ((lat_clamped / lat_scale).floor() + 0.5) * lat_scale;
    let lon_snapped = ((lon / lon_scale).floor() + 0.5) * lon_scale;

    // Store as nanodegrees (OSM format)
    let lat_nano = (lat_snapped * 1e9).round() as i64;
    let lon_nano = (lon_snapped * 1e9).round() as i64;

    (lat_nano, lon_nano)
}

/// Echo a PBF file by verifying it and copying it unchanged.
///
/// The function checks that the input PBF exists, copies it directly to the output to
/// create a bitwise identical file, and reads through the elements to count them.
///
/// # Errors
/// Returns an error if the input file is missing or cannot be read.
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
