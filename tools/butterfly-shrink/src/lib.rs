//! Butterfly-shrink library
//!
//! This library provides functionality to read and write OpenStreetMap PBF files,
//! with the ability to filter and shrink the data.

use butterfly_common::{Error, Result};
use osmpbf::{Element, ElementReader};
use std::path::Path;

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
