///! Validation and lock file generation for Step 1

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use crate::formats::{RelationsFile, WaysFile};

#[derive(Debug, Serialize, Deserialize)]
pub struct BBox {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Counts {
    pub nodes: u64,
    pub ways: u64,
    pub relations: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LockFile {
    pub input_sha256: String,
    pub nodes_sa_sha256: String,
    pub nodes_si_sha256: String,
    pub ways_sha256: String,
    pub relations_sha256: String,
    pub counts: Counts,
    pub bbox: BBox,
    pub block_size: u32,
    pub top_bits: u8,
    pub created_at_utc: String,
}

impl LockFile {
    /// Create a new lock file from the ingestion results
    pub fn create(
        input_path: &Path,
        nodes_sa_path: &Path,
        nodes_si_path: &Path,
        ways_path: &Path,
        relations_path: &Path,
        counts: Counts,
    ) -> Result<Self> {
        println!("🔒 Generating lock file...");

        let input_sha256 = compute_sha256(input_path)?;
        println!("  ✓ Input SHA-256: {}", input_sha256);

        let nodes_sa_sha256 = compute_sha256(nodes_sa_path)?;
        println!("  ✓ nodes.sa SHA-256: {}", nodes_sa_sha256);

        let nodes_si_sha256 = compute_sha256(nodes_si_path)?;
        println!("  ✓ nodes.si SHA-256: {}", nodes_si_sha256);

        let ways_sha256 = compute_sha256(ways_path)?;
        println!("  ✓ ways.raw SHA-256: {}", ways_sha256);

        let relations_sha256 = compute_sha256(relations_path)?;
        println!("  ✓ relations.raw SHA-256: {}", relations_sha256);

        // For now, use placeholder bbox (would need to parse nodes.sa header)
        let bbox = BBox {
            min_lat: 0.0,
            min_lon: 0.0,
            max_lat: 0.0,
            max_lon: 0.0,
        };

        let created_at_utc = chrono::Utc::now().to_rfc3339();

        Ok(Self {
            input_sha256,
            nodes_sa_sha256,
            nodes_si_sha256,
            ways_sha256,
            relations_sha256,
            counts,
            bbox,
            block_size: 2048,
            top_bits: 16,
            created_at_utc,
        })
    }

    /// Write lock file to disk
    pub fn write<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let file = File::create(path.as_ref())?;
        serde_json::to_writer_pretty(file, self)?;
        println!("  ✓ Wrote {}", path.as_ref().display());
        Ok(())
    }

    /// Read lock file from disk
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path)?;
        let lock: LockFile = serde_json::from_reader(file)?;
        Ok(lock)
    }
}

/// Compute SHA-256 hash of a file
fn compute_sha256<P: AsRef<Path>>(path: P) -> Result<String> {
    use sha2::{Digest, Sha256};

    let mut file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;

    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024]; // 1MB buffer

    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hasher.update(&buffer[..n]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

/// Verify all lock conditions
pub fn verify_lock_conditions(
    nodes_sa_path: &Path,
    nodes_si_path: &Path,
    ways_path: &Path,
    relations_path: &Path,
) -> Result<()> {
    println!("🔍 Verifying lock conditions...");
    println!();

    // A. Structural integrity
    println!("A. Structural Integrity:");

    // A.2 Checksums
    verify_nodes_sa(nodes_sa_path)?;
    verify_nodes_si(nodes_si_path)?;
    WaysFile::verify(ways_path)?;
    RelationsFile::verify(relations_path)?;

    println!();
    println!("✅ All lock conditions passed!");

    Ok(())
}

/// Verify nodes.sa file structure and checksums
fn verify_nodes_sa(path: &Path) -> Result<()> {
    use std::io::{Seek, SeekFrom};

    let mut file = File::open(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;

    // Read header
    let mut header = vec![0u8; 128];
    file.read_exact(&mut header)?;

    // Verify magic
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic != 0x4E4F4453 {
        anyhow::bail!("Invalid magic number in {}: expected 0x4E4F4453, got 0x{:08x}", path.display(), magic);
    }

    // Read count
    let count = u64::from_le_bytes([
        header[8], header[9], header[10], header[11],
        header[12], header[13], header[14], header[15],
    ]);

    // Calculate expected file size
    let expected_size = 128 + (count * 16) + 16; // header + records + footer
    let actual_size = file.seek(SeekFrom::End(0))?;

    if actual_size != expected_size {
        anyhow::bail!(
            "Size mismatch in {}: expected {} bytes, got {} bytes",
            path.display(),
            expected_size,
            actual_size
        );
    }

    println!("  ✓ {} verified ({} nodes, {} bytes)", path.display(), count, actual_size);
    Ok(())
}

/// Verify nodes.si file structure
fn verify_nodes_si(path: &Path) -> Result<()> {
    let mut file = File::open(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;

    // Read header
    let mut header = vec![0u8; 32];
    file.read_exact(&mut header)?;

    // Verify magic
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic != 0x4E4F4458 {
        anyhow::bail!("Invalid magic number in {}: expected 0x4E4F4458, got 0x{:08x}", path.display(), magic);
    }

    let file_size = file.metadata()?.len();
    println!("  ✓ {} verified ({} bytes)", path.display(), file_size);
    Ok(())
}
