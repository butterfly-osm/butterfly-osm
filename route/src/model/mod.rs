//! Declarative Custom Model System
//!
//! Replaces hardcoded Rust profiles (car.rs, bike.rs, foot.rs) with JSON model files.
//! Adding a new mode = dropping a `{name}.model.json` file. No Rust code changes.
//!
//! ## Architecture
//!
//! 1. **ModelSchema** — serde structs for JSON model files
//! 2. **CompiledModel** — dense arrays indexed by dictionary value_id for O(1) evaluation
//! 3. **evaluate_way() / evaluate_turn()** — evaluate tags against compiled model
//! 4. **ModeInfo** — dynamic mode discovery with deterministic alphabetical indexing

pub mod compile;
pub mod evaluate;
pub mod profiling;
pub mod schema;
pub mod types;

pub use compile::{CompiledModel, compile_model};
pub use evaluate::{evaluate_turn_full, evaluate_way};
pub use schema::ModelSchema;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

// MAX_MODES is defined in types.rs and re-exported
pub use types::MAX_MODES;

/// Mode information — dynamic replacement for the old Mode enum
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModeInfo {
    pub name: String,
    pub index: u8,
}

/// Discover available modes by scanning *.model.json files in the models directory.
/// Returns modes sorted alphabetically by name (deterministic indexing).
pub fn discover_modes(model_dir: &Path) -> Result<Vec<ModeInfo>> {
    let mut names: Vec<String> = Vec::new();

    for entry in std::fs::read_dir(model_dir)
        .with_context(|| format!("Failed to read model directory: {}", model_dir.display()))?
    {
        let entry = entry?;
        let filename = entry.file_name();
        let filename_str = filename.to_string_lossy();

        if filename_str.ends_with(".model.json") {
            // Read and parse just the name field
            let content = std::fs::read_to_string(entry.path())?;
            let schema: ModelSchema = serde_json::from_str(&content)
                .with_context(|| format!("Failed to parse {}", entry.path().display()))?;
            names.push(schema.name);
        }
    }

    // Sort alphabetically for deterministic indexing
    names.sort();

    anyhow::ensure!(
        !names.is_empty(),
        "No model files (*.model.json) found in {}",
        model_dir.display()
    );

    anyhow::ensure!(
        names.len() <= MAX_MODES,
        "Too many models: {} found, max {}. Remove some *.model.json files.",
        names.len(),
        MAX_MODES
    );

    // Check for duplicates
    for i in 1..names.len() {
        anyhow::ensure!(
            names[i] != names[i - 1],
            "Duplicate mode name: '{}'",
            names[i]
        );
    }

    Ok(names
        .into_iter()
        .enumerate()
        .map(|(i, name)| ModeInfo {
            name,
            index: i as u8,
        })
        .collect())
}

/// Load and parse a model schema from a JSON file
pub fn load_model_schema(path: &Path) -> Result<ModelSchema> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read model file: {}", path.display()))?;
    let schema: ModelSchema = serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse model JSON: {}", path.display()))?;
    Ok(schema)
}

/// Compute SHA-256 of a model file
pub fn compute_model_sha256(path: &Path) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};
    let content = std::fs::read(path)?;
    let hash = Sha256::digest(&content);
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&hash);
    Ok(sha)
}

/// Find the model file path for a given mode name
pub fn model_file_path(model_dir: &Path, mode_name: &str) -> PathBuf {
    model_dir.join(format!("{}.model.json", mode_name))
}

/// Build manifest — written by pipeline, validated by server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildManifest {
    pub build_timestamp: String,
    pub pipeline_version: String,
    pub modes: Vec<ManifestMode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestMode {
    pub name: String,
    pub index: u8,
    pub model_version: u32,
    pub model_sha256: String,
}

impl BuildManifest {
    pub fn write_to(&self, path: &Path) -> Result<()> {
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read build manifest: {}", path.display()))?;
        let manifest: BuildManifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_discover_modes_from_models_dir() {
        let models_dir = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../models"));
        if !models_dir.exists() {
            return; // Skip if models dir not present
        }
        let modes = discover_modes(&models_dir).unwrap();

        // Should find all models alphabetically: bike, bus, car, foot, motorcycle, scooter, truck, wheelchair
        assert_eq!(modes.len(), 8);
        let names: Vec<&str> = modes.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "bike",
                "bus",
                "car",
                "foot",
                "motorcycle",
                "scooter",
                "truck",
                "wheelchair"
            ]
        );
        for (i, m) in modes.iter().enumerate() {
            assert_eq!(m.index, i as u8);
        }
    }

    #[test]
    fn test_discover_modes_empty_dir() {
        let tmp = TempDir::new().unwrap();
        let result = discover_modes(tmp.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No model files"));
    }

    #[test]
    fn test_discover_modes_too_many() {
        let tmp = TempDir::new().unwrap();
        // Create 9 model files
        for i in 0..9 {
            let name = format!("mode{}", i);
            let content = format!(
                r#"{{"name":"{}","version":1,"speed":{{"unit":"km/h","highway":{{}},"overrides":[]}},"access":{{"highway":{{}}}},"oneway":{{"respect":false,"tag":"oneway","forward_values":[],"reverse_values":[],"default_oneway_highways":[]}},"priority":[],"highway_class":{{}},"class_bits":{{}},"turn_penalties":{{"turn_penalty_ds":0,"turn_bias":1.0,"u_turn_penalty_ds":0,"min_degree_for_penalty":3,"signal_delay_ds":0,"class_change_penalty_ds_per_diff":0,"max_class_diff_for_penalty":0}},"turn_restrictions":{{"respect":false,"restriction_tag":"restriction","exception_values":[]}}}}"#,
                name
            );
            std::fs::write(tmp.path().join(format!("{}.model.json", name)), content).unwrap();
        }
        let result = discover_modes(tmp.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Too many models"));
    }

    #[test]
    fn test_manifest_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("build_manifest.json");

        let manifest = BuildManifest {
            build_timestamp: "2026-02-08T16:30:00Z".to_string(),
            pipeline_version: "3.0.0".to_string(),
            modes: vec![ManifestMode {
                name: "car".to_string(),
                index: 0,
                model_version: 1,
                model_sha256: "abc123".to_string(),
            }],
        };

        manifest.write_to(&path).unwrap();
        let loaded = BuildManifest::read_from(&path).unwrap();
        assert_eq!(loaded.modes.len(), 1);
        assert_eq!(loaded.modes[0].name, "car");
    }
}
