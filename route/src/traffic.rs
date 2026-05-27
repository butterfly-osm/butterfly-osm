//! Traffic profiles — declarative JSON files describing how to scale edge
//! travel times by [`crate::density::DensityClass`] for traffic-aware
//! routing.
//!
//! ## File layout
//!
//! ```text
//! traffic/<name>.traffic.json
//! ```
//!
//! ## Example
//!
//! ```json
//! {
//!   "name": "rush_hour",
//!   "base_model": "car",
//!   "speed_factors": {
//!     "urban_high": 0.55,
//!     "urban_medium": 0.70,
//!     "urban_low": 0.85,
//!     "suburban": 0.90,
//!     "rural": 0.95
//!   }
//! }
//! ```
//!
//! ## Semantics
//!
//! Each factor `f_C` for density class `C` is applied at step 8 as a
//! multiplicative time-dilation: an edge's travel time `t` becomes `t / f_C`.
//! Factors below 1.0 *slow down* traffic (congestion); 1.0 is freeflow;
//! above 1.0 represents a road that's faster than the OSM speed limit
//! (rare — bounded at 1.5 to keep the search hierarchy stable).
//!
//! ## Schema validation
//!
//! - `name` and `base_model` must be non-empty.
//! - `speed_factors` MUST contain all five density-class keys
//!   (urban_high, urban_medium, urban_low, suburban, rural).
//! - Each factor MUST be in `[0.1, 1.5]`.
//! - Unknown keys are rejected (typo guard).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::density::DensityClass;

/// Min/max bounds for any single speed factor.
pub const MIN_FACTOR: f32 = 0.1;
pub const MAX_FACTOR: f32 = 1.5;

/// Parsed traffic profile.
#[derive(Debug, Clone, PartialEq)]
pub struct TrafficProfile {
    pub name: String,
    pub base_model: String,
    /// Indexed by `DensityClass::to_u8() as usize`.
    pub factors: [f32; 5],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TrafficProfileJson {
    name: String,
    base_model: String,
    speed_factors: BTreeMap<String, f32>,
}

impl TrafficProfile {
    /// Freeflow profile — every factor = 1.0, equivalent to current behavior.
    /// Useful as a sanity-check baseline.
    pub fn freeflow(base_model: &str) -> Self {
        Self {
            name: "freeflow".to_string(),
            base_model: base_model.to_string(),
            factors: [1.0; 5],
        }
    }

    /// Returns true iff every factor equals 1.0 within float tolerance.
    pub fn is_freeflow(&self) -> bool {
        self.factors.iter().all(|f| (f - 1.0).abs() < 1e-6)
    }

    /// Lookup factor for a density class.
    #[inline]
    pub fn factor_for(&self, class: DensityClass) -> f32 {
        self.factors[class.to_u8() as usize]
    }

    /// Load + validate a profile from a JSON file on disk.
    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read traffic profile {}", path.display()))?;
        Self::from_json(&raw)
            .with_context(|| format!("failed to parse traffic profile {}", path.display()))
    }

    /// Parse and validate a profile from a JSON string.
    pub fn from_json(raw: &str) -> Result<Self> {
        let parsed: TrafficProfileJson = serde_json::from_str(raw)?;
        Self::from_parsed(parsed)
    }

    fn from_parsed(parsed: TrafficProfileJson) -> Result<Self> {
        anyhow::ensure!(
            !parsed.name.trim().is_empty(),
            "traffic profile: 'name' must be non-empty"
        );
        anyhow::ensure!(
            !parsed.base_model.trim().is_empty(),
            "traffic profile '{}': 'base_model' must be non-empty",
            parsed.name
        );

        // Build factor array, requiring all five keys, no extras.
        let mut factors = [f32::NAN; 5];
        for (key, value) in &parsed.speed_factors {
            let class = DensityClass::parse(key).with_context(|| {
                format!(
                    "traffic profile '{}': unknown speed_factors key '{}' (allowed: urban_high, urban_medium, urban_low, suburban, rural)",
                    parsed.name, key
                )
            })?;
            anyhow::ensure!(
                value.is_finite(),
                "traffic profile '{}': speed_factors.{} = {} is not finite",
                parsed.name,
                key,
                value
            );
            anyhow::ensure!(
                (MIN_FACTOR..=MAX_FACTOR).contains(value),
                "traffic profile '{}': speed_factors.{} = {} out of range [{}, {}]",
                parsed.name,
                key,
                value,
                MIN_FACTOR,
                MAX_FACTOR
            );
            factors[class.to_u8() as usize] = *value;
        }

        for class in DensityClass::ALL {
            anyhow::ensure!(
                factors[class.to_u8() as usize].is_finite(),
                "traffic profile '{}': missing speed_factors.{}",
                parsed.name,
                class.as_str()
            );
        }

        Ok(Self {
            name: parsed.name,
            base_model: parsed.base_model,
            factors,
        })
    }

    /// Serialize back out (used by lock-file provenance).
    pub fn to_json_string(&self) -> Result<String> {
        let mut speed_factors = BTreeMap::new();
        for class in DensityClass::ALL {
            speed_factors.insert(
                class.as_str().to_string(),
                self.factors[class.to_u8() as usize],
            );
        }
        let payload = TrafficProfileJson {
            name: self.name.clone(),
            base_model: self.base_model.clone(),
            speed_factors,
        };
        Ok(serde_json::to_string_pretty(&payload)?)
    }
}

/// Discover `*.traffic.json` files in a directory. Returns an error if the
/// directory does not exist; an empty Vec if it exists but has no profiles.
pub fn discover_profiles(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read traffic dir {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path
            .file_name()
            .and_then(|f| f.to_str())
            .is_some_and(|n| n.ends_with(".traffic.json"))
        {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rush_hour_json() -> &'static str {
        r#"{
          "name": "rush_hour",
          "base_model": "car",
          "speed_factors": {
            "urban_high": 0.55,
            "urban_medium": 0.70,
            "urban_low": 0.85,
            "suburban": 0.90,
            "rural": 0.95
          }
        }"#
    }

    #[test]
    fn parses_well_formed_profile() {
        let p = TrafficProfile::from_json(rush_hour_json()).unwrap();
        assert_eq!(p.name, "rush_hour");
        assert_eq!(p.base_model, "car");
        assert!((p.factor_for(DensityClass::UrbanHigh) - 0.55).abs() < 1e-6);
        assert!((p.factor_for(DensityClass::Rural) - 0.95).abs() < 1e-6);
        assert!(!p.is_freeflow());
    }

    #[test]
    fn freeflow_factory_is_freeflow() {
        let p = TrafficProfile::freeflow("car");
        assert!(p.is_freeflow());
        for c in DensityClass::ALL {
            assert!((p.factor_for(c) - 1.0).abs() < 1e-6);
        }
    }

    #[test]
    fn rejects_missing_density_key() {
        let s = r#"{
          "name": "incomplete", "base_model": "car",
          "speed_factors": {
            "urban_high": 0.5, "urban_medium": 0.7,
            "urban_low": 0.85, "suburban": 0.9
          }
        }"#;
        let err = TrafficProfile::from_json(s).unwrap_err();
        assert!(err.to_string().contains("missing speed_factors.rural"));
    }

    #[test]
    fn rejects_unknown_key() {
        let s = r#"{
          "name": "typo", "base_model": "car",
          "speed_factors": {
            "urban_high": 0.5, "urban_medium": 0.7,
            "urban_low": 0.85, "suburbann": 0.9, "rural": 0.95
          }
        }"#;
        let err = TrafficProfile::from_json(s).unwrap_err();
        assert!(err.to_string().contains("unknown speed_factors key"));
    }

    #[test]
    fn rejects_out_of_range_factor() {
        let s = r#"{
          "name": "bad", "base_model": "car",
          "speed_factors": {
            "urban_high": 0.05, "urban_medium": 0.7,
            "urban_low": 0.85, "suburban": 0.9, "rural": 0.95
          }
        }"#;
        let err = TrafficProfile::from_json(s).unwrap_err();
        assert!(err.to_string().contains("out of range"));
    }

    #[test]
    fn rejects_empty_name() {
        let s = r#"{
          "name": "", "base_model": "car",
          "speed_factors": {
            "urban_high": 0.55, "urban_medium": 0.7,
            "urban_low": 0.85, "suburban": 0.9, "rural": 0.95
          }
        }"#;
        let err = TrafficProfile::from_json(s).unwrap_err();
        assert!(err.to_string().contains("name"));
    }

    #[test]
    fn round_trips_through_json() {
        let p = TrafficProfile::from_json(rush_hour_json()).unwrap();
        let s = p.to_json_string().unwrap();
        let p2 = TrafficProfile::from_json(&s).unwrap();
        assert_eq!(p, p2);
    }

    #[test]
    fn ships_realistic_and_rush_hour_profiles() {
        // Post-#392: only two profiles ship — `car_realistic` (baked
        // into base car) and `rush_hour` (variant). Freeflow + offpeak
        // were dropped (freeflow became identical to post-#390
        // legal-limit base, offpeak overlapped with realistic).
        let workspace = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let traffic_dir = workspace.parent().unwrap().join("traffic");
        if !traffic_dir.exists() {
            return;
        }
        let files = discover_profiles(&traffic_dir).unwrap();
        let names: Vec<String> = files
            .iter()
            .map(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .trim_end_matches(".traffic.json")
                    .to_string()
            })
            .collect();
        assert!(
            names.contains(&"car_realistic".to_string()),
            "got: {:?}",
            names
        );
        assert!(names.contains(&"rush_hour".to_string()), "got: {:?}", names);

        for f in &files {
            let p = TrafficProfile::load(f).expect("ship profile must load");
            assert!(
                p.factor_for(DensityClass::UrbanHigh) <= p.factor_for(DensityClass::Rural) + 1e-6
                    || p.is_freeflow(),
                "non-monotone factors in {}: {:?}",
                p.name,
                p.factors
            );
        }
    }

    #[test]
    fn discovers_profile_files() {
        let tmp = tempfile::TempDir::new().unwrap();
        std::fs::write(tmp.path().join("rush_hour.traffic.json"), rush_hour_json()).unwrap();
        std::fs::write(tmp.path().join("not_a_profile.json"), "{}").unwrap();
        let files = discover_profiles(tmp.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert!(
            files[0]
                .to_string_lossy()
                .ends_with("rush_hour.traffic.json")
        );
    }
}
