//! Per-country morphology tables.
//!
//! Each country has a TOML pack under `morphology/<iso2>.toml`. The pack
//! describes:
//!
//! - rendering conventions: field separators, where the postcode lives
//!   relative to the city, etc.
//! - street-type tables: long-form ↔ short-form abbreviation pairs (e.g.
//!   `Rue` ↔ `R.` in FR, `Straße` ↔ `Str.` in DE), and street-marker
//!   substrings used to identify a street as belonging to a given style.
//! - postcode validation regex (sanity-only, not used for parsing).
//!
//! The morphology tables are loaded ONCE at startup. Each augmentation
//! and canary rewriter takes a `&Morphology` so it can speak the right
//! country's idiom — moving BE/FR/NL/DE/US/GB out of `match` arms and
//! into data.

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Where the postcode goes in the rendered string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PostcodePosition {
    /// Postcode appears BEFORE the city.
    Leading,
    /// Postcode appears AFTER the city.
    Trailing,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CountryMeta {
    pub iso2: String,
    /// Display name. Loaded from TOML for documentation but not used
    /// at runtime (we identify by ISO code).
    #[allow(dead_code)]
    pub name: String,
}

#[allow(dead_code)] // fields documented for future renderer plumbing
#[derive(Debug, Clone, Deserialize)]
pub struct RenderRules {
    /// Separator placed AFTER the housenumber and before the
    /// postcode/city block (typically `, `).
    pub field_separator_after_house: String,
    /// Separator between postcode and city (typically a single space).
    pub postcode_city_separator: String,
    /// Where the postcode lives relative to the city.
    pub postcode_position: PostcodePosition,
}

#[allow(dead_code)] // postcode validation is data-only at this stage
#[derive(Debug, Clone, Deserialize)]
pub struct PostcodeRules {
    pub regex: String,
    /// Format string. Currently only `"{}"` (identity) is honoured —
    /// non-trivial canonicalization is a future extension.
    #[serde(default = "default_postcode_format")]
    pub format: String,
}

fn default_postcode_format() -> String {
    "{}".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StreetTypes {
    /// Pairs of `[long, short]` forms. Both forms keep their case.
    /// The augmenter applies `.to_lowercase()` for the lower-case
    /// variant; capitalized variants come from the TOML directly.
    #[serde(default)]
    pub long_to_short: Vec<[String; 2]>,

    /// Sub-tables of substring markers, keyed by language tag (`fr`,
    /// `nl`, `de`, etc.). Used by canary cross-country rewrites — the
    /// rewriter looks up the source country's markers, finds one in
    /// the input, and swaps it for the corresponding marker in the
    /// target country's morphology.
    #[serde(default)]
    pub markers: HashMap<String, Vec<String>>,
}

/// Loaded morphology for one country.
#[derive(Debug, Clone, Deserialize)]
pub struct Morphology {
    pub country: CountryMeta,
    /// Rendering conventions — separators, postcode position. Available
    /// for future renderer upgrades; the current generator's BIO renderer
    /// is in `bio.rs::render_canonical`. Kept here so per-country
    /// rendering can plug in without re-introducing a hardcoded path.
    #[allow(dead_code)]
    pub render: RenderRules,
    /// Postcode regex (sanity-only; not used for parsing).
    #[allow(dead_code)]
    pub postcode: PostcodeRules,
    pub street_types: StreetTypes,
}

impl Morphology {
    /// Load from a TOML file.
    pub fn from_toml_path<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("reading morphology TOML at {}", path.display()))?;
        toml::from_str::<Self>(&s)
            .with_context(|| format!("parsing morphology TOML at {}", path.display()))
    }

    /// Load by ISO code from a directory of `<iso2>.toml` files.
    pub fn load_from_dir<P: AsRef<Path>>(dir: P, iso2: &str) -> Result<Self> {
        let iso = iso2.trim().to_ascii_lowercase();
        if iso.len() != 2 {
            return Err(anyhow!("invalid ISO code {iso2:?}"));
        }
        let path = dir.as_ref().join(format!("{}.toml", iso));
        Self::from_toml_path(path)
    }

    /// Build the abbreviation table used by `augment::abbr_contract`
    /// and `augment::abbr_expand`. Returns pairs of (long, short)
    /// covering both capitalized AND lowercase variants, in the order
    /// the augmenter should try them (longest-first to avoid partial
    /// matches).
    pub fn abbreviations(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = Vec::with_capacity(self.street_types.long_to_short.len() * 2);
        for pair in &self.street_types.long_to_short {
            out.push((pair[0].clone(), pair[1].clone()));
            // Add a lowercase variant if distinct from the cased one.
            let long_l = pair[0].to_lowercase();
            let short_l = pair[1].to_lowercase();
            if long_l != pair[0] || short_l != pair[1] {
                out.push((long_l, short_l));
            }
        }
        // Sort longest-first so "Boulevard" matches before "Bd"-prefixed
        // street names with the bare "Bd" abbreviation.
        out.sort_by_key(|(l, _)| std::cmp::Reverse(l.len()));
        out
    }

    /// Lookup of street markers for a given language tag.
    pub fn markers_for(&self, lang: &str) -> Option<&[String]> {
        self.street_types
            .markers
            .get(lang)
            .map(std::vec::Vec::as_slice)
    }

    /// All marker substrings flattened (any lang).
    pub fn all_markers(&self) -> Vec<&str> {
        let mut out = Vec::new();
        for v in self.street_types.markers.values() {
            for s in v {
                out.push(s.as_str());
            }
        }
        out
    }
}

/// Registry of loaded morphology tables, keyed by uppercase ISO code.
///
/// Currently the corpus generator only needs the source country's
/// morphology + an explicit list of canary targets (loaded individually
/// via [`Morphology::load_from_dir`]), but this registry is the
/// natural shape for a future "load everything once and look up by
/// ISO" pattern (e.g. when a single corpus pass spans multiple
/// countries via PBF concatenation).
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MorphologyRegistry {
    by_iso: HashMap<String, Morphology>,
}

#[allow(dead_code)]
impl MorphologyRegistry {
    /// Build by loading the requested ISO codes from `dir/<iso2>.toml`.
    pub fn load<P: AsRef<Path>, S: AsRef<str>>(dir: P, isos: &[S]) -> Result<Self> {
        let mut by_iso = HashMap::new();
        for iso in isos {
            let m = Morphology::load_from_dir(&dir, iso.as_ref())?;
            by_iso.insert(iso.as_ref().trim().to_ascii_uppercase(), m);
        }
        Ok(Self { by_iso })
    }

    pub fn get(&self, iso2: &str) -> Option<&Morphology> {
        self.by_iso.get(&iso2.trim().to_ascii_uppercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn morphology_dir() -> PathBuf {
        // Cargo tests run with CARGO_MANIFEST_DIR pointing at the crate root.
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("morphology");
        p
    }

    #[test]
    fn loads_belgium_pack() {
        let m = Morphology::load_from_dir(morphology_dir(), "BE").unwrap();
        assert_eq!(m.country.iso2, "BE");
        assert_eq!(m.render.postcode_position, PostcodePosition::Leading);
        assert!(m.markers_for("fr").is_some());
        assert!(m.markers_for("nl").is_some());
        let abbrs = m.abbreviations();
        assert!(abbrs.iter().any(|(l, _)| l == "Boulevard"));
        // Longest-first ordering.
        assert!(
            abbrs.first().unwrap().0.len() >= abbrs.last().unwrap().0.len(),
            "abbreviations not sorted longest-first"
        );
    }

    #[test]
    fn loads_us_pack_with_trailing_postcode() {
        let m = Morphology::load_from_dir(morphology_dir(), "us").unwrap();
        assert_eq!(m.country.iso2, "US");
        assert_eq!(m.render.postcode_position, PostcodePosition::Trailing);
        let abbrs = m.abbreviations();
        assert!(abbrs.iter().any(|(l, s)| l == "Boulevard" && s == "Blvd"));
        assert!(abbrs.iter().any(|(l, s)| l == "Northwest" && s == "NW"));
    }

    #[test]
    fn loads_germany_pack() {
        let m = Morphology::load_from_dir(morphology_dir(), "DE").unwrap();
        let markers = m.markers_for("de").unwrap();
        assert!(markers.iter().any(|s| s == "straße"));
        assert!(markers.iter().any(|s| s == "platz"));
    }

    #[test]
    fn registry_loads_multiple() {
        let reg = MorphologyRegistry::load(morphology_dir(), &["BE", "FR", "DE"]).unwrap();
        assert!(reg.get("BE").is_some());
        assert!(reg.get("fr").is_some()); // case-insensitive
        assert!(reg.get("DE").is_some());
        assert!(reg.get("XX").is_none());
    }

    #[test]
    fn unknown_country_errs() {
        assert!(Morphology::load_from_dir(morphology_dir(), "ZZ").is_err());
    }

    #[test]
    fn loads_all_15_country_packs() {
        // Every country with a shipped data pack must have a morphology
        // pack. This is the contract for multi-country training: if you
        // ship a `geocode/data/packs/<iso>.toml`, you ship a morphology
        // pack here.
        let packs = [
            "AT", "AU", "BE", "BR", "CH", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "LU", "NL",
            "US",
        ];
        for iso in packs {
            let m = Morphology::load_from_dir(morphology_dir(), iso)
                .unwrap_or_else(|e| panic!("loading morphology for {iso}: {e:?}"));
            assert_eq!(m.country.iso2, iso, "iso2 mismatch for {iso}");
            assert!(
                !m.street_types.long_to_short.is_empty(),
                "{iso} has empty long_to_short"
            );
            assert!(
                !m.street_types.markers.is_empty(),
                "{iso} has empty markers"
            );
        }
    }

    #[test]
    fn loads_brazil_pack_with_pt_markers() {
        let m = Morphology::load_from_dir(morphology_dir(), "BR").unwrap();
        let markers = m.markers_for("pt").unwrap();
        assert!(markers.iter().any(|s| s == "rua"));
        assert!(markers.iter().any(|s| s == "avenida"));
    }

    #[test]
    fn loads_switzerland_with_three_languages() {
        let m = Morphology::load_from_dir(morphology_dir(), "CH").unwrap();
        assert!(m.markers_for("de").is_some());
        assert!(m.markers_for("fr").is_some());
        assert!(m.markers_for("it").is_some());
    }

    #[test]
    fn loads_japan_with_kanji_markers() {
        let m = Morphology::load_from_dir(morphology_dir(), "JP").unwrap();
        let markers = m.markers_for("ja").unwrap();
        assert!(markers.iter().any(|s| s == "丁目"));
    }
}
