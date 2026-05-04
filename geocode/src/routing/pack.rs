//! Country packs — data-driven per-country knowledge (#96 §"Per-Country
//! Shard Contents", §"Country Routing").
//!
//! A country pack is a TOML file that tells the geocoder everything
//! country-specific: the postcode regex, lexical cues for the
//! classifier, dominant scripts, neighbour codes for cross-border
//! ambiguity, a WGS84 bounding box for reverse dispatch, source priors
//! per #96 country packs, and per-country OSM tag conventions.
//!
//! Adding a country = drop a `<iso2>.toml` into `geocode/data/packs/`.
//! Zero Rust changes. The build-shard CLI reads the pack, the
//! classifier reads the pack, the parser reads the pack, the bbox
//! reverse dispatcher reads the pack.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use regex::Regex;
use serde::Deserialize;

use super::CountryId;

/// Position hint for a postcode within an address string. Used by the
/// parser as a tie-breaker when the regex fires multiple times.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PostcodePosition {
    Leading,
    Trailing,
    #[default]
    Anywhere,
}

#[derive(Debug, Deserialize)]
struct PackToml {
    country: CountryHeader,
    #[serde(default)]
    postcode: Option<PostcodeSection>,
    #[serde(default)]
    scripts: Option<ScriptSection>,
    #[serde(default)]
    lexical_cues: Option<LexicalSection>,
    bbox: BboxSection,
    #[serde(default)]
    neighbours: Option<NeighboursSection>,
    #[serde(default)]
    source_priors: Option<SourcePriorsSection>,
    #[serde(default)]
    osm_tags: Option<OsmTagsSection>,
}

#[derive(Debug, Deserialize)]
struct CountryHeader {
    iso2: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct PostcodeSection {
    regex: String,
    #[serde(default)]
    position: PostcodePosition,
    /// `none`, `collapse_whitespace`, or `strip_country_prefix`.
    #[serde(default = "default_canon")]
    canonicalize: String,
}

fn default_canon() -> String {
    "none".to_string()
}

#[derive(Debug, Deserialize)]
struct ScriptSection {
    #[serde(default)]
    dominant: Vec<String>,
    #[serde(default)]
    secondary: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct LexicalSection {
    #[serde(default, rename = "match")]
    matches: Vec<LexicalMatch>,
}

#[derive(Debug, Deserialize)]
struct LexicalMatch {
    substring: String,
    boost: f32,
    #[serde(default = "default_match_kind")]
    kind: String,
}

fn default_match_kind() -> String {
    "substring".to_string()
}

#[derive(Debug, Deserialize)]
struct BboxSection {
    min_lat: f64,
    max_lat: f64,
    min_lon: f64,
    max_lon: f64,
}

#[derive(Debug, Deserialize, Default)]
struct NeighboursSection {
    #[serde(default)]
    codes: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct SourcePriorsSection {
    #[serde(default = "default_prior")]
    osm: f32,
    #[serde(default = "default_prior")]
    authoritative: f32,
}

fn default_prior() -> f32 {
    1.0
}

#[derive(Debug, Deserialize, Default)]
struct OsmTagsSection {
    #[serde(default = "default_postcode_tag")]
    postcode: String,
    #[serde(default = "default_street_tag")]
    street: String,
    #[serde(default = "default_housenumber_tag")]
    housenumber: String,
    #[serde(default = "default_city_tag")]
    city: String,
}

fn default_postcode_tag() -> String {
    "addr:postcode".to_string()
}
fn default_street_tag() -> String {
    "addr:street".to_string()
}
fn default_housenumber_tag() -> String {
    "addr:housenumber".to_string()
}
fn default_city_tag() -> String {
    "addr:city".to_string()
}

/// Compiled, ready-to-query country pack. Built once at startup, held
/// in [`PackRegistry`].
#[derive(Debug)]
pub struct CountryPack {
    pub country: CountryId,
    pub name: String,
    pub postcode_regex: Option<Regex>,
    pub postcode_position: PostcodePosition,
    pub postcode_canonicalize: PostcodeCanonicalize,
    pub script_dominant: Vec<String>,
    pub script_secondary: Vec<String>,
    pub lexical_cues: Vec<LexicalCue>,
    pub bbox: Bbox,
    pub neighbours: Vec<CountryId>,
    pub source_priors: SourcePriors,
    pub osm_tags: OsmTags,
}

#[derive(Debug, Clone)]
pub struct LexicalCue {
    pub substring_lower: String,
    pub boost: f32,
    pub kind: LexicalKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LexicalKind {
    Substring,
    Word,
}

#[derive(Debug, Clone, Copy)]
pub struct Bbox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl Bbox {
    #[must_use]
    pub fn area_deg2(&self) -> f64 {
        (self.max_lat - self.min_lat) * (self.max_lon - self.min_lon)
    }

    #[must_use]
    pub fn contains(&self, lat: f64, lon: f64) -> bool {
        lat >= self.min_lat && lat <= self.max_lat && lon >= self.min_lon && lon <= self.max_lon
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SourcePriors {
    pub osm: f32,
    pub authoritative: f32,
}

#[derive(Debug, Clone)]
pub struct OsmTags {
    pub postcode: String,
    pub street: String,
    pub housenumber: String,
    pub city: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PostcodeCanonicalize {
    #[default]
    None,
    /// NL: "1011 AB" → "1011AB".
    CollapseWhitespace,
    /// LU: "L-2453" → "2453".
    StripCountryPrefix,
}

impl CountryPack {
    pub fn from_toml_str(toml_str: &str) -> Result<Self> {
        let parsed: PackToml = toml::from_str(toml_str).context("parsing country pack TOML")?;

        let country = CountryId::from_iso2(&parsed.country.iso2).ok_or_else(|| {
            anyhow::anyhow!(
                "[country].iso2 = '{}' is not a valid ISO 3166-1 alpha-2 code",
                parsed.country.iso2
            )
        })?;

        let postcode_regex = match &parsed.postcode {
            Some(p) => Some(Regex::new(&p.regex).with_context(|| {
                format!(
                    "compiling [postcode].regex for {}: {}",
                    parsed.country.iso2, p.regex
                )
            })?),
            None => None,
        };
        let postcode_position = parsed
            .postcode
            .as_ref()
            .map(|p| p.position)
            .unwrap_or_default();
        let postcode_canonicalize = parsed
            .postcode
            .as_ref()
            .map(|p| match p.canonicalize.as_str() {
                "none" => PostcodeCanonicalize::None,
                "collapse_whitespace" => PostcodeCanonicalize::CollapseWhitespace,
                "strip_country_prefix" => PostcodeCanonicalize::StripCountryPrefix,
                other => {
                    tracing::warn!(
                        country = %parsed.country.iso2,
                        canonicalize = other,
                        "unknown postcode canonicalize kind, defaulting to 'none'"
                    );
                    PostcodeCanonicalize::None
                }
            })
            .unwrap_or_default();

        let lexical_cues: Vec<LexicalCue> = parsed
            .lexical_cues
            .map(|s| s.matches)
            .unwrap_or_default()
            .into_iter()
            .map(|m| LexicalCue {
                substring_lower: m.substring.to_lowercase(),
                boost: m.boost,
                kind: match m.kind.as_str() {
                    "word" => LexicalKind::Word,
                    _ => LexicalKind::Substring,
                },
            })
            .collect();

        let scripts = parsed.scripts.unwrap_or(ScriptSection {
            dominant: vec![],
            secondary: vec![],
        });
        let neighbours = parsed
            .neighbours
            .unwrap_or_default()
            .codes
            .into_iter()
            .filter_map(|c| CountryId::from_iso2(&c))
            .collect();
        let priors = parsed.source_priors.unwrap_or_default();
        let tags = parsed.osm_tags.unwrap_or_default();

        let pack = CountryPack {
            country,
            name: parsed.country.name,
            postcode_regex,
            postcode_position,
            postcode_canonicalize,
            script_dominant: scripts.dominant,
            script_secondary: scripts.secondary,
            lexical_cues,
            bbox: Bbox {
                min_lat: parsed.bbox.min_lat,
                max_lat: parsed.bbox.max_lat,
                min_lon: parsed.bbox.min_lon,
                max_lon: parsed.bbox.max_lon,
            },
            neighbours,
            source_priors: SourcePriors {
                osm: priors.osm,
                authoritative: priors.authoritative,
            },
            osm_tags: OsmTags {
                postcode: if tags.postcode.is_empty() {
                    default_postcode_tag()
                } else {
                    tags.postcode
                },
                street: if tags.street.is_empty() {
                    default_street_tag()
                } else {
                    tags.street
                },
                housenumber: if tags.housenumber.is_empty() {
                    default_housenumber_tag()
                } else {
                    tags.housenumber
                },
                city: if tags.city.is_empty() {
                    default_city_tag()
                } else {
                    tags.city
                },
            },
        };
        pack.validate()?;
        Ok(pack)
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading country pack at {}", path.display()))?;
        Self::from_toml_str(&raw)
            .with_context(|| format!("parsing country pack at {}", path.display()))
    }

    fn validate(&self) -> Result<()> {
        if !(-90.0..=90.0).contains(&self.bbox.min_lat) {
            bail!("{}: bbox.min_lat out of range", self.country);
        }
        if !(-90.0..=90.0).contains(&self.bbox.max_lat) {
            bail!("{}: bbox.max_lat out of range", self.country);
        }
        if !(-180.0..=180.0).contains(&self.bbox.min_lon) {
            bail!("{}: bbox.min_lon out of range", self.country);
        }
        if !(-180.0..=180.0).contains(&self.bbox.max_lon) {
            bail!("{}: bbox.max_lon out of range", self.country);
        }
        if self.bbox.min_lat >= self.bbox.max_lat {
            bail!("{}: bbox.min_lat >= bbox.max_lat", self.country);
        }
        if self.bbox.min_lon >= self.bbox.max_lon {
            bail!("{}: bbox.min_lon >= bbox.max_lon", self.country);
        }
        Ok(())
    }

    /// Score this pack against an input string. Higher = more likely
    /// this is the right country. The classifier softmaxes scores
    /// across all packs.
    #[must_use]
    pub fn score(&self, raw: &str, lower: &str) -> f32 {
        let mut s = 0.0_f32;

        if let Some(re) = &self.postcode_regex
            && re.is_match(raw)
        {
            s += 3.0;
        }

        for cue in &self.lexical_cues {
            let hit = match cue.kind {
                LexicalKind::Substring => lower.contains(&cue.substring_lower),
                LexicalKind::Word => contains_word(lower, &cue.substring_lower),
            };
            if hit {
                s += cue.boost;
            }
        }

        if !self.script_dominant.is_empty() || !self.script_secondary.is_empty() {
            let total = raw.chars().filter(|c| !c.is_whitespace()).count();
            if total > 0 {
                let mut dominant_hits = 0usize;
                let mut secondary_hits = 0usize;
                for c in raw.chars() {
                    if c.is_whitespace() {
                        continue;
                    }
                    let scr = script_of(c);
                    if self.script_dominant.iter().any(|s| s == scr) {
                        dominant_hits += 1;
                    } else if self.script_secondary.iter().any(|s| s == scr) {
                        secondary_hits += 1;
                    }
                }
                let dom_frac = dominant_hits as f32 / total as f32;
                let sec_frac = secondary_hits as f32 / total as f32;
                s += dom_frac * 1.5;
                s += sec_frac * 0.6;
            }
        }

        s
    }

    /// Apply the postcode canonicalization rule. The result is what
    /// goes into the shard postcode index.
    #[must_use]
    pub fn canonicalize_postcode(&self, raw: &str) -> String {
        match self.postcode_canonicalize {
            PostcodeCanonicalize::None => raw.to_string(),
            PostcodeCanonicalize::CollapseWhitespace => {
                raw.split_whitespace().collect::<Vec<_>>().join("")
            }
            PostcodeCanonicalize::StripCountryPrefix => {
                // Strip a leading 1-3 letter prefix followed by '-', case-
                // insensitive. Handles LU's "L-2453" → "2453",
                // hypothetically GB's "GB-..." → "...", etc. The regex
                // engine isn't needed — string ops suffice.
                strip_alpha_prefix(raw).unwrap_or_else(|| raw.to_string())
            }
        }
    }
}

/// Strip a leading 1-3 letter alphabetic prefix followed by `-`. Used
/// for postcodes like "L-2453" (LU) that prefix the digit body with a
/// country letter. Returns `None` when no prefix is found.
fn strip_alpha_prefix(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && i < 3 && bytes[i].is_ascii_alphabetic() {
        i += 1;
    }
    if i == 0 || i >= bytes.len() || bytes[i] != b'-' {
        return None;
    }
    Some(s[i + 1..].to_string())
}

fn contains_word(text: &str, word: &str) -> bool {
    let bytes = text.as_bytes();
    let wbytes = word.as_bytes();
    if wbytes.is_empty() || bytes.len() < wbytes.len() {
        return false;
    }
    let mut i = 0;
    while i + wbytes.len() <= bytes.len() {
        if &bytes[i..i + wbytes.len()] == wbytes {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphabetic();
            let after_ok =
                i + wbytes.len() == bytes.len() || !bytes[i + wbytes.len()].is_ascii_alphabetic();
            if before_ok && after_ok {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Return the Unicode script of `c` as a static string. Covers every
/// script the shipped packs reference. Uses BMP block ranges (cheap,
/// no `unicode-script` dependency).
fn script_of(c: char) -> &'static str {
    let cp = c as u32;
    if c.is_ascii_alphabetic() {
        return "Latin";
    }
    if (0x0080..=0x024F).contains(&cp) {
        return "Latin";
    }
    if (0x0370..=0x03FF).contains(&cp) {
        return "Greek";
    }
    if (0x0400..=0x04FF).contains(&cp) {
        return "Cyrillic";
    }
    if (0x0590..=0x05FF).contains(&cp) {
        return "Hebrew";
    }
    if (0x0600..=0x06FF).contains(&cp) {
        return "Arabic";
    }
    if (0x0900..=0x097F).contains(&cp) {
        return "Devanagari";
    }
    if (0x0E00..=0x0E7F).contains(&cp) {
        return "Thai";
    }
    if (0x3040..=0x309F).contains(&cp) {
        return "Hiragana";
    }
    if (0x30A0..=0x30FF).contains(&cp) {
        return "Katakana";
    }
    if (0xAC00..=0xD7AF).contains(&cp) {
        return "Hangul";
    }
    if (0x3400..=0x4DBF).contains(&cp) || (0x4E00..=0x9FFF).contains(&cp) {
        return "Han";
    }
    "Other"
}

/// Registry of loaded country packs.
#[derive(Debug, Default)]
pub struct PackRegistry {
    packs: HashMap<CountryId, Arc<CountryPack>>,
}

impl PackRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Load every `*.toml` file under `dir`. Errors only if the
    /// directory cannot be read or zero packs load.
    pub fn load_from_dir<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        let mut reg = Self::new();
        let entries = std::fs::read_dir(dir)
            .with_context(|| format!("reading pack directory {}", dir.display()))?;
        let mut errors: Vec<String> = Vec::new();
        for entry in entries {
            let entry = entry.context("iterating pack directory")?;
            let path = entry.path();
            if path
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("toml"))
                != Some(true)
            {
                continue;
            }
            match CountryPack::from_file(&path) {
                Ok(pack) => {
                    reg.insert(pack);
                }
                Err(e) => {
                    errors.push(format!("{}: {:#}", path.display(), e));
                }
            }
        }
        if reg.packs.is_empty() {
            bail!(
                "no country packs loaded from {} (errors: {:?})",
                dir.display(),
                errors
            );
        }
        if !errors.is_empty() {
            for e in &errors {
                tracing::warn!(error = %e, "country pack failed to load");
            }
        }
        Ok(reg)
    }

    /// Embedded packs shipped with the binary. The hot path; used by
    /// the classifier when no `--pack-dir` is given.
    pub fn shipped() -> Result<Self> {
        let raw_packs: &[(CountryId, &str)] = &[
            (CountryId::BE, include_str!("../../data/packs/be.toml")),
            (CountryId::FR, include_str!("../../data/packs/fr.toml")),
            (CountryId::NL, include_str!("../../data/packs/nl.toml")),
            (CountryId::LU, include_str!("../../data/packs/lu.toml")),
            (CountryId::DE, include_str!("../../data/packs/de.toml")),
            (CountryId::AT, include_str!("../../data/packs/at.toml")),
            (CountryId::CH, include_str!("../../data/packs/ch.toml")),
            (CountryId::GB, include_str!("../../data/packs/gb.toml")),
            (CountryId::ES, include_str!("../../data/packs/es.toml")),
            (CountryId::IT, include_str!("../../data/packs/it.toml")),
            (CountryId::US, include_str!("../../data/packs/us.toml")),
            (CountryId::JP, include_str!("../../data/packs/jp.toml")),
            (CountryId::BR, include_str!("../../data/packs/br.toml")),
            (CountryId::IN, include_str!("../../data/packs/in.toml")),
            (CountryId::AU, include_str!("../../data/packs/au.toml")),
        ];
        let mut reg = Self::new();
        for (cid, raw) in raw_packs {
            let pack = CountryPack::from_toml_str(raw)
                .with_context(|| format!("compiling shipped pack {}", cid))?;
            assert_eq!(
                pack.country, *cid,
                "shipped pack {}.toml declares wrong country: {}",
                cid, pack.country
            );
            reg.insert(pack);
        }
        Ok(reg)
    }

    pub fn insert(&mut self, pack: CountryPack) -> &Arc<CountryPack> {
        let cid = pack.country;
        self.packs.insert(cid, Arc::new(pack));
        self.packs.get(&cid).expect("just inserted")
    }

    #[must_use]
    pub fn get(&self, c: CountryId) -> Option<&Arc<CountryPack>> {
        self.packs.get(&c)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<CountryPack>> {
        self.packs.values()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.packs.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.packs.is_empty()
    }

    /// Sorted list of loaded countries (ISO2 lex order). Stable for
    /// `/health` and tests.
    #[must_use]
    pub fn countries(&self) -> Vec<CountryId> {
        let mut v: Vec<CountryId> = self.packs.keys().copied().collect();
        v.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        v
    }

    /// Where the shipped packs live in the source tree.
    #[must_use]
    pub fn default_dir() -> PathBuf {
        PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/data/packs"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_packs_compile() {
        let reg = PackRegistry::shipped().expect("shipped packs must compile");
        assert_eq!(
            reg.len(),
            15,
            "expected 15 shipped packs, got {}",
            reg.len()
        );

        for c in [
            CountryId::BE,
            CountryId::FR,
            CountryId::NL,
            CountryId::LU,
            CountryId::DE,
            CountryId::AT,
            CountryId::CH,
            CountryId::GB,
            CountryId::ES,
            CountryId::IT,
            CountryId::US,
            CountryId::JP,
            CountryId::BR,
            CountryId::IN,
            CountryId::AU,
        ] {
            assert!(reg.get(c).is_some(), "pack missing for {}", c);
        }
    }

    #[test]
    fn shipped_packs_have_valid_bboxes() {
        let reg = PackRegistry::shipped().unwrap();
        for pack in reg.iter() {
            assert!(
                pack.bbox.min_lat < pack.bbox.max_lat,
                "{}: bbox lat invariant",
                pack.country
            );
            assert!(
                pack.bbox.min_lon < pack.bbox.max_lon,
                "{}: bbox lon invariant",
                pack.country
            );
        }
    }

    #[test]
    fn jp_pack_recognises_japanese_addresses() {
        let reg = PackRegistry::shipped().unwrap();
        let jp = reg.get(CountryId::JP).expect("jp pack");
        let text = "東京都千代田区千代田1-1";
        let s = jp.score(text, &text.to_lowercase());
        assert!(s > 1.0, "JP pack score for Tokyo address: {s}");
    }

    #[test]
    fn us_pack_recognises_us_address() {
        let reg = PackRegistry::shipped().unwrap();
        let us = reg.get(CountryId::US).expect("us pack");
        let text = "1600 Pennsylvania Ave NW Washington DC 20500";
        let s = us.score(text, &text.to_lowercase());
        assert!(s > 1.0, "US pack score for DC address: {s}");
    }

    #[test]
    fn br_pack_recognises_cep() {
        let reg = PackRegistry::shipped().unwrap();
        let br = reg.get(CountryId::BR).expect("br pack");
        let text = "Avenida Paulista 1578 São Paulo 01310-200";
        let s = br.score(text, &text.to_lowercase());
        assert!(s > 1.0, "BR pack score for São Paulo address: {s}");
    }

    #[test]
    fn nl_postcode_canonicalize_collapses_whitespace() {
        let reg = PackRegistry::shipped().unwrap();
        let nl = reg.get(CountryId::NL).expect("nl pack");
        assert_eq!(nl.canonicalize_postcode("1011 AB"), "1011AB");
    }

    #[test]
    fn lu_postcode_canonicalize_strips_l_prefix() {
        let reg = PackRegistry::shipped().unwrap();
        let lu = reg.get(CountryId::LU).expect("lu pack");
        assert_eq!(lu.canonicalize_postcode("L-2453"), "2453");
        assert_eq!(lu.canonicalize_postcode("2453"), "2453");
    }

    #[test]
    fn unknown_iso2_in_pack_rejected() {
        let bad = r#"
[country]
iso2 = "ZZZ"
name = "Bad"
[bbox]
min_lat = 0.0
max_lat = 1.0
min_lon = 0.0
max_lon = 1.0
"#;
        let r = CountryPack::from_toml_str(bad);
        assert!(r.is_err());
    }

    #[test]
    fn invalid_bbox_rejected() {
        let bad = r#"
[country]
iso2 = "BE"
name = "Belgium"
[bbox]
min_lat = 60.0
max_lat = 50.0
min_lon = 0.0
max_lon = 1.0
"#;
        let r = CountryPack::from_toml_str(bad);
        assert!(r.is_err());
    }
}
