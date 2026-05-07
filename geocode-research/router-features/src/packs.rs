//! Parses `geocode/data/packs/*.toml` to extract:
//! - lexical-cue substrings (markers)
//! - postcode regex per country
//!
//! Falls back to morphology packs in
//! `geocode-training/corpus-gen/morphology/*.toml` if present (only 6 of
//! 15 countries).

use crate::COUNTRIES;
use regex::Regex;
use serde::Deserialize;
use std::collections::BTreeSet;
use std::path::Path;

#[derive(Deserialize, Debug)]
struct PackRoot {
    postcode: PackPostcode,
    #[serde(default)]
    lexical_cues: Option<LexicalCues>,
}

#[derive(Deserialize, Debug)]
struct PackPostcode {
    regex: String,
}

#[derive(Deserialize, Debug)]
struct LexicalCues {
    #[serde(rename = "match", default)]
    matches: Vec<LexicalMatch>,
}

#[derive(Deserialize, Debug)]
struct LexicalMatch {
    substring: String,
}

pub struct PackBundle {
    /// One entry per country in `COUNTRIES` order.
    pub postcode_regexes: Vec<Regex>,
    /// Sorted, deduplicated list of all lexical-cue substrings across packs.
    pub markers: Vec<String>,
}

pub fn load_packs(packs_dir: &Path) -> anyhow::Result<PackBundle> {
    let mut postcodes: Vec<Regex> = Vec::with_capacity(COUNTRIES.len());
    let mut markers: BTreeSet<String> = BTreeSet::new();

    for c in COUNTRIES {
        let path = packs_dir.join(format!("{}.toml", c.to_ascii_lowercase()));
        let body = std::fs::read_to_string(&path)
            .map_err(|e| anyhow::anyhow!("read {}: {}", path.display(), e))?;
        let pack: PackRoot = toml::from_str(&body)
            .map_err(|e| anyhow::anyhow!("parse {}: {}", path.display(), e))?;
        let re = Regex::new(&pack.postcode.regex)
            .map_err(|e| anyhow::anyhow!("regex {} from {}: {}", &pack.postcode.regex, c, e))?;
        postcodes.push(re);

        if let Some(lc) = pack.lexical_cues {
            for m in lc.matches {
                let lower = m.substring.to_lowercase();
                if !lower.is_empty() {
                    markers.insert(lower);
                }
            }
        }
    }

    Ok(PackBundle {
        postcode_regexes: postcodes,
        markers: markers.into_iter().collect(),
    })
}
