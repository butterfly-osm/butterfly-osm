//! Cross-shard regression canary.
//!
//! Source-country addresses rewritten with target-country conventions,
//! kept OUT of the training corpus and emitted to a separate JSONL
//! file. The goal: at eval time, run the trained parser on the canary
//! file and verify it still tags / routes the SOURCE country correctly
//! despite seeing the target country's surface forms. If accuracy
//! drops, the training corpus is leaking shard quirks.
//!
//! The country label stays as the SOURCE country — that's the whole
//! point. The parser must learn geography over orthography.
//!
//! The rewrite logic: find a source-country street marker (e.g. NL
//! `straat`, FR `rue`) in the input, swap it for the equivalent
//! target-country marker. The `Morphology` table for both countries
//! lists their markers per language; rewriting is just substring
//! substitution.

use crate::gold::GoldRecord;
use crate::morphology::Morphology;
use crate::output::TrainRecord;
use rand_chacha::ChaCha20Rng;

/// Pairs of "source-marker" → "target-marker" — what to swap and what
/// to swap it for. We construct these at canary-build time by zipping
/// the source's `markers` with the target's `markers` (parallel by
/// position within each language tag).
fn rewrite_pairs(source: &Morphology, target: &Morphology) -> Vec<(String, String)> {
    let mut pairs: Vec<(String, String)> = Vec::new();
    // Use the union of language tags across both packs.
    let mut langs: Vec<&str> = source
        .street_types
        .markers
        .keys()
        .map(|s| s.as_str())
        .collect();
    for k in target.street_types.markers.keys() {
        if !langs.contains(&k.as_str()) {
            langs.push(k.as_str());
        }
    }
    for lang in langs {
        let src = source.markers_for(lang);
        let dst = target.markers_for(lang);
        // If the target speaks a different language, fall back to ANY
        // language in the target — this is the cross-language canary
        // (NL → FR, BE → DE, etc).
        let dst_fallback: Vec<&str> = target.all_markers();
        let (src, dst_pool): (&[String], Vec<&str>) = match (src, dst) {
            (Some(s), Some(d)) => (s, d.iter().map(String::as_str).collect()),
            (Some(s), None) => (s, dst_fallback.clone()),
            _ => continue,
        };
        if dst_pool.is_empty() {
            continue;
        }
        for (i, src_marker) in src.iter().enumerate() {
            // Pair by position within the markers list when possible
            // (i.e. NL `straat`[0] → FR `rue`[0]); fall back to first
            // target marker if the index doesn't line up.
            let dst_marker = dst_pool.get(i).copied().unwrap_or(dst_pool[0]);
            // Capitalized form too.
            let mut sc = src_marker.chars();
            let mut dc = dst_marker.chars();
            let src_cap: String = sc
                .next()
                .map(|c| c.to_uppercase().collect::<String>())
                .unwrap_or_default()
                + sc.as_str();
            let dst_cap: String = dc
                .next()
                .map(|c| c.to_uppercase().collect::<String>())
                .unwrap_or_default()
                + dc.as_str();
            pairs.push((src_cap, dst_cap));
            pairs.push((src_marker.clone(), dst_marker.to_string()));
        }
    }
    // Longest-first so substring matches don't shadow longer ones.
    pairs.sort_by_key(|(s, _)| std::cmp::Reverse(s.len()));
    pairs
}

fn rewrite_street(s: &str, pairs: &[(String, String)]) -> Option<String> {
    for (from, to) in pairs {
        if let Some(idx) = s.find(from.as_str()) {
            let mut out = String::with_capacity(s.len() + 4);
            out.push_str(&s[..idx]);
            out.push_str(to);
            out.push_str(&s[idx + from.len()..]);
            return Some(out);
        }
    }
    None
}

/// Rewrite a single gold record from the source country's idiom into
/// the target country's idiom. Returns `None` if no marker matched —
/// the record is just not relevant for this canary pair.
pub fn rewrite_with(
    g: &GoldRecord,
    source: &Morphology,
    target: &Morphology,
    _rng: &mut ChaCha20Rng,
) -> Option<TrainRecord> {
    let pairs = rewrite_pairs(source, target);
    let new_street = rewrite_street(g.street.as_deref()?, &pairs)?;
    let mut g2 = g.clone();
    g2.street = Some(new_street);
    let kind = format!(
        "canary_{}_as_{}",
        source.country.iso2.to_lowercase(),
        target.country.iso2.to_lowercase()
    );
    Some(rendered_record(&g2, &kind))
}

fn rendered_record(g: &GoldRecord, kind: &str) -> TrainRecord {
    let labeled = crate::bio::render_canonical(g);
    TrainRecord {
        text: labeled.text,
        bio_labels: labeled.bio_labels,
        country: g.country.clone(),
        source_record_id: format!("osm:n{}", g.osm_id),
        augmentation: kind.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::morphology::Morphology;
    use rand::SeedableRng;
    use std::path::PathBuf;

    fn morph_dir() -> PathBuf {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("morphology");
        p
    }

    #[test]
    fn rewrites_be_street_as_fr_keeps_country_label() {
        let be = Morphology::load_from_dir(morph_dir(), "BE").unwrap();
        let fr = Morphology::load_from_dir(morph_dir(), "FR").unwrap();
        let g = GoldRecord {
            osm_id: 1,
            country: "BE".to_string(),
            street: Some("Brusselsestraat".to_string()), // NL marker "straat"
            housenumber: Some("12".to_string()),
            postcode: Some("3000".to_string()),
            city: Some("Leuven".to_string()),
            unit: None,
            lat: 50.0,
            lon: 5.0,
        };
        let mut rng = ChaCha20Rng::from_seed([0u8; 32]);
        let out = rewrite_with(&g, &be, &fr, &mut rng).unwrap();
        assert_eq!(out.country, "BE", "country label must stay as source");
        assert!(
            !out.text.contains("straat"),
            "straat marker should be rewritten, got: {}",
            out.text
        );
        assert!(out.augmentation.starts_with("canary_be_as_fr"));
    }

    #[test]
    fn unmatched_record_returns_none() {
        let be = Morphology::load_from_dir(morph_dir(), "BE").unwrap();
        let fr = Morphology::load_from_dir(morph_dir(), "FR").unwrap();
        let g = GoldRecord {
            osm_id: 1,
            country: "BE".to_string(),
            street: Some("Foo Bar Baz".to_string()), // no marker present
            housenumber: Some("1".to_string()),
            postcode: Some("3000".to_string()),
            city: Some("Leuven".to_string()),
            unit: None,
            lat: 0.0,
            lon: 0.0,
        };
        let mut rng = ChaCha20Rng::from_seed([0u8; 32]);
        assert!(rewrite_with(&g, &be, &fr, &mut rng).is_none());
    }
}

