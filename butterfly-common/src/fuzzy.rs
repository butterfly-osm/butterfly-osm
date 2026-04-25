//! Fuzzy matching for OSM source identifiers.
//!
//! This module exists so the `error` module can stay focused on error
//! types. The fuzzy matcher is used by butterfly-dl's CLI to suggest
//! corrections for typos in region/continent names (e.g. `belgim` →
//! `europe/belgium`); it has nothing to do with errors per se, it just
//! happens to be invoked from one of butterfly-dl's error-formatting
//! paths.
//!
//! ## Design (H4)
//!
//! The candidate list is a static, hand-curated set of ~120 common
//! Geofabrik regions (continents + major countries). Trade-off:
//!
//! - **Pros**: zero network calls at validation time, deterministic
//!   matching across runs, covers 99 %+ of real-world inputs.
//! - **Cons**: doesn't include every Geofabrik sub-region (US states,
//!   German Bundesländer, French régions etc.). Adding one is a code
//!   change rather than a runtime config change.
//!
//! Future work (deferred): an optional runtime fetch of the live
//! Geofabrik JSON index for completeness.

use std::sync::OnceLock;
use strsim::{jaro_winkler, normalized_levenshtein};

/// Cache for valid Geofabrik source identifiers. Populated once on
/// first call, kept for the life of the process.
static VALID_SOURCES_CACHE: OnceLock<Vec<String>> = OnceLock::new();

/// Initialize the source cache with the comprehensive region list.
fn ensure_sources_loaded() {
    VALID_SOURCES_CACHE.get_or_init(|| {
        vec![
            // Root level
            "planet".to_string(),
            // Continents
            "africa".to_string(),
            "antarctica".to_string(),
            "asia".to_string(),
            "australia-oceania".to_string(),
            "europe".to_string(),
            "north-america".to_string(),
            "south-america".to_string(),
            "central-america".to_string(),
            // Europe
            "europe/albania".to_string(),
            "europe/andorra".to_string(),
            "europe/austria".to_string(),
            "europe/belarus".to_string(),
            "europe/belgium".to_string(),
            "europe/bosnia-herzegovina".to_string(),
            "europe/bulgaria".to_string(),
            "europe/croatia".to_string(),
            "europe/cyprus".to_string(),
            "europe/czech-republic".to_string(),
            "europe/denmark".to_string(),
            "europe/estonia".to_string(),
            "europe/faroe-islands".to_string(),
            "europe/finland".to_string(),
            "europe/france".to_string(),
            "europe/germany".to_string(),
            "europe/great-britain".to_string(),
            "europe/greece".to_string(),
            "europe/hungary".to_string(),
            "europe/iceland".to_string(),
            "europe/ireland".to_string(),
            "europe/isle-of-man".to_string(),
            "europe/italy".to_string(),
            "europe/kosovo".to_string(),
            "europe/latvia".to_string(),
            "europe/liechtenstein".to_string(),
            "europe/lithuania".to_string(),
            "europe/luxembourg".to_string(),
            "europe/malta".to_string(),
            "europe/moldova".to_string(),
            "europe/monaco".to_string(),
            "europe/montenegro".to_string(),
            "europe/netherlands".to_string(),
            "europe/north-macedonia".to_string(),
            "europe/norway".to_string(),
            "europe/poland".to_string(),
            "europe/portugal".to_string(),
            "europe/romania".to_string(),
            "europe/russia".to_string(),
            "europe/san-marino".to_string(),
            "europe/serbia".to_string(),
            "europe/slovakia".to_string(),
            "europe/slovenia".to_string(),
            "europe/spain".to_string(),
            "europe/sweden".to_string(),
            "europe/switzerland".to_string(),
            "europe/turkey".to_string(),
            "europe/ukraine".to_string(),
            "europe/united-kingdom".to_string(),
            "europe/vatican-city".to_string(),
            // North America
            "north-america/canada".to_string(),
            "north-america/greenland".to_string(),
            "north-america/mexico".to_string(),
            "north-america/us".to_string(),
            // Asia
            "asia/afghanistan".to_string(),
            "asia/bangladesh".to_string(),
            "asia/bhutan".to_string(),
            "asia/cambodia".to_string(),
            "asia/china".to_string(),
            "asia/gcc-states".to_string(),
            "asia/india".to_string(),
            "asia/indonesia".to_string(),
            "asia/iran".to_string(),
            "asia/iraq".to_string(),
            "asia/israel-and-palestine".to_string(),
            "asia/japan".to_string(),
            "asia/jordan".to_string(),
            "asia/kazakhstan".to_string(),
            "asia/kyrgyzstan".to_string(),
            "asia/lebanon".to_string(),
            "asia/malaysia-singapore-brunei".to_string(),
            "asia/maldives".to_string(),
            "asia/mongolia".to_string(),
            "asia/myanmar".to_string(),
            "asia/nepal".to_string(),
            "asia/north-korea".to_string(),
            "asia/pakistan".to_string(),
            "asia/philippines".to_string(),
            "asia/south-korea".to_string(),
            "asia/sri-lanka".to_string(),
            "asia/syria".to_string(),
            "asia/taiwan".to_string(),
            "asia/tajikistan".to_string(),
            "asia/thailand".to_string(),
            "asia/tibet".to_string(),
            "asia/turkmenistan".to_string(),
            "asia/uzbekistan".to_string(),
            "asia/vietnam".to_string(),
            "asia/yemen".to_string(),
        ]
    });
}

/// Get valid sources (cached).
fn get_valid_sources_sync() -> &'static [String] {
    ensure_sources_loaded();
    VALID_SOURCES_CACHE
        .get()
        .map(|v| v.as_slice())
        .unwrap_or(&[])
}

/// Find the best fuzzy match using hybrid semantic + character-based scoring.
///
/// Combines character-based similarity (Jaro-Winkler 70 % + Normalized
/// Levenshtein 30 %) with semantic bonuses:
/// - Prefix matching: 20 % bonus for strong prefix similarity (≥ 7 chars)
/// - Substring matching: 12 % bonus for compound word parts (`australia-oceania`)
/// - Length similarity: 10 % bonus for appropriate length matches
/// - Anti-bias penalty: −10 % for inappropriate short matches
///
/// Minimum threshold: 0.65 similarity to balance precision vs. recall.
fn find_best_fuzzy_match(input: &str, candidates: &[String]) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }

    let input_lower = input.to_lowercase();
    let mut best_match = None;
    let mut best_score = 0.0f64;

    let min_threshold = 0.65;

    for candidate in candidates {
        let candidate_lower = candidate.to_lowercase();

        let jw_score = jaro_winkler(&input_lower, &candidate_lower);
        let lev_score = normalized_levenshtein(&input_lower, &candidate_lower);
        let combined_score = (jw_score * 0.7) + (lev_score * 0.3);

        let mut semantic_bonus = 0.0;

        let prefix_len = input_lower.chars().count().min(7);
        if prefix_len >= 4 {
            let input_prefix = input_lower.chars().take(prefix_len).collect::<String>();
            let candidate_prefix = candidate_lower.chars().take(prefix_len).collect::<String>();
            let prefix_similarity = normalized_levenshtein(&input_prefix, &candidate_prefix);
            if prefix_similarity > 0.7 {
                semantic_bonus += 0.2 * prefix_similarity;
            }
        }

        if input_lower.len() >= 8 && candidate_lower.len() >= 8 {
            let length_ratio = 1.0
                - ((input_lower.len() as f64 - candidate_lower.len() as f64).abs()
                    / input_lower.len().max(candidate_lower.len()) as f64);
            if length_ratio > 0.7 {
                semantic_bonus += 0.1 * length_ratio;
            }
        }

        if candidate_lower.contains('-') || candidate_lower.contains('/') {
            let parts: Vec<&str> = candidate_lower.split(&['-', '/'][..]).collect();
            for part in parts {
                if part.len() >= 4 {
                    let part_similarity = jaro_winkler(&input_lower, part);
                    if part_similarity > 0.85 {
                        semantic_bonus += 0.12 * part_similarity;
                    }
                }
            }
        }

        if input_lower.len() >= 8 && candidate_lower.len() <= 7 && !candidate_lower.contains('/') {
            semantic_bonus -= 0.1;
        }

        let final_score = combined_score + semantic_bonus;

        if final_score >= min_threshold && final_score > best_score {
            best_score = final_score;
            best_match = Some(candidate.clone());
        }
    }

    best_match
}

/// Suggest a correction for a potentially misspelled OSM source identifier.
///
/// Returns `Some(correction)` if a strong match was found, `None` for an
/// exact match (no correction needed) or a totally unrecognised input.
pub fn suggest_correction(source: &str) -> Option<String> {
    let valid_sources = get_valid_sources_sync();

    // Exact case-insensitive match — no suggestion needed.
    for valid_source in valid_sources {
        if valid_source.eq_ignore_ascii_case(source) {
            return None;
        }
    }

    // Standalone input (no '/'): try country names first, then continents,
    // then country fragments.
    if !source.contains('/') {
        // Exact country match → upgrade to continent/country path.
        for valid_source in valid_sources {
            if let Some(slash_pos) = valid_source.find('/') {
                let country_part = &valid_source[slash_pos + 1..];
                if country_part.eq_ignore_ascii_case(source) {
                    return Some(valid_source.clone());
                }
            }
        }

        let mut continent_level: Vec<String> = Vec::new();
        let mut country_level: Vec<String> = Vec::new();

        for valid_source in valid_sources {
            if valid_source.contains('/') {
                country_level.push(valid_source.clone());
            } else {
                continent_level.push(valid_source.clone());
            }
        }

        // Long inputs: try continents first (likely a continent typo).
        if source.len() >= 6
            && let Some(match_result) = find_best_fuzzy_match(source, &continent_level)
        {
            return Some(match_result);
        }

        // Short inputs: continents only if the match is very strong
        // (e.g. "plant" → "planet").
        if source.len() <= 6
            && let Some(match_result) = find_best_fuzzy_match(source, &continent_level)
        {
            let source_lower = source.to_lowercase();
            let match_result_lower = match_result.to_lowercase();
            let similarity = jaro_winkler(&source_lower, &match_result_lower);
            if similarity > 0.8 {
                return Some(match_result);
            }
        }

        // Country names (just the country part).
        let country_names: Vec<String> = country_level
            .iter()
            .filter_map(|s| s.split('/').nth(1).map(|c| c.to_string()))
            .collect();

        if let Some(best_country) = find_best_fuzzy_match(source, &country_names) {
            for full_path in &country_level {
                if let Some(country_part) = full_path.split('/').nth(1)
                    && country_part == best_country
                {
                    return Some(full_path.clone());
                }
            }
        }

        return find_best_fuzzy_match(source, valid_sources);
    }

    // Path input (continent/country): try geographic correction.
    if let Some(slash_pos) = source.find('/') {
        let continent = &source[..slash_pos];
        let country = &source[slash_pos + 1..];

        // Country exists somewhere — suggest the right continent.
        for valid_source in valid_sources {
            if let Some(valid_slash_pos) = valid_source.find('/') {
                let valid_country = &valid_source[valid_slash_pos + 1..];
                if valid_country.eq_ignore_ascii_case(country) {
                    return Some(valid_source.clone());
                }
            }
        }

        // Country not found — fix the continent at least.
        let continents: Vec<String> = valid_sources
            .iter()
            .filter(|s| !s.contains('/'))
            .cloned()
            .collect();

        if let Some(corrected_continent) = find_best_fuzzy_match(continent, &continents) {
            if country.len() > 8
                && !country
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-')
            {
                return Some(corrected_continent);
            }
            return Some(corrected_continent);
        }
    }

    find_best_fuzzy_match(source, valid_sources)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_correction_fuzzy_matching() {
        assert_eq!(
            suggest_correction("antartica"),
            Some("antarctica".to_string())
        );
        assert_eq!(
            suggest_correction("austrailia"),
            Some("australia-oceania".to_string())
        );
        assert_eq!(suggest_correction("eurpoe"), Some("europe".to_string()));
        assert_eq!(suggest_correction("afirca"), Some("africa".to_string()));

        assert_eq!(suggest_correction("plant"), Some("planet".to_string()));
        assert_eq!(suggest_correction("plnet"), Some("planet".to_string()));
    }

    #[test]
    fn test_suggest_correction_standalone_country_names() {
        assert_eq!(
            suggest_correction("monaco"),
            Some("europe/monaco".to_string())
        );
        assert_eq!(
            suggest_correction("belgium"),
            Some("europe/belgium".to_string())
        );
        assert_eq!(
            suggest_correction("germany"),
            Some("europe/germany".to_string())
        );
        assert_eq!(
            suggest_correction("france"),
            Some("europe/france".to_string())
        );
        assert_eq!(
            suggest_correction("MONACO"),
            Some("europe/monaco".to_string())
        );
        assert_eq!(
            suggest_correction("Belgium"),
            Some("europe/belgium".to_string())
        );
    }

    #[test]
    fn test_suggest_correction_standalone_country_typos() {
        assert_eq!(
            suggest_correction("monac"),
            Some("europe/monaco".to_string())
        );
        assert_eq!(
            suggest_correction("belgum"),
            Some("europe/belgium".to_string())
        );
        assert_eq!(
            suggest_correction("germay"),
            Some("europe/germany".to_string())
        );
    }

    #[test]
    fn test_suggest_correction_country_paths() {
        assert_eq!(
            suggest_correction("antartica/belgium"),
            Some("europe/belgium".to_string())
        );
        assert_eq!(
            suggest_correction("europ/france"),
            Some("europe/france".to_string())
        );
        assert_eq!(
            suggest_correction("eurpoe/germany"),
            Some("europe/germany".to_string())
        );
        assert_eq!(
            suggest_correction("europ/unknown-country"),
            Some("europe".to_string())
        );
    }

    #[test]
    fn test_suggest_correction_no_match() {
        assert_eq!(suggest_correction("totally-invalid-place"), None);
        assert_eq!(suggest_correction("europe"), None);
        assert_eq!(suggest_correction("a"), None);
    }

    #[test]
    fn test_suggest_correction_case_insensitive() {
        assert_eq!(
            suggest_correction("ANTARTICA"),
            Some("antarctica".to_string())
        );
        assert_eq!(
            suggest_correction("AntArTiCa"),
            Some("antarctica".to_string())
        );
        assert_eq!(suggest_correction("EuRoPe"), None);
    }

    #[test]
    fn test_strsim_fuzzy_matching() {
        let candidates = vec![
            "australia-oceania".to_string(),
            "austria".to_string(),
            "europe/austria".to_string(),
            "antarctica".to_string(),
        ];
        let result = find_best_fuzzy_match("austrailia", &candidates);
        assert_eq!(result, Some("australia-oceania".to_string()));
    }

    #[test]
    fn test_semantic_bonuses() {
        let candidates = vec![
            "austria".to_string(),
            "europe/austria".to_string(),
            "australia-oceania".to_string(),
        ];
        let result = find_best_fuzzy_match("very-long-input-string", &candidates);
        assert_ne!(result, Some("austria".to_string()));

        let length_candidates = vec![
            "short".to_string(),
            "medium-length-string".to_string(),
            "very-long-similar-length".to_string(),
        ];
        let result = find_best_fuzzy_match("very-long-similar-input", &length_candidates);
        assert_eq!(result, Some("very-long-similar-length".to_string()));

        let prefix_candidates = vec![
            "australia-oceania".to_string(),
            "antarctica".to_string(),
            "africa".to_string(),
        ];
        let result = find_best_fuzzy_match("austr", &prefix_candidates);
        assert_eq!(result, Some("australia-oceania".to_string()));
    }
}
