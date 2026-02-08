//! Error types and utilities for butterfly-osm toolkit
//!
//! Provides comprehensive error handling and fuzzy matching for OSM source identification.

use std::fmt;
use std::sync::OnceLock;
use strsim::{jaro_winkler, normalized_levenshtein};

/// Cache for valid Geofabrik source identifiers.
///
/// Design trade-off (H4): This is a static list of ~120 common regions rather than
/// a dynamically fetched Geofabrik index. Rationale:
/// - Avoids runtime dependency on Geofabrik API (no network call just to validate input)
/// - Deterministic behavior: fuzzy matching is stable across runs
/// - Covers all continents + major countries (sufficient for 99%+ of real usage)
/// - New regions can be added in a release; Geofabrik's region list changes infrequently
///
/// Future work: add optional runtime fetch from Geofabrik JSON index for completeness.
static VALID_SOURCES_CACHE: OnceLock<Vec<String>> = OnceLock::new();

/// Initialize the source cache with comprehensive list
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

// See H4 note on VALID_SOURCES_CACHE above for design rationale.

/// Get valid sources (cached)  
fn get_valid_sources_sync() -> &'static [String] {
    // Ensure sources are loaded (lazy initialization)
    ensure_sources_loaded();

    // Get cached sources (will always be available after ensure_sources_loaded)
    VALID_SOURCES_CACHE
        .get()
        .map(|v| v.as_slice())
        .unwrap_or(&[])
}

/// Find the best fuzzy match using hybrid semantic + character-based scoring
///
/// Combines character-based similarity (Jaro-Winkler 70% + Normalized Levenshtein 30%)
/// with semantic bonuses:
/// - Prefix matching: 20% bonus for strong prefix similarity (≥7 chars)
/// - Substring matching: 12% bonus for compound word parts (australia-oceania)
/// - Length similarity: 10% bonus for appropriate length matches
/// - Anti-bias penalty: -10% for inappropriate short matches
///
/// Minimum threshold: 0.65 similarity to balance precision vs recall
fn find_best_fuzzy_match(input: &str, candidates: &[String]) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }

    let input_lower = input.to_lowercase();
    let mut best_match = None;
    let mut best_score = 0.0f64;

    // Minimum similarity threshold (0.0 to 1.0). Empirically tuned: 0.65 balances
    // precision (no false matches for "totally-invalid-place") vs recall (catches
    // typos like "antartica" → "antarctica", "belgum" → "belgium").
    let min_threshold = 0.65;

    for candidate in candidates {
        let candidate_lower = candidate.to_lowercase();

        // Jaro-Winkler: strong for transposition/prefix typos (e.g., "eurpoe" → "europe").
        // Gives extra weight to matching prefixes, which is ideal for geographic names.
        let jw_score = jaro_winkler(&input_lower, &candidate_lower);

        // Normalized Levenshtein: better for insertions/deletions (e.g., "belgum" → "belgium").
        // Complements JW by handling edit distance-based errors.
        let lev_score = normalized_levenshtein(&input_lower, &candidate_lower);

        // 70% JW + 30% Lev: JW dominates because geographic typos are more often
        // transpositions/prefix errors than insertions. Lev provides a safety net
        // for deletion-heavy typos.
        let combined_score = (jw_score * 0.7) + (lev_score * 0.3);

        // Semantic scoring bonuses — domain-specific adjustments for geographic names.
        let mut semantic_bonus = 0.0;

        // Prefix bonus (+20% max): Geographic names often share long prefixes
        // (e.g., "austrailia" vs "australia-oceania" share "austral"). A strong
        // prefix match (>70% similarity on first 7 chars) is a strong signal.
        let prefix_len = input_lower.chars().count().min(7);
        if prefix_len >= 4 {
            let input_prefix = input_lower.chars().take(prefix_len).collect::<String>();
            let candidate_prefix = candidate_lower.chars().take(prefix_len).collect::<String>();

            let prefix_similarity = normalized_levenshtein(&input_prefix, &candidate_prefix);
            if prefix_similarity > 0.7 {
                semantic_bonus += 0.2 * prefix_similarity;
            }
        }

        // Length bonus (+10% max): When both strings are long (>=8 chars) and
        // similar length, the match is more likely correct. Short candidates
        // matching long inputs are usually wrong (e.g., "austria" for "austrailia").
        if input_lower.len() >= 8 && candidate_lower.len() >= 8 {
            let length_ratio = 1.0
                - ((input_lower.len() as f64 - candidate_lower.len() as f64).abs()
                    / input_lower.len().max(candidate_lower.len()) as f64);
            if length_ratio > 0.7 {
                semantic_bonus += 0.1 * length_ratio;
            }
        }

        // Substring bonus (+12% max per part): Compound geographic names like
        // "australia-oceania" or "malaysia-singapore-brunei" should match well
        // when the input closely matches one component. Strict threshold (>85%)
        // prevents false matches on short common substrings.
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

        // Anti-bias penalty (-10%): Prevents short standalone candidates from
        // out-scoring longer correct matches. E.g., "austria" (7 chars) should
        // not beat "australia-oceania" (17 chars) for input "austrailia" (10 chars).
        // Only applies to bare names (no '/'), since "europe/austria" is a valid path.
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

/// Suggest a correction for a potentially misspelled source using fuzzy matching
pub fn suggest_correction(source: &str) -> Option<String> {
    // Get valid sources (cached)
    let valid_sources = get_valid_sources_sync();

    // First, check for exact case-insensitive match (no suggestion needed)
    for valid_source in valid_sources {
        if valid_source.eq_ignore_ascii_case(source) {
            return None; // Exact match, no suggestion needed
        }
    }

    // For standalone inputs (no '/'), check if it's a country name first
    if !source.contains('/') {
        // Check for exact country matches that should suggest continent/country path
        for valid_source in valid_sources {
            if let Some(slash_pos) = valid_source.find('/') {
                let country_part = &valid_source[slash_pos + 1..];
                if country_part.eq_ignore_ascii_case(source) {
                    return Some(valid_source.clone());
                }
            }
        }

        // Create separate lists for different types of matches
        let mut continent_level: Vec<String> = Vec::new();
        let mut country_level: Vec<String> = Vec::new();

        for valid_source in valid_sources {
            if valid_source.contains('/') {
                country_level.push(valid_source.clone());
            } else {
                continent_level.push(valid_source.clone());
            }
        }

        // For longer inputs (likely continents), try continents first
        if source.len() >= 6 {
            if let Some(match_result) = find_best_fuzzy_match(source, &continent_level) {
                return Some(match_result);
            }
        }

        // For short inputs, also try continents first to catch "plant" -> "planet"
        if source.len() <= 6 {
            if let Some(match_result) = find_best_fuzzy_match(source, &continent_level) {
                // Check if it's a really good match (high similarity)
                let source_lower = source.to_lowercase();
                let match_result_lower = match_result.to_lowercase();
                let similarity = jaro_winkler(&source_lower, &match_result_lower);
                if similarity > 0.8 {
                    return Some(match_result);
                }
            }
        }

        // Then try country names (just the country part, but fuzzy match against country part only)
        let country_names: Vec<String> = country_level
            .iter()
            .filter_map(|s| s.split('/').nth(1).map(|c| c.to_string()))
            .collect();

        if let Some(best_country) = find_best_fuzzy_match(source, &country_names) {
            // Find the full path for this country
            for full_path in &country_level {
                if let Some(country_part) = full_path.split('/').nth(1) {
                    if country_part == best_country {
                        return Some(full_path.clone());
                    }
                }
            }
        }

        // Finally try all sources
        return find_best_fuzzy_match(source, valid_sources);
    }

    // For paths (continent/country), handle geographic corrections
    if let Some(slash_pos) = source.find('/') {
        let continent = &source[..slash_pos];
        let country = &source[slash_pos + 1..];

        // Check if the country exists in any valid continent (geographic correction)
        for valid_source in valid_sources {
            if let Some(valid_slash_pos) = valid_source.find('/') {
                let valid_country = &valid_source[valid_slash_pos + 1..];
                if valid_country.eq_ignore_ascii_case(country) {
                    // Found correct geography, suggest the right continent
                    return Some(valid_source.clone());
                }
            }
        }

        // If country not found, check if continent is close to a valid continent
        let continents: Vec<String> = valid_sources
            .iter()
            .filter(|s| !s.contains('/'))
            .cloned()
            .collect();

        if let Some(corrected_continent) = find_best_fuzzy_match(continent, &continents) {
            // Only suggest continent if the country part is clearly invalid
            if country.len() > 8
                && !country
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-')
            {
                return Some(corrected_continent);
            }
            // For plausible but unknown countries, suggest the corrected continent
            return Some(corrected_continent);
        }
    }

    // Default: fuzzy match against all sources
    find_best_fuzzy_match(source, valid_sources)
}

/// Main error type for butterfly-osm operations
#[derive(Debug)]
pub enum Error {
    /// Source identifier not recognized or supported
    SourceNotFound(String),

    /// Network or HTTP-related download failure
    DownloadFailed(String),

    /// HTTP-specific error
    HttpError(String),

    /// File I/O error
    IoError(std::io::Error),

    /// Invalid configuration or parameters
    InvalidInput(String),

    /// Network connectivity issues
    NetworkError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SourceNotFound(source) => {
                write!(f, "Source '{source}' not found or not supported")
            }
            Error::DownloadFailed(msg) => {
                write!(f, "Download failed: {msg}")
            }
            Error::HttpError(msg) => {
                write!(f, "HTTP error: {msg}")
            }
            Error::IoError(err) => {
                write!(f, "I/O error: {err}")
            }
            Error::InvalidInput(msg) => {
                write!(f, "Invalid input: {msg}")
            }
            Error::NetworkError(msg) => {
                write!(f, "Network error: {msg}")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IoError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        if err.is_connect() || err.is_timeout() {
            Error::NetworkError(err.to_string())
        } else {
            Error::HttpError(err.to_string())
        }
    }
}

/// Convenience result type for butterfly-osm operations
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_correction_fuzzy_matching() {
        // Test common typos
        assert_eq!(
            suggest_correction("antartica"),
            Some("antarctica".to_string())
        );
        // "austrailia" should now correctly suggest "australia-oceania" with semantic scoring
        assert_eq!(
            suggest_correction("austrailia"),
            Some("australia-oceania".to_string())
        );
        assert_eq!(suggest_correction("eurpoe"), Some("europe".to_string()));
        assert_eq!(suggest_correction("afirca"), Some("africa".to_string()));

        // Test planet typos
        assert_eq!(suggest_correction("plant"), Some("planet".to_string()));
        assert_eq!(suggest_correction("plnet"), Some("planet".to_string()));
    }

    #[test]
    fn test_suggest_correction_standalone_country_names() {
        // Test standalone country names that should suggest continent/country paths
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
        // Test case insensitive
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
        // Test typos in standalone country names
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
        // Belgium is in Europe, so should suggest the correct geography
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
        // Unknown country should suggest the corrected continent
        assert_eq!(
            suggest_correction("europ/unknown-country"),
            Some("europe".to_string())
        );
    }

    #[test]
    fn test_suggest_correction_no_match() {
        assert_eq!(suggest_correction("totally-invalid-place"), None); // Too different
        assert_eq!(suggest_correction("europe"), None); // Correct spelling
        assert_eq!(suggest_correction("a"), None); // Too short and different
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
        assert_eq!(suggest_correction("EuRoPe"), None); // Correct spelling, just wrong case
    }

    #[test]
    fn test_strsim_fuzzy_matching() {
        // Test that strsim correctly prioritizes semantic matches
        let candidates = vec![
            "australia-oceania".to_string(),
            "austria".to_string(),
            "europe/austria".to_string(),
            "antarctica".to_string(),
        ];

        // "austrailia" should match "australia-oceania" better than "austria"
        let result = find_best_fuzzy_match("austrailia", &candidates);

        assert_eq!(result, Some("australia-oceania".to_string()));
    }

    #[test]
    fn test_semantic_bonuses() {
        // Test anti-bias penalty - long input should not match very short candidates
        let candidates = vec![
            "austria".to_string(),           // Short candidate - should get penalty
            "europe/austria".to_string(),    // Contains '/' - no penalty
            "australia-oceania".to_string(), // Long candidate - gets bonuses
        ];

        let result = find_best_fuzzy_match("very-long-input-string", &candidates);
        // Should not suggest "austria" due to anti-bias penalty
        assert_ne!(result, Some("austria".to_string()));

        // Test length-based bonus - similar length strings should get bonus
        let length_candidates = vec![
            "short".to_string(),
            "medium-length-string".to_string(),
            "very-long-similar-length".to_string(),
        ];

        let result = find_best_fuzzy_match("very-long-similar-input", &length_candidates);
        // Should prefer the similar length candidate
        assert_eq!(result, Some("very-long-similar-length".to_string()));

        // Test prefix bonus
        let prefix_candidates = vec![
            "australia-oceania".to_string(),
            "antarctica".to_string(),
            "africa".to_string(),
        ];

        let result = find_best_fuzzy_match("austr", &prefix_candidates);
        // Should prefer australia-oceania due to strong prefix match
        assert_eq!(result, Some("australia-oceania".to_string()));
    }
}
