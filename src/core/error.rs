//! Error types for butterfly-dl library
//!
//! Provides comprehensive error handling for download operations.

use std::fmt;

use std::sync::OnceLock;
use reqwest::Client;
use serde_json::Value;

/// Cache for dynamically loaded valid sources
static VALID_SOURCES_CACHE: OnceLock<Vec<String>> = OnceLock::new();

/// Get valid sources, loading them dynamically from Geofabrik if needed
async fn get_valid_sources() -> &'static Vec<String> {
    VALID_SOURCES_CACHE.get_or_init(|| {
        // Fallback list in case we can't fetch from Geofabrik
        let mut sources = vec![
            "planet".to_string(),
            // Continents  
            "africa".to_string(), "antarctica".to_string(), "asia".to_string(), 
            "australia".to_string(), "europe".to_string(), "north-america".to_string(), 
            "south-america".to_string(), "central-america".to_string(), "oceania".to_string(),
        ];
        
        // Try to fetch from Geofabrik synchronously (blocking is OK for initialization)
        if let Ok(rt) = tokio::runtime::Runtime::new() {
            if let Ok(geofabrik_sources) = rt.block_on(fetch_geofabrik_sources()) {
                sources.extend(geofabrik_sources);
            }
        }
        
        sources
    })
}

/// Fetch valid sources from Geofabrik JSON
async fn fetch_geofabrik_sources() -> std::result::Result<Vec<String>, reqwest::Error> {
    let client = Client::new();
    let response = client
        .get("https://download.geofabrik.de/index-v1.json")
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await?;
    
    let json: Value = response.json().await?;
    let mut sources = Vec::new();
    
    if let Some(features) = json["features"].as_array() {
        for feature in features {
            if let Some(properties) = feature["properties"].as_object() {
                if let Some(id) = properties["id"].as_str() {
                    // Skip special entries and only include downloadable regions
                    if !id.starts_with("_") && properties.get("urls").is_some() {
                        sources.push(id.to_string());
                    }
                }
            }
        }
    }
    
    Ok(sources)
}

/// Synchronous version for use in the main suggestion function
fn get_valid_sources_sync() -> Vec<String> {
    // Try to get cached sources first
    if let Some(cached) = VALID_SOURCES_CACHE.get() {
        return cached.clone();
    }
    
    // Fallback to basic list if not cached yet
    vec![
        "planet".to_string(),
        "africa".to_string(), "antarctica".to_string(), "asia".to_string(), 
        "australia".to_string(), "europe".to_string(), "north-america".to_string(), 
        "south-america".to_string(), "central-america".to_string(), "oceania".to_string(),
        // Basic European countries as fallback
        "europe/germany".to_string(), "europe/france".to_string(), "europe/belgium".to_string(), 
        "europe/netherlands".to_string(), "europe/italy".to_string(), "europe/spain".to_string(),
        "europe/united-kingdom".to_string(), "europe/poland".to_string(), "europe/switzerland".to_string(), 
        "europe/austria".to_string(), "europe/monaco".to_string(), "europe/luxembourg".to_string(),
    ]
}

/// Calculate Levenshtein distance between two strings
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let s1_len = s1_chars.len();
    let s2_len = s2_chars.len();
    
    if s1_len == 0 { return s2_len; }
    if s2_len == 0 { return s1_len; }
    
    let mut matrix = vec![vec![0; s2_len + 1]; s1_len + 1];
    
    // Initialize first row and column
    for i in 0..=s1_len { matrix[i][0] = i; }
    for j in 0..=s2_len { matrix[0][j] = j; }
    
    // Fill the matrix
    for i in 1..=s1_len {
        for j in 1..=s2_len {
            let cost = if s1_chars[i-1] == s2_chars[j-1] { 0 } else { 1 };
            matrix[i][j] = (matrix[i-1][j] + 1)           // deletion
                .min(matrix[i][j-1] + 1)                  // insertion
                .min(matrix[i-1][j-1] + cost);            // substitution
        }
    }
    
    matrix[s1_len][s2_len]
}

/// Suggest a correction for a potentially misspelled source using fuzzy matching
pub fn suggest_correction(source: &str) -> Option<String> {
    let source_lower = source.to_lowercase();
    let mut best_match = None;
    let mut best_distance = usize::MAX;
    
    // Maximum distance we consider a reasonable typo (about 25% of the word length, minimum 1, maximum 3)
    let max_distance = (source.len() / 3).max(1).min(3);
    
    // Get valid sources (cached or fallback)
    let valid_sources = get_valid_sources_sync();
    
    // First, check if this is a standalone country name that should be continent/country
    if !source.contains('/') {
        for valid_source in &valid_sources {
            if let Some(slash_pos) = valid_source.find('/') {
                let country_part = &valid_source[slash_pos + 1..];
                if country_part.eq_ignore_ascii_case(&source) {
                    // Exact match for country name - suggest the full continent/country path
                    return Some(valid_source.clone());
                }
                
                // Also check fuzzy match against just the country part
                let distance = levenshtein_distance(&source_lower, country_part);
                if distance > 0 && distance <= max_distance && distance < best_distance {
                    best_distance = distance;
                    best_match = Some(valid_source.clone());
                }
            }
        }
        
        // If we found a country match, return it immediately (prioritize country paths)
        if best_match.is_some() {
            return best_match;
        }
    }
    
    // Then check regular fuzzy matching against all sources
    for valid_source in &valid_sources {
        let distance = levenshtein_distance(&source_lower, valid_source);
        
        // If it's an exact match (ignoring case), no need to suggest
        if distance == 0 {
            return None;
        }
        
        if distance <= max_distance && distance < best_distance {
            best_distance = distance;
            best_match = Some(valid_source.clone());
        }
    }
    
    // Also check if it's a country path where only the continent is misspelled
    if let Some(slash_pos) = source.find('/') {
        let continent = &source[..slash_pos];
        let country = &source[slash_pos + 1..];
        let continent_lower = continent.to_lowercase();
        
        // First, check if the country exists in any valid continent (find correct geography)
        let mut correct_continent_for_country = None;
        for valid_source in &valid_sources {
            if let Some(valid_slash_pos) = valid_source.find('/') {
                let valid_country = &valid_source[valid_slash_pos + 1..];
                if valid_country.eq_ignore_ascii_case(country) {
                    correct_continent_for_country = Some(&valid_source[..valid_slash_pos]);
                    break;
                }
            }
        }
        
        // If we found the correct continent for this country, prioritize that
        if let Some(correct_continent) = correct_continent_for_country {
            best_match = Some(format!("{}/{}", correct_continent, country));
        } else {
            // Otherwise, find the best matching continent but acknowledge we don't know the country
            let continent_sources = ["africa", "antarctica", "asia", "australia", "europe", 
                                   "north-america", "south-america", "central-america", "oceania"];
            
            for &valid_continent in &continent_sources {
                let distance = levenshtein_distance(&continent_lower, valid_continent);
                if distance > 0 && distance <= max_distance && distance < best_distance {
                    best_distance = distance;
                    best_match = Some(valid_continent.to_string());
                }
            }
        }
    }
    
    best_match
}

/// Main error type for butterfly-dl operations
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
                write!(f, "Source '{}' not found or not supported", source)
            }
            Error::DownloadFailed(msg) => {
                write!(f, "Download failed: {}", msg)
            }
            Error::HttpError(msg) => {
                write!(f, "HTTP error: {}", msg)
            }
            Error::IoError(err) => {
                write!(f, "I/O error: {}", err)
            }
            Error::InvalidInput(msg) => {
                write!(f, "Invalid input: {}", msg)
            }
            Error::NetworkError(msg) => {
                write!(f, "Network error: {}", msg)
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

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        if err.is_connect() || err.is_timeout() {
            Error::NetworkError(err.to_string())
        } else {
            Error::HttpError(err.to_string())
        }
    }
}


/// Convenience result type for butterfly-dl operations
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_correction_fuzzy_matching() {
        // Test common typos
        assert_eq!(suggest_correction("antartica"), Some("antarctica".to_string()));
        assert_eq!(suggest_correction("austrailia"), Some("australia".to_string()));
        assert_eq!(suggest_correction("eurpoe"), Some("europe".to_string()));
        assert_eq!(suggest_correction("afirca"), Some("africa".to_string()));
        // Test planet typos
        assert_eq!(suggest_correction("plant"), Some("planet".to_string()));
        assert_eq!(suggest_correction("plnet"), Some("planet".to_string()));
    }

    #[test]
    fn test_suggest_correction_standalone_country_names() {
        // Test standalone country names that should suggest continent/country paths
        assert_eq!(suggest_correction("monaco"), Some("europe/monaco".to_string()));
        assert_eq!(suggest_correction("belgium"), Some("europe/belgium".to_string()));
        assert_eq!(suggest_correction("germany"), Some("europe/germany".to_string()));
        assert_eq!(suggest_correction("france"), Some("europe/france".to_string()));
        // Test case insensitive
        assert_eq!(suggest_correction("MONACO"), Some("europe/monaco".to_string()));
        assert_eq!(suggest_correction("Belgium"), Some("europe/belgium".to_string()));
    }

    #[test]
    fn test_suggest_correction_standalone_country_typos() {
        // Test typos in standalone country names
        assert_eq!(suggest_correction("monac"), Some("europe/monaco".to_string()));
        assert_eq!(suggest_correction("belgum"), Some("europe/belgium".to_string()));
        assert_eq!(suggest_correction("germay"), Some("europe/germany".to_string()));
    }

    #[test]
    fn test_suggest_correction_country_paths() {
        // Belgium is in Europe, so should suggest the correct geography
        assert_eq!(suggest_correction("antartica/belgium"), Some("europe/belgium".to_string()));
        assert_eq!(suggest_correction("europ/france"), Some("europe/france".to_string()));
        assert_eq!(suggest_correction("eurpoe/germany"), Some("europe/germany".to_string()));
        // Unknown country should suggest the corrected continent
        assert_eq!(suggest_correction("europ/unknown-country"), Some("europe".to_string()));
    }

    #[test]
    fn test_suggest_correction_no_match() {
        assert_eq!(suggest_correction("totally-invalid-place"), None); // Too different
        assert_eq!(suggest_correction("europe"), None); // Correct spelling
        assert_eq!(suggest_correction("a"), None); // Too short and different
    }

    #[test]
    fn test_suggest_correction_case_insensitive() {
        assert_eq!(suggest_correction("ANTARTICA"), Some("antarctica".to_string()));
        assert_eq!(suggest_correction("AntArTiCa"), Some("antarctica".to_string()));
        assert_eq!(suggest_correction("EuRoPe"), None); // Correct spelling, just wrong case
    }

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("", "abc"), 3);
        assert_eq!(levenshtein_distance("abc", ""), 3);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
        assert_eq!(levenshtein_distance("antartica", "antarctica"), 1); // missing 'c'
        assert_eq!(levenshtein_distance("austrailia", "australia"), 1); // extra 'i' 
        assert_eq!(levenshtein_distance("eurpoe", "europe"), 2); // transposition
    }
}