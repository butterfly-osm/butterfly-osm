//! Error types for butterfly-dl library
//!
//! Provides comprehensive error handling for download operations.

use std::fmt;

/// Known valid continents and countries for fuzzy matching
const VALID_SOURCES: &[&str] = &[
    // Special sources
    "planet",
    // Continents
    "africa", "antarctica", "asia", "australia", "europe", "north-america", "south-america", "central-america", "oceania",
    // Common countries/regions
    "europe/germany", "europe/france", "europe/belgium", "europe/netherlands", "europe/italy", "europe/spain",
    "europe/united-kingdom", "europe/poland", "europe/switzerland", "europe/austria", "europe/monaco",
    "asia/china", "asia/japan", "asia/india", "asia/russia", "north-america/us", "north-america/canada",
    "south-america/brazil", "south-america/argentina", "africa/south-africa", "australia/australia",
];

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
    
    for &valid_source in VALID_SOURCES {
        let distance = levenshtein_distance(&source_lower, valid_source);
        
        // If it's an exact match (ignoring case), no need to suggest
        if distance == 0 {
            return None;
        }
        
        if distance <= max_distance && distance < best_distance {
            best_distance = distance;
            best_match = Some(valid_source.to_string());
        }
    }
    
    // Also check if it's a country path where only the continent is misspelled
    if let Some(slash_pos) = source.find('/') {
        let continent = &source[..slash_pos];
        let country = &source[slash_pos + 1..];
        let continent_lower = continent.to_lowercase();
        
        // First, check if the country exists in any valid continent (find correct geography)
        let mut correct_continent_for_country = None;
        for &valid_source in VALID_SOURCES {
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