//! Source resolution logic for butterfly-dl
//!
//! Handles HTTP source routing for OpenStreetMap data downloads.

use crate::core::error::Result;

/// Represents different download sources
#[derive(Debug, Clone, PartialEq)]
pub enum DownloadSource {
    /// HTTP source with direct URL
    Http { url: String },
}

/// Configuration for download sources
pub struct SourceConfig {
    /// HTTP URL for planet files
    pub planet_http_url: String,

    /// Base URL for Geofabrik downloads
    pub geofabrik_base_url: String,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            planet_http_url: "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf"
                .to_string(),
            geofabrik_base_url: "https://download.geofabrik.de".to_string(),
        }
    }
}

/// Resolves a source string to a download source
pub fn resolve_source(source: &str, config: &SourceConfig) -> Result<DownloadSource> {
    match source {
        "planet" => resolve_planet_source(config),
        path if path.contains('/') => Ok(DownloadSource::Http {
            url: format!("{}/{}-latest.osm.pbf", config.geofabrik_base_url, path),
        }),
        continent => Ok(DownloadSource::Http {
            url: format!("{}/{}-latest.osm.pbf", config.geofabrik_base_url, continent),
        }),
    }
}

/// Resolves planet source to HTTP download
fn resolve_planet_source(config: &SourceConfig) -> Result<DownloadSource> {
    Ok(DownloadSource::Http {
        url: config.planet_http_url.clone(),
    })
}

/// Generates output filename from source
pub fn resolve_output_filename(source: &str) -> String {
    match source {
        "planet" => "planet-latest.osm.pbf".to_string(),
        path if path.contains('/') => {
            let name = path.split('/').next_back().unwrap_or(path);
            format!("{name}-latest.osm.pbf")
        }
        continent => format!("{continent}-latest.osm.pbf"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_planet_source() {
        let config = SourceConfig::default();
        let source = resolve_source("planet", &config).unwrap();

        match source {
            DownloadSource::Http { url } => {
                assert_eq!(
                    url,
                    "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf"
                );
            }
        }
    }

    #[test]
    fn test_resolve_continent_source() {
        let config = SourceConfig::default();
        let source = resolve_source("europe", &config).unwrap();

        match source {
            DownloadSource::Http { url } => {
                assert_eq!(url, "https://download.geofabrik.de/europe-latest.osm.pbf");
            }
        }
    }

    #[test]
    fn test_resolve_country_source() {
        let config = SourceConfig::default();
        let source = resolve_source("europe/belgium", &config).unwrap();

        match source {
            DownloadSource::Http { url } => {
                assert_eq!(
                    url,
                    "https://download.geofabrik.de/europe/belgium-latest.osm.pbf"
                );
            }
        }
    }

    #[test]
    fn test_resolve_output_filename() {
        assert_eq!(resolve_output_filename("planet"), "planet-latest.osm.pbf");
        assert_eq!(resolve_output_filename("europe"), "europe-latest.osm.pbf");
        assert_eq!(
            resolve_output_filename("europe/belgium"),
            "belgium-latest.osm.pbf"
        );
    }
}
