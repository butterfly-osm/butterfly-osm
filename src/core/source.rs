//! Source resolution logic for butterfly-dl
//!
//! Handles intelligent routing between S3 and HTTP sources based on the requested data.

use crate::core::error::{Error, Result};

/// Represents different download sources
#[derive(Debug, Clone, PartialEq)]
pub enum DownloadSource {
    /// S3 source with bucket, key, and region information
    #[cfg(feature = "s3")]
    S3 {
        bucket: String,
        key: String,
        region: String,
    },
    /// HTTP source with direct URL
    Http {
        url: String,
    },
}

/// Configuration for download sources
pub struct SourceConfig {
    /// S3 bucket for planet files (when S3 feature is enabled)
    #[cfg(feature = "s3")]
    pub planet_s3_bucket: String,
    #[cfg(feature = "s3")]
    pub planet_s3_key: String,
    #[cfg(feature = "s3")]
    pub planet_s3_region: String,
    
    /// Fallback HTTP URL for planet files
    pub planet_http_url: String,
    
    /// Base URL for Geofabrik downloads
    pub geofabrik_base_url: String,
}

impl Default for SourceConfig {
    fn default() -> Self {
        Self {
            #[cfg(feature = "s3")]
            planet_s3_bucket: "osm-planet-eu-central-1".to_string(),
            #[cfg(feature = "s3")]
            planet_s3_key: "planet-latest.osm.pbf".to_string(),
            #[cfg(feature = "s3")]
            planet_s3_region: "eu-central-1".to_string(),
            
            planet_http_url: "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf".to_string(),
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

/// Resolves planet source based on feature availability
fn resolve_planet_source(config: &SourceConfig) -> Result<DownloadSource> {
    #[cfg(feature = "s3")]
    {
        Ok(DownloadSource::S3 {
            bucket: config.planet_s3_bucket.clone(),
            key: config.planet_s3_key.clone(),
            region: config.planet_s3_region.clone(),
        })
    }
    
    #[cfg(not(feature = "s3"))]
    {
        Ok(DownloadSource::Http {
            url: config.planet_http_url.clone(),
        })
    }
}

/// Generates output filename from source
pub fn resolve_output_filename(source: &str) -> String {
    match source {
        "planet" => "planet-latest.osm.pbf".to_string(),
        path if path.contains('/') => {
            let name = path.split('/').last().unwrap_or(path);
            format!("{}-latest.osm.pbf", name)
        },
        continent => format!("{}-latest.osm.pbf", continent),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_planet_source() {
        let config = SourceConfig::default();
        let source = resolve_source("planet", &config).unwrap();
        
        #[cfg(feature = "s3")]
        {
            match source {
                DownloadSource::S3 { bucket, key, region } => {
                    assert_eq!(bucket, "osm-planet-eu-central-1");
                    assert_eq!(key, "planet-latest.osm.pbf");
                    assert_eq!(region, "eu-central-1");
                }
                _ => panic!("Expected S3 source for planet with s3 feature"),
            }
        }
        
        #[cfg(not(feature = "s3"))]
        {
            match source {
                DownloadSource::Http { url } => {
                    assert_eq!(url, "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf");
                }
                _ => panic!("Expected HTTP source for planet without s3 feature"),
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
            _ => panic!("Expected HTTP source for continent"),
        }
    }

    #[test]
    fn test_resolve_country_source() {
        let config = SourceConfig::default();
        let source = resolve_source("europe/belgium", &config).unwrap();
        
        match source {
            DownloadSource::Http { url } => {
                assert_eq!(url, "https://download.geofabrik.de/europe/belgium-latest.osm.pbf");
            }
            _ => panic!("Expected HTTP source for country"),
        }
    }

    #[test]
    fn test_resolve_output_filename() {
        assert_eq!(resolve_output_filename("planet"), "planet-latest.osm.pbf");
        assert_eq!(resolve_output_filename("europe"), "europe-latest.osm.pbf");
        assert_eq!(resolve_output_filename("europe/belgium"), "belgium-latest.osm.pbf");
    }
}