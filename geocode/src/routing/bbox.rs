//! Country bounding-box dispatch (#96 §Country Routing).
//!
//! Bboxes live in the country packs ([`super::CountryPack::bbox`]).
//! This module is the lookup orchestrator that decides which loaded
//! country owns a given lat/lon point — primarily used by the reverse
//! endpoint when the caller didn't pin a country.
//!
//! The previous module (PR #169) had hardcoded bboxes for the 7
//! European countries. The new module reads from the
//! [`super::PackRegistry`] so adding a country's bbox is a TOML edit,
//! not a code change.

use super::{Classifier, CountryId, PackRegistry};

/// (min_lat, max_lat, min_lon, max_lon) in WGS84 degrees.
///
/// Returns `None` when the country isn't in the shipped pack set —
/// callers should treat this as "we don't know about this country
/// yet". Most code paths can use [`country_for_point`] / [`supported_countries_for_point`]
/// directly, which already handle the "no pack" case.
#[must_use]
pub fn bbox(c: CountryId) -> Option<(f64, f64, f64, f64)> {
    let reg = registry();
    reg.get(c).map(|p| {
        (
            p.bbox.min_lat,
            p.bbox.max_lat,
            p.bbox.min_lon,
            p.bbox.max_lon,
        )
    })
}

/// Test whether a point falls inside a country's bbox. Returns false
/// when the country isn't in the loaded pack set.
#[must_use]
pub fn contains(c: CountryId, lat: f64, lon: f64) -> bool {
    registry()
        .get(c)
        .map(|p| p.bbox.contains(lat, lon))
        .unwrap_or(false)
}

/// Select the most-specific (smallest-bbox) country containing the
/// point. Used by the reverse-geocoding spatial dispatch when the
/// caller didn't pin a country.
#[must_use]
pub fn country_for_point(lat: f64, lon: f64) -> Option<CountryId> {
    registry()
        .iter()
        .filter(|p| p.bbox.contains(lat, lon))
        .min_by(|a, b| {
            a.bbox
                .area_deg2()
                .partial_cmp(&b.bbox.area_deg2())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|p| p.country)
}

/// Return every country whose bbox contains the point. Border-region
/// queries with shards loaded for both countries can use this to fan
/// out, falling back to [`country_for_point`] if only one shard is
/// loaded.
#[must_use]
pub fn supported_countries_for_point(lat: f64, lon: f64) -> Vec<CountryId> {
    registry()
        .iter()
        .filter(|p| p.bbox.contains(lat, lon))
        .map(|p| p.country)
        .collect()
}

fn registry() -> &'static PackRegistry {
    Classifier::shipped().registry()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brussels_is_belgium() {
        assert_eq!(country_for_point(50.8467, 4.3525), Some(CountryId::BE));
    }

    #[test]
    fn paris_is_france() {
        assert_eq!(country_for_point(48.8584, 2.2945), Some(CountryId::FR));
    }

    #[test]
    fn amsterdam_is_netherlands() {
        assert_eq!(country_for_point(52.3791, 4.9003), Some(CountryId::NL));
    }

    #[test]
    fn luxembourg_city_is_luxembourg() {
        assert_eq!(country_for_point(49.6116, 6.1319), Some(CountryId::LU));
    }

    #[test]
    fn berlin_is_germany() {
        assert_eq!(country_for_point(52.5163, 13.3777), Some(CountryId::DE));
    }

    #[test]
    fn vienna_is_austria() {
        assert_eq!(country_for_point(48.2085, 16.3725), Some(CountryId::AT));
    }

    #[test]
    fn zurich_is_switzerland() {
        assert_eq!(country_for_point(47.3779, 8.5403), Some(CountryId::CH));
    }

    #[test]
    fn tokyo_is_japan() {
        assert_eq!(country_for_point(35.6762, 139.6503), Some(CountryId::JP));
    }

    #[test]
    fn nyc_is_us() {
        assert_eq!(country_for_point(40.7128, -74.006), Some(CountryId::US));
    }

    #[test]
    fn sao_paulo_is_brazil() {
        assert_eq!(country_for_point(-23.5505, -46.6333), Some(CountryId::BR));
    }

    #[test]
    fn delhi_is_india() {
        assert_eq!(country_for_point(28.6139, 77.209), Some(CountryId::IN));
    }

    #[test]
    fn sydney_is_australia() {
        assert_eq!(country_for_point(-33.8688, 151.2093), Some(CountryId::AU));
    }

    #[test]
    fn aachen_overlap_picks_smallest_bbox() {
        // Aachen Hbf: BE/NL/DE triangle. BE has the smallest bbox of
        // the three, so it wins the country_for_point race.
        let all = supported_countries_for_point(50.7676, 6.0911);
        assert!(all.contains(&CountryId::DE));
        assert_eq!(country_for_point(50.7676, 6.0911), Some(CountryId::BE));
    }

    #[test]
    fn ocean_outside_any_country() {
        assert_eq!(country_for_point(40.0, -30.0), None);
    }
}
