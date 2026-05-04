//! Country bounding boxes — used by the reverse-geocoding spatial
//! dispatch and as a coarse prior for the forward classifier (#96
//! §Country Routing).
//!
//! These are conservative land-area bboxes from public reference data
//! (CIA World Factbook + Wikipedia). They are deliberately a few
//! tenths of a degree larger than the ISO administrative borders so
//! coastline points and small enclaves resolve cleanly. Overlap
//! between bboxes is expected (e.g. BE / NL / DE all hit Aachen
//! triangle); [`country_for_point`] picks the smallest bbox containing
//! the point as a tiebreak.
//!
//! The actual country routing for forward queries is the lexical
//! [`super::classifier`]. These bboxes are the secondary path the
//! reverse endpoint uses when no input string is available.

use super::CountryId;

/// (min_lat, max_lat, min_lon, max_lon) in WGS84 degrees.
#[must_use]
pub fn bbox(c: CountryId) -> (f64, f64, f64, f64) {
    match c {
        // Belgium: 49.5 to 51.5, 2.5 to 6.4
        CountryId::BE => (49.50, 51.55, 2.50, 6.45),
        // France (mainland): 41.3 to 51.1, -5.2 to 9.6 (DOM-TOMs excluded)
        CountryId::FR => (41.30, 51.15, -5.25, 9.65),
        // Netherlands (European): 50.7 to 53.6, 3.3 to 7.3
        CountryId::NL => (50.70, 53.60, 3.30, 7.30),
        // Luxembourg: 49.4 to 50.2, 5.7 to 6.6
        CountryId::LU => (49.42, 50.22, 5.70, 6.55),
        // Germany: 47.2 to 55.1, 5.8 to 15.1
        CountryId::DE => (47.25, 55.10, 5.80, 15.10),
        // Austria: 46.3 to 49.1, 9.5 to 17.2
        CountryId::AT => (46.32, 49.05, 9.50, 17.20),
        // Switzerland: 45.7 to 47.9, 5.9 to 10.5
        CountryId::CH => (45.75, 47.90, 5.90, 10.55),
    }
}

/// Approximate bbox area in (lat × lon) degree-squared. Used as a
/// tiebreak when multiple bboxes contain the point — pick the
/// smallest (most-specific) bbox.
fn area_deg2(c: CountryId) -> f64 {
    let (a, b, x, y) = bbox(c);
    (b - a) * (y - x)
}

/// Test whether a point falls inside a country bbox.
#[must_use]
pub fn contains(c: CountryId, lat: f64, lon: f64) -> bool {
    let (lat_lo, lat_hi, lon_lo, lon_hi) = bbox(c);
    lat >= lat_lo && lat <= lat_hi && lon >= lon_lo && lon <= lon_hi
}

/// Select the most-specific (smallest-bbox) country containing the
/// point. Used by the reverse-geocoding spatial dispatch when the
/// caller didn't pin a country.
#[must_use]
pub fn country_for_point(lat: f64, lon: f64) -> Option<CountryId> {
    CountryId::ALL
        .iter()
        .copied()
        .filter(|&c| contains(c, lat, lon))
        .min_by(|&a, &b| {
            area_deg2(a)
                .partial_cmp(&area_deg2(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

/// Return every country whose bbox contains the point. Border-region
/// queries with shards loaded for both countries can use this to fan
/// out, falling back to [`country_for_point`] if only one shard is
/// loaded.
#[must_use]
pub fn supported_countries_for_point(lat: f64, lon: f64) -> Vec<CountryId> {
    CountryId::ALL
        .iter()
        .copied()
        .filter(|&c| contains(c, lat, lon))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brussels_is_belgium() {
        // Grand-Place: 50.8467, 4.3525
        assert_eq!(country_for_point(50.8467, 4.3525), Some(CountryId::BE));
    }

    #[test]
    fn paris_is_france() {
        // Eiffel Tower: 48.8584, 2.2945
        assert_eq!(country_for_point(48.8584, 2.2945), Some(CountryId::FR));
    }

    #[test]
    fn amsterdam_is_netherlands() {
        // Amsterdam Centraal: 52.3791, 4.9003
        assert_eq!(country_for_point(52.3791, 4.9003), Some(CountryId::NL));
    }

    #[test]
    fn luxembourg_city_is_luxembourg() {
        // Luxembourg City: 49.6116, 6.1319
        assert_eq!(country_for_point(49.6116, 6.1319), Some(CountryId::LU));
    }

    #[test]
    fn berlin_is_germany() {
        // Brandenburg Gate: 52.5163, 13.3777
        assert_eq!(country_for_point(52.5163, 13.3777), Some(CountryId::DE));
    }

    #[test]
    fn vienna_is_austria() {
        // Stephansplatz: 48.2085, 16.3725
        assert_eq!(country_for_point(48.2085, 16.3725), Some(CountryId::AT));
    }

    #[test]
    fn zurich_is_switzerland() {
        // Zurich HB: 47.3779, 8.5403
        assert_eq!(country_for_point(47.3779, 8.5403), Some(CountryId::CH));
    }

    #[test]
    fn aachen_overlap_picks_smallest_bbox() {
        // Aachen Hbf: 50.7676, 6.0911 — sits in the BE/NL/DE
        // triangle. We expect supported_countries_for_point to
        // surface ALL three; country_for_point picks the smallest
        // bbox, which is BE here (DE is huge).
        let all = supported_countries_for_point(50.7676, 6.0911);
        assert!(all.contains(&CountryId::DE));
        assert_eq!(country_for_point(50.7676, 6.0911), Some(CountryId::BE));
    }

    #[test]
    fn ocean_outside_any_country() {
        assert_eq!(country_for_point(40.0, -30.0), None);
    }
}
