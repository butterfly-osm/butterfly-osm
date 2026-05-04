//! Cheap deterministic country classifier (#96 §Country Routing).
//!
//! Belgium-only MVP returns `[(CountryId::BE, 1.0)]`. Shape matches
//! the architecture so the multi-country version (postcode regex,
//! script detection, lexical cues) can extend in place.

use super::CountryId;

#[must_use]
pub fn classify_country(_text: &str) -> Vec<(CountryId, f32)> {
    vec![(CountryId::BE, 1.0)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_returns_be() {
        let r = classify_country("Rue Wayez 122 1070 Anderlecht");
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, CountryId::BE);
        assert!((r[0].1 - 1.0).abs() < f32::EPSILON);
    }
}
