//! Country routing — first-class stage that runs BEFORE full parsing
//! per #96.

pub mod bbox;
pub mod classifier;

pub use bbox::{country_for_point, supported_countries_for_point};
pub use classifier::classify_country;
use serde::{Deserialize, Serialize};

/// Country identifier (ISO 3166-1 alpha-2).
///
/// The set of variants here is the set of countries the geocoder
/// knows how to build a shard for. Adding a country is a 4-step
/// change: extend this enum, add the lexical signals to
/// [`classifier`], add the lat/lon bounding box to [`bbox`], and ship
/// a shard built via `build-shard --country <code>`.
///
/// Per #96 cluster #1 (BE / FR / NL / LU / DE) and cluster #2
/// (AT / DE / CH) is the multi-country MVP scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CountryId {
    /// Belgium.
    BE,
    /// France.
    FR,
    /// Netherlands.
    NL,
    /// Luxembourg.
    LU,
    /// Germany.
    DE,
    /// Austria.
    AT,
    /// Switzerland.
    CH,
}

impl CountryId {
    /// All countries the geocoder is wired for.
    pub const ALL: &'static [CountryId] = &[
        CountryId::BE,
        CountryId::FR,
        CountryId::NL,
        CountryId::LU,
        CountryId::DE,
        CountryId::AT,
        CountryId::CH,
    ];

    /// ISO 3166-1 alpha-2 code, uppercase.
    #[must_use]
    pub fn iso2(self) -> &'static str {
        match self {
            CountryId::BE => "BE",
            CountryId::FR => "FR",
            CountryId::NL => "NL",
            CountryId::LU => "LU",
            CountryId::DE => "DE",
            CountryId::AT => "AT",
            CountryId::CH => "CH",
        }
    }

    /// Parse an ISO 3166-1 alpha-2 code (case-insensitive). Returns
    /// `None` for codes outside the supported set.
    #[must_use]
    pub fn from_iso2(code: &str) -> Option<Self> {
        match code.trim().to_ascii_uppercase().as_str() {
            "BE" => Some(CountryId::BE),
            "FR" => Some(CountryId::FR),
            "NL" => Some(CountryId::NL),
            "LU" => Some(CountryId::LU),
            "DE" => Some(CountryId::DE),
            "AT" => Some(CountryId::AT),
            "CH" => Some(CountryId::CH),
            _ => None,
        }
    }

    /// Encode as a single byte for on-disk shard headers (BFGS v3).
    /// Stable: never reuse a code for a different country across
    /// versions.
    #[must_use]
    pub fn to_u8(self) -> u8 {
        match self {
            CountryId::BE => 1,
            CountryId::FR => 2,
            CountryId::NL => 3,
            CountryId::LU => 4,
            CountryId::DE => 5,
            CountryId::AT => 6,
            CountryId::CH => 7,
        }
    }

    /// Decode a header byte to a country.
    #[must_use]
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(CountryId::BE),
            2 => Some(CountryId::FR),
            3 => Some(CountryId::NL),
            4 => Some(CountryId::LU),
            5 => Some(CountryId::DE),
            6 => Some(CountryId::AT),
            7 => Some(CountryId::CH),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso2_round_trip_all_countries() {
        for &c in CountryId::ALL {
            assert_eq!(CountryId::from_iso2(c.iso2()), Some(c));
        }
    }

    #[test]
    fn iso2_case_insensitive() {
        assert_eq!(CountryId::from_iso2("be"), Some(CountryId::BE));
        assert_eq!(CountryId::from_iso2(" Fr "), Some(CountryId::FR));
        assert_eq!(CountryId::from_iso2("Nl"), Some(CountryId::NL));
    }

    #[test]
    fn iso2_unknown_returns_none() {
        assert_eq!(CountryId::from_iso2("XX"), None);
        assert_eq!(CountryId::from_iso2(""), None);
        assert_eq!(CountryId::from_iso2("GBR"), None);
    }

    #[test]
    fn u8_round_trip_all_countries() {
        for &c in CountryId::ALL {
            assert_eq!(CountryId::from_u8(c.to_u8()), Some(c));
        }
    }

    #[test]
    fn u8_codes_are_distinct_and_nonzero() {
        let mut seen = std::collections::HashSet::new();
        for &c in CountryId::ALL {
            let b = c.to_u8();
            assert_ne!(b, 0, "0 is reserved for 'unknown' on disk");
            assert!(seen.insert(b), "duplicate u8 code for {:?}", c);
        }
    }
}
