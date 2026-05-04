//! Country routing — first-class stage that runs BEFORE full parsing
//! per #96.

pub mod classifier;

pub use classifier::classify_country;
use serde::{Deserialize, Serialize};

/// Country identifier.
///
/// MVP only ships [`Self::BE`]. The enum is `non_exhaustive` so adding
/// variants in a follow-up phase does not break downstream callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CountryId {
    /// Belgium (ISO 3166-1 alpha-2: BE).
    BE,
}

impl CountryId {
    #[must_use]
    pub fn iso2(self) -> &'static str {
        match self {
            CountryId::BE => "BE",
        }
    }

    #[must_use]
    pub fn from_iso2(code: &str) -> Option<Self> {
        match code.trim().to_ascii_uppercase().as_str() {
            "BE" => Some(CountryId::BE),
            _ => None,
        }
    }
}
