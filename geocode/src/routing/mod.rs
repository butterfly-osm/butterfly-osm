//! Country routing — first-class stage that runs BEFORE full parsing
//! per #96.
//!
//! ## Serve-the-world (`butterfly-osm#96`)
//!
//! [`CountryId`] is a 2-byte ISO 3166-1 alpha-2 code, NOT an enum.
//! Adding a country is dropping a TOML pack into
//! `geocode/data/packs/<iso2>.toml` and rebuilding a shard — zero
//! Rust changes. The constants on [`CountryId`] (`BE`, `FR`, `JP`,
//! `US`, …) are sugar for the most common codes; the underlying
//! type accepts any 2-uppercase-letter input via [`CountryId::from_iso2`].
//!
//! The previous enum (PR #169) had 7 hardcoded variants
//! (BE/FR/NL/LU/DE/AT/CH). That model was inherently European.
//! The newtype model unblocks #96 §"global address resolution at
//! 50-100K+ addr/sec" — Japanese, Brazilian, Indian, US, Australian
//! shards now sit symmetrically next to European ones.

pub mod bbox;
pub mod classifier;
pub mod pack;

pub use bbox::{country_for_point, supported_countries_for_point};
pub use classifier::{Classifier, classify_country};
pub use pack::{CountryPack, PackRegistry};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use std::sync::{OnceLock, RwLock};

/// Country identifier — ISO 3166-1 alpha-2, stored as 2 ASCII uppercase
/// bytes.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CountryId(pub [u8; 2]);

impl CountryId {
    // ---- European cluster (#96 cluster #1 + #2) ----
    pub const BE: CountryId = CountryId(*b"BE");
    pub const FR: CountryId = CountryId(*b"FR");
    pub const NL: CountryId = CountryId(*b"NL");
    pub const LU: CountryId = CountryId(*b"LU");
    pub const DE: CountryId = CountryId(*b"DE");
    pub const AT: CountryId = CountryId(*b"AT");
    pub const CH: CountryId = CountryId(*b"CH");

    // ---- Other European packs shipped by default ----
    pub const GB: CountryId = CountryId(*b"GB");
    pub const ES: CountryId = CountryId(*b"ES");
    pub const IT: CountryId = CountryId(*b"IT");

    // ---- Non-European MVP set demonstrating "serve the world" ----
    pub const US: CountryId = CountryId(*b"US");
    pub const JP: CountryId = CountryId(*b"JP");
    pub const BR: CountryId = CountryId(*b"BR");
    pub const IN: CountryId = CountryId(*b"IN");
    pub const AU: CountryId = CountryId(*b"AU");

    /// Parse an ISO 3166-1 alpha-2 code (case-insensitive). Whitespace
    /// is trimmed. Non-ASCII or non-alphabetic input → `None`.
    #[must_use]
    pub fn from_iso2(code: &str) -> Option<Self> {
        let s = code.trim();
        let bytes = s.as_bytes();
        if bytes.len() != 2 {
            return None;
        }
        let a = bytes[0];
        let b = bytes[1];
        if !a.is_ascii_alphabetic() || !b.is_ascii_alphabetic() {
            return None;
        }
        Some(CountryId([a.to_ascii_uppercase(), b.to_ascii_uppercase()]))
    }

    /// Return the two-letter ISO code as a `&'static str`. Cached via
    /// a small global intern map — first call per code allocates and
    /// leaks 2 bytes; subsequent calls are an `RwLock` read. Bounded
    /// at 26×26 = 676 codes total (~4 KB worst case leak).
    #[must_use]
    pub fn iso2(self) -> &'static str {
        iso2_intern(self.0)
    }

    /// Same as [`Self::iso2`] but returns a `&str` borrowed from `self`.
    /// Does not touch the intern table — useful in hot paths.
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.0).unwrap_or("??")
    }

    /// The raw 2-byte representation. Used by the BFGS v4 shard header
    /// (offset 6-7).
    #[must_use]
    pub fn as_bytes(self) -> [u8; 2] {
        self.0
    }

    /// Construct directly from 2 bytes. Caller is responsible for
    /// validating that bytes are uppercase ASCII alphabetic; use
    /// [`Self::from_iso2`] for untrusted input.
    #[must_use]
    pub const fn from_bytes(b: [u8; 2]) -> Self {
        CountryId(b)
    }
}

impl std::fmt::Debug for CountryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountryId({})", self.as_str())
    }
}

impl std::fmt::Display for CountryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for CountryId {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CountryId {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        CountryId::from_iso2(&s)
            .ok_or_else(|| serde::de::Error::custom(format!("invalid ISO 3166-1 alpha-2: '{s}'")))
    }
}

fn iso2_intern(bytes: [u8; 2]) -> &'static str {
    use std::collections::HashMap;
    static TABLE: OnceLock<RwLock<HashMap<[u8; 2], &'static str>>> = OnceLock::new();
    let table = TABLE.get_or_init(|| RwLock::new(HashMap::with_capacity(64)));
    // Recover from poisoning rather than crashing the process: the
    // intern table is an append-only cache, every entry is a stable
    // 2-byte ASCII code → leaked `&'static str`. A panic in another
    // thread leaves the data structurally valid; the worst case is a
    // duplicate entry, which `or_insert_with` handles by returning the
    // existing value.
    if let Some(s) = table.read().unwrap_or_else(|e| e.into_inner()).get(&bytes) {
        return s;
    }
    let mut w = table.write().unwrap_or_else(|e| e.into_inner());
    w.entry(bytes).or_insert_with(|| {
        let s = std::str::from_utf8(&bytes).expect("CountryId bytes must be UTF-8");
        Box::leak(s.to_string().into_boxed_str())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso2_round_trip_constants() {
        for c in [
            CountryId::BE,
            CountryId::FR,
            CountryId::NL,
            CountryId::LU,
            CountryId::DE,
            CountryId::AT,
            CountryId::CH,
            CountryId::GB,
            CountryId::ES,
            CountryId::IT,
            CountryId::US,
            CountryId::JP,
            CountryId::BR,
            CountryId::IN,
            CountryId::AU,
        ] {
            assert_eq!(CountryId::from_iso2(c.iso2()), Some(c));
        }
    }

    #[test]
    fn iso2_case_insensitive() {
        assert_eq!(CountryId::from_iso2("be"), Some(CountryId::BE));
        assert_eq!(CountryId::from_iso2(" Fr "), Some(CountryId::FR));
        assert_eq!(CountryId::from_iso2("Nl"), Some(CountryId::NL));
        assert_eq!(CountryId::from_iso2("jp"), Some(CountryId::JP));
        assert_eq!(CountryId::from_iso2("Us"), Some(CountryId::US));
    }

    #[test]
    fn iso2_unknown_strings_rejected() {
        assert_eq!(CountryId::from_iso2(""), None);
        assert_eq!(CountryId::from_iso2("GBR"), None);
        assert_eq!(CountryId::from_iso2("12"), None);
        assert_eq!(CountryId::from_iso2("B1"), None);
    }

    #[test]
    fn iso2_returns_static_str() {
        let s1 = CountryId::JP.iso2();
        let s2 = CountryId::JP.iso2();
        assert_eq!(s1.as_ptr(), s2.as_ptr());
        assert_eq!(s1, "JP");
    }

    #[test]
    fn arbitrary_iso2_works_without_compile_time_constant() {
        let zw = CountryId::from_iso2("ZW").expect("Zimbabwe is two letters");
        assert_eq!(zw.iso2(), "ZW");
        let by = zw.as_bytes();
        assert_eq!(by, *b"ZW");
        assert_eq!(CountryId::from_bytes(by), zw);
    }

    #[test]
    fn serde_round_trip() {
        let c = CountryId::FR;
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, "\"FR\"");
        let back: CountryId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn serde_rejects_invalid_iso2() {
        let r: Result<CountryId, _> = serde_json::from_str("\"GBR\"");
        assert!(r.is_err());
    }

    #[test]
    fn debug_display_show_iso2() {
        assert_eq!(format!("{}", CountryId::DE), "DE");
        assert_eq!(format!("{:?}", CountryId::DE), "CountryId(DE)");
    }
}
