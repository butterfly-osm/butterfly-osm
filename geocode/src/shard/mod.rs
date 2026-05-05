//! Per-country address shard.
//!
//! ## Format (`BFGS` v5)
//!
//! Single binary file, mmap-friendly, little-endian. Designed for
//! zero-copy reads: every section is 4-byte aligned, every fixed-size
//! array is laid out so it can be cast directly off the mmap with
//! `bytemuck::cast_slice`. Pattern matches butterfly-route's "Pattern B"
//! (body+file CRC).
//!
//! ## Serve-the-world (#96)
//!
//! v4+ stores the country as a 2-byte ISO 3166-1 alpha-2 code
//! (e.g. `b"BE"`, `b"JP"`) at header offset 6-7, replacing v3's single
//! `u8` enum index. The bump unblocks "global address resolution at
//! 50-100K+ addr/sec" — adding a country is now a TOML pack drop, no
//! Rust changes, no per-country byte-code allocation.
//!
//! ## Authoritative-source ingestion (#96 §"Data Sources")
//!
//! v5 carries a per-record `source` byte (1 = OSM, 2 = OpenAddresses).
//! OpenAddresses is the canonical authoritative source for
//! butterfly-geocode (≈600 M addresses across ~40 countries, weekly
//! cadence, ODbL-compatible mix of upstream licenses). OSM is the
//! global fallback for countries OpenAddresses does not yet cover.
//!
//! Records are 36 bytes: the original 32-byte v3 layout plus a
//! `source` u8 plus 3 reserved pad bytes for 4-byte alignment. The
//! pad bytes are reserved for future per-record metadata and MUST be
//! zero at write time so the file CRC stays deterministic.
//!
//! ### Why v4 → v5
//!
//! v4 (PR #173) used the source byte 2 for "BOSA BeSt" (Belgium-only
//! authoritative). v5 reassigns code 2 to OpenAddresses (which
//! ingests BOSA upstream and several dozen other national/regional
//! authoritative datasets through one normalised schema). Because the
//! byte's semantics changed for the same code, v4 shards are NOT
//! readable by a v5 reader — operators rebuild via `butterfly-geocode
//! build-shard --country <ISO2> --source <osm|openaddresses>`.
//!
//! ```text
//!   Header (64 bytes):
//!     magic            "BFGS"        u32   (= 0x53464642)
//!     version          u16           (= 4)
//!     country_iso2     [u8; 2]       ASCII uppercase ISO 3166-1 alpha-2 ("BE", "JP", "US", ...)
//!     record_count     u32
//!     _pad             u32
//!     strings_off      u64
//!     strings_len      u64
//!     records_off      u64           (4-byte aligned)
//!     records_len      u64           (record_count * 36 bytes)
//!     index_off        u64           (4-byte aligned)
//!     index_len        u64
//!
//!   Strings: concatenated UTF-8 bytes (offset+len indexed from records).
//!   Padded with zero bytes to 4-byte boundary.
//!
//!   Records (36 bytes each, 4-byte aligned):
//!     lat_e7        i32
//!     lon_e7        i32
//!     street_off    u32
//!     street_len    u16
//!     loc_off       u32
//!     loc_len       u16
//!     house_off     u32
//!     house_len     u16
//!     pc_off        u32
//!     pc_len        u16
//!     source        u8        (SourceTag::to_u8 — 1=OSM, 2=OpenAddresses)
//!     _pad          u8[3]
//!
//!   Index region (4-byte aligned). Four sub-indices stored back-to-back
//!   in this order: postcode, locality, street, postcode|street.
//!   Each sub-index is a CSR triple:
//!     u32 num_keys
//!     u32 keys_data_len               (bytes)
//!     u32[num_keys + 1] keys_offsets  (byte offsets into keys_data, last = keys_data_len)
//!     u8[keys_data_len] keys_data     (concatenated normalized keys, sorted lexicographically)
//!     u8 padding to next u32 boundary
//!     u32[num_keys + 1] postings_offsets  (in u32 units, last = total_postings)
//!     u32[total_postings] postings_data
//!
//!   Footer (16 bytes):
//!     u64 body_crc64
//!     u64 file_crc64
//! ```

pub mod builder;
pub mod mmap;
pub mod reader;

pub const MAGIC: u32 = u32::from_le_bytes(*b"BFGS");
/// Current on-disk version. v5 reassigns the per-record source byte:
/// code 2 was BOSA BeSt in v4 (PR #173), and is now OpenAddresses in
/// v5 (PR #96 §Data Sources — single authoritative ingestion path
/// across ~40 countries via the OpenAddresses normalised schema).
/// Because byte 2's semantics changed for the same code, v4 shards
/// are NOT readable by a v5 reader — operators rebuild.
pub const VERSION: u16 = 5;
/// v3 (pre-#96 single-byte enum country) — retained so the reader
/// produces a precise upgrade message for the country-code change.
pub const VERSION_V3: u16 = 3;
/// v4 (#173 BOSA-as-byte-2) — retained so the reader produces a
/// precise upgrade message for the v4 → v5 rebuild.
pub const VERSION_V4: u16 = 4;
pub const HEADER_BYTES: usize = 64;
/// 32-byte v3 layout + `source` u8 + 3 pad bytes for 4-byte
/// alignment = 36. The pad bytes are reserved for future per-record
/// metadata (e.g. confidence score, quality flag) and MUST be zero
/// at write time so the file CRC stays deterministic.
pub const RECORD_BYTES: usize = 36;
pub const FOOTER_BYTES: usize = 16;

/// Authoritative-source tag for an [`AddressRecord`] (#96 §"Data
/// Sources").
///
/// **Stable on-disk encoding** for the BFGS version this code knows
/// about. The wire byte's meaning is version-dependent: v4 used
/// `2 = BOSA`, v5 uses `2 = OpenAddresses`. Operators rebuild on
/// version bumps.
///
/// In v5 the geocoder canonicalises every authoritative dataset
/// through OpenAddresses' normalised schema. OpenAddresses already
/// ingests BOSA (Belgium), BAN (France), BAG (Netherlands),
/// BD-Adresses (Luxembourg), state-level datasets for Germany,
/// Austria, Switzerland, the US, Australia, Brazil, Japan, etc. Per-
/// country authoritative ingestion as separate `SourceTag` variants
/// is deferred to a follow-up — until then OpenAddresses is the
/// single tag for non-OSM data. See `geocode-data/SOURCES.md` for
/// per-country source URLs and the schema contract.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum SourceTag {
    /// OpenStreetMap `addr:*` tags. Global fallback for countries
    /// OpenAddresses does not yet cover (or where coverage is
    /// materially worse than OSM).
    #[default]
    Osm,
    /// OpenAddresses (https://openaddresses.io). Federation of
    /// authoritative open-data address datasets normalised through
    /// the OpenAddresses schema.
    OpenAddresses,
}

impl SourceTag {
    /// Encode as a single byte for on-disk records (BFGS v5). Stable
    /// within a major version; semantic reassignments (e.g. v4's
    /// `2 = BOSA` → v5's `2 = OpenAddresses`) require a `VERSION` bump.
    #[must_use]
    pub fn to_u8(self) -> u8 {
        match self {
            SourceTag::Osm => 1,
            SourceTag::OpenAddresses => 2,
        }
    }

    /// Decode a record byte to a tag. Returns `None` for unknown
    /// codes (forward-compatible: a future BFGS bump can introduce new
    /// codes without breaking the reader's header parse, and the v5
    /// reader's record path returns `None` so callers fall back to
    /// `SourceTag::Osm` on unknown bytes).
    #[must_use]
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(SourceTag::Osm),
            2 => Some(SourceTag::OpenAddresses),
            _ => None,
        }
    }

    /// Stable human-readable name (used in CLI flags and metrics).
    /// Lowercase to match the `--source` CLI value.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            SourceTag::Osm => "osm",
            SourceTag::OpenAddresses => "openaddresses",
        }
    }

    /// Parse the `--source` CLI value (case-insensitive). Accepts
    /// `openaddresses` or the short alias `oa`.
    #[must_use]
    pub fn from_name(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "osm" => Some(SourceTag::Osm),
            "openaddresses" | "open-addresses" | "oa" => Some(SourceTag::OpenAddresses),
            _ => None,
        }
    }
}

/// Normalised address record. Crosses the importer/builder boundary;
/// `source_id` is **not** persisted in the shard (see [`SourceTag`])
/// — it lives only in the in-memory ingestion path so the merge
/// dedup can match OpenAddresses records back to their upstream
/// stable id.
#[derive(Debug, Clone)]
pub struct AddressRecord {
    pub lat: f64,
    pub lon: f64,
    pub street: String,
    pub locality: String,
    pub housenumber: String,
    pub postcode: String,
    /// Authoritative-source tag (#96 §"Data Sources"). Persisted in
    /// the BFGS v5 record byte. Default `SourceTag::Osm` is a
    /// conscious choice — pre-existing OSM PBF importers don't have
    /// to set it explicitly. The OpenAddresses loader sets it
    /// explicitly to [`SourceTag::OpenAddresses`].
    pub source: SourceTag,
    /// Upstream stable id (e.g. OpenAddresses `id` field, derived
    /// from upstream BOSA/BAN/BAG ids per OA's per-source conform
    /// rules). **Not** persisted in v5 shards — used by the merge
    /// dedup path only. `None` for OSM where the stable id is the
    /// OSM node/way id and would require a separate map.
    pub source_id: Option<String>,
}

impl Default for AddressRecord {
    fn default() -> Self {
        AddressRecord {
            lat: 0.0,
            lon: 0.0,
            street: String::new(),
            locality: String::new(),
            housenumber: String::new(),
            postcode: String::new(),
            source: SourceTag::Osm,
            source_id: None,
        }
    }
}

#[cfg(test)]
mod tag_tests {
    use super::*;

    #[test]
    fn source_tag_byte_round_trip() {
        for tag in [SourceTag::Osm, SourceTag::OpenAddresses] {
            assert_eq!(SourceTag::from_u8(tag.to_u8()), Some(tag));
        }
    }

    #[test]
    fn source_tag_name_round_trip() {
        for tag in [SourceTag::Osm, SourceTag::OpenAddresses] {
            assert_eq!(SourceTag::from_name(tag.name()), Some(tag));
        }
    }

    #[test]
    fn source_tag_name_aliases() {
        assert_eq!(
            SourceTag::from_name("OpenAddresses"),
            Some(SourceTag::OpenAddresses)
        );
        assert_eq!(
            SourceTag::from_name("open-addresses"),
            Some(SourceTag::OpenAddresses)
        );
        assert_eq!(SourceTag::from_name("oa"), Some(SourceTag::OpenAddresses));
        assert_eq!(SourceTag::from_name("OSM"), Some(SourceTag::Osm));
        // BOSA was a stopgap in v4 (PR #173) — v5 routes everything
        // authoritative through OpenAddresses, so the alias drops.
        assert_eq!(SourceTag::from_name("bosa"), None);
        assert_eq!(SourceTag::from_name("nope"), None);
    }

    #[test]
    fn unknown_byte_decodes_to_none() {
        assert_eq!(SourceTag::from_u8(0), None);
        // BOSA's old code 2 is now OpenAddresses in v5 — see
        // VERSION_V4 path in the reader for the upgrade message.
        assert_eq!(SourceTag::from_u8(3), None);
        assert_eq!(SourceTag::from_u8(99), None);
        assert_eq!(SourceTag::from_u8(255), None);
    }
}
