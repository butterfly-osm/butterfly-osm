//! Per-country address shard.
//!
//! ## Format (`BFGS` v4)
//!
//! Single binary file, mmap-friendly, little-endian. Designed for
//! zero-copy reads: every section is 4-byte aligned, every fixed-size
//! array is laid out so it can be cast directly off the mmap with
//! `bytemuck::cast_slice`. Pattern matches butterfly-route's "Pattern B"
//! (body+file CRC):
//!
//! - **body_crc** = CRC over body bytes (everything between header and
//!   footer)
//! - **file_crc** = CRC over header + body bytes (everything except
//!   the footer)
//!
//! ## Multi-country (#96)
//!
//! v3 introduced the per-shard country code (one shard per country,
//! see #96 §"Per-Country Shard Contents") stored at header byte 6.
//!
//! ## Authoritative-source ingestion (#96 §"Data Sources", v4)
//!
//! v4 extends the per-record layout with a single `source` byte
//! (1 = OSM, 2 = BOSA BeSt, 3 = BAN, 4 = BAG, 5 = G-NAF, 6 = BEV,
//! 7 = swisstopo). The byte answers the audit question "what shipped
//! this record?" without forcing the reader to track provenance
//! out-of-band. See [`SourceTag`] for the canonical encoding.
//!
//! Records went from 32 → 36 bytes (added `source` u8 + 3 reserved
//! pad bytes for 4-byte alignment). The old 32-byte v3 layout fails
//! to load (version mismatch); operators rebuild every shard with
//! `build-shard --source <osm|bosa|...> --country <iso2>`.
//!
//! ```text
//!   Header (64 bytes):
//!     magic            "BFGS"        u32   (= 0x53464642)
//!     version          u16           (= 4)
//!     country_code     u8            (CountryId::to_u8 — 1=BE, 2=FR, ...)
//!     _pad             u8
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
//!     source        u8        (SourceTag::to_u8 — 1=OSM, 2=BOSA, ...)
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
//!
//! Old `BFGS v1`/v2/v3 shards fail to load against the v4 reader
//! (version mismatch). They must be rebuilt — every shard must be
//! re-run via `build-shard --country <iso2> --source <tag>`.

pub mod builder;
pub mod mmap;
pub mod reader;

pub const MAGIC: u32 = u32::from_le_bytes(*b"BFGS");
/// Current on-disk version. v4 introduces the per-record source byte
/// (#96 §"Data Sources" — OSM/BOSA/BAN/BAG/G-NAF/...). v3 introduced
/// the per-shard country code. v2 was the Belgium-only MVP.
pub const VERSION: u16 = 4;
pub const HEADER_BYTES: usize = 64;
/// 32 byte v3 layout + `source` u8 + 3 pad bytes for 4-byte
/// alignment = 36. The pad bytes are reserved for future per-record
/// metadata (e.g. confidence score, quality flag) and MUST be zero
/// at write time so the file CRC stays deterministic.
pub const RECORD_BYTES: usize = 36;
pub const FOOTER_BYTES: usize = 16;

/// Authoritative-source tag for an [`AddressRecord`] (#96 §"Data
/// Sources").
///
/// Stable on-disk encoding: never reuse a code for a different source
/// across versions. Adding a new source = new variant + new arm in
/// `to_u8` / `from_u8` + new arm in [`SourceTag::name`].
///
/// `Osm` is the default fallback for shards built from PBF tags. Every
/// other variant tracks an authoritative open-data dataset. See
/// `geocode-data/SOURCES.md` for the per-country importer contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SourceTag {
    /// OpenStreetMap `addr:*` tags.
    Osm,
    /// Belgian BeSt Address (BOSA).
    Bosa,
    /// French Base Adresse Nationale (BAN).
    Ban,
    /// Dutch Basisregistratie Adressen en Gebouwen (BAG).
    Bag,
    /// Australian Geocoded National Address File (G-NAF).
    Gnaf,
    /// Austrian Bundesamt für Eich- und Vermessungswesen (BEV).
    Bev,
    /// Swiss Federal Office of Topography (swisstopo).
    Swisstopo,
}

impl SourceTag {
    /// Encode as a single byte for on-disk records (BFGS v4). Stable
    /// across versions.
    #[must_use]
    pub fn to_u8(self) -> u8 {
        match self {
            SourceTag::Osm => 1,
            SourceTag::Bosa => 2,
            SourceTag::Ban => 3,
            SourceTag::Bag => 4,
            SourceTag::Gnaf => 5,
            SourceTag::Bev => 6,
            SourceTag::Swisstopo => 7,
        }
    }

    /// Decode a record byte to a tag. Returns `None` for unknown
    /// codes (forward-compatible: a future shard can introduce new
    /// codes without breaking older readers' header parse).
    #[must_use]
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(SourceTag::Osm),
            2 => Some(SourceTag::Bosa),
            3 => Some(SourceTag::Ban),
            4 => Some(SourceTag::Bag),
            5 => Some(SourceTag::Gnaf),
            6 => Some(SourceTag::Bev),
            7 => Some(SourceTag::Swisstopo),
            _ => None,
        }
    }

    /// Stable human-readable name (used in CLI flags and metrics).
    /// Lowercase to match the `--source` CLI value.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            SourceTag::Osm => "osm",
            SourceTag::Bosa => "bosa",
            SourceTag::Ban => "ban",
            SourceTag::Bag => "bag",
            SourceTag::Gnaf => "gnaf",
            SourceTag::Bev => "bev",
            SourceTag::Swisstopo => "swisstopo",
        }
    }

    /// Parse the `--source` CLI value (case-insensitive).
    #[must_use]
    pub fn from_name(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "osm" => Some(SourceTag::Osm),
            "bosa" | "best" | "bosa-best" => Some(SourceTag::Bosa),
            "ban" => Some(SourceTag::Ban),
            "bag" => Some(SourceTag::Bag),
            "gnaf" | "g-naf" => Some(SourceTag::Gnaf),
            "bev" => Some(SourceTag::Bev),
            "swisstopo" => Some(SourceTag::Swisstopo),
            _ => None,
        }
    }
}

/// Normalised address record. Crosses the importer/builder boundary;
/// `source_id` is **not** persisted in the shard (see [`SourceTag`])
/// — it lives only in the in-memory ingestion path so the merge
/// dedup can match BOSA records back to their upstream stable id.
#[derive(Debug, Clone)]
pub struct AddressRecord {
    pub lat: f64,
    pub lon: f64,
    pub street: String,
    pub locality: String,
    pub housenumber: String,
    pub postcode: String,
    /// Authoritative-source tag (#96 §"Data Sources"). Persisted in
    /// the BFGS v4 record byte. Default `SourceTag::Osm` is a
    /// conscious choice — pre-existing OSM PBF importers don't have
    /// to set it explicitly. Every authoritative-source loader
    /// (BOSA, BAN, ...) sets it explicitly.
    pub source: SourceTag,
    /// Upstream stable id (e.g. BOSA `address_id`, BAN `id`).
    /// **Not** persisted in v4 shards — used by the merge dedup
    /// path only. `None` for OSM where the stable id is the OSM
    /// node/way id and would require a separate map.
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
        for tag in [
            SourceTag::Osm,
            SourceTag::Bosa,
            SourceTag::Ban,
            SourceTag::Bag,
            SourceTag::Gnaf,
            SourceTag::Bev,
            SourceTag::Swisstopo,
        ] {
            assert_eq!(SourceTag::from_u8(tag.to_u8()), Some(tag));
        }
    }

    #[test]
    fn source_tag_name_round_trip() {
        for tag in [
            SourceTag::Osm,
            SourceTag::Bosa,
            SourceTag::Ban,
            SourceTag::Bag,
            SourceTag::Gnaf,
            SourceTag::Bev,
            SourceTag::Swisstopo,
        ] {
            assert_eq!(SourceTag::from_name(tag.name()), Some(tag));
        }
    }

    #[test]
    fn source_tag_name_aliases() {
        assert_eq!(SourceTag::from_name("BOSA"), Some(SourceTag::Bosa));
        assert_eq!(SourceTag::from_name("best"), Some(SourceTag::Bosa));
        assert_eq!(SourceTag::from_name("bosa-best"), Some(SourceTag::Bosa));
        assert_eq!(SourceTag::from_name("g-naf"), Some(SourceTag::Gnaf));
        assert_eq!(SourceTag::from_name("nope"), None);
    }

    #[test]
    fn unknown_byte_decodes_to_none() {
        assert_eq!(SourceTag::from_u8(0), None);
        assert_eq!(SourceTag::from_u8(99), None);
        assert_eq!(SourceTag::from_u8(255), None);
    }
}
