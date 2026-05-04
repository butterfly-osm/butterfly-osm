//! Per-country address shard.
//!
//! ## Format (`BFGS` v1)
//!
//! Single binary file. Numeric fields little-endian. Pattern matches
//! butterfly-route's "Pattern B" (body+file CRC).
//!
//! ```text
//!   Header (64 bytes):
//!     magic            "BFGS"        u32  (= 0x53464642)
//!     version          u16           (= 1)
//!     _pad             u16
//!     record_count     u32
//!     _pad             u32
//!     strings_off      u64
//!     strings_len      u64
//!     records_off      u64
//!     records_len      u64
//!     index_off        u64
//!     index_len        u64
//!   Strings: concatenated UTF-8 bytes (offset+len indexed from records)
//!   Records: 32-byte AddressRecord rows
//!     lat_e7 i32, lon_e7 i32,
//!     street(off u32, len u16), locality(off u32, len u16),
//!     house(off u32, len u16),  postcode(off u32, len u16),
//!     pad u32
//!   Index: { postcode | locality | street | postcode|street }
//!     each: u32 entry_count, then entries:
//!       u16 key_len, key_bytes, u32 list_len, u32×list_len record-ids
//!   Footer (16 bytes): u64 body_crc64, u64 file_crc64
//! ```

pub mod builder;
pub mod reader;

pub const MAGIC: u32 = u32::from_le_bytes(*b"BFGS");
pub const VERSION: u16 = 1;
pub const HEADER_BYTES: usize = 64;
pub const RECORD_BYTES: usize = 36;
pub const FOOTER_BYTES: usize = 16;

#[derive(Debug, Clone)]
pub struct AddressRecord {
    pub lat: f64,
    pub lon: f64,
    pub street: String,
    pub locality: String,
    pub housenumber: String,
    pub postcode: String,
}
