//! Per-country address shard.
//!
//! ## Format (`BFGS` v2)
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
//! ```text
//!   Header (64 bytes):
//!     magic            "BFGS"        u32   (= 0x53464642)
//!     version          u16           (= 2)
//!     _pad             u16
//!     record_count     u32
//!     _pad             u32
//!     strings_off      u64
//!     strings_len      u64
//!     records_off      u64           (4-byte aligned)
//!     records_len      u64           (record_count * 32 bytes)
//!     index_off        u64           (4-byte aligned)
//!     index_len        u64
//!
//!   Strings: concatenated UTF-8 bytes (offset+len indexed from records).
//!   Padded with zero bytes to 4-byte boundary.
//!
//!   Records (32 bytes each, 4-byte aligned):
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
//! Old `BFGS v1` shards (with the broken duplicate CRC) fail to load
//! against the v2 reader (version mismatch). They must be rebuilt.

pub mod builder;
pub mod mmap;
pub mod reader;

pub const MAGIC: u32 = u32::from_le_bytes(*b"BFGS");
pub const VERSION: u16 = 2;
pub const HEADER_BYTES: usize = 64;
pub const RECORD_BYTES: usize = 32;
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
