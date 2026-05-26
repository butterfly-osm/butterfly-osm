//! `step7/cch.<mode>.middles` — per-mode CCH shortcut middle pointers,
//! split out of `cch.topo` into a separate **cold** section (#359 /
//! #352 phase 2).
//!
//! The middles array is only consulted during shortcut unpack (P2P
//! route geometry reconstruction). Matrix / isochrone / bucket-M2M
//! never touch it. Co-locating the bytes in a dedicated section lets
//! matrix-only workloads `madvise(DONTNEED)` the entire middle range
//! without affecting the topology's hot pages — projected
//! ~300-420 MB RSS savings per Belgium mode under 24-thread matrix
//! load (codex assessment on #352).
//!
//! # Format (v1)
//!
//! ```text
//! header (24 bytes):
//!   magic       : u32   = 0x53444944 ("DIDS" LE — Directed Middle pointer Section)
//!   version     : u16   = 1
//!   flags       : u16
//!                       bits 0..=1: up_middle width   (00=u32, 01=u16, 10=u24)
//!                       bits 2..=3: down_middle width
//!                       bits 4..=15: reserved (must be 0)
//!   n_up_edges  : u64
//!   n_down_edges: u64
//!
//! body:
//!   up_middle_bytes    (n_up_edges × {2, 3, or 4} bytes, padded to u64)
//!   down_middle_bytes  (n_down_edges × {2, 3, or 4} bytes, padded to u64)
//!
//! footer (16 bytes):
//!   body_crc : u64    (CRC-64 over body)
//!   file_crc : u64    (CRC-64 over header || body)
//! ```
//!
//! All multi-byte integers are little-endian. The width-picker is
//! shared with the `cch.topo` v5 writer — same convention, same
//! sentinel mapping (`u32::MAX` ↔ `u16::MAX` / `U24_SENTINEL`).

use std::sync::Arc;

use anyhow::Result;

use super::cch_topo::{
    MIDDLE_BYTE12_KNOWN_MASK, MIDDLE_DOWN_SHIFT, MIDDLE_WIDTH_CODE_MASK, encode_middles_to_bytes,
    mmap_weight_array, pick_middle_width, weight_array_from_bytes, weight_width_from_code,
    width_code_from_weight_width,
};
use super::cch_weights::{WeightArray, WeightWidth};
use super::crc;

const MAGIC: u32 = 0x53444944; // "DIDS" LE
const VERSION: u16 = 1;
const HEADER_LEN: usize = 24;
const FOOTER_LEN: usize = 16;

#[inline]
const fn pad_to_u64(n: usize) -> usize {
    (8 - (n & 7)) & 7
}

/// Serialise a `(up_middle, down_middle)` pair as a complete
/// `CchMiddles` section body — header + width-picked encoded bytes +
/// footer.
pub fn encode_section(up_middle: &[u32], down_middle: &[u32]) -> Vec<u8> {
    let up_width = pick_middle_width(up_middle);
    let down_width = pick_middle_width(down_middle);
    let flags: u16 = u16::from(width_code_from_weight_width(up_width))
        | (u16::from(width_code_from_weight_width(down_width)) << MIDDLE_DOWN_SHIFT);
    debug_assert_eq!(
        u8::try_from(flags).map(|b| b & !MIDDLE_BYTE12_KNOWN_MASK),
        Ok(0),
        "cch.middles flag pollution"
    );

    let up_data_bytes = up_middle.len() * up_width.bytes_per_entry();
    let up_pad = pad_to_u64(up_data_bytes);
    let down_data_bytes = down_middle.len() * down_width.bytes_per_entry();
    let down_pad = pad_to_u64(down_data_bytes);
    let body_size = up_data_bytes + up_pad + down_data_bytes + down_pad;

    let mut out = Vec::with_capacity(HEADER_LEN + body_size + FOOTER_LEN);
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&flags.to_le_bytes());
    out.extend_from_slice(&(up_middle.len() as u64).to_le_bytes());
    out.extend_from_slice(&(down_middle.len() as u64).to_le_bytes());
    debug_assert_eq!(out.len(), HEADER_LEN);

    out.extend_from_slice(&encode_middles_to_bytes(up_middle, up_width));
    for _ in 0..up_pad {
        out.push(0);
    }
    out.extend_from_slice(&encode_middles_to_bytes(down_middle, down_width));
    for _ in 0..down_pad {
        out.push(0);
    }
    debug_assert_eq!(out.len(), HEADER_LEN + body_size);

    let body_slice = &out[HEADER_LEN..];
    let mut body_d = crc::Digest::new();
    body_d.update(body_slice);
    let body_crc = body_d.finalize();
    let mut file_d = crc::Digest::new();
    file_d.update(&out);
    let file_crc = file_d.finalize();
    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(&file_crc.to_le_bytes());

    out
}

/// Parsed result of [`decode_section_owned`].
#[derive(Debug)]
pub struct DecodedMiddles {
    pub up_middle: WeightArray,
    pub down_middle: WeightArray,
}

/// Parse a `CchMiddles` section from owned bytes. Verifies both CRCs
/// and returns the two arrays as [`WeightArray`] (same in-memory
/// shape used by `CchTopo::{up,down}_middle`).
pub fn decode_section_owned(bytes: &[u8]) -> Result<DecodedMiddles> {
    let (n_up, n_down, up_width, down_width, body_size) = parse_header_and_size(bytes)?;

    let up_data_bytes = n_up * up_width.bytes_per_entry();
    let up_pad = pad_to_u64(up_data_bytes);
    let down_data_bytes = n_down * down_width.bytes_per_entry();

    let body = &bytes[HEADER_LEN..HEADER_LEN + body_size];

    // body CRC
    let mut body_d = crc::Digest::new();
    body_d.update(body);
    let computed_body = body_d.finalize();
    let stored_body =
        u64::from_le_bytes(bytes[HEADER_LEN + body_size..HEADER_LEN + body_size + 8].try_into().unwrap());
    anyhow::ensure!(
        computed_body == stored_body,
        "cch.middles body CRC mismatch: computed {computed_body:#018x}, stored {stored_body:#018x}"
    );

    let mut file_d = crc::Digest::new();
    file_d.update(&bytes[..HEADER_LEN + body_size]);
    let computed_file = file_d.finalize();
    let stored_file = u64::from_le_bytes(
        bytes[HEADER_LEN + body_size + 8..HEADER_LEN + body_size + 16]
            .try_into()
            .unwrap(),
    );
    anyhow::ensure!(
        computed_file == stored_file,
        "cch.middles file CRC mismatch"
    );

    let up_middle = weight_array_from_bytes(body[..up_data_bytes].to_vec(), n_up, up_width);
    let down_start = up_data_bytes + up_pad;
    let down_middle = weight_array_from_bytes(
        body[down_start..down_start + down_data_bytes].to_vec(),
        n_down,
        down_width,
    );

    Ok(DecodedMiddles {
        up_middle,
        down_middle,
    })
}

/// MMAP-backed parse of a `CchMiddles` section. The body is large
/// enough to benefit from mmap (Belgium-class data: hundreds of MB
/// per mode), and the read pattern at unpack time is sparse — the
/// kernel can demand-page just the slices a route actually touches.
/// Combined with the section being cold (matrix workloads don't
/// touch middles at all), this maximises the RSS savings.
pub fn decode_section_from_mmap(
    mmap: Arc<memmap2::Mmap>,
    byte_offset: usize,
    byte_len: usize,
) -> Result<DecodedMiddles> {
    anyhow::ensure!(
        byte_offset.saturating_add(byte_len) <= mmap.len(),
        "cch.middles section out of bounds"
    );
    let bytes = &mmap[byte_offset..byte_offset + byte_len];
    let (n_up, n_down, up_width, down_width, body_size) = parse_header_and_size(bytes)?;

    let up_data_bytes = n_up * up_width.bytes_per_entry();
    let up_pad = pad_to_u64(up_data_bytes);

    anyhow::ensure!(
        byte_len == HEADER_LEN + body_size + FOOTER_LEN,
        "cch.middles size mismatch: got {byte_len}, expected {}",
        HEADER_LEN + body_size + FOOTER_LEN
    );

    let up_abs_off = byte_offset + HEADER_LEN;
    let down_abs_off = byte_offset + HEADER_LEN + up_data_bytes + up_pad;
    let up_middle = mmap_weight_array(&mmap, up_abs_off, n_up, up_width)?;
    let down_middle = mmap_weight_array(&mmap, down_abs_off, n_down, down_width)?;
    Ok(DecodedMiddles {
        up_middle,
        down_middle,
    })
}

fn parse_header_and_size(
    bytes: &[u8],
) -> Result<(usize, usize, WeightWidth, WeightWidth, usize)> {
    anyhow::ensure!(
        bytes.len() >= HEADER_LEN + FOOTER_LEN,
        "cch.middles section too short: {} bytes",
        bytes.len()
    );
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    anyhow::ensure!(magic == MAGIC, "cch.middles bad magic: 0x{magic:08X}");
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    anyhow::ensure!(version == VERSION, "cch.middles unsupported version: {version}");
    let flags = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
    // High 8 bits must be zero (reserved); flag byte uses the same bit
    // layout as cch.topo header byte 12 — bits 0..=3 carry the two
    // 2-bit width codes, bits 4..=15 stay reserved.
    anyhow::ensure!(
        (flags >> 8) == 0 && (flags as u8) & !MIDDLE_BYTE12_KNOWN_MASK == 0,
        "cch.middles unknown flag bits: 0x{flags:04X}"
    );
    let up_width = weight_width_from_code((flags as u8) & MIDDLE_WIDTH_CODE_MASK)?;
    let down_width =
        weight_width_from_code((flags as u8 >> MIDDLE_DOWN_SHIFT) & MIDDLE_WIDTH_CODE_MASK)?;
    let n_up = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    let n_down = u64::from_le_bytes(bytes[16..24].try_into().unwrap()) as usize;

    let up_data_bytes = n_up * up_width.bytes_per_entry();
    let up_pad = pad_to_u64(up_data_bytes);
    let down_data_bytes = n_down * down_width.bytes_per_entry();
    let down_pad = pad_to_u64(down_data_bytes);
    let body_size = up_data_bytes + up_pad + down_data_bytes + down_pad;

    Ok((n_up, n_down, up_width, down_width, body_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_u16_u24_mixed() -> Result<()> {
        let up: Vec<u32> = vec![u32::MAX, 100, u32::MAX, 200];
        let down: Vec<u32> = vec![u32::MAX, 100_000, 1_000_000, u32::MAX];
        let bytes = encode_section(&up, &down);
        let decoded = decode_section_owned(&bytes)?;
        // up max=200 → u16
        assert_eq!(decoded.up_middle.width(), WeightWidth::U16);
        // down max=1M → u24
        assert_eq!(decoded.down_middle.width(), WeightWidth::U24);
        assert_eq!(decoded.up_middle.to_vec_u32(), up);
        assert_eq!(decoded.down_middle.to_vec_u32(), down);
        Ok(())
    }

    #[test]
    fn roundtrip_u32_overflow() -> Result<()> {
        let up: Vec<u32> = vec![crate::formats::U24_SENTINEL + 1, u32::MAX];
        let down: Vec<u32> = vec![1, 2];
        let bytes = encode_section(&up, &down);
        let decoded = decode_section_owned(&bytes)?;
        assert_eq!(decoded.up_middle.width(), WeightWidth::U32);
        assert_eq!(decoded.down_middle.width(), WeightWidth::U16);
        assert_eq!(decoded.up_middle.to_vec_u32(), up);
        assert_eq!(decoded.down_middle.to_vec_u32(), down);
        Ok(())
    }

    #[test]
    fn corrupted_body_crc_rejected() {
        let bytes = encode_section(&[u32::MAX, 1, 2], &[u32::MAX, 1, 2]);
        let mut bad = bytes.clone();
        let n = bad.len();
        bad[n - 16] ^= 0xFF;
        let err = decode_section_owned(&bad).unwrap_err();
        assert!(format!("{err:#}").contains("CRC"));
    }

    #[test]
    fn corrupted_file_crc_rejected() {
        let bytes = encode_section(&[u32::MAX, 1, 2], &[u32::MAX, 1, 2]);
        let mut bad = bytes.clone();
        let n = bad.len();
        bad[n - 1] ^= 0xFF;
        let err = decode_section_owned(&bad).unwrap_err();
        assert!(format!("{err:#}").contains("CRC"));
    }

    #[test]
    fn rejects_unknown_flag_bits() {
        let mut bytes = encode_section(&[u32::MAX], &[u32::MAX]);
        bytes[6] |= 0b0001_0000; // set reserved bit
        let err = decode_section_owned(&bytes).unwrap_err();
        assert!(format!("{err:#}").contains("unknown flag"));
    }

    #[test]
    fn empty_arrays_roundtrip() -> Result<()> {
        let bytes = encode_section(&[], &[]);
        let decoded = decode_section_owned(&bytes)?;
        assert_eq!(decoded.up_middle.len(), 0);
        assert_eq!(decoded.down_middle.len(), 0);
        Ok(())
    }
}
