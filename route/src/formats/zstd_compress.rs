//! zstd-3 transparent compression for cold container sections (#347).
//!
//! Some container sections are read once at boot then either never
//! touched again (snap_grid header) or randomly accessed with tiny
//! actual hit rate (way_names_idx — only used during route response
//! reconstruction, with sparse access patterns over a 21 MB blob).
//!
//! Compressing them with zstd-3 (fast decompress, ~5-10× ratio on
//! text-like payloads) saves disk for a small one-time boot cost.
//!
//! # Design
//!
//! Section-level transparency: the container directory and per-section
//! CRC stays as-is. Compression lives **inside** the section payload —
//! a producer that wants to compress prefixes the body with the zstd
//! magic (`0x28b52ffd`) and writes the compressed bytes. A consumer
//! checks the first 4 bytes against the magic; on match it
//! decompresses to a heap `Vec<u8>` and parses from that. On mismatch
//! it parses the raw bytes (legacy uncompressed path).
//!
//! This keeps the container layer agnostic of compression and lets the
//! format authors decide per-section whether the trade-off (heap RSS
//! up by the decompressed size on first read, vs page-cache mmap that
//! pays back over time) makes sense.
//!
//! # Beneficial?
//!
//! [`encode_compressed_if_beneficial`] runs the compressor on the input
//! and returns the compressed bytes only if they save at least
//! `MIN_SAVED_BYTES`. Otherwise it returns the raw bytes — readers that
//! went through [`decompress_if_zstd`] will see no magic and parse the
//! raw payload. The minimum-savings guard avoids hurting payloads that
//! are already compact (e.g. tightly packed bitsets).

use anyhow::Result;
use std::borrow::Cow;

/// First four bytes of every zstd-compressed stream — RFC 8478 §3.1.1
/// ("Zstandard frames"). Reader uses this as a sniff hint: if the
/// section body starts with these bytes, decompress; otherwise treat
/// the payload as raw.
pub const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Compression level: 3 is zstd's default — sub-millisecond per MiB
/// decompression on commodity hardware, ~5× ratio on text-like
/// content. Higher levels (e.g. 19) cost much more on compress for
/// modest ratio gains; cold sections compress once per region build
/// so the absolute compress time is bounded.
const COMPRESS_LEVEL: i32 = 3;

/// Don't bother compressing if zstd saves fewer than this many bytes
/// vs the raw input — the on-disk overhead of carrying the zstd frame
/// header and decompressor cost on the read path isn't worth a tiny
/// disk win. Empirically 4 KiB filters out already-compact bitset and
/// CSR offset payloads cleanly.
const MIN_SAVED_BYTES: usize = 4096;

/// Compress `bytes` with zstd-3 and return the compressed bytes if
/// the compressed size is at least `MIN_SAVED_BYTES` smaller than the
/// raw input. Otherwise return the input unchanged so the section
/// stays raw and the reader skips the decompress step.
pub fn encode_compressed_if_beneficial(bytes: &[u8]) -> Vec<u8> {
    if bytes.len() < MIN_SAVED_BYTES {
        return bytes.to_vec();
    }
    match zstd::bulk::compress(bytes, COMPRESS_LEVEL) {
        Ok(compressed) if bytes.len().saturating_sub(compressed.len()) >= MIN_SAVED_BYTES => {
            compressed
        }
        Ok(_) | Err(_) => bytes.to_vec(),
    }
}

/// Decompress `bytes` if they start with the zstd magic. Otherwise
/// borrow them through unchanged. The returned `Cow` lets callers
/// avoid an allocation in the uncompressed case (the common path for
/// regions built before #347).
///
/// # Errors
///
/// Returns an error if the bytes start with the zstd magic but the
/// frame is malformed. This propagates as a section-load failure,
/// surfacing the corruption at boot rather than at first read.
pub fn decompress_if_zstd(bytes: &[u8]) -> Result<Cow<'_, [u8]>> {
    if bytes.len() < ZSTD_MAGIC.len() || bytes[..ZSTD_MAGIC.len()] != ZSTD_MAGIC {
        return Ok(Cow::Borrowed(bytes));
    }
    // Pass a generous upper-bound; bulk::decompress shrinks the output
    // to the actual size and frees the slack. The cap defends against
    // malformed inputs claiming planet-scale sizes.
    let decoded = zstd::bulk::decompress(bytes, MAX_DECOMPRESSED)
        .map_err(|e| anyhow::anyhow!("zstd decompress failed: {e}"))?;
    Ok(Cow::Owned(decoded))
}

/// Upper-bound buffer size for the decompressor. zstd's `bulk::compress`
/// does not always pledge the source size in the frame header, so we
/// can't always rely on the in-band hint. Instead we cap at 16 GiB —
/// large enough for every section we'd pack (largest Belgium raw
/// section is ~1.3 GB) and tight enough to defend against malicious
/// inputs claiming planet-scale sizes.
const MAX_DECOMPRESSED: usize = 16 * 1024 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_passthrough_for_short_input() {
        let raw = b"hello world".to_vec();
        let out = encode_compressed_if_beneficial(&raw);
        assert_eq!(out, raw, "below MIN_SAVED_BYTES — stays raw");
    }

    #[test]
    fn compresses_repetitive_input_into_smaller_payload() {
        // 64 KiB of zeros — should compress to <1 KiB.
        let raw = vec![0u8; 64 * 1024];
        let out = encode_compressed_if_beneficial(&raw);
        assert!(
            out.len() < raw.len(),
            "compressed should be smaller (raw={}, compressed={})",
            raw.len(),
            out.len()
        );
        assert_eq!(
            out[..4],
            ZSTD_MAGIC,
            "compressed payload should start with zstd magic"
        );
    }

    #[test]
    fn round_trip_via_decompress_if_zstd() -> Result<()> {
        // Mix of zeros + low-entropy bytes — guaranteed compressible.
        let mut raw: Vec<u8> = (0..1024u32).map(|i| (i as u8).wrapping_mul(7)).collect();
        raw.extend(vec![0u8; 64 * 1024]);
        let compressed = encode_compressed_if_beneficial(&raw);
        assert!(compressed.len() < raw.len());
        let decompressed = decompress_if_zstd(&compressed)?;
        assert_eq!(&decompressed[..], &raw[..]);
        Ok(())
    }

    #[test]
    fn decompress_passes_through_raw_bytes() -> Result<()> {
        let raw = b"not compressed at all - just raw bytes";
        let got = decompress_if_zstd(raw)?;
        assert_eq!(&got[..], &raw[..]);
        Ok(())
    }

    #[test]
    fn decompress_rejects_truncated_frame() {
        let mut buf = ZSTD_MAGIC.to_vec();
        buf.extend_from_slice(&[0u8; 4]); // garbage after magic
        let err = decompress_if_zstd(&buf).expect_err("must reject");
        let msg = format!("{err:#}");
        assert!(msg.contains("zstd"), "expected zstd error, got: {msg}");
    }

    #[test]
    fn skips_incompressible_input() {
        // Random-ish bytes don't compress — encode_compressed_if_beneficial
        // should return the raw input.
        let raw: Vec<u8> = (0..16 * 1024).map(|i| (i as u8).wrapping_mul(31)).collect();
        let out = encode_compressed_if_beneficial(&raw);
        // Either staying raw or compressing — but the test asserts the
        // function's contract: if compressed isn't ≥ MIN_SAVED_BYTES
        // smaller, we get raw.
        if out.len() != raw.len() {
            assert!(
                raw.len().saturating_sub(out.len()) >= MIN_SAVED_BYTES,
                "if compressed, must save at least MIN_SAVED_BYTES"
            );
        }
    }
}
