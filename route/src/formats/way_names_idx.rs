//! `shared/way_names_idx` — compact mmap-able lookup table for OSM way
//! names (#282).
//!
//! Replaces the boot-time `HashMap<i64, String>` that previously held
//! every named way in RAM (~30-50 MB on Belgium's 754 K names, ~3-5 GiB
//! at planet scale). The on-disk index uses sorted `[i64]` keys +
//! `[u32]` offsets + a packed UTF-8 blob. Lookup is binary search over
//! the key array plus one slice into the blob.
//!
//! # Why sorted-array + binary search, not boomphf
//!
//! The issue spec suggested `boomphf` for true O(1) PHF lookup. We
//! chose sorted-array + binary search because:
//!
//! - **mmap-friendly**: the keys, offsets, and blob are all
//!   `bytemuck::Pod` arrays read straight from the container mapping.
//!   `boomphf::Mphf` is `Vec`-backed and needs a one-shot serde
//!   deserialisation into the heap on boot.
//! - **No dependency**: avoids pulling in `boomphf` (+ `serde`) for a
//!   single boot-time data structure.
//! - **Performance is adequate**: at 754 K keys, binary search is
//!   ~20 i64 comparisons (~1 µs worst-case). Road-name lookup is on
//!   the turn-by-turn path, called a handful of times per route, so
//!   the O(log n) vs O(1) gap is not user-visible.
//!
//! If a future profiling pass shows road-name lookup as a real
//! bottleneck — unlikely at any realistic scale — a `boomphf`-style
//! PHF can be slotted in behind the same `WayNamesIdx::get` API.
//!
//! # On-disk layout
//!
//! ```text
//!   [u8;4]   MAGIC ("WHNI")
//!   u16      VERSION (1)
//!   u16      _pad
//!   u32      n_entries
//!   u32      names_blob_len
//!   [u8;20]  _pad (header pads to 40 B for i64/u32 alignment)
//!   body:
//!     [i64; n_entries]       sorted ascending — OSM way ids
//!     [u32; n_entries + 1]   offsets into names blob (offsets[n] = blob_len)
//!     [u8;  names_blob_len]  concatenated UTF-8 names
//!   [u64;2]  footer: body_crc || file_crc
//! ```
//!
//! All multi-byte integers are little-endian. Strings are *not*
//! null-terminated; their bounds come from the offset array.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use super::crc::Digest;
use super::mmap::ArcCow;

pub const WAY_NAMES_MAGIC: u32 = 0x5748_4E49; // "WHNI"
pub const WAY_NAMES_VERSION: u16 = 1;
pub const HEADER_SIZE: usize = 40;
pub const FOOTER_SIZE: usize = 16;

/// Parsed `shared/way_names_idx` section.
#[derive(Debug, Clone)]
pub struct WayNamesIdx {
    pub n_entries: u32,
    /// Sorted ascending OSM way ids. Binary-searchable.
    pub way_ids: ArcCow<i64>,
    /// `offsets[i]` = byte start of name `i` in `names`.
    /// `offsets[n_entries]` = `names.len()` (sentinel).
    pub offsets: ArcCow<u32>,
    /// Packed UTF-8 name bytes. Slice via `offsets[i]..offsets[i+1]`.
    pub names: ArcCow<u8>,
}

impl WayNamesIdx {
    /// Look up a way name by OSM way id. Returns `None` if the id is
    /// not present.
    ///
    /// O(log n) binary search over `way_ids`, then one slice + UTF-8
    /// view into `names`. Returns `None` on UTF-8 corruption (which
    /// should never happen — the writer enforces valid UTF-8).
    #[inline]
    pub fn get(&self, way_id: i64) -> Option<&str> {
        let ids = self.way_ids.as_slice();
        let idx = ids.binary_search(&way_id).ok()?;
        let off = self.offsets.as_slice();
        let start = *off.get(idx)? as usize;
        let end = *off.get(idx + 1)? as usize;
        std::str::from_utf8(self.names.as_slice().get(start..end)?).ok()
    }

    /// Number of named ways indexed.
    #[inline]
    pub fn len(&self) -> usize {
        self.n_entries as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n_entries == 0
    }
}

/// Build a `WayNamesIdx` from an iterator over `(way_id, name)` pairs.
/// The pairs may arrive in any order; the writer sorts by way_id before
/// emitting. Duplicate way_ids are de-duplicated — the LAST seen name
/// wins (matches the previous `HashMap::insert` behaviour).
///
/// Returns the materialised index ready to be written via `write_to`.
pub fn build_from_pairs(pairs: impl IntoIterator<Item = (i64, String)>) -> Result<WayNamesIdx> {
    let mut entries: Vec<(i64, String)> = pairs.into_iter().collect();
    // Sort + dedup (keep last) — same semantics as repeated HashMap::insert.
    entries.sort_by_key(|(id, _)| *id);
    // PR #324 review: `Vec::dedup_by` walks left-to-right but passes
    // the closure args in **opposite order from the slice** (see std
    // docs: "if same_bucket(a, b) returns true, a is removed"). So
    // when our closure runs on adjacent items, `a` is the LATER one
    // in the slice and `b` is the EARLIER one. Returning true removes
    // `a` (the later) and keeps `b` (the earlier).
    //
    // For last-write-wins on `(way_id, name)`, we swap `a.1` into
    // `b.1` BEFORE returning true. That leaves the LATER entry's name
    // in the slot that survives, achieving the same semantic as
    // `for (id, name) in pairs { map.insert(id, name); }`.
    entries.dedup_by(|a, b| {
        if a.0 == b.0 {
            std::mem::swap(&mut a.1, &mut b.1);
            true
        } else {
            false
        }
    });

    let n_entries = entries.len() as u32;
    let mut way_ids: Vec<i64> = Vec::with_capacity(entries.len());
    let mut offsets: Vec<u32> = Vec::with_capacity(entries.len() + 1);
    let mut names: Vec<u8> = Vec::with_capacity(entries.len() * 24); // rough avg

    for (id, name) in &entries {
        way_ids.push(*id);
        let off: u32 = names.len().try_into().map_err(|_| {
            anyhow::anyhow!(
                "way_names blob exceeds u32 offset capacity ({} bytes)",
                names.len()
            )
        })?;
        offsets.push(off);
        names.extend_from_slice(name.as_bytes());
    }
    // Sentinel offset = total blob length.
    let total: u32 = names.len().try_into().map_err(|_| {
        anyhow::anyhow!(
            "way_names blob exceeds u32 offset capacity ({} bytes)",
            names.len()
        )
    })?;
    offsets.push(total);

    Ok(WayNamesIdx {
        n_entries,
        way_ids: ArcCow::Owned(way_ids),
        offsets: ArcCow::Owned(offsets),
        names: ArcCow::Owned(names),
    })
}

/// Serialise a `WayNamesIdx` into an owned `Vec<u8>` in the canonical
/// on-disk format. Shared by `write_to` (file path) and
/// `pack_way_names_idx` (appended directly to the container writer).
pub fn serialise_to_bytes(idx: &WayNamesIdx) -> Result<Vec<u8>> {
    let names_blob_len: u32 = idx
        .names
        .as_slice()
        .len()
        .try_into()
        .map_err(|_| anyhow::anyhow!("names blob length overflows u32"))?;

    let body_len = (idx.way_ids.as_slice().len() * 8)
        + (idx.offsets.as_slice().len() * 4)
        + idx.names.as_slice().len();
    let mut buf = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

    // Header.
    buf.extend_from_slice(&WAY_NAMES_MAGIC.to_le_bytes());
    buf.extend_from_slice(&WAY_NAMES_VERSION.to_le_bytes());
    buf.extend_from_slice(&[0u8; 2]); // _pad
    buf.extend_from_slice(&idx.n_entries.to_le_bytes());
    buf.extend_from_slice(&names_blob_len.to_le_bytes());
    buf.resize(HEADER_SIZE, 0);

    // Body.
    let body_start = HEADER_SIZE;
    for id in idx.way_ids.as_slice() {
        buf.extend_from_slice(&id.to_le_bytes());
    }
    for off in idx.offsets.as_slice() {
        buf.extend_from_slice(&off.to_le_bytes());
    }
    buf.extend_from_slice(idx.names.as_slice());
    let body_end = buf.len();

    // CRCs.
    let mut body_digest = Digest::new();
    body_digest.update(&buf[body_start..body_end]);
    let body_crc = body_digest.finalize();

    let mut file_digest = Digest::new();
    file_digest.update(&buf[..body_end]);
    let file_crc = file_digest.finalize();

    buf.extend_from_slice(&body_crc.to_le_bytes());
    buf.extend_from_slice(&file_crc.to_le_bytes());
    Ok(buf)
}

/// Write a `WayNamesIdx` to disk in the canonical format.
pub fn write_to<P: AsRef<Path>>(path: P, idx: &WayNamesIdx) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("creating {}", path.as_ref().display()))?;
    let mut w = BufWriter::new(file);
    let bytes = serialise_to_bytes(idx)?;
    w.write_all(&bytes)?;
    w.flush()?;
    Ok(())
}

/// Read a `WayNamesIdx` from a verified mmap byte slice (zero-copy).
/// Caller is responsible for prior CRC verification (e.g. via
/// `LazyContainer::verify_now`). The returned views borrow from the
/// supplied `Arc<Mmap>` — `Cow::Borrowed`-equivalent.
pub fn read_from_mmap_unverified(
    mmap: Arc<memmap2::Mmap>,
    byte_offset: usize,
    byte_len: usize,
) -> Result<WayNamesIdx> {
    let total_len = byte_offset
        .checked_add(byte_len)
        .ok_or_else(|| anyhow::anyhow!("way_names section offset+len overflows usize"))?;
    anyhow::ensure!(
        total_len <= mmap.len(),
        "way_names section [{}, {}) exceeds mmap len {}",
        byte_offset,
        total_len,
        mmap.len()
    );
    let section_bytes = &mmap[byte_offset..total_len];

    // #347: if the section body starts with the zstd magic, decompress
    // to an owned Vec<u8> and parse from that. The returned `ArcCow`
    // views are `Owned` rather than `Mmap` — they consume ~25 MB of
    // heap (vs ~120 MB compressed in the mmap's page cache) but we
    // accept the trade because way_names_idx is cold and sparse-access
    // anyway.
    let owned_decompressed = crate::formats::zstd_compress::decompress_if_zstd(section_bytes)?;
    let is_compressed = matches!(owned_decompressed, std::borrow::Cow::Owned(_));
    let bytes: &[u8] = &owned_decompressed;
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "way_names section too short: {} bytes",
        bytes.len()
    );

    // Parse header.
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    anyhow::ensure!(
        magic == WAY_NAMES_MAGIC,
        "way_names bad magic: expected 0x{:08X}, got 0x{:08X}",
        WAY_NAMES_MAGIC,
        magic
    );
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    anyhow::ensure!(
        version == WAY_NAMES_VERSION,
        "way_names unsupported version {} (expected {})",
        version,
        WAY_NAMES_VERSION
    );
    let n_entries = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let names_blob_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap());

    // Compute body extents with checked arithmetic.
    let way_ids_bytes = (n_entries as usize)
        .checked_mul(8)
        .ok_or_else(|| anyhow::anyhow!("way_ids length overflows usize"))?;
    let offsets_bytes = (n_entries as usize + 1)
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("offsets length overflows usize"))?;
    let names_bytes = names_blob_len as usize;
    let body_len = way_ids_bytes
        .checked_add(offsets_bytes)
        .and_then(|x| x.checked_add(names_bytes))
        .ok_or_else(|| anyhow::anyhow!("way_names body length overflows usize"))?;
    let expected_total = HEADER_SIZE
        .checked_add(body_len)
        .and_then(|x| x.checked_add(FOOTER_SIZE))
        .ok_or_else(|| anyhow::anyhow!("way_names total length overflows usize"))?;
    anyhow::ensure!(
        bytes.len() == expected_total,
        "way_names size mismatch: got {}, expected {}",
        bytes.len(),
        expected_total
    );

    // Build views. Zero-copy mmap path for the uncompressed case;
    // owned Vec path for the zstd-decompressed case (#347).
    let way_ids_local_off = HEADER_SIZE;
    let offsets_local_off = way_ids_local_off + way_ids_bytes;
    let names_local_off = offsets_local_off + offsets_bytes;

    let (way_ids, offsets, names) = if is_compressed {
        // Decompressed bytes — copy out each subarray as owned Vec.
        let way_ids_slice: &[i64] =
            bytemuck::cast_slice(&bytes[way_ids_local_off..way_ids_local_off + way_ids_bytes]);
        let offsets_slice: &[u32] =
            bytemuck::cast_slice(&bytes[offsets_local_off..offsets_local_off + offsets_bytes]);
        let names_slice = &bytes[names_local_off..names_local_off + names_bytes];
        (
            ArcCow::from_vec(way_ids_slice.to_vec()),
            ArcCow::from_vec(offsets_slice.to_vec()),
            ArcCow::from_vec(names_slice.to_vec()),
        )
    } else {
        // Mmap-backed zero-copy.
        let way_ids_off = byte_offset + way_ids_local_off;
        let offsets_off = byte_offset + offsets_local_off;
        let names_off = byte_offset + names_local_off;
        let way_ids = ArcCow::from_mmap(Arc::clone(&mmap), way_ids_off, n_entries as usize)?;
        let offsets = ArcCow::from_mmap(Arc::clone(&mmap), offsets_off, n_entries as usize + 1)?;
        let names = ArcCow::from_mmap(Arc::clone(&mmap), names_off, names_blob_len as usize)?;
        (way_ids, offsets, names)
    };

    Ok(WayNamesIdx {
        n_entries,
        way_ids,
        offsets,
        names,
    })
}

/// Read a `WayNamesIdx` from a file, copying into owned `Vec`s (legacy
/// file-tree path). Used by callers that have a file path rather than a
/// container mmap.
pub fn read_owned<P: AsRef<Path>>(path: P) -> Result<WayNamesIdx> {
    let mut file = File::open(path.as_ref())
        .with_context(|| format!("opening {}", path.as_ref().display()))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    anyhow::ensure!(
        buf.len() >= HEADER_SIZE + FOOTER_SIZE,
        "way_names file too short: {} bytes",
        buf.len()
    );

    let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    anyhow::ensure!(magic == WAY_NAMES_MAGIC, "way_names bad magic");
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    anyhow::ensure!(version == WAY_NAMES_VERSION, "way_names bad version");
    let n_entries = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let names_blob_len = u32::from_le_bytes(buf[12..16].try_into().unwrap()) as usize;

    let way_ids_off = HEADER_SIZE;
    let way_ids_end = way_ids_off + n_entries * 8;
    let offsets_off = way_ids_end;
    let offsets_end = offsets_off + (n_entries + 1) * 4;
    let names_off = offsets_end;
    let names_end = names_off + names_blob_len;
    let footer_off = names_end;
    anyhow::ensure!(
        footer_off + FOOTER_SIZE == buf.len(),
        "way_names size mismatch: got {}, expected {}",
        buf.len(),
        footer_off + FOOTER_SIZE,
    );

    // CRC verification.
    let mut body_digest = Digest::new();
    body_digest.update(&buf[HEADER_SIZE..footer_off]);
    let body_crc = body_digest.finalize();
    let stored_body_crc = u64::from_le_bytes(buf[footer_off..footer_off + 8].try_into().unwrap());
    anyhow::ensure!(
        body_crc == stored_body_crc,
        "way_names body CRC mismatch: got 0x{:016X}, expected 0x{:016X}",
        body_crc,
        stored_body_crc
    );
    let mut file_digest = Digest::new();
    file_digest.update(&buf[..footer_off]);
    let file_crc = file_digest.finalize();
    let stored_file_crc =
        u64::from_le_bytes(buf[footer_off + 8..footer_off + 16].try_into().unwrap());
    anyhow::ensure!(
        file_crc == stored_file_crc,
        "way_names file CRC mismatch: got 0x{:016X}, expected 0x{:016X}",
        file_crc,
        stored_file_crc
    );

    // Decode into owned Vecs.
    let way_ids: Vec<i64> = buf[way_ids_off..way_ids_end]
        .chunks_exact(8)
        .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let offsets: Vec<u32> = buf[offsets_off..offsets_end]
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
        .collect();
    let names: Vec<u8> = buf[names_off..names_end].to_vec();

    Ok(WayNamesIdx {
        n_entries: n_entries as u32,
        way_ids: ArcCow::Owned(way_ids),
        offsets: ArcCow::Owned(offsets),
        names: ArcCow::Owned(names),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_lookup_roundtrip() {
        let pairs = vec![
            (1_000_i64, "Main Street".to_string()),
            (2_500_i64, "Park Ave".to_string()),
            (42_i64, "First Street".to_string()),
            (1_000_i64, "Main Street (renamed)".to_string()),
        ];
        let idx = build_from_pairs(pairs).unwrap();
        assert_eq!(idx.n_entries, 3, "duplicate key should be deduped");
        assert_eq!(idx.get(42), Some("First Street"));
        // Last-write-wins on duplicate.
        assert_eq!(idx.get(1_000), Some("Main Street (renamed)"));
        assert_eq!(idx.get(2_500), Some("Park Ave"));
        assert_eq!(idx.get(99_999), None);
    }

    #[test]
    fn empty_idx() {
        let idx = build_from_pairs(Vec::<(i64, String)>::new()).unwrap();
        assert_eq!(idx.n_entries, 0);
        assert!(idx.is_empty());
        assert_eq!(idx.get(0), None);
    }

    #[test]
    fn write_read_roundtrip() {
        let pairs: Vec<(i64, String)> = (0..1000).map(|i| (i * 7, format!("street-{i}"))).collect();
        let idx = build_from_pairs(pairs).unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        write_to(tmp.path(), &idx).unwrap();
        let loaded = read_owned(tmp.path()).unwrap();
        assert_eq!(loaded.n_entries, 1000);
        for i in 0..1000 {
            assert_eq!(loaded.get(i * 7), Some(&format!("street-{i}")[..]));
        }
        assert_eq!(loaded.get(99_999_999), None);
    }

    #[test]
    fn bad_magic_rejected() {
        // Build a buffer with a wrong magic and confirm read rejects.
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        let mut header = vec![0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        tmp.write_all(&header).unwrap();
        tmp.write_all(&[0u8; FOOTER_SIZE]).unwrap();
        let r = read_owned(tmp.path());
        assert!(r.is_err());
    }
}
