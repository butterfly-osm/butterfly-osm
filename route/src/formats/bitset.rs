//! Bit-packed boolean field with zero-copy borrow support (#147).
//!
//! The CCH topology stores `is_shortcut` flags as a packed bitset on disk
//! (one bit per edge). Before #147 the format reader unpacked them into a
//! `Vec<bool>` (one byte per edge), wasting ~7× the memory. With #147 the
//! reader can wrap the on-disk `&[u64]` slice directly, paying zero
//! resident bytes beyond what the kernel demand-pages.
//!
//! `BitsetField` is the field type — it can either own a packed `Vec<u64>`
//! (legacy path / writers / construction sites) or borrow a slice from
//! mmap. Both expose the same `bit(i)` API.
//!
//! # `ArcCow` migration (#296)
//!
//! The borrowed variant previously took `&'static [u64]`, which forced
//! callers to leak the `Arc<Mmap>` so the slice could outlive any
//! `ServerState`. That made unloading containers impossible. The field
//! now uses [`ArcCow<u64>`], which carries an `Arc<Mmap>` clone in the
//! borrowed case so dropping the `BitsetField` (and the `ServerState`
//! that owns it) decrements the Arc's strong count and lets the mapping
//! get unmapped once the last clone drops.

use anyhow::Result;
use std::sync::Arc;

use super::mmap::ArcCow;

/// A packed boolean field. Bit `i` is `(words[i / 64] >> (i % 64)) & 1`.
///
/// `len` is the *logical* boolean count (the trailing bits in the last
/// word that lie past `len` are not part of the content).
#[derive(Debug, Clone)]
pub struct BitsetField {
    /// Backing storage: `ceil(len / 64)` u64 words, little-endian on disk.
    /// Owned when constructed in memory or read from a plain file;
    /// Arc-backed mmap view when read from a container section. See
    /// [`ArcCow`] for the eviction story (#296).
    words: ArcCow<u64>,
    /// Logical bool count.
    len: usize,
}

impl BitsetField {
    /// Build from an owned packed `Vec<u64>`. `len` must be ≤ `words.len() * 64`.
    pub fn from_owned_words(words: Vec<u64>, len: usize) -> Self {
        debug_assert!(len <= words.len() * 64);
        Self {
            words: ArcCow::from_vec(words),
            len,
        }
    }

    /// Build from a borrowed `&'static [u64]` slice (legacy zero-copy
    /// path used by test fixtures that leak a `Box<[u64]>`). Production
    /// loaders should use [`Self::from_mmap_unverified`], which keeps
    /// the `Arc<Mmap>` strong-count tied to the returned struct so the
    /// mapping can be dropped on eviction (#296).
    ///
    /// Note: the bytes are copied into a `Vec<u64>` to avoid carrying a
    /// `'static` lifetime through `ArcCow`. Test fixtures use this path
    /// exclusively; production goes through [`Self::from_mmap_unverified`].
    pub fn from_borrowed_words(words: &'static [u64], len: usize) -> Self {
        debug_assert!(len <= words.len() * 64);
        Self {
            words: ArcCow::from_vec(words.to_vec()),
            len,
        }
    }

    /// Production mmap-backed constructor (#296). Validates that
    /// `mmap[byte_offset..byte_offset + n_words * 8]` is in bounds and
    /// 8-byte aligned for `u64`, then wraps it as an `ArcCow::Mmap`.
    /// Holds an `Arc<Mmap>` clone for the returned struct's lifetime —
    /// dropping the struct decrements the Arc's strong count.
    ///
    /// `len` is the logical boolean count and must be ≤ `n_words * 64`.
    ///
    /// CRC walking is the caller's responsibility (typically driven
    /// through the lazy CRC layer before this call). Hence the
    /// `_unverified` suffix.
    pub fn read_from_mmap_unverified(
        mmap: Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
        len: usize,
    ) -> Result<Self> {
        anyhow::ensure!(
            byte_len.is_multiple_of(8),
            "BitsetField byte_len must be a multiple of 8 (u64 words), got {byte_len}",
        );
        let n_words = byte_len / 8;
        anyhow::ensure!(
            len <= n_words * 64,
            "BitsetField logical len {len} exceeds capacity n_words*64 = {}",
            n_words * 64
        );
        let words = ArcCow::<u64>::from_mmap(mmap, byte_offset, n_words)?;
        Ok(Self { words, len })
    }

    /// Build from a `Vec<bool>` (legacy in-memory construction). Packs
    /// into a fresh `Vec<u64>`; not zero-copy.
    pub fn from_bools(bools: &[bool]) -> Self {
        let n_words = bools.len().div_ceil(64);
        let mut out = vec![0u64; n_words];
        for (i, &b) in bools.iter().enumerate() {
            if b {
                out[i / 64] |= 1u64 << (i % 64);
            }
        }
        Self::from_owned_words(out, bools.len())
    }

    /// Bit at position `i`. Panics on out-of-bounds (mirrors `Vec<bool>::[i]`).
    #[inline]
    pub fn bit(&self, i: usize) -> bool {
        assert!(
            i < self.len,
            "BitsetField index out of bounds: {} >= {}",
            i,
            self.len
        );
        let w = self.words[i / 64];
        (w >> (i % 64)) & 1 == 1
    }

    /// Logical bool count.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Iterate over bits as `bool`. Used by validation paths that need
    /// to count `false` entries; not on the hot query path.
    pub fn iter_bits(&self) -> impl Iterator<Item = bool> + '_ {
        (0..self.len).map(|i| self.bit(i))
    }

    /// Borrow the underlying packed words (for serialisation).
    pub fn as_words(&self) -> &[u64] {
        &self.words
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_owned() {
        let pattern: Vec<bool> = (0..130).map(|i| i % 3 == 0).collect();
        let bs = BitsetField::from_bools(&pattern);
        assert_eq!(bs.len(), 130);
        for (i, &b) in pattern.iter().enumerate() {
            assert_eq!(bs.bit(i), b);
        }
    }

    #[test]
    fn round_trip_borrowed() {
        // Build a packed bitset, leak it for a 'static borrow (mimics mmap).
        let pattern: Vec<bool> = (0..200).map(|i| (i * 7) % 5 == 0).collect();
        let owned = BitsetField::from_bools(&pattern);
        let leaked: &'static [u64] = Box::leak(owned.as_words().to_vec().into_boxed_slice());
        let view = BitsetField::from_borrowed_words(leaked, pattern.len());
        for (i, &b) in pattern.iter().enumerate() {
            assert_eq!(view.bit(i), b);
        }
    }

    #[test]
    #[should_panic]
    fn oob_panics() {
        let bs = BitsetField::from_bools(&[true, false, true]);
        let _ = bs.bit(3);
    }

    #[test]
    fn round_trip_from_mmap_unverified() -> Result<()> {
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Build a known bit pattern and dump packed words into a file.
        let pattern: Vec<bool> = (0..256).map(|i| (i * 11) % 7 == 0).collect();
        let owned = BitsetField::from_bools(&pattern);
        let words = owned.as_words().to_vec();

        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(bytemuck::cast_slice(&words))?;
        tmp.flush()?;

        let mmap = super::super::mmap::map_readonly(tmp.path())?;
        let byte_len = words.len() * 8;
        let bs =
            BitsetField::read_from_mmap_unverified(Arc::clone(&mmap), 0, byte_len, pattern.len())?;
        for (i, &b) in pattern.iter().enumerate() {
            assert_eq!(bs.bit(i), b, "bit {i}");
        }
        Ok(())
    }

    #[test]
    fn from_mmap_unverified_rejects_odd_byte_len() -> Result<()> {
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(&[0u8; 16])?;
        tmp.flush()?;
        let mmap = super::super::mmap::map_readonly(tmp.path())?;
        // byte_len = 7 is not a multiple of 8 → reject.
        assert!(BitsetField::read_from_mmap_unverified(Arc::clone(&mmap), 0, 7, 1).is_err());
        // len exceeds capacity (16 bytes = 2 words = 128 bits, asking for 129).
        assert!(BitsetField::read_from_mmap_unverified(Arc::clone(&mmap), 0, 16, 129).is_err());
        Ok(())
    }
}
