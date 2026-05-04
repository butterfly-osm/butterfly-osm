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

use std::borrow::Cow;

/// A packed boolean field. Bit `i` is `(words[i / 64] >> (i % 64)) & 1`.
///
/// `len` is the *logical* boolean count (the trailing bits in the last
/// word that lie past `len` are not part of the content).
#[derive(Debug, Clone)]
pub struct BitsetField {
    /// Backing storage: `ceil(len / 64)` u64 words, little-endian on disk.
    words: Cow<'static, [u64]>,
    /// Logical bool count.
    len: usize,
}

impl BitsetField {
    /// Build from an owned packed `Vec<u64>`. `len` must be ≤ `words.len() * 64`.
    pub fn from_owned_words(words: Vec<u64>, len: usize) -> Self {
        debug_assert!(len <= words.len() * 64);
        Self {
            words: Cow::Owned(words),
            len,
        }
    }

    /// Build from a borrowed `&'static [u64]` slice (zero-copy mmap path).
    /// `len` must be ≤ `words.len() * 64`.
    pub fn from_borrowed_words(words: &'static [u64], len: usize) -> Self {
        debug_assert!(len <= words.len() * 64);
        Self {
            words: Cow::Borrowed(words),
            len,
        }
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
}
