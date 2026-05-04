//! Byte-level tokenizer (#96 §Tagger).
//!
//! Tokenization is **literal**: each input byte becomes a token id.
//! Token ids 0..256 map to the corresponding byte; ids 256..260 are
//! reserved for special tokens (PAD, BOS, EOS, UNK).
//!
//! Why byte-level rather than BPE/wordpiece:
//!
//! - **No vocabulary file**, so the parser is robust to mixed scripts
//!   (Latin / Cyrillic / Arabic / CJK) without a per-language BPE.
//! - **Trivial tokenization** at inference: O(N) byte iteration,
//!   no merge table, no pretokenization rules to keep in sync between
//!   training and inference.
//! - **Stable for OSM-derived training data**, which is overwhelmingly
//!   Latin-script in BE/FR/NL/DE address corpora — the small parser
//!   in #96 §Tagger does not need a sophisticated tokenizer.
//!
//! UTF-8 input is encoded to bytes; multi-byte codepoints span multiple
//! tokens. The transformer learns the byte-pair structure during training.

/// Number of distinct token ids the model embeds. 256 byte values
/// plus 4 special tokens.
pub const VOCAB_SIZE: usize = 260;

/// Pad token id. Used to right-pad sequences to a uniform length.
pub const PAD: u32 = 256;

/// Beginning-of-sequence token id.
pub const BOS: u32 = 257;

/// End-of-sequence token id.
pub const EOS: u32 = 258;

/// Unknown — never emitted by [`ByteTokenizer::encode`] (every byte is
/// representable). Reserved for forward compatibility if a sub-byte
/// vocab is added.
pub const UNK: u32 = 259;

/// Marker enum for the special tokens. Useful when constructing
/// fixtures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpecialToken {
    Pad,
    Bos,
    Eos,
    Unk,
}

impl SpecialToken {
    #[must_use]
    pub fn id(self) -> u32 {
        match self {
            SpecialToken::Pad => PAD,
            SpecialToken::Bos => BOS,
            SpecialToken::Eos => EOS,
            SpecialToken::Unk => UNK,
        }
    }
}

/// Maximum input length in tokens (including BOS + EOS). Inputs
/// longer than this are truncated. Matches [`ModelConfig::max_seq_len`]
/// from [`crate::tagger::transformer`] — keep them in sync.
pub const MAX_SEQ_LEN: usize = 128;

/// Byte-level tokenizer.
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteTokenizer;

impl ByteTokenizer {
    /// Encode a string into a token sequence: `[BOS, b0, b1, ..., EOS]`,
    /// truncated to [`MAX_SEQ_LEN`].
    ///
    /// Truncation drops bytes from the **end** to preserve the BOS/EOS
    /// framing. The training corpus rarely exceeds 60 bytes for
    /// addresses, so truncation should never fire on real input.
    #[must_use]
    pub fn encode(self, text: &str) -> Vec<u32> {
        let bytes = text.as_bytes();
        let max_body = MAX_SEQ_LEN.saturating_sub(2);
        let body_len = bytes.len().min(max_body);
        let mut out = Vec::with_capacity(body_len + 2);
        out.push(BOS);
        for &b in &bytes[..body_len] {
            out.push(b as u32);
        }
        out.push(EOS);
        out
    }

    /// Encode and right-pad to `target_len`. The mask vector is `1`
    /// for real tokens, `0` for pad. The transformer's attention block
    /// masks out positions where the mask is zero.
    #[must_use]
    pub fn encode_padded(self, text: &str, target_len: usize) -> (Vec<u32>, Vec<u32>) {
        let mut ids = self.encode(text);
        let mut mask = vec![1u32; ids.len()];
        if ids.len() < target_len {
            ids.resize(target_len, PAD);
            mask.resize(target_len, 0);
        } else if ids.len() > target_len {
            ids.truncate(target_len);
            mask.truncate(target_len);
            // ensure last slot is EOS so the country head still sees a sentinel
            if let Some(last) = ids.last_mut() {
                *last = EOS;
            }
        }
        (ids, mask)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vocab_size_covers_all_specials() {
        assert!(PAD < VOCAB_SIZE as u32);
        assert!(BOS < VOCAB_SIZE as u32);
        assert!(EOS < VOCAB_SIZE as u32);
        assert!(UNK < VOCAB_SIZE as u32);
        const _: () = assert!(PAD >= 256);
    }

    #[test]
    fn encode_wraps_with_bos_eos() {
        let ids = ByteTokenizer.encode("hi");
        assert_eq!(ids.first().copied(), Some(BOS));
        assert_eq!(ids.last().copied(), Some(EOS));
        assert_eq!(ids.len(), 4); // BOS + 'h' + 'i' + EOS
        assert_eq!(ids[1], b'h' as u32);
        assert_eq!(ids[2], b'i' as u32);
    }

    #[test]
    fn encode_padded_pads_with_zero_mask() {
        let (ids, mask) = ByteTokenizer.encode_padded("hi", 8);
        assert_eq!(ids.len(), 8);
        assert_eq!(mask.len(), 8);
        assert_eq!(&mask[..4], &[1, 1, 1, 1]);
        assert_eq!(&mask[4..], &[0, 0, 0, 0]);
        assert_eq!(ids[4], PAD);
    }

    #[test]
    fn encode_truncates_oversized_input() {
        let long = "x".repeat(MAX_SEQ_LEN * 2);
        let ids = ByteTokenizer.encode(&long);
        assert_eq!(ids.len(), MAX_SEQ_LEN);
        assert_eq!(ids[0], BOS);
        assert_eq!(*ids.last().unwrap(), EOS);
    }

    #[test]
    fn utf8_multibyte_codepoint_spans_multiple_tokens() {
        // "é" is two UTF-8 bytes: 0xC3 0xA9
        let ids = ByteTokenizer.encode("é");
        assert_eq!(ids.len(), 4); // BOS + 0xC3 + 0xA9 + EOS
        assert_eq!(ids[1], 0xC3);
        assert_eq!(ids[2], 0xA9);
    }
}
