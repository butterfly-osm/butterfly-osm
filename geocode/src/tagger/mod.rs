//! Byte-level transformer tagger (#96 §Tagger).
//!
//! ## What this module is
//!
//! A from-scratch byte-level transformer (positional encoding +
//! multi-head self-attention + FFN + layer norm) implemented on
//! [`candle_core`]. It exposes:
//!
//! - [`tokenizer`] — byte-level tokenization with BOS/EOS/PAD
//!   specials. No external vocab; "vocab" is literally the 256
//!   possible byte values plus 4 reserved tokens.
//! - [`transformer`] — model architecture (encoder + BIO head + country
//!   head). All weights are real `candle_nn` parameters.
//! - [`inference`] — forward pass, BIO span extraction, country
//!   posterior softmax. `candle_core::Tensor::detach()` is used to
//!   skip grad bookkeeping when inferring.
//! - [`training`] — JSONL corpus loader, Adam optimizer, weighted
//!   cross-entropy on BIO + country, eval hook. Writes safetensors.
//!
//! ## What this module is NOT
//!
//! - **Not a state-of-the-art model.** A tiny architecture
//!   (`d_model=64`, `n_layers=2`, `n_heads=4`) is shipped as a
//!   proof-of-life that the training loop converges and that
//!   inference is wired correctly end-to-end. A production model
//!   needs much more data, more parameters, and the shard-agnostic
//!   augmentation strategy from #96 — which is filed as Phase 2 of
//!   #98.
//! - **No LoRA / regional adapters.** Hooks for adapter injection
//!   are filed against #96 §Tagger but not implemented here.
//! - **No GPU.** Pure CPU `candle_core::Device::Cpu`.
//!
//! See `parser/neural.rs` for the consumer that wraps this module
//! into the `ParsedQuery` shape.

pub mod inference;
pub mod tokenizer;
pub mod training;
pub mod transformer;

pub use inference::{InferenceOutput, infer};
pub use tokenizer::{BOS, ByteTokenizer, EOS, PAD, SpecialToken, UNK, VOCAB_SIZE};
pub use transformer::{ModelConfig, NUM_BIO_LABELS, TaggerModel};
