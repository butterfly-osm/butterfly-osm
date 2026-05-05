//! Phase 2 pipeline shared library.
//!
//! Two binaries depend on this crate:
//!
//! - `phase2-corpus` — reads a BFGS shard, iterates records, emits a
//!   JSONL of `Phase2Sample`s (canonical query + N augmentations,
//!   each carrying the gold record id + lat/lon).
//! - `phase2-label` — reads the corpus, runs the heuristic parser to
//!   generate hypothesis sets, executes each hypothesis's retrieval
//!   program, computes labels, emits Phase 2 feature rows.
//!
//! Splitting into two binaries lets the corpus generation step stay
//! cheap (no shard lookups, no parser invocation) and the labeling
//! step stay focused on the executor invocation pattern. They are
//! pipelined: `phase2-corpus → JSONL → phase2-label → JSONL`.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]

pub mod augment;
pub mod sample;
