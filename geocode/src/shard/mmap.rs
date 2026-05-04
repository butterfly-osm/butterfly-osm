//! Read-only mmap helper for the BFGS shard.
//!
//! ## Workspace `unsafe_code` carveout
//!
//! `memmap2::Mmap::map` is `unsafe fn` — Rust cannot prove the file's
//! bytes won't change underneath the slice we hand out. The workspace's
//! `unsafe_code` lint is `deny` everywhere except this single call site,
//! mirroring the same pattern as `route/src/formats/mmap.rs`.
//!
//! ## Safety contract
//!
//! - The shard is opened **read-only** and treated as immutable for the
//!   lifetime of the server process. Operators rebuild by writing a new
//!   file at a new path; never mutate the live file in place.
//! - The returned [`Arc<Mmap>`] is the sole owner of the mapping. Every
//!   `&[u8]` we hand out via the [`Shard`](super::reader::Shard) borrows
//!   from this `Arc`; the mapping outlives every borrow.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use memmap2::Mmap;

/// Memory-map `path` read-only.
///
/// SAFETY: see module-level SAFETY block. The mapping outlives every
/// slice handed out by accessors, the file is treated as immutable, and
/// this is the workspace's only `unsafe_code` site for this crate.
#[allow(unsafe_code)]
pub fn map_readonly(path: &Path) -> Result<Arc<Mmap>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening {} for mmap", path.display()))?;
    // SAFETY: the file is opened above with `File::open` (read-only on
    // POSIX). We treat the bytes as immutable for the lifetime of this
    // process — operators must publish a new path to roll a new build,
    // never mutate this file in place.
    let mmap =
        unsafe { Mmap::map(&file) }.with_context(|| format!("mmapping {}", path.display()))?;
    Ok(Arc::new(mmap))
}
