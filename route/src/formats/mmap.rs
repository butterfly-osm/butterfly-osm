//! Read-only memory mapping for `*.butterfly` containers (#90 phase 3).
//!
//! Maps a single multi-GB container file once at startup and hands out
//! `&[u8]` slices into the mapping. Demand-paged: the OS pages in only
//! the pages a request actually touches, so idle resident memory stays
//! tiny even for a 12 GB unified Belgium build.
//!
//! # Workspace `unsafe_code` carveout
//!
//! `memmap2::Mmap::map` is `unsafe fn` — Rust cannot prove that the
//! file's bytes won't change underneath the slice we hand out. The
//! workspace's `unsafe_code` lint is `deny` everywhere except this
//! single call site, which is `#[allow(unsafe_code)]` and carries the
//! documented SAFETY block below.
//!
//! Safety contract we satisfy:
//! - The container is opened **read-only** and treated as immutable for
//!   the lifetime of the server process. Operators rebuild by writing a
//!   new file at a new path and signalling the server, never by mutating
//!   the live file in place.
//! - The returned `Arc<Mmap>` is the sole owner of the mapping. Every
//!   `&[u8]` we hand out via accessor methods borrows from this `Arc`
//!   with matching lifetime — the mapping outlives every borrow.
//! - The `Send`/`Sync` impls of `Mmap` are sufficient for the
//!   read-only single-writer (no writer) pattern we use.

use anyhow::{Context, Result};
use memmap2::Mmap;
use std::path::Path;
use std::sync::Arc;

/// Memory-map `path` read-only.
///
/// SAFETY: see module-level SAFETY block. The mapping outlives every
/// slice handed out via [`MmapSlice`], the file is treated as immutable,
/// and the call below is the workspace's only `unsafe_code` site.
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

/// Hint the kernel that we no longer need the given byte range to be
/// resident. The call is a hint — the kernel may ignore it. On Linux,
/// `MADV_DONTNEED` causes anonymous pages to be discarded; for
/// file-backed read-only mappings it drops the page-cache reference,
/// which lets the kernel reclaim the pages from RSS without invalidating
/// the mapping. A subsequent read re-pages the bytes from disk at
/// standard page-fault cost.
///
/// Used by #149 to reclaim the `cch_weights.up`/`.down` byte ranges
/// after the routing hot path migrated off them onto the pre-built flat
/// adjacency structures. The Cow slices that point into this byte range
/// stay valid (the mmap itself is not unmapped); accessing them simply
/// triggers a soft page fault, which is fine for cold consumers
/// (validators, transit fingerprint hash, derived custom-weight builds).
///
/// SAFETY: the caller must guarantee that `range` is a sub-slice of a
/// live `Mmap` whose lifetime exceeds this call. We document this in the
/// workspace's `unsafe_code` carveout policy: `Mmap::map` and `madvise`
/// against the same range are the only two carveouts. See
/// `feedback_no_unsafe.md` and the module-level SAFETY block above.
#[allow(unsafe_code)]
pub fn madvise_dontneed(range: &[u8]) -> std::io::Result<()> {
    if range.is_empty() {
        return Ok(());
    }

    // `madvise(2)` on Linux requires the start address to be page-aligned
    // and the length to be a whole number of pages. We round the start
    // *up* to the next page boundary and the end *down*, advising only
    // the inner whole-page span. The trimmed unaligned head and tail
    // (≤ 1 page each) stay resident; for multi-GB weight sections this
    // is rounding error.
    let page_size = page_size();
    let start_addr = range.as_ptr() as usize;
    let end_addr = start_addr.saturating_add(range.len());
    let aligned_start = start_addr.div_ceil(page_size) * page_size;
    let aligned_end = (end_addr / page_size) * page_size;
    if aligned_end <= aligned_start {
        // Range smaller than one page after alignment; nothing to advise.
        return Ok(());
    }
    let aligned_len = aligned_end - aligned_start;

    // SAFETY: `range` is guaranteed by the caller to be a sub-slice of a
    // live mmap mapping (see doc comment). The aligned subrange
    // `[aligned_start, aligned_start + aligned_len)` lies entirely
    // within `range` because we rounded inward on both ends.
    // MADV_DONTNEED is a hint to the kernel; on Linux it drops the
    // page-cache reference for file-backed ranges.
    let rc = unsafe {
        libc::madvise(
            aligned_start as *mut libc::c_void,
            aligned_len,
            libc::MADV_DONTNEED,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[allow(unsafe_code)]
fn page_size() -> usize {
    // SAFETY: `sysconf(_SC_PAGESIZE)` is a thread-safe libc query with
    // no preconditions. Returns -1 on error; we fall back to 4 KiB which
    // matches every Linux/x86_64 deployment we ship to.
    let rc = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if rc <= 0 { 4096 } else { rc as usize }
}

/// Owned slice into a memory-mapped file.
///
/// Holds an `Arc<Mmap>` so the mapping stays alive as long as any
/// `MmapSlice` does. The byte range is checked once at construction; the
/// hot-path `as_bytes()` does no additional bounds checking.
#[derive(Clone)]
pub struct MmapSlice {
    mmap: Arc<Mmap>,
    start: usize,
    len: usize,
}

impl MmapSlice {
    /// Build a slice over `mmap[start..start+len]`. Returns an error
    /// when the requested range is out of bounds.
    pub fn new(mmap: Arc<Mmap>, start: usize, len: usize) -> Result<Self> {
        let end = start
            .checked_add(len)
            .with_context(|| format!("mmap slice overflow: start={start} len={len}"))?;
        anyhow::ensure!(
            end <= mmap.len(),
            "mmap slice out of bounds: start={start} len={len} mmap_len={}",
            mmap.len()
        );
        Ok(Self { mmap, start, len })
    }

    /// Return the slice as raw bytes. Constant-time, no bounds check.
    pub fn as_bytes(&self) -> &[u8] {
        &self.mmap[self.start..self.start + self.len]
    }

    /// Length of the slice in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// True iff the slice is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Reference to the owning mmap, for sharing into another slice.
    pub fn arc(&self) -> &Arc<Mmap> {
        &self.mmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn map_readonly_roundtrip() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        let payload = b"hello mmap";
        tmp.write_all(payload)?;
        tmp.flush()?;
        let mmap = map_readonly(tmp.path())?;
        assert_eq!(&mmap[..], payload);
        Ok(())
    }

    #[test]
    fn mmap_slice_in_bounds_ok() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(b"abcdefgh")?;
        tmp.flush()?;
        let mmap = map_readonly(tmp.path())?;
        let s = MmapSlice::new(Arc::clone(&mmap), 2, 4)?;
        assert_eq!(s.as_bytes(), b"cdef");
        assert_eq!(s.len(), 4);
        assert!(!s.is_empty());
        Ok(())
    }

    #[test]
    fn mmap_slice_out_of_bounds_rejected() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(b"abc")?;
        tmp.flush()?;
        let mmap = map_readonly(tmp.path())?;
        // Past EOF
        assert!(MmapSlice::new(Arc::clone(&mmap), 0, 4).is_err());
        // Overflow
        assert!(MmapSlice::new(Arc::clone(&mmap), usize::MAX, 1).is_err());
        Ok(())
    }
}
