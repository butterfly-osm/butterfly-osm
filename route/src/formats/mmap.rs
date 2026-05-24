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
///
/// On non-Linux targets the call is a no-op so the rest of the build
/// stays portable; the optimisation is Linux-specific because the
/// `MADV_DONTNEED` semantics we rely on (drop page-cache reference,
/// re-page on fault) match Linux's behaviour, not BSD/macOS's.
#[cfg(target_os = "linux")]
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

/// Stub for non-Linux targets. Returns `Ok(())` without advising.
/// Production deployment is Linux-only (see `Dockerfile`); this exists
/// so `cargo check` / IDE support work on macOS/Windows dev hosts.
#[cfg(not(target_os = "linux"))]
pub fn madvise_dontneed(_range: &[u8]) -> std::io::Result<()> {
    Ok(())
}

#[cfg(target_os = "linux")]
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

/// Mmap-backed or heap-owned slice of `T`, without the `'static`
/// lifetime fiction that `Cow<'static, [T]>` previously demanded.
///
/// **Why this exists** (#296): The pre-#296 pattern was to leak the
/// `Arc<Mmap>` once at boot so derived `&[T]` views could be typed as
/// `&'static [T]` and stored in `Cow::Borrowed`. That made
/// `ServerState` eviction useless — the leaked Arc kept the mapping
/// resident forever. `ArcCow<T>` carries an `Arc<Mmap>` clone for the
/// borrowed case, so dropping the `ArcCow` (or the `ServerState`
/// holding it) decrements the Arc's strong count. When the count
/// reaches 0, `Mmap::drop` calls `munmap` and the kernel reclaims the
/// pages.
///
/// `T: bytemuck::Pod` is the same constraint
/// [`bytemuck::cast_slice`] needs. All current
/// `Cow<'static, [T]>` sites already satisfy this — they hold POD
/// records (`u8`, `u32`, `u64`, `i32`, `EbgNode`, `PackedPoint`, ...).
#[derive(Clone)]
pub enum ArcCow<T: bytemuck::Pod> {
    /// Heap-owned. Used by writers, in-memory builders, owning
    /// readers, and unit-test fixtures.
    Owned(Vec<T>),
    /// Zero-copy view into a memory-mapped container. `byte_offset`
    /// and `n_elements` are validated at construction time; the hot
    /// path does no bounds check beyond the `cast_slice`.
    Mmap {
        mmap: Arc<Mmap>,
        byte_offset: usize,
        n_elements: usize,
    },
}

impl<T: bytemuck::Pod> ArcCow<T> {
    /// Construct the borrowed variant by validating that
    /// `mmap[byte_offset..byte_offset + n_elements*size_of::<T>()]`
    /// is in bounds and aligned for `T`. Returns the same kind of
    /// `ArcCow<T>` that the writers' `ArcCow::Owned(...)` produces,
    /// so call sites read the field identically.
    pub fn from_mmap(mmap: Arc<Mmap>, byte_offset: usize, n_elements: usize) -> Result<Self> {
        let elem = std::mem::size_of::<T>();
        let byte_len = n_elements
            .checked_mul(elem)
            .with_context(|| format!("ArcCow byte len overflow: n={n_elements} elem={elem}"))?;
        let end = byte_offset
            .checked_add(byte_len)
            .with_context(|| format!("ArcCow offset overflow: off={byte_offset} len={byte_len}"))?;
        anyhow::ensure!(
            end <= mmap.len(),
            "ArcCow out of bounds: byte_offset={byte_offset} byte_len={byte_len} mmap_len={}",
            mmap.len()
        );
        let align = std::mem::align_of::<T>();
        // Address arithmetic only — no pointer offsetting.
        // `as_ptr() as usize` is safe; we just check the would-be address.
        let ptr_addr = (mmap.as_ptr() as usize).wrapping_add(byte_offset);
        anyhow::ensure!(
            ptr_addr % align == 0,
            "ArcCow misaligned for {}: ptr={ptr_addr:#x} align={align}",
            std::any::type_name::<T>()
        );
        Ok(Self::Mmap {
            mmap,
            byte_offset,
            n_elements,
        })
    }

    /// Convert `Vec<T>` into the owned variant — explicit constructor
    /// so call sites don't need to spell out `ArcCow::Owned(...)`.
    #[inline]
    pub fn from_vec(v: Vec<T>) -> Self {
        Self::Owned(v)
    }

    /// Borrow the live slice. Lifetime is tied to `&self`; the
    /// `Arc<Mmap>` (when `Mmap` variant) keeps the underlying
    /// mapping alive while `self` lives.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        match self {
            Self::Owned(v) => v.as_slice(),
            Self::Mmap {
                mmap,
                byte_offset,
                n_elements,
            } => {
                let elem = std::mem::size_of::<T>();
                let bytes = &mmap[*byte_offset..*byte_offset + *n_elements * elem];
                bytemuck::cast_slice(bytes)
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(v) => v.len(),
            Self::Mmap { n_elements, .. } => *n_elements,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, T> {
        self.as_slice().iter()
    }

    /// Clone-on-write access mirroring `Cow::to_mut`. If `self` is the
    /// `Mmap` variant, copy the bytes into a fresh `Vec<T>` and switch
    /// the enum to `Owned` before returning `&mut [T]`. Subsequent
    /// `to_mut` calls on the (now owned) value return the same Vec
    /// without re-allocating.
    ///
    /// The mmap variant materialises on demand because file pages are
    /// strictly read-only — mutating them in place would be undefined
    /// behaviour and would also be wrong (the mapping is shared with
    /// every other `ArcCow` clone over the same range).
    pub fn to_mut(&mut self) -> &mut [T] {
        if let Self::Mmap { .. } = self {
            let owned: Vec<T> = self.as_slice().to_vec();
            *self = Self::Owned(owned);
        }
        match self {
            Self::Owned(v) => v.as_mut_slice(),
            // `Mmap` was replaced above; unreachable after the switch.
            Self::Mmap { .. } => unreachable!("ArcCow::to_mut: Mmap variant was just replaced"),
        }
    }
}

// `Deref<Target = [T]>` preserves the ergonomics of
// `Cow<'_, [T]>`: `cow.iter()`, `&cow[i]`, `cow[..]`, etc. work
// unchanged.
impl<T: bytemuck::Pod> std::ops::Deref for ArcCow<T> {
    type Target = [T];
    #[inline]
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

// `From<Vec<T>>` keeps existing `.into()` call sites compiling.
impl<T: bytemuck::Pod> From<Vec<T>> for ArcCow<T> {
    #[inline]
    fn from(v: Vec<T>) -> Self {
        Self::Owned(v)
    }
}

impl<T: bytemuck::Pod + std::fmt::Debug> std::fmt::Debug for ArcCow<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ArcCow").field(&self.as_slice()).finish()
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

    #[test]
    fn arccow_owned_roundtrip() {
        let v: Vec<u32> = (0..10).collect();
        let cow = ArcCow::from_vec(v.clone());
        assert_eq!(cow.as_slice(), v.as_slice());
        assert_eq!(cow.len(), 10);
        assert!(!cow.is_empty());
        // Deref ergonomics
        let view: &[u32] = &cow;
        assert_eq!(view[3], 3);
    }

    #[test]
    fn arccow_mmap_roundtrip() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        // 8 u32s = 32 bytes
        let values: Vec<u32> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        tmp.write_all(bytemuck::cast_slice(&values))?;
        tmp.flush()?;
        let mmap = map_readonly(tmp.path())?;
        let cow = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), 0, 8)?;
        assert_eq!(cow.as_slice(), &values[..]);
        // sub-range
        let cow_sub = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), 8, 4)?;
        assert_eq!(cow_sub.as_slice(), &[30u32, 40, 50, 60]);
        Ok(())
    }

    #[test]
    fn arccow_mmap_out_of_bounds_rejected() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(&[0u8; 16])?;
        tmp.flush()?;
        let mmap = map_readonly(tmp.path())?;
        // Need 8 u32s = 32 bytes; file is 16.
        assert!(ArcCow::<u32>::from_mmap(Arc::clone(&mmap), 0, 8).is_err());
        // Element count overflow
        assert!(ArcCow::<u32>::from_mmap(Arc::clone(&mmap), 0, usize::MAX).is_err());
        Ok(())
    }

    #[test]
    fn arccow_mmap_misaligned_rejected() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(&[0u8; 32])?;
        tmp.flush()?;
        let mmap = map_readonly(tmp.path())?;
        // u32 needs 4-byte alignment; offset 1 is misaligned.
        assert!(ArcCow::<u32>::from_mmap(Arc::clone(&mmap), 1, 4).is_err());
        Ok(())
    }

    #[test]
    fn arccow_drops_arc_when_dropped() -> Result<()> {
        let mut tmp = NamedTempFile::new()?;
        tmp.write_all(&[0u8; 16])?;
        tmp.flush()?;
        let mmap = map_readonly(tmp.path())?;
        let before = Arc::strong_count(&mmap);
        let cow = ArcCow::<u8>::from_mmap(Arc::clone(&mmap), 0, 16)?;
        assert_eq!(Arc::strong_count(&mmap), before + 1);
        drop(cow);
        assert_eq!(Arc::strong_count(&mmap), before);
        Ok(())
    }
}
