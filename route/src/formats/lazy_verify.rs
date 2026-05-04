//! Lazy / on-first-access CRC verification for `*.butterfly` containers (#160).
//!
//! ## Problem
//!
//! Eager CRC walk at boot reads every byte of every section the server
//! cares about. For Belgium that's ~26 GB → a multi-GB transient RSS
//! peak and ~12 s wall-clock dominated by I/O. For planet-scale
//! containers it scales linearly.
//!
//! ## Solution
//!
//! Defer the per-section CRC walk to **first access** of each section.
//! Boot reads the manifest only (already cheap — directory CRC + a
//! handful of u64 reads) and registers each section in `Unverified`
//! state. The first reader transitions the section to `Verifying`,
//! computes the CRC on a dedicated CPU-bound thread, and stores
//! `Verified` (or `Failed`) on completion. Subsequent readers
//! short-circuit on `Verified` or block on `Failed`.
//!
//! ## State machine
//!
//! ```text
//!  Unverified ──CAS──▶ Verifying ──CRC ok──▶ Verified
//!                          │
//!                          └────CRC fail────▶ Failed { reason }
//! ```
//!
//! All transitions are forward-only — no section ever leaves a terminal
//! state. The encoding fits in a `u8`:
//!
//! | discriminant | meaning |
//! |--------------|---------|
//! | `0` | Unverified |
//! | `1` | Verifying  |
//! | `2` | Verified   |
//! | `3` | Failed     |
//!
//! ## Memory ordering (codex consultation)
//!
//! The mmap bytes are immutable from the verifier's POV — it does not
//! publish new Rust data through the state transition, only the *fact*
//! that bytes-on-disk match `expected_crc`. Acquire/Release on the
//! `AtomicU8` is sufficient; SeqCst is not needed (no global ordering
//! across multiple atomics is required).
//!
//! - Reader load: `Acquire` — a reader observing `Verified` or `Failed`
//!   is guaranteed to see any data the verifier published before the
//!   terminal store (e.g. the failure reason in `parking_lot::Mutex`).
//! - CAS `Unverified → Verifying`: success `Relaxed` (no data published
//!   on the winning side at that moment); failure `Acquire` so the loser
//!   can branch on the observed state.
//! - Verifier terminal store: `Release` — pairs with reader Acquire and
//!   publishes the failure reason / verify duration.
//!
//! ## Notify discipline
//!
//! `tokio::sync::Notify::notify_waiters()` wakes everyone currently
//! parked. We re-load state in a loop after wake-up because Notify
//! provides no memory visibility guarantee on its own — the
//! Acquire load on the AtomicU8 is what makes the wake-up safe.

use anyhow::{Context, Result};
use parking_lot::{Condvar, Mutex};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Instant;
use tokio::sync::Notify;

use super::butterfly_dat::{Container, SectionEntry};
use super::crc;

/// Per-section verification state. `repr(u8)` so the discriminant fits
/// in an `AtomicU8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SectionVerifyState {
    /// CRC has not been computed yet.
    Unverified = 0,
    /// A worker is currently computing the CRC.
    Verifying = 1,
    /// CRC matched the manifest. Bytes are trusted.
    Verified = 2,
    /// CRC did not match the manifest. The byte slice is poisoned;
    /// every future reader should see the failure (HTTP 503).
    Failed = 3,
}

impl SectionVerifyState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Unverified,
            1 => Self::Verifying,
            2 => Self::Verified,
            3 => Self::Failed,
            other => panic!("invalid SectionVerifyState discriminant {}", other),
        }
    }
}

/// Per-section runtime metadata: state machine + wake primitives.
///
/// Held inside an `Arc` so the verifier worker thread, sync waiters,
/// and async waiters all share one instance per section. Cheap to
/// clone (Arc bump + 1 ptr).
pub struct SectionRuntime {
    pub name: String,
    pub kind: super::butterfly_dat::SectionKind,
    pub offset: u64,
    pub len: u64,
    pub expected_crc: u64,

    /// State machine, read with Acquire / written with Release at
    /// terminal transitions per the codex memory-ordering note above.
    state: AtomicU8,

    /// Failure reason. Written before storing `Failed` (Release). A
    /// reader that observes `Failed` (Acquire load) is guaranteed to
    /// see this lock's contents.
    failure_reason: Mutex<Option<String>>,

    /// Verification wall time once computed. Used by metrics and
    /// `route/docs/160-results.md` measurement methodology.
    verify_duration_s: Mutex<Option<f64>>,

    /// Async wake primitive for tokio waiters. `notify_waiters()` is
    /// idempotent across many calls (subsequent ones are no-ops if no
    /// waiter is parked).
    notify: Notify,

    /// Sync wake primitive for non-async waiters (the boot path runs
    /// off the tokio runtime; tests call from sync code). Paired with
    /// the lock below. Always taken in the order
    /// `lock → wait/notify → unlock`.
    sync_lock: Mutex<()>,
    sync_cvar: Condvar,
}

impl SectionRuntime {
    fn new(entry: &SectionEntry) -> Self {
        Self {
            name: entry.name.clone(),
            kind: entry.kind,
            offset: entry.offset,
            len: entry.len,
            expected_crc: entry.crc,
            state: AtomicU8::new(SectionVerifyState::Unverified as u8),
            failure_reason: Mutex::new(None),
            verify_duration_s: Mutex::new(None),
            notify: Notify::new(),
            sync_lock: Mutex::new(()),
            sync_cvar: Condvar::new(),
        }
    }

    /// Snapshot the current state with Acquire ordering. Cheap; safe
    /// to call from any thread.
    pub fn state(&self) -> SectionVerifyState {
        SectionVerifyState::from_u8(self.state.load(Ordering::Acquire))
    }

    /// Read the failure reason if the section is `Failed`. Returns
    /// `None` for any other state (including races: a transition from
    /// `Verifying` to `Failed` between this call and the prior state
    /// load is fine — the reason will simply be `None` until the
    /// caller re-checks state).
    pub fn failure_reason(&self) -> Option<String> {
        if self.state() == SectionVerifyState::Failed {
            self.failure_reason.lock().clone()
        } else {
            None
        }
    }

    /// Verification wall time once available, else `None`.
    pub fn verify_duration_s(&self) -> Option<f64> {
        *self.verify_duration_s.lock()
    }

    /// Wake every parked waiter (sync and async). Called once after a
    /// terminal state transition.
    fn wake_all(&self) {
        // Wake async waiters first; cheap if no one is parked.
        self.notify.notify_waiters();
        // Wake sync waiters under the cvar lock to avoid the classic
        // "lost wakeup" race: any thread that has just observed
        // Verifying and is about to wait_while will hit the recheck
        // before parking, because we hold sync_lock here.
        let _g = self.sync_lock.lock();
        self.sync_cvar.notify_all();
    }
}

/// LazyContainer: manifest-only boot + on-first-access CRC verification.
///
/// Wraps a [`Container`] and the live mmap with one [`SectionRuntime`]
/// per directory entry. The runtime is the gate every `bytes()`
/// accessor flows through. After the first `Verified` transition,
/// access is constant-time (one Acquire load + slice arithmetic).
pub struct LazyContainer {
    container: Container,
    mmap: Arc<memmap2::Mmap>,
    sections: BTreeMap<String, Arc<SectionRuntime>>,
}

impl LazyContainer {
    /// Open a container in lazy mode: read manifest + directory only,
    /// register every section as `Unverified`. Per-section CRCs are
    /// deferred to first access.
    ///
    /// The directory CRC is still verified at open time (cheap — single
    /// fixed-size read covered by `Container::open`).
    pub fn open_lazy(path: &std::path::Path) -> Result<Self> {
        let mmap = super::mmap::map_readonly(path)?;
        let container = Container::open(path)
            .with_context(|| format!("opening container manifest {}", path.display()))?;

        let sections = container
            .sections
            .iter()
            .map(|e| (e.name.clone(), Arc::new(SectionRuntime::new(e))))
            .collect();

        Ok(Self {
            container,
            mmap,
            sections,
        })
    }

    /// Eager open: read manifest, then verify every section
    /// immediately. Equivalent to the pre-#160 behaviour. Useful for
    /// tools that want to validate a container in one pass and for
    /// tests that exercise the verified-state branches.
    pub fn open_eager(path: &std::path::Path) -> Result<Self> {
        let lazy = Self::open_lazy(path)?;
        for entry in lazy.container.sections.iter() {
            lazy.verify_now(&entry.name).with_context(|| {
                format!(
                    "eager verification of section '{}' in {}",
                    entry.name,
                    path.display()
                )
            })?;
        }
        Ok(lazy)
    }

    /// Borrow the underlying [`Container`] manifest (directory, lookup
    /// helpers, etc).
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// Borrow the live mmap as `Arc<Mmap>`. The Arc keeps the mapping
    /// alive for any zero-copy slice handed out elsewhere.
    pub fn mmap_arc(&self) -> &Arc<memmap2::Mmap> {
        &self.mmap
    }

    /// Look up the per-section runtime by name. Returns `None` if the
    /// section is not in the directory.
    pub fn runtime(&self, name: &str) -> Option<&Arc<SectionRuntime>> {
        self.sections.get(name)
    }

    /// Iterate every (name, runtime) pair. Order is sorted by name
    /// (BTreeMap iteration order).
    pub fn iter_runtimes(&self) -> impl Iterator<Item = (&String, &Arc<SectionRuntime>)> {
        self.sections.iter()
    }

    /// Number of sections in the manifest.
    pub fn n_sections(&self) -> usize {
        self.sections.len()
    }

    /// Snapshot states by name, sorted. Convenient for tests and
    /// `/health` / metrics computation.
    pub fn state_snapshot(&self) -> Vec<(String, SectionVerifyState)> {
        self.sections
            .iter()
            .map(|(name, rt)| (name.clone(), rt.state()))
            .collect()
    }

    /// Borrow the bytes of a section, blocking until the section is
    /// `Verified`. If the section is `Failed`, returns an error
    /// carrying the recorded failure reason.
    ///
    /// Sync entry point. Safe to call from non-async code (e.g. boot,
    /// tests).
    pub fn section_bytes(&self, name: &str) -> Result<&[u8]> {
        let rt = self
            .sections
            .get(name)
            .ok_or_else(|| anyhow::anyhow!("section '{}' not in manifest", name))?;
        self.ensure_verified_sync(rt)?;
        Ok(&self.mmap[rt.offset as usize..(rt.offset + rt.len) as usize])
    }

    /// Like [`Self::section_bytes`] but returns `Ok(None)` when the
    /// section is not in the manifest. Used by callers that handle the
    /// optional-section pattern (back-compat with old containers).
    pub fn section_bytes_optional(&self, name: &str) -> Result<Option<&[u8]>> {
        match self.sections.get(name) {
            Some(rt) => {
                self.ensure_verified_sync(rt)?;
                let bytes = &self.mmap[rt.offset as usize..(rt.offset + rt.len) as usize];
                Ok(Some(bytes))
            }
            None => Ok(None),
        }
    }

    /// Ensure the section is `Verified`, blocking the current thread
    /// until either Verified or Failed is reached. Drives verification
    /// inline on the calling thread when the caller wins the
    /// `Unverified → Verifying` CAS — this matches the design rule
    /// "first reader of a section runs the CRC".
    ///
    /// Inlining the CRC on the calling thread (rather than spawning a
    /// dedicated worker) is the simplest correct policy for the sync
    /// path: the calling thread is by definition not on the tokio
    /// worker pool, so we don't risk blocking async tasks.
    fn ensure_verified_sync(&self, rt: &Arc<SectionRuntime>) -> Result<()> {
        loop {
            let observed = rt.state.load(Ordering::Acquire);
            match SectionVerifyState::from_u8(observed) {
                SectionVerifyState::Verified => return Ok(()),
                SectionVerifyState::Failed => {
                    let reason = rt
                        .failure_reason
                        .lock()
                        .clone()
                        .unwrap_or_else(|| "unknown failure".to_string());
                    anyhow::bail!("section '{}' is poisoned: {}", rt.name, reason);
                }
                SectionVerifyState::Unverified => {
                    // Try to claim verification. Success → drive inline.
                    match rt.state.compare_exchange(
                        SectionVerifyState::Unverified as u8,
                        SectionVerifyState::Verifying as u8,
                        Ordering::Relaxed,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            self.run_verifier(rt);
                            // After run_verifier returns, the state is
                            // either Verified or Failed. Loop once more
                            // to dispatch on it.
                        }
                        Err(_) => {
                            // Another thread won the race; restart loop
                            // and re-load the state.
                        }
                    }
                }
                SectionVerifyState::Verifying => {
                    // Another thread is computing. Park on the
                    // sync_cvar until they wake us, then re-check.
                    let mut g = rt.sync_lock.lock();
                    // Re-check inside the lock to avoid a lost wakeup
                    // (the verifier wakes under the same lock).
                    let again = rt.state.load(Ordering::Acquire);
                    if SectionVerifyState::from_u8(again) == SectionVerifyState::Verifying {
                        rt.sync_cvar.wait(&mut g);
                    }
                    // Loop to re-dispatch on the (likely terminal) new state.
                }
            }
        }
    }

    /// Async entry point. Same semantics as
    /// [`Self::ensure_verified_sync`] but parks on a tokio Notify when
    /// another thread is verifying. Drives verification inline (on
    /// `spawn_blocking`) when the caller wins the CAS — see
    /// `verify_in_background_blocking`.
    pub async fn ensure_verified_async(&self, name: &str) -> Result<()> {
        let rt = self
            .sections
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("section '{}' not in manifest", name))?;
        loop {
            let observed = rt.state.load(Ordering::Acquire);
            match SectionVerifyState::from_u8(observed) {
                SectionVerifyState::Verified => return Ok(()),
                SectionVerifyState::Failed => {
                    let reason = rt
                        .failure_reason
                        .lock()
                        .clone()
                        .unwrap_or_else(|| "unknown failure".to_string());
                    anyhow::bail!("section '{}' is poisoned: {}", rt.name, reason);
                }
                SectionVerifyState::Unverified => {
                    match rt.state.compare_exchange(
                        SectionVerifyState::Unverified as u8,
                        SectionVerifyState::Verifying as u8,
                        Ordering::Relaxed,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // Drive verification on the blocking thread
                            // pool — CRC compute is CPU-bound and large.
                            let mmap = Arc::clone(&self.mmap);
                            let rt_for_blocking = Arc::clone(&rt);
                            tokio::task::spawn_blocking(move || {
                                run_verifier_for(&rt_for_blocking, &mmap);
                            })
                            .await
                            .context("verifier worker join failed")?;
                            // Loop to dispatch on the now-terminal state.
                        }
                        Err(_) => {
                            // Lost the race; loop and re-load.
                        }
                    }
                }
                SectionVerifyState::Verifying => {
                    // Park until the verifier wakes us. Notify wakes
                    // every current waiter; after wake we re-load
                    // state with Acquire (the load above on next loop
                    // iteration).
                    let notified = rt.notify.notified();
                    // Re-check before parking to avoid missing a fast
                    // transition that beats us into the wait.
                    if SectionVerifyState::from_u8(rt.state.load(Ordering::Acquire))
                        != SectionVerifyState::Verifying
                    {
                        continue;
                    }
                    notified.await;
                }
            }
        }
    }

    /// Synchronous one-shot verification of a named section. Equivalent
    /// to calling [`Self::section_bytes`] for its side effects and
    /// discarding the slice. Used by `open_eager` and the warmup path.
    pub fn verify_now(&self, name: &str) -> Result<()> {
        let _bytes = self.section_bytes(name)?;
        Ok(())
    }

    /// Run the verifier on the current thread for the given runtime.
    /// Caller must have already won the `Unverified → Verifying` CAS.
    /// On exit, the runtime is in either `Verified` or `Failed` state
    /// and every parked waiter has been woken.
    fn run_verifier(&self, rt: &Arc<SectionRuntime>) {
        run_verifier_for(rt, &self.mmap);
    }

    /// Kick off a background warmup pass: every still-`Unverified`
    /// section has its CRC computed in parallel on rayon. Returns
    /// immediately; metrics + state observation tell you when each
    /// section finished.
    ///
    /// Useful for the operator escape hatch `--warmup-on-boot`, which
    /// matches pre-#160 behaviour (verify everything at boot) but
    /// without blocking the listener.
    pub fn spawn_warmup(self: &Arc<Self>) {
        let lc = Arc::clone(self);
        std::thread::Builder::new()
            .name("butterfly-warmup".to_string())
            .spawn(move || {
                use rayon::prelude::*;
                let names: Vec<String> = lc.sections.keys().cloned().collect();
                names.par_iter().for_each(|name| {
                    if let Some(rt) = lc.sections.get(name) {
                        // Only act on sections still Unverified — anything
                        // touched in the meantime by a query is already
                        // (or about to be) terminal.
                        let s = rt.state.load(Ordering::Acquire);
                        if SectionVerifyState::from_u8(s) == SectionVerifyState::Unverified {
                            // Try to claim. If we lose, someone else is
                            // verifying; let them finish. If we win,
                            // run the verifier inline.
                            if rt
                                .state
                                .compare_exchange(
                                    SectionVerifyState::Unverified as u8,
                                    SectionVerifyState::Verifying as u8,
                                    Ordering::Relaxed,
                                    Ordering::Acquire,
                                )
                                .is_ok()
                            {
                                run_verifier_for(rt, &lc.mmap);
                            }
                        }
                    }
                });
                tracing::info!("background warmup pass complete");
            })
            .expect("spawn warmup thread");
    }
}

/// Compute the CRC of a section against its manifest entry, transition
/// the runtime to `Verified` or `Failed`, and wake every waiter.
///
/// This is the shared body of every verification entry point. It does
/// the actual work; the entry points (sync, async, warmup) handle the
/// state-machine choreography around it.
fn run_verifier_for(rt: &Arc<SectionRuntime>, mmap: &memmap2::Mmap) {
    debug_assert_eq!(
        rt.state.load(Ordering::Acquire),
        SectionVerifyState::Verifying as u8,
        "run_verifier_for called without holding the Verifying claim"
    );

    let started = Instant::now();
    let start = rt.offset as usize;
    let end = start + rt.len as usize;

    // Bounds check defensively. The manifest is supposed to enforce
    // this in `Container::open`, but a corrupted directory could in
    // principle slip through; we'd rather fail the section than panic.
    let bytes_opt = mmap.get(start..end);
    let result = match bytes_opt {
        None => Err(format!(
            "section bytes [{},{}) exceed mmap len {}",
            start,
            end,
            mmap.len()
        )),
        Some(bytes) => {
            let computed = crc::checksum(bytes);
            if computed == rt.expected_crc {
                Ok(())
            } else {
                Err(format!(
                    "CRC mismatch: computed 0x{:016X}, manifest 0x{:016X}",
                    computed, rt.expected_crc
                ))
            }
        }
    };

    let elapsed = started.elapsed().as_secs_f64();
    *rt.verify_duration_s.lock() = Some(elapsed);

    match result {
        Ok(()) => {
            tracing::debug!(
                section = %rt.name,
                bytes = rt.len,
                duration_s = elapsed,
                "section verified"
            );
            crate::server::metrics::record_section_verified(&rt.name, elapsed);
            // Release: every load(Acquire) of Verified after this point
            // is guaranteed to also see verify_duration_s set above.
            rt.state
                .store(SectionVerifyState::Verified as u8, Ordering::Release);
        }
        Err(reason) => {
            tracing::error!(
                section = %rt.name,
                reason = %reason,
                "section verification FAILED — future requests touching this section will return 503"
            );
            // Write reason BEFORE storing Failed so any reader that
            // observes Failed via Acquire load is guaranteed to see
            // the reason in the lock.
            *rt.failure_reason.lock() = Some(reason.clone());
            crate::server::metrics::record_section_failed(&rt.name);
            rt.state
                .store(SectionVerifyState::Failed as u8, Ordering::Release);
        }
    }

    rt.wake_all();
}

#[cfg(test)]
mod tests {
    use super::super::butterfly_dat::{ContainerWriter, SectionKind};
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::NamedTempFile;

    fn write_demo(path: &std::path::Path) -> Result<()> {
        let mut w = ContainerWriter::create(path)?;
        w.append_bytes(
            SectionKind::EbgNodes,
            "shared/ebg.nodes",
            b"hello ebg nodes",
        )?;
        w.append_bytes(SectionKind::CchTopo, "shared/cch.topo", b"cch topo bytes")?;
        w.append_bytes(
            SectionKind::CchWeightsTime,
            "mode/car/weights.time",
            b"car time weights",
        )?;
        w.finalize()
    }

    #[test]
    fn open_lazy_starts_with_unverified_state() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;

        let lc = LazyContainer::open_lazy(tmp.path())?;
        assert_eq!(lc.n_sections(), 3);
        for (_, rt) in lc.iter_runtimes() {
            assert_eq!(rt.state(), SectionVerifyState::Unverified);
            assert!(rt.verify_duration_s().is_none());
        }
        Ok(())
    }

    #[test]
    fn open_eager_marks_all_verified() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;

        let lc = LazyContainer::open_eager(tmp.path())?;
        assert_eq!(lc.n_sections(), 3);
        for (_, rt) in lc.iter_runtimes() {
            assert_eq!(rt.state(), SectionVerifyState::Verified);
            assert!(rt.verify_duration_s().is_some());
        }
        Ok(())
    }

    #[test]
    fn first_section_bytes_call_transitions_to_verified() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;

        let lc = LazyContainer::open_lazy(tmp.path())?;
        let bytes = lc.section_bytes("shared/ebg.nodes")?;
        assert_eq!(bytes, b"hello ebg nodes");
        let rt = lc.runtime("shared/ebg.nodes").unwrap();
        assert_eq!(rt.state(), SectionVerifyState::Verified);
        // Other sections still untouched.
        let other = lc.runtime("shared/cch.topo").unwrap();
        assert_eq!(other.state(), SectionVerifyState::Unverified);
        Ok(())
    }

    #[test]
    fn second_call_is_constant_time_no_re_verify() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;

        let lc = LazyContainer::open_lazy(tmp.path())?;
        lc.section_bytes("shared/cch.topo")?;
        let rt = lc.runtime("shared/cch.topo").unwrap();
        let first_dur = rt.verify_duration_s();
        // A second call should NOT re-run the verifier — the recorded
        // duration must not change.
        lc.section_bytes("shared/cch.topo")?;
        assert_eq!(rt.verify_duration_s(), first_dur);
        Ok(())
    }

    #[test]
    fn corrupted_section_transitions_to_failed_with_reason() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        let entry = {
            let lc = LazyContainer::open_lazy(tmp.path())?;
            let rt = lc.runtime("mode/car/weights.time").unwrap();
            (rt.offset, rt.len)
        };
        // Flip a byte inside the payload after the manifest is sealed.
        {
            let mut f = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
            f.seek(SeekFrom::Start(entry.0))?;
            f.write_all(&[0xFF])?;
        }
        let lc = LazyContainer::open_lazy(tmp.path())?;
        let res = lc.section_bytes("mode/car/weights.time");
        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("CRC mismatch"),
            "expected CRC mismatch in error, got: {}",
            err
        );
        let rt = lc.runtime("mode/car/weights.time").unwrap();
        assert_eq!(rt.state(), SectionVerifyState::Failed);
        let reason = rt.failure_reason().expect("failure_reason set");
        assert!(reason.contains("CRC mismatch"));
        Ok(())
    }

    #[test]
    fn missing_section_returns_error() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        let lc = LazyContainer::open_lazy(tmp.path())?;
        let res = lc.section_bytes("does/not/exist");
        assert!(res.is_err());
        Ok(())
    }

    #[test]
    fn optional_missing_section_returns_none() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        let lc = LazyContainer::open_lazy(tmp.path())?;
        assert!(lc.section_bytes_optional("does/not/exist")?.is_none());
        Ok(())
    }

    #[test]
    fn concurrent_first_readers_do_not_double_verify() -> Result<()> {
        // Many threads racing on the same Unverified section. Only one
        // computes; the rest park on the cvar and wake up on Verified.
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        let lc = Arc::new(LazyContainer::open_lazy(tmp.path())?);
        let n_threads = 16;
        let barrier = Arc::new(std::sync::Barrier::new(n_threads));
        let mut handles = Vec::with_capacity(n_threads);
        for _ in 0..n_threads {
            let lc = Arc::clone(&lc);
            let b = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                b.wait();
                let bytes = lc.section_bytes("shared/cch.topo").unwrap();
                assert_eq!(bytes, b"cch topo bytes");
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Verification happened exactly once: duration is set.
        let rt = lc.runtime("shared/cch.topo").unwrap();
        assert_eq!(rt.state(), SectionVerifyState::Verified);
        assert!(rt.verify_duration_s().is_some());
        Ok(())
    }

    #[test]
    fn warmup_pass_verifies_remaining_sections() -> Result<()> {
        let tmp = NamedTempFile::new()?;
        write_demo(tmp.path())?;
        let lc = Arc::new(LazyContainer::open_lazy(tmp.path())?);
        // Touch one to verify it inline.
        lc.section_bytes("shared/ebg.nodes")?;

        lc.spawn_warmup();
        // Wait until every section is terminal.
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let all_done = lc.iter_runtimes().all(|(_, rt)| {
                matches!(
                    rt.state(),
                    SectionVerifyState::Verified | SectionVerifyState::Failed
                )
            });
            if all_done {
                break;
            }
            if Instant::now() > deadline {
                panic!("warmup pass did not finish within 5 s");
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        for (_, rt) in lc.iter_runtimes() {
            assert_eq!(rt.state(), SectionVerifyState::Verified);
        }
        Ok(())
    }
}
