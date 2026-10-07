//! RSS-checkpoint instrumentation (#152).
//!
//! Reads `/proc/self/smaps_rollup` (preferred — more accurate than
//! `/proc/self/status` because it walks the process VMAs and aggregates
//! the rollup fields, including the anon/file-backed RSS split we care
//! about) and emits a `tracing::info!` line tagged `RSS_CHECKPOINT` at
//! every boot phase. Disabled by default; turned on by
//! `--rss-checkpoints` or `BUTTERFLY_RSS_CHECKPOINTS=1`.
//!
//! The lines are deterministic and grep-friendly:
//!
//! ```text
//! RSS_CHECKPOINT phase=load.shared total_kb=4012345 anon_kb=234567 file_kb=3777778 elapsed_s=1.234
//! ```
//!
//! This instrumentation stays in the codebase as the foundation for
//! the #153/#154/#155 measurement discipline. It is NOT a one-shot
//! diagnostic.
//!
//! `ps -o rss` is explicitly NOT used — codex flagged it: it only
//! exposes a coarse current RSS value, whereas `smaps_rollup` provides
//! the rollup fields and the anon/file-backed breakdown we need for
//! post-mmap steady-state measurement.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

static ENABLED: AtomicBool = AtomicBool::new(false);
static START: OnceLock<Instant> = OnceLock::new();

/// Turn the instrumentation on. Idempotent. Safe to call from any
/// thread.
pub fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
    let _ = START.set(Instant::now());
}

/// Returns true iff `set_enabled(true)` has been called. Cheap to
/// poll on the boot path so the checkpoint helper short-circuits when
/// the operator hasn't asked for instrumentation.
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Sample `/proc/self/smaps_rollup` and emit one
/// `RSS_CHECKPOINT phase=<phase> total_kb=N anon_kb=M file_kb=K
/// elapsed_s=Z` line at `tracing::info!` level.
///
/// No-op when [`is_enabled`] returns false.
pub fn checkpoint(phase: &str) {
    if !is_enabled() {
        return;
    }
    let elapsed_s = START
        .get()
        .map(|t| t.elapsed().as_secs_f64())
        .unwrap_or(0.0);
    match read_smaps_rollup() {
        Ok(rss) => {
            tracing::info!(
                target: "rss_checkpoint",
                phase = phase,
                total_kb = rss.rss_kb,
                anon_kb = rss.anon_kb,
                file_kb = rss.file_kb,
                elapsed_s = format!("{elapsed_s:.3}"),
                "RSS_CHECKPOINT phase={phase} total_kb={t} anon_kb={a} file_kb={f} elapsed_s={elapsed_s:.3}",
                t = rss.rss_kb,
                a = rss.anon_kb,
                f = rss.file_kb,
            );
        }
        Err(e) => {
            tracing::warn!(
                phase = phase,
                error = %e,
                "RSS_CHECKPOINT failed to read smaps_rollup"
            );
        }
    }
}

/// Parsed smaps_rollup snapshot. All values in kibibytes (the units
/// that `/proc/.../smaps_rollup` reports natively).
#[derive(Debug, Clone, Copy)]
pub struct RssSnapshot {
    /// `Rss:` — total resident set size.
    pub rss_kb: u64,
    /// `Rss_anon` style: `Anonymous:` — heap + dirty COW pages. The
    /// architecture KPI for #152.
    pub anon_kb: u64,
    /// File-backed resident bytes. Computed as `rss - anon`. Matches
    /// the file-backed mmap pages (`butterfly.dat` content paged in
    /// by demand).
    pub file_kb: u64,
}

/// Read `/proc/self/smaps_rollup`. Returns the three KPI fields.
///
/// Public so handlers can sample without going through the
/// checkpoint logging path (e.g. /metrics endpoint extension).
pub fn read_smaps_rollup() -> std::io::Result<RssSnapshot> {
    let s = std::fs::read_to_string("/proc/self/smaps_rollup")?;
    let mut rss_kb = 0u64;
    let mut anon_kb = 0u64;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("Rss:") {
            rss_kb = parse_kb_field(rest);
        } else if let Some(rest) = line.strip_prefix("Anonymous:") {
            anon_kb = parse_kb_field(rest);
        }
    }
    let file_kb = rss_kb.saturating_sub(anon_kb);
    Ok(RssSnapshot {
        rss_kb,
        anon_kb,
        file_kb,
    })
}

/// Page-fault counters of this process since it started (`/proc/self/stat`
/// fields 10 and 12). A MAJOR fault is a page read from disk — on the
/// mmapped container that is a served section the kernel evicted (#636).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FaultCounts {
    pub minor: u64,
    pub major: u64,
}

/// Parse the body of `/proc/self/stat`. The `comm` field may hold spaces
/// and parentheses, so the fields are counted from the LAST `)`: after it
/// come state, ppid, pgrp, session, tty_nr, tpgid, flags, minflt, cminflt,
/// majflt, … (proc(5)).
pub fn parse_proc_stat_faults(stat: &str) -> Option<FaultCounts> {
    let tail = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = tail.split_ascii_whitespace().collect();
    Some(FaultCounts {
        minor: fields.get(7)?.parse().ok()?,
        major: fields.get(9)?.parse().ok()?,
    })
}

/// Read this process's fault counters.
pub fn read_proc_self_faults() -> std::io::Result<FaultCounts> {
    let s = std::fs::read_to_string("/proc/self/stat")?;
    parse_proc_stat_faults(&s).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unparseable /proc/self/stat",
        )
    })
}

/// #636: publish the process's residency on `/metrics` so a deployment can
/// see the container being evicted instead of guessing from latency:
/// `butterfly_route_process_rss_bytes` (+ `_anon_bytes`, `_file_bytes` —
/// the file-backed part IS the mmapped container's resident pages) and the
/// cumulative `butterfly_route_process_major_faults` / `_minor_faults`.
/// Called on every `/metrics` scrape; two cheap `/proc` reads.
pub fn export_process_gauges() {
    if let Ok(snap) = read_smaps_rollup() {
        metrics::gauge!("butterfly_route_process_rss_bytes").set((snap.rss_kb * 1024) as f64);
        metrics::gauge!("butterfly_route_process_rss_anon_bytes").set((snap.anon_kb * 1024) as f64);
        metrics::gauge!("butterfly_route_process_rss_file_bytes").set((snap.file_kb * 1024) as f64);
    }
    if let Ok(f) = read_proc_self_faults() {
        metrics::gauge!("butterfly_route_process_major_faults").set(f.major as f64);
        metrics::gauge!("butterfly_route_process_minor_faults").set(f.minor as f64);
    }
}

/// Parse a `/proc` field tail like `"   1234 kB"` or `" 1234 kB"`
/// to `1234`. Tolerates leading whitespace and trailing unit token.
fn parse_kb_field(tail: &str) -> u64 {
    for tok in tail.split_whitespace() {
        if let Ok(v) = tok.parse::<u64>() {
            return v;
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kb_field_handles_typical_format() {
        assert_eq!(parse_kb_field("        1234 kB"), 1234);
        assert_eq!(parse_kb_field("0 kB"), 0);
        assert_eq!(parse_kb_field("   42 kB"), 42);
    }

    #[test]
    fn proc_stat_faults_count_from_the_last_paren() {
        // comm with spaces and a parenthesis, as proc(5) allows
        let line = "4242 (butterfly (x) y) S 1 4242 4242 0 -1 4194560 123456 0 789 0 10 20 0 0 20 0 64 0 100 1 2 3
";
        assert_eq!(
            parse_proc_stat_faults(line),
            Some(FaultCounts {
                minor: 123_456,
                major: 789
            })
        );
        assert_eq!(parse_proc_stat_faults("garbage"), None);
    }

    #[test]
    fn read_proc_self_faults_works_on_linux() {
        if std::path::Path::new("/proc/self/stat").exists() {
            let f = read_proc_self_faults().expect("/proc/self/stat");
            assert!(f.minor > 0, "a running process has minor faults: {f:?}");
        }
    }

    #[test]
    fn read_smaps_rollup_returns_nonzero_on_linux() {
        // smaps_rollup exists on every modern Linux kernel; this
        // test runs in CI on Linux only. On non-Linux the file
        // doesn't exist and we'd get an error, but the test target
        // is Linux for this workspace anyway.
        if std::path::Path::new("/proc/self/smaps_rollup").exists() {
            let snap = read_smaps_rollup().expect("smaps_rollup readable");
            assert!(snap.rss_kb > 0, "RSS should be > 0 for live process");
            assert!(
                snap.anon_kb <= snap.rss_kb,
                "anon ({}) must not exceed RSS ({})",
                snap.anon_kb,
                snap.rss_kb
            );
        }
    }

    #[test]
    fn checkpoint_is_noop_when_disabled() {
        // The default state is disabled; calling checkpoint should
        // not panic and should produce no output observable here.
        // We don't assert log absence (that requires a tracing
        // subscriber test harness), but we assert no panic.
        ENABLED.store(false, Ordering::Relaxed);
        checkpoint("test_disabled");
    }
}
