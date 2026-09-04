//! Test-support: ONE skip mechanism for data-bound tests (#587).
//!
//! Before #587 the repo carried two ways of saying "this test needs the
//! Belgium artifacts": `#[ignore]` (opaque — `cargo test` reports the
//! test as ignored and NEVER runs it, even on a machine that has the
//! data) and an ad-hoc `BUTTERFLY_TEST_DATA_DIR` probe copy-pasted into
//! three integration-test files (live, but dead code whenever the same
//! test was also `#[ignore]`d). This module is the single probe both
//! `route/src/**` unit tests and `route/tests/**` integration tests now
//! call, so a plain `cargo test --workspace` on a data-full runner gains
//! the coverage for free and on a bare CI runner prints ONE skip line
//! per test file instead of silently doing nothing.
//!
//! `#[ignore]` survives only for the cases a data directory cannot fix:
//! a live network fetch or a running server.
//!
//! # Layout probing
//!
//! `BUTTERFLY_TEST_DATA_DIR` may point either at a directory of region
//! subdirectories (`<dir>/belgium/...`, the layout this repo's pipeline
//! writes under `data/`) or straight at one region's own directory
//! (`<dir>/<name>.butterfly`, `<dir>/transit/...`, the layout a deploy
//! staging area uses). [`region_root`] accepts both so nobody has to
//! reshuffle a 25 GB directory to run the tests.
//!
//! # Why this lives in the library, not in `tests/common/`
//!
//! Integration tests link the NON-test build of the crate, so a
//! `#[cfg(test)]` module is invisible to them, and a `tests/common/`
//! module is invisible to the unit tests inside `route/src/server/`.
//! Both sets need the same probe, and "one mechanism" is the whole point
//! of #587 — so the probe is a small, dependency-free, `#[doc(hidden)]`
//! public module.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

/// Environment variable pointing at the test data.
pub const DATA_DIR_ENV: &str = "BUTTERFLY_TEST_DATA_DIR";

/// Region directory name used by this repo's own pipeline output.
const REGION: &str = "belgium";

fn skipped_scopes() -> &'static Mutex<HashSet<String>> {
    static SCOPES: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    SCOPES.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Candidate roots that may CONTAIN region directories.
fn data_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(v) = std::env::var(DATA_DIR_ENV)
        && !v.is_empty()
    {
        roots.push(PathBuf::from(v));
    }
    // Integration tests run with CWD at the package root (`route/`);
    // unit tests may run from the workspace root.
    roots.push(PathBuf::from("../data"));
    roots.push(PathBuf::from("data"));
    roots.push(PathBuf::from("../../data"));
    roots
}

/// The directory that holds the Belgium artifacts themselves.
///
/// Returns the first candidate that looks populated: it either contains
/// a `*.butterfly` container or a `transit/` feed directory. Both the
/// `<root>/belgium/` and the flat `<root>/` layouts are accepted.
pub fn region_root() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    for root in data_roots() {
        candidates.push(root.join(REGION));
        candidates.push(root);
    }
    candidates.into_iter().find(|c| looks_populated(c))
}

fn looks_populated(dir: &Path) -> bool {
    if !dir.is_dir() {
        return false;
    }
    if dir.join("transit").is_dir() || dir.join("step5").is_dir() {
        return true;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return false;
    };
    entries.flatten().any(|e| {
        e.path()
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("butterfly"))
    })
}

/// Resolve `rel` under the region root, e.g. `transit/gtfs/sncb.zip`.
/// `None` when either the root or the specific asset is absent.
pub fn asset(rel: &str) -> Option<PathBuf> {
    let p = region_root()?.join(rel);
    p.exists().then_some(p)
}

/// The Belgium routing artifact: a `*.butterfly` container, or the
/// region directory itself when it holds an unpacked `stepN/` tree.
pub fn container() -> Option<PathBuf> {
    let root = region_root()?;
    for name in ["baseline.butterfly", "belgium.butterfly"] {
        let p = root.join(name);
        if p.is_file() {
            return Some(p);
        }
    }
    let mut packed: Vec<PathBuf> = std::fs::read_dir(&root)
        .ok()?
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("butterfly"))
        })
        .collect();
    packed.sort();
    if let Some(first) = packed.into_iter().next() {
        return Some(first);
    }
    root.join("step5").is_dir().then_some(root)
}

/// Announce, at most ONCE per `scope` (use the test file's name), that a
/// group of tests is skipping for want of data, then return `None`.
///
/// The one-line-per-file budget is deliberate: 18 skipped transit tests
/// used to be 18 identical lines of noise, which is why nobody noticed
/// they had stopped running.
pub fn skip<T>(scope: &str, needs: &str) -> Option<T> {
    let mut seen = skipped_scopes().lock().unwrap_or_else(|e| e.into_inner());
    if seen.insert(scope.to_string()) {
        eprintln!(
            "SKIP {scope}: no {needs} — set {DATA_DIR_ENV}=<dir> (a directory of \
             region subdirectories, or one region's own directory) to run these"
        );
    }
    None
}

/// [`asset`] with the skip line attached.
pub fn require_asset(scope: &str, rel: &str) -> Option<PathBuf> {
    match asset(rel) {
        Some(p) => Some(p),
        None => skip(scope, rel),
    }
}

/// [`container`] with the skip line attached.
pub fn require_container(scope: &str) -> Option<PathBuf> {
    match container() {
        Some(p) => Some(p),
        None => skip(
            scope,
            "Belgium routing artifact (*.butterfly or stepN/ tree)",
        ),
    }
}

/// Load the Belgium `ServerState` ONCE per test binary and hand out
/// clones of the `Arc`.
///
/// Loading is the expensive part (mmap + index build); sharing it is what
/// makes running ~20 previously-`#[ignore]`d consistency tests on a
/// data-full runner affordable. Returns `None` (after one skip line) when
/// the artifact is absent.
pub fn belgium_state(scope: &str) -> Option<std::sync::Arc<crate::server::state::ServerState>> {
    static STATE: OnceLock<Option<std::sync::Arc<crate::server::state::ServerState>>> =
        OnceLock::new();
    let loaded = STATE.get_or_init(|| {
        let path = container()?;
        let state = if path.is_dir() {
            crate::server::state::ServerState::load(&path, None)
        } else {
            crate::server::state::ServerState::load_from_container(&path, None)
        };
        match state {
            Ok(s) => Some(std::sync::Arc::new(s)),
            Err(e) => {
                // A present-but-unreadable artifact is a real failure, not a
                // skip: say so loudly rather than reporting "no data".
                panic!("failed to load Belgium artifact {}: {e:#}", path.display());
            }
        }
    });
    match loaded {
        Some(s) => Some(s.clone()),
        None => skip(
            scope,
            "Belgium routing artifact (*.butterfly or stepN/ tree)",
        ),
    }
}
