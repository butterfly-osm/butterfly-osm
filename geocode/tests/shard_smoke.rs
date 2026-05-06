//! Smoke test for production shards (#81).
//!
//! Walks `geocode/data/shards/` and verifies that every `*.bfgs` file:
//!
//! 1. Opens cleanly (passes BFGS v5 CRC + magic + country header checks).
//! 2. Reports a non-zero record count.
//! 3. Round-trips at least one record through `Shard::record(0)` —
//!    catches partial truncation that the CRC alone might miss when
//!    the trailing bytes happen to checksum cleanly.
//! 4. Loads into a `ServerState` (via the multi-shard loader) so we
//!    catch country-collision and shard-routing wiring at the same
//!    time.
//!
//! `#[ignore]` because the shards are >50 MB and not committed —
//! operators rebuild via `scripts/build_country_shards.sh` and then
//! run this with:
//!
//! ```text
//! cargo test -p butterfly-geocode --release --test shard_smoke -- --ignored
//! ```

use std::path::PathBuf;

use butterfly_geocode::server::ServerState;
use butterfly_geocode::shard::reader::Shard;

fn shards_dir() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.join("data").join("shards")
}

#[test]
#[ignore = "requires production shards built via scripts/build_country_shards.sh"]
fn every_shipped_shard_opens_and_serves() {
    let dir = shards_dir();
    assert!(
        dir.is_dir(),
        "missing {} — run scripts/build_country_shards.sh first",
        dir.display()
    );

    let entries: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("reading {}: {e}", dir.display()))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("bfgs"))
                .unwrap_or(false)
        })
        .collect();

    assert!(
        !entries.is_empty(),
        "no *.bfgs files in {} — build them first",
        dir.display()
    );

    // Per-shard checks. We aggregate failures so a broken AT shard
    // doesn't mask a broken DE shard — surface every problem in one
    // run.
    let mut failures = Vec::new();
    let mut total_records: usize = 0;
    for path in &entries {
        match Shard::open(path) {
            Ok(s) => {
                let n = s.record_count();
                if n == 0 {
                    failures.push(format!("{}: empty shard", path.display()));
                    continue;
                }
                if s.record(0).is_none() {
                    failures.push(format!(
                        "{}: record(0) returned None despite record_count={n}",
                        path.display()
                    ));
                    continue;
                }
                total_records += n;
                eprintln!(
                    "[ok] {}  country={}  records={n}",
                    path.display(),
                    s.country().iso2()
                );
            }
            Err(e) => failures.push(format!("{}: open failed: {e}", path.display())),
        }
    }

    if !failures.is_empty() {
        panic!("shard smoke check failed:\n  - {}", failures.join("\n  - "));
    }

    // Multi-shard load — exercises the same code path the production
    // server uses at boot.
    let state = ServerState::load_from_dir(&dir)
        .unwrap_or_else(|e| panic!("ServerState::load_from_dir({}): {e}", dir.display()));
    let n_loaded = state.shards.len();
    assert_eq!(
        n_loaded,
        entries.len(),
        "ServerState loaded {n_loaded} countries from {} shards on disk",
        entries.len()
    );

    eprintln!(
        "smoke: {} shards, {n_loaded} countries, {total_records} total records",
        entries.len()
    );
}

#[test]
#[ignore = "requires production shards built via scripts/build_country_shards.sh"]
fn every_shard_serves_a_record_lookup() {
    // Walk each shipped shard and read record(0). This is the smallest
    // possible "did the shard load and is it queryable" check that
    // does not require booting the full HTTP server (the per-country
    // proof files at geocode/data/proof/81-<iso2>-shard.txt cover the
    // HTTP-level smoke).
    let dir = shards_dir();
    if !dir.is_dir() {
        eprintln!("skipping: {} does not exist", dir.display());
        return;
    }
    let mut count = 0;
    for entry in std::fs::read_dir(&dir).unwrap() {
        let path = entry.unwrap().path();
        if path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.eq_ignore_ascii_case("bfgs"))
            != Some(true)
        {
            continue;
        }
        let shard = Shard::open(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let r = shard.record(0).unwrap_or_else(|| {
            panic!(
                "{}: record(0) returned None despite record_count={}",
                path.display(),
                shard.record_count()
            )
        });
        // Sanity check: lat/lon must be on Earth.
        assert!(
            (-90.0..=90.0).contains(&r.lat),
            "{}: lat={} out of range",
            path.display(),
            r.lat
        );
        assert!(
            (-180.0..=180.0).contains(&r.lon),
            "{}: lon={} out of range",
            path.display(),
            r.lon
        );
        eprintln!(
            "[ok] {}  country={}  rec0=({:.4},{:.4}) {:?}",
            path.display(),
            shard.country().iso2(),
            r.lat,
            r.lon,
            r.street.chars().take(20).collect::<String>()
        );
        count += 1;
    }
    assert!(count > 0, "no shards in {}", dir.display());
}
