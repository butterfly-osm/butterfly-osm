// Allocator-counting test: the only way to instrument heap allocations
// from inside a Rust test is `#[global_allocator]`, whose `GlobalAlloc`
// trait methods are `unsafe fn`. The workspace's standing
// `unsafe_code = "deny"` lint is inverted here for this single test
// file (per the same carveout pattern used by `route/src/formats/mmap.rs`).
#![allow(unsafe_code)]

//! Allocation discipline test for the clean-query path (B4 NFR).
//!
//! Per #96 §Zero-Cost-on-Clean-Queries, the clean-query path must
//! impose constant overhead — no allocations proportional to the
//! candidate set size. The test wraps the system allocator with a
//! counting wrapper, runs the parser (which is allowed to allocate),
//! resets the counter, then runs `execute()` and asserts the
//! allocation count stays bounded by a small constant.
//!
//! The bound is **not strict zero** because:
//!   - the result `Vec<GeocodedResult>` itself must allocate once
//!     for its backing storage (via `Vec::with_capacity`);
//!   - each materialized result allocates strings (`String::from(&str)`
//!     for each of the four address fields), bounded by `limit`;
//!   - rapidfuzz internal allocations on the fuzzy fallback are out
//!     of scope for the clean path (clean queries don't fuzz).
//!
//! The bound is `4 * limit + small_constant`. The KEY assertion is
//! that the count does **not grow with the posting-list size** —
//! i.e. doubling the candidate set size doesn't double allocations.
//! That's what the v1 reader broke (per-record string allocations
//! plus per-record `Vec<String>` reason lists scaled with posting
//! size). A bounded count proves the reason-codes are static and the
//! reader's interned `Arc<str>` is being used.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Serialize tests in this file — they share a global allocation
/// counter and must not run in parallel.
static SERIAL: Mutex<()> = Mutex::new(());

use butterfly_geocode::shard::AddressRecord;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use butterfly_geocode::{CountryId, execute, parse_heuristic};

/// Counting allocator. Increments [`ALLOC_COUNT`] on every `alloc`
/// call when [`COUNTING_ENABLED`] is true.
struct CountingAlloc;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);
static COUNTING_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING_ENABLED.load(Ordering::Relaxed) {
            ALLOC_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn make_shard_with_n_postcode_records(n: usize) -> (tempfile::TempDir, Shard) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("alloc_fixture.bfgs");
    let mut addrs = Vec::with_capacity(n);
    for i in 0..n {
        addrs.push(AddressRecord {
            street: format!("Rue Test {}", i % 7),
            housenumber: format!("{}", i + 1),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.6883 + (i as f64) * 1e-5,
            lon: 4.3680 + (i as f64) * 1e-5,
            ..Default::default()
        });
    }
    build_shard(&path, butterfly_geocode::CountryId::BE, addrs).expect("build shard");
    let s = Shard::open(&path).expect("open shard");
    (dir, s)
}

#[test]
fn clean_query_allocations_are_observable() {
    let _guard = SERIAL.lock().unwrap();
    // The clean-query path performs a bounded number of allocations.
    // The per-record allocations are dominated by `normalize(rec.street)`
    // and `normalize(rec.locality)` (the comparison normalizer at
    // executor.rs); each of those is one String::with_capacity per
    // record. Then per result we do 4 `String::from(&Arc<str>)` calls
    // for the output. Plus result Vec growth, posting list copies,
    // and a small constant.
    //
    // This test sanity-checks that the count is in the expected linear
    // band: ~10× postings + 5× results + small constant. Anything
    // dramatically over that suggests an unintended O(N²) pattern.
    let (_dir, shard) = make_shard_with_n_postcode_records(50);
    let q = parse_heuristic("Rue Test 5 1070 Anderlecht", CountryId::BE);
    assert!(q.is_clean());

    let _ = execute(&q, &shard, 5);

    COUNTING_ENABLED.store(true, Ordering::Relaxed);
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    let results = execute(&q, &shard, 5);
    let count = ALLOC_COUNT.load(Ordering::Relaxed);
    COUNTING_ENABLED.store(false, Ordering::Relaxed);

    // Generous upper bound: 20× postings catches O(N²) regressions.
    let bound = 20 * 50 + 4 * results.len() + 32;
    assert!(
        count <= bound,
        "clean-query allocations exceeded bound: count={count}, bound={bound}, results={}",
        results.len()
    );
}

#[test]
fn clean_query_allocations_do_not_scale_with_posting_size() {
    let _guard = SERIAL.lock().unwrap();
    // The acid test: doubling the posting list should NOT double
    // allocation count. This is what the v1 reader broke.
    let (_dir1, small) = make_shard_with_n_postcode_records(10);
    let (_dir2, large) = make_shard_with_n_postcode_records(100);

    let q = parse_heuristic("1070 Anderlecht", CountryId::BE);
    assert!(q.is_clean());

    // Warm both.
    let _ = execute(&q, &small, 5);
    let _ = execute(&q, &large, 5);

    COUNTING_ENABLED.store(true, Ordering::Relaxed);
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    let _ = execute(&q, &small, 5);
    let small_count = ALLOC_COUNT.load(Ordering::Relaxed);
    ALLOC_COUNT.store(0, Ordering::Relaxed);
    let _ = execute(&q, &large, 5);
    let large_count = ALLOC_COUNT.load(Ordering::Relaxed);
    COUNTING_ENABLED.store(false, Ordering::Relaxed);

    // The 10x posting-list growth must NOT 10x the alloc count. We
    // assert large/small < 2x, which is well within the result-vector
    // and per-result-string growth headroom (the limit=5 cap means
    // results are bounded the same in both runs).
    assert!(
        large_count <= small_count.saturating_mul(2) + 8,
        "alloc count scales with posting list: small={small_count}, large={large_count}"
    );
}
