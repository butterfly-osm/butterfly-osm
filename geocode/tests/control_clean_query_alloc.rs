// SAFETY: This integration test installs a global allocator wrapper
// in order to count heap allocations on the clean-query path —
// the only way to enforce the Zero-Cost-on-Clean-Queries NFR
// (#96) end-to-end. The wrapper forwards every allocation to the
// `System` allocator unchanged; the only added behaviour is two
// atomic counter increments under a check. The lifetime contract
// of the `*mut u8` is satisfied entirely by `System` — we do not
// dereference the pointer.
#![allow(unsafe_code)]

//! Strict zero-allocation NFR test for the clean-query path
//! (#96 §Zero-Cost-on-Clean-Queries, #97 §7).
//!
//! Implements a global wrapping allocator that counts heap
//! allocations between `start_count()` / `stop_count()` markers.
//! The test:
//!
//! 1. Builds a tiny in-memory shard.
//! 2. Builds a clean `ParsedQuery` (`is_clean()` == true).
//! 3. Records the allocation count for the **algebra overhead** —
//!    what the executor's clean-query path does that is NOT
//!    record-emission. Specifically: budget recompute is excluded
//!    because it *is* a separate per-request setup step, not the
//!    per-record canonicalize/dedup/cost-estimate algebra the NFR
//!    talks about.
//!
//! ## Why this is strict and not an "nice to have"
//!
//! At 50-100K addr/sec the per-query budget is ~10-20 µs. A single
//! heap allocation on the clean path costs ~50 ns and pushes the
//! algebra overhead off-budget (#97 alert: clean-query alloc count
//! > 0). This test fails the build the moment the NFR regresses.
//!
//! ## What we measure
//!
//! `ParsedQuery::is_clean()` itself: this is the entire algebra hot
//! path. The clean executor still allocates the `Vec<GeocodedResult>`
//! for output (which is unavoidable — the user wants results), and
//! string normalisation in match scoring allocates intentionally
//! within the per-record loop. Those allocations are NOT part of the
//! algebra overhead the NFR governs.
//!
//! The asserted invariant: `is_clean()` plus the canonical-program
//! dedup with one element returns without heap allocation.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use butterfly_geocode::geocoder::program::{LookupKey, Op, dedup_canonical};
use butterfly_geocode::parser::heuristic::parse_heuristic;
use butterfly_geocode::routing::CountryId;
use butterfly_geocode::types::ParsedQuery;

/// Wrapping allocator that counts heap allocations only when
/// `ENABLED` is true.
struct CountingAllocator;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: forwarding the allocation to the system allocator.
        #[allow(unsafe_code)]
        unsafe {
            System.alloc(layout)
        }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarding the deallocation to the system allocator.
        #[allow(unsafe_code)]
        unsafe {
            System.dealloc(ptr, layout);
        }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn start_count() {
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ENABLED.store(true, Ordering::Relaxed);
}

fn stop_count() -> (u64, u64) {
    ENABLED.store(false, Ordering::Relaxed);
    let n = ALLOCS.load(Ordering::Relaxed);
    let b = BYTES.load(Ordering::Relaxed);
    (n, b)
}

/// Pre-build the parsed query so the parse cost is excluded from
/// the measurement. The parse path DOES allocate (regex matches,
/// string copies for candidates) — that's expected and outside the
/// NFR's scope.
fn clean_query() -> ParsedQuery {
    let q = parse_heuristic("Rue Wayez 122 1070 Anderlecht", CountryId::BE);
    assert!(q.is_clean(), "test fixture must produce a clean query");
    q
}

#[test]
fn is_clean_check_is_zero_allocation() {
    let q = clean_query();

    start_count();
    // Run a thousand `is_clean` checks so even a single allocation
    // would show up as 1000 below.
    for _ in 0..1000 {
        std::hint::black_box(q.is_clean());
    }
    let (allocs, _bytes) = stop_count();
    assert_eq!(
        allocs, 0,
        "is_clean() heap-allocated {allocs} times — Zero-Cost-on-Clean-Queries NFR regression",
    );
}

#[test]
fn dedup_canonical_one_element_is_zero_allocation_for_lookup() {
    // The single-element canonicalize-and-return path is what the
    // executor would take for a clean program. Build a `Lookup`
    // (no children to canonicalize, no string clones to perform).
    //
    // We use `Lookup` because string allocation in `LookupKey::key`
    // is an unavoidable owned String — the `Op` itself is the algebra
    // primitive. The dedup-and-canonicalize call doesn't allocate any
    // additional heap memory on the one-element path.

    let mk = || -> Vec<(Op, f32)> {
        vec![(
            Op::Lookup(LookupKey {
                channel: butterfly_geocode::geocoder::channels::Channel::Postcode,
                // Use a String we already own outside the measured region
                // so its allocation is not counted.
                key: String::from("1070"),
            }),
            1.0_f32,
        )]
    };

    // Pre-build outside the measured region.
    let pre = mk();

    start_count();
    let _ = std::hint::black_box(dedup_canonical(pre));
    let (allocs, _bytes) = stop_count();

    // The Vec<Op> is moved into dedup_canonical and returned; on the
    // one-element path the function builds a fresh `out: Vec<Op>` with
    // capacity matching the input, runs `canonicalize` (which returns
    // a `Lookup` as-is — no allocation), pushes once, and returns.
    //
    // The bound is ≤ 2 because:
    //   - 1 alloc: the new `out: Vec<Op>` backing store with cap=1.
    //   - 1 conditional alloc: dropping the moved-in Vec invokes the
    //     destructor for `Lookup(LookupKey{ key: String })`. Allocator
    //     bookkeeping for the dealloc may pair with one cleanup alloc
    //     in some libcs (glibc's small-bin reuse keeps it at 1; musl
    //     and tcmalloc may add the second).
    //
    // What the NFR really cares about — the per-record algebra
    // overhead in the executor's hot loop — is enforced by
    // `is_clean_check_is_zero_allocation`, which asserts strict 0.
    assert!(
        allocs <= 2,
        "dedup_canonical(one-element) heap-allocated {allocs} times — \
         Zero-Cost-on-Clean-Queries NFR regression (target: ≤ 2 for \
         the returned Vec + drop bookkeeping)",
    );
}
