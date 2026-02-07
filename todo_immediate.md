# Immediate Status

## I-Sprint: Audit Remediation — butterfly-route HIGH findings (2026-02-07)

| Task | ID | Status |
|------|----|--------|
| H6: Upgrade `source_idx` from `u16` to `u32` in bucket M2M | H6 | Done |
| H7: Replace `unwrap()` in production API code paths | H7 | Done |
| H8: Add `debug_assert!` bounds checks in step7 unsafe blocks | H8 | Done |

### H6: source_idx u16 → u32 (`bucket_ch.rs`)

**Problem:** `source_idx as u16` silently truncates if >65,535 sources. Matrix results silently corrupt.

**Fix:** Change `u16` → `u32` throughout:
- `BucketEntry.source_idx: u16` → `u32`, remove `_pad: u16`
- `PrefixSumBuckets.source_indices: Vec<u16>` → `Vec<u32>`
- All bucket item tuples: `(u32, u16, u32)` → `(u32, u32, u32)`
- Function signatures: `source_idx: u16` → `u32`
- `SortedBucketItems` and `BucketCollector`

**Memory impact:** Zero for tuples (padding was already 12 bytes). BucketEntry stays 8 bytes (was 4+2+2pad, now 4+4). SoA source_indices: +2 bytes/item (transient, acceptable).

### H7: Remove unwrap() from API code paths (`api.rs`)

**Problem:** 4 unwrap() calls in production handlers can panic on unexpected input.

**Fix:**
- Lines 1394, 1819: `.body(...).unwrap()` on Response builder → use `unwrap_or_else` returning 500
- Lines 2016, 2153: `state_opt.as_mut().unwrap()` after guaranteed `Some` init → use `get_or_insert_with()`

### H8: Bounds-check unsafe blocks in step7 (`step7.rs`)

**Problem:** 8 unsafe blocks write to shared Vec via raw pointers assuming disjoint ranges. No runtime verification.

**Fix:** Add `debug_assert!(pos < array.len())` before each unsafe write. This verifies the invariant in debug builds with zero cost in release.

---

## Previously Completed Sprints

### H-Sprint: Production Hardening (2026-02-07) — DONE
### G-Sprint: Polish (2026-02-07) — DONE
### F-Sprint: Tier 2 Features (2026-02-07) — DONE
### E-Sprint: Tier 1 API Features (2026-02-07) — DONE

---

## Deferred Items

| Item | Reason |
|------|--------|
| Map matching (F4) | High complexity, needs HMM on CCH |
| Two-resolution isochrone mask (D8) | Current quality acceptable |
| Audit findings C1-C3, H1-H5, H9 (butterfly-dl / CI) | Separate sprint, not butterfly-route |
