# Immediate Status

No active sprint. All butterfly-route work is complete.

---

## Previously Completed Sprints

### I-Sprint: Audit Remediation — butterfly-route HIGHs (2026-02-07) — DONE

| Task | ID | Fix |
|------|----|-----|
| Upgrade `source_idx` from `u16` to `u32` in bucket M2M | H6 | Widened type across `bucket_ch.rs` and `nbg_ch/query.rs`. Zero memory cost. |
| Replace `unwrap()` in production API code paths | H7 | 2x `unwrap_or_else` (Response builders), 2x `get_or_insert_with` (PhastState). |
| Add `debug_assert!` bounds checks in step7 unsafe blocks | H8 | 8 bounds assertions before raw-pointer writes. Zero cost in release. |

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
| Audit findings C1-C3 (CI benchmarks, FFI panics, FFI use-after-free) | butterfly-dl / CI scope, not butterfly-route |
| Audit findings H1-H5, H9 (FFI errors, threading, CI gaps, sources, Makefile, magic numbers) | butterfly-dl / CI / butterfly-common scope |
| Audit findings M1-M9 (unsafe scopes, step6 perf, anyhow, contour holes, rate limiting) | Backlog priority |
| Audit findings L1-L5 (DLL naming, CI caching, simplify tolerance, failing test, semver) | Low priority |
