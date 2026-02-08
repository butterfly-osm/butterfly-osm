# Immediate Status

No active sprint. All audit findings remediated (butterfly-route HIGHs + CRITICALs + remaining HIGHs + L3).

---

## Previously Completed Sprints

### J-Sprint: Audit Remediation — CRITICALs, remaining HIGHs, L3 (2026-02-08) — DONE

| Task | ID | Fix |
|------|----|-----|
| Fix broken CI benchmark job | C1 | Removed fake benchmarks/ reference. Added real release build verification with binary `--help` checks. |
| Add c-bindings tests to CI | H3 | Added `cargo test -p butterfly-dl --features c-bindings` to test matrix. |
| Fix CI target dir caching | L2 | Cache only `~/.cargo/registry` + `~/.cargo/git`, not `target/`. |
| FFI panic safety (`catch_unwind`) | C2 | Wrapped all 5 `extern "C"` functions in `catch_unwind`. `RUNTIME` changed from `.expect()` to `.ok()` returning `Option<Runtime>`. `butterfly_version()` uses static byte string (infallible). |
| FFI use-after-free analysis | C3 | FALSE POSITIVE. `block_on()` blocks calling thread — `user_data` guaranteed valid. Added explicit safety comments. |
| Add `butterfly_last_error_message()` | H1 | Thread-local `LAST_ERROR` stores full error `Display` output. New `butterfly_last_error_message()` returns detailed string (caller frees). |
| Document FFI threading model | H2 | Added module-level doc comment explaining `block_on` semantics, concurrency safety, and Tokio context restriction. |
| Document static source list | H4 | Added design rationale comment on `VALID_SOURCES_CACHE`: static list avoids Geofabrik API dependency, covers ~120 regions. |
| Move Makefile to butterfly-dl | H5 | Moved `Makefile` to `tools/butterfly-dl/Makefile`. Updated all cargo commands to use `-p butterfly-dl`. |
| Document fuzzy matching weights | H9 | Added inline comments for all weights: 0.7/0.3 JW/Lev split, +20% prefix, +12% substring, +10% length, -10% anti-bias, 0.65 threshold. |
| Fix simplification tolerance | L3 | Simplify in grid coordinates (Mercator) before WGS84 conversion. Tolerance = `simplify_tolerance_m / cell_size_m`. Eliminates 36% lat/lon distortion. |

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
| M1-M2 (FFI unsafe scopes, non-UTF8 paths) | Low impact, butterfly-dl only |
| M3-M4 (step6 perf: PQ + Vec adjacency) | Not a bottleneck in practice |
| M5 (anyhow in library code) | Style issue, not a bug |
| M6 (contour holes / multi-polygon) | No real-world impact |
| M7 (rate limiting) | Deployment-level concern |
| M8-M9 (cross-compile, Makefile install) | Low priority |
| L1 (Windows DLL naming) | Low priority |
| L4 (pre-existing failing test) | butterfly-dl, not our bug |
| L5 (semver precision) | Low priority |
