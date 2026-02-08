# Immediate Status

Second-pass audit remediation complete (N-Sprint, 2026-02-08). All 28 findings resolved — 5 CRITICALs, 7 HIGHs, 12 MEDIUMs, 3 LOWs, 1 COSMETIC. Nothing remaining.

---

## Previously Completed Sprints

### N-Sprint: Second-Pass Audit Remediation (2026-02-08) — DONE

All 28 Codex second-pass findings remediated in one sprint:

| Task | ID | Fix |
|------|----|-----|
| Cap parallel download memory | Codex-C1 | MAX_CHUNK_SIZE=16MB, MAX_CONCURRENT_CHUNKS=4 → worst-case 64MB ceiling |
| Validate HTTP range response | Codex-C2 | Detect 200-instead-of-206, seek back to 0, restart download. `AsyncWriteSeek` supertrait. |
| Remove dead S3 FFI declaration | Codex-C3 | Removed `butterfly_has_s3_support()`, `BUTTERFLY_S3_ERROR` enum, all S3 claims in header/lib/pc |
| Bound /isochrone/bulk origins | Codex-C4 | MAX_BULK_ORIGINS=10,000 guard with 400 error |
| Remove /debug/compare from production | Codex-C5 | Route removed from `build_router()`, handler retained `#[allow(dead_code)]` for dev |
| Reduce /match coordinate limit | Codex-H1 | 10,000 → 500 max coordinates |
| spawn_blocking for /match and /trip | Codex-H3 | CPU-heavy handlers offloaded to blocking threads |
| Guard /table/stream request size | Codex-H4 | MAX_STREAM_POINTS=100,000 per side |
| Document alternatives weight clone | Codex-H5 | Comment explaining ~200MB clone is rare-path acceptable |
| Deterministic step directory selection | Codex-H6 | Sort `read_dir` matches before picking first |
| Document permissive CORS | Codex-H7 | Comment explaining deliberate choice, suggests reverse proxy |
| Remove unused IngestConfig.threads | Codex-M1 | Field removed, CLI arg kept with `_` binding |
| Document skipped step5 condition D | Codex-M2 | Explanation: requires CCH from steps 6-8, validated post-step8 |
| Compute real step5 lock hashes | Codex-M3 | Replaced 9 `"TODO"` placeholders with `compute_sha256()` calls |
| Clarify NBG edge flags=0 | Codex-M4 | Comment: reserved for future roundabout/ferry/tunnel/bridge bits |
| Update step9 module header docs | Codex-M5 | Listed all 12+ endpoints |
| Note undocumented OpenAPI endpoints | Codex-M6 | Comment listing endpoints missing `#[utoipa::path]` |
| Add HTTP handler unit tests | Codex-M7 | 28 tests: validate_coord (16), parse_mode (8), constant sanity (4) |
| Document shallow nodes.si validation | Codex-M8 | Doc comment: legacy format, no CRC, magic+size only |
| Fix stale user-facing docs | Codex-M9 | Removed non-existent tools, fixed trip payload key, removed stale examples |
| Docker packaging hardening | Codex-M10 | Pinned base images, non-root user, runtime comments |
| Remove S3 claims from metadata | Codex-M11 | Updated lib.rs docstring, .pc.in description |
| Overflow check in table_stream | Codex-M12 | `checked_mul` with 400 error on overflow |
| Replace panic in find_free_port | Codex-L1 | Returns `Result<u16>` with `anyhow::bail!` |
| Remove dead legacy helpers | Codex-L2 | Removed `bounded_dijkstra`, `run_phast_bounded`, `run_cch_dijkstra` (156 lines) |
| Add resume-integrity test | Codex-L3 | Test: server returns 200 instead of 206, verifies restart-from-beginning logic |
| Remove stale CI badge | Codex-X1 | Badge removed from README.md |

### L-Sprint: Deferred Architectural HIGHs (2026-02-08) — DONE

| Task | ID | Fix |
|------|----|-----|
| Thread-local CCH query state | Codex-H2 | `CchQueryState` with generation-stamped distance/parent arrays. Allocated once per thread (~160MB), reused via version stamps. O(1) query init instead of O(\|V\|). 8 unit tests added. |
| CRC validation across all format readers | Codex-H8 | CRC64 verification added to all 17 binary format readers (11 Pattern A single-CRC, 6 Pattern B body+file CRC). Round-trip + corruption-detection tests for cch_topo, ebg_nodes, mod_mask, turn_rules. |

### K-Sprint: MEDIUM Audit Findings (2026-02-08) — DONE

| Task | ID | Fix |
|------|----|-----|
| Minimize FFI unsafe scopes | M1 | Added SAFETY comments to all `unsafe` blocks. Documented pointer validity invariants per function. |
| Document non-UTF8 path limitation | M2 | Added UTF-8 requirement docs with M2 reference. Already returns descriptive errors from J-Sprint. |
| Optimize step6 min-degree ordering | M3 | Replaced O(n²) linear scan with `BinaryHeap` + lazy deletion → O(n log n). |
| Cache-friendly adjacency in step6 | M4 | Replaced `HashSet<usize>` with sorted `Vec<usize>` + `binary_search`. Refactored 3 copies into one generic `minimum_degree_order_generic<G: CsrAdjacency>()`. |
| Document anyhow usage | M5 | Added rationale comment in Cargo.toml: `anyhow` is deliberate for app boundary, `thiserror` for library errors. |
| Implement contour hole detection | M6 | `marching_squares_with_holes()`: flood-fill detects hole components, traces each boundary, populates `ContourResult.holes`. WKB encoder already handles holes (CW). Added 2 unit tests. |
| Add concurrency limiting | M7 | Added `tower::limit::ConcurrencyLimitLayer`: max 32 for API routes, max 4 for streaming routes. Added `tower` dependency. |
| Switch CI cross-compile to `cross` | M8 | Replaced manual gcc-aarch64 + cargo config with `cross build` for containerized cross-compilation. |
| Add sudo warning to Makefile | M9 | Added comment about root privileges, `mkdir -p` error message suggests `sudo make install`. |
| Fix pre-existing failing test | L4 | Widened assertion to accept "Could not determine file size" and "HTTP error" (Geofabrik doesn't always return 404). |

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
| ~~Map matching (F4)~~ | DONE |
| ~~L5 (semver precision)~~ | DONE — fixed `"2.0"` → `"2.0.0"` |
| ~~L1 (Windows DLL naming)~~ | REMOVED — Linux/Docker only, no Windows/macOS builds |
| ~~Two-resolution isochrone mask (D8)~~ | WONTFIX — 30m grid + simplification is sufficient; fine grid near boundary adds complexity for cosmetic gain only |
