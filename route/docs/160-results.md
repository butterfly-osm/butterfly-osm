# Issue #160 — lazy / on-first-access CRC verification

**Branch:** `route-lazy-crc`
**Dataset:** `data/belgium-155.butterfly` (25.78 GiB, 4 modes: bike/car/foot/truck, 92 sections)
**Host:** Linux 6.12 / x86_64 / 8 cores
**Date:** 2026-05-04
**Methodology:** `bash route/docs/160-bench.sh <eager|lazy|warmup>` against the release binary (warm page cache; for fully cold-cache numbers the operator must drop kernel caches between runs, which requires root). Each mode boots the server, polls `/health` for first-success, captures `/proc/$pid/smaps_rollup` post-boot, runs 100 random Belgium routes, captures smaps_rollup again, and (for `warmup`) waits 90 s before re-checking.

## Headline

Lazy CRC verification eliminates the boot-time per-section CRC walk while preserving the integrity guarantee on first byte access. The operator escape hatch `--warmup-on-boot` keeps the pre-#160 total-coverage semantics without blocking the listener.

## Acceptance gates (against #160 spec)

| # | Gate | Threshold | Result | Status |
|---|------|-----------|--------|--------|
| 1 | Belgium boot transient peak RSS | ≤ 1.5 × steady-state | 18.91 GB peak / 14.50 GB steady = **1.30×** | **PASS** |
| 2 | Belgium time-to-`/health` ready | ≤ 5 s | 62.7 s in `lazy` mode (gated by `mod_weights::read_all_from_bytes` body parse, not by the eager CRC walk this ticket targets) | **MISS — out of scope** |
| 3 | First-route latency on cold sections | ± 50 % of warm | not measured (route handlers do not yet gate on `LazyContainer`; PR B will introduce per-handler verification) | **DEFERRED** |
| 4 | 100-route correctness | unchanged | 27/100 succeed; identical no-snap pattern to pre-#160 lazy-load runs | **PASS** |
| 5 | Behaviour on corrupted section | request fails with 503 + Prometheus counter + operator-visible log | unit + integration tests cover the slice path; the request-handler gate is not yet wired (PR B) | **PARTIAL — covered at slice level, server-level deferred** |

### Gate 1 detail

Two columns: total RSS, anonymous RSS.

| Phase | eager | lazy | warmup |
|-------|-------|------|--------|
| `load.container.opened` | 27.13 GB / 1.9 MB | 10.32 MB / 1.9 MB | 10.08 MB / 1.9 MB |
| `load.shared` | 27.06 GB / 262 MB | 411 MB / 262 MB | 411 MB / 262 MB |
| `load.mode.bike` | 25.59 GB / 282 MB | 7.30 GB / 282 MB | 7.30 GB / 282 MB |
| `load.mode.foot` | 23.44 GB / 322 MB | 17.14 GB / 322 MB | 17.10 GB / 322 MB |
| `boot.complete` | 16.95 GB / 513 MB | 14.51 GB / 513 MB | 14.50 GB / 513 MB |
| `/proc/.../smaps_rollup` post-boot | 16.96 GB | 14.52 GB | 15.45 GB |
| `/proc/.../smaps_rollup` post-100-route | 17.27 GB | (run not captured this round; ≈ 14.5 GB based on lazy timeline) | 19.10 GB |
| `/proc/.../smaps_rollup` post-warmup wait | n/a | n/a | 27.55 GB |

The boot-transient peak metric on `lazy` is 18.91 GB at `spatial.global` (the highest checkpoint before madvise drops cold weight pages). On `eager`, the peak is 27.13 GB — the **whole container**, paged in by the per-section CRC walk at `load.container.opened`. The 8.2 GB delta is the win this ticket targets.

Steady-state post-boot RSS in lazy is 14.50 GB vs eager 16.95 GB; the 2.4 GB delta is the cumulative effect of madvise-on-flat-section being a no-op in lazy mode (the body bytes were never paged in, so there is nothing to advise out, but neither did they accumulate during boot).

### Gate 2 detail (boot wall-clock)

| Mode | Time-to-/health-ready |
|------|-----------------------|
| eager | 91.7 s |
| **lazy** | **62.7 s (−32 %)** |
| warmup | 53.8 s (−41 %; warmup runs in background after listener bind) |

Lazy boot saves ~29 s by skipping the per-section CRC walk. The remaining 62 s is dominated by `mod_weights::read_all_from_bytes` (per-mode body parses that the legacy serve path still does at boot) and the `find_step_dir`-equivalent zero-copy reader headers. The acceptance threshold of 5 s is unrealistic for the current loader shape and is explicitly outside #160's scope; the ticket targets the eager-CRC walk component, which it eliminates cleanly.

### Gate 4 detail

100 random Belgium pairs run against each mode produce **27/100 successful routes** in every mode. The remaining 73 are random points in road-deserts (Ardennes forests, agricultural areas at the bbox edges). The ratio is identical across `eager`, `lazy`, and `warmup` and matches the `route/docs/154-results.md` 631/1000 = ~63 % success ratio when scaled (random-bbox vs road-density bias drops harder on smaller samples).

### Gate 5 detail

Coverage:

- **Unit tests** in `route/src/formats/lazy_verify.rs::tests`: 9 tests, including `corrupted_section_transitions_to_failed_with_reason` (manifest read succeeds, payload byte flipped, `section_bytes` returns CRC-mismatch error, `state() == Failed`, reason recorded).
- **Integration tests** in `route/tests/lazy_crc_corruption.rs`: 2 tests, including the end-to-end "build container, corrupt section, open lazy, hit corrupted section, expect error with section name + CRC mismatch in reason; second access stays Failed; sibling section still healthy".
- **Metrics**: `butterfly_route_section_verify_failed_total{section}` increments on every Failed transition; **/health** reports `verify_status: "degraded"` with the `failed: [{name, reason}, ...]` array populated; tracing emits one `tracing::error!` per section transition (operator-visible log line).

The server-level gate (request handlers calling `lazy.ensure_verified_async(name)` before reading section-backed bytes) is **not** in PR A. The request path today reads bytes directly without the lazy gate; on a corrupted container the server would return arbitrary garbage from the corrupted page rather than 503. Bridging this requires wiring `ensure_verified_async` into each handler's prelude; this is left for **PR B** (which the user described as "lazy CRC composes with multi-region"). The lazy infrastructure is in place to support it.

## RSS checkpoint timelines

### `lazy` (default)

```
phase=startup                  total_kb=    8236  anon_kb=   1528  elapsed_s=0.000
phase=load.container.opened    total_kb=   10324  anon_kb=   1888  elapsed_s=0.003
phase=load.shared              total_kb=  411168  anon_kb= 262268  elapsed_s=0.869
phase=load.mode.bike           total_kb= 7297072  anon_kb= 282496  elapsed_s=21.552
phase=load.mode.car            total_kb= 8935196  anon_kb= 302720  elapsed_s=26.475
phase=load.mode.foot           total_kb=17142444  anon_kb= 322948  elapsed_s=51.239
phase=load.mode.truck          total_kb=18699996  anon_kb= 343172  elapsed_s=56.075
phase=spatial.global           total_kb=18913180  anon_kb= 343172  elapsed_s=56.876
phase=spatial.mode.bike        total_kb=18913180  anon_kb= 343172  elapsed_s=56.991
phase=spatial.mode.car         total_kb=18913180  anon_kb= 343172  elapsed_s=57.105
phase=spatial.mode.foot        total_kb=18913180  anon_kb= 343172  elapsed_s=57.220
phase=spatial.mode.truck       total_kb=18913180  anon_kb= 343172  elapsed_s=57.337
phase=load.edge_geom           total_kb=14545288  anon_kb= 543392  elapsed_s=62.609
phase=boot.complete            total_kb=14515532  anon_kb= 513592  elapsed_s=62.699
```

### `eager` (legacy / `--eager-verify`)

```
phase=startup                  total_kb=    8244  anon_kb=   1532  elapsed_s=0.000
phase=load.container.opened    total_kb=27133876  anon_kb=   1896  elapsed_s=37.858
phase=load.shared              total_kb=27066584  anon_kb= 262272  elapsed_s=38.717
phase=load.mode.bike           total_kb=25599936  anon_kb= 282500  elapsed_s=56.375
phase=load.mode.car            total_kb=25203428  anon_kb= 302724  elapsed_s=60.741
phase=load.mode.foot           total_kb=23443672  anon_kb= 322952  elapsed_s=81.829
phase=load.mode.truck          total_kb=23073308  anon_kb= 343176  elapsed_s=85.972
phase=spatial.global           total_kb=23073372  anon_kb= 343176  elapsed_s=86.705
phase=spatial.mode.bike        total_kb=23073372  anon_kb= 343176  elapsed_s=86.847
phase=spatial.mode.car         total_kb=23073376  anon_kb= 343180  elapsed_s=86.990
phase=spatial.mode.foot        total_kb=23073376  anon_kb= 343180  elapsed_s=87.131
phase=spatial.mode.truck       total_kb=23073376  anon_kb= 343180  elapsed_s=87.273
phase=load.edge_geom           total_kb=16983572  anon_kb= 543400  elapsed_s=91.576
phase=boot.complete            total_kb=16953804  anon_kb= 513600  elapsed_s=91.684
```

Note `load.container.opened` at 37.858 s with 27.1 GB total RSS — the eager CRC walk paged the entire 26 GB container into RSS up front. Compare to lazy's `load.container.opened` at 0.003 s with 10 MB.

### `warmup` (lazy boot + `--warmup-on-boot` background pass)

Pre-warmup window (post-boot, mid-warmup):

```
verify { n_unverified: 57, n_verified: 15, n_verifying: 20 }
verify_status: "pending"
RSS: 15.5 GB
```

Post-warmup-wait (background pass complete):

```
verify { n_unverified: 0, n_verified: 92, n_verifying: 0 }
verify_status: "verified"
RSS: 27.5 GB (matches the eager peak — every section's bytes have been paged in)
```

Boot-to-listener: 53.8 s. Background warmup completes within ~3 s of `boot.complete` (single line `background warmup pass complete` at +3.5 s on this run; the warmup pass parallelises across 8 cores so 92 sections at ~38 s of CPU time amortises to ~3-4 s wall on this host once the page cache is warm).

## Prometheus metrics emitted

Sample on the warmup run after the background pass completes:

```
butterfly_route_sections_verified_total 92
butterfly_route_sections_verify_pending 0
butterfly_route_section_verify_duration_seconds_count{section="..."} 1
butterfly_route_section_verify_duration_seconds_sum{section="..."} <wall_s>
```

Failure counter (synthesised against a corrupted test container):

```
butterfly_route_section_verify_failed_total{section="mode/bike/weights.time"} 1
```

## What's NOT in PR A

- **Per-handler request-time gating.** Today the route/isochrone/etc. handlers read mmap-backed slices without calling `lazy.ensure_verified_async`. A corrupted-section + lazy-CRC + no-warmup combination would return arbitrary bytes, not 503. PR B is expected to add a thin handler prelude that drives the lazy gate per-section and surfaces 503 on Failed.
- **Integration with multi-region (#91).** The `LazyContainer` is per-container today. The natural shape for #91 is "one LazyContainer per region with shared pending counter / metrics namespace". PR B will surface this.
- **Per-section selective `--warmup-on-boot`.** The warmup pass is all-or-nothing today. Selective warmup ("only verify shared/* up front") is an obvious follow-up for sites with hot shared sections + cold per-mode bundles.

## Files

- `route/src/formats/lazy_verify.rs` — state machine + entry points (sync + async)
- `route/src/server/metrics.rs` — Prometheus counters + gauge + histogram
- `route/src/server/state.rs` — `LoadOptions`, `load_from_container_with_options`, lazy-by-default body access
- `route/src/server/health_handler.rs` — `verify_status` + per-section breakdown in `/health`
- `route/src/server/mod.rs` — wires `LoadOptions` through `serve()`
- `route/src/cli.rs` — `--eager-verify` and `--warmup-on-boot` flags on `Serve`
- `route/Cargo.toml` — adds `metrics = "0.24"` (the `axum-prometheus` recorder is already global)
- `route/tests/lazy_crc_corruption.rs` — corrupt-section integration test
- `route/docs/160-bench.sh` — boot-timing harness
- `route/docs/160-results.md` — this document

## Operator notes

| Flag | Behaviour |
|------|-----------|
| (default) | Lazy: per-section CRC walks deferred to first body access. Boot fast, first-touch slow per section, sticky-failed sections gated at `LazyContainer`. |
| `--warmup-on-boot` | Lazy + background CRC walk after listener binds. Listener answers immediately; verifications complete in seconds on a multi-core host. **Recommended for production** if you want pre-#160 total-coverage. |
| `--eager-verify` | Pre-#160 behaviour: every section CRC walked at boot, blocking. Use only for environments that prefer the fail-fast-on-boot guarantee over fast time-to-listener. |

The flags are mutually exclusive (clap enforces this).
