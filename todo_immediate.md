# Immediate Status

## All Sprints Complete (2026-02-07)

Everything below is **done**. This file now serves as a reference of completed work.

---

## Completed Sprints

### H-Sprint: Production Hardening (2026-02-07)

| Task | Status |
|------|--------|
| H0: Upgrade all dependencies (axum 0.8, tower-http 0.6, utoipa 5, rand 0.9, geo 0.29, arrow 54) | Done |
| H1: Graceful shutdown (SIGINT + SIGTERM) | Done |
| H2: Structured logging (`tracing` + `tracing-subscriber`, text/JSON `--log-format`) | Done |
| H3: Request timeouts (120s API, 600s streaming) | Done |
| H4: Response compression (gzip + brotli) | Done |
| H5: Input validation (coordinate bounds, time_s 1-7200, number max 100) | Done |
| H6: Prometheus metrics (`GET /metrics`, per-endpoint histograms) | Done |
| H7: Enhanced health endpoint (uptime, node/edge counts, modes) | Done |
| H8: Panic recovery (`CatchPanicLayer`) | Done |
| H9: Dockerfile (multi-stage, `debian:bookworm-slim`) | Done |
| Workspace lint enforcement (`warnings = "deny"`, `clippy::all = "deny"`) | Done |
| 300+ clippy lint fixes across 80 files | Done |

**Verified:**
- Docker image builds and runs
- Health endpoint returns version, uptime, node/edge/road counts
- Input validation returns 400 for out-of-range params
- Response compression (gzip) confirmed
- Prometheus metrics with histograms
- Graceful shutdown on SIGTERM
- All 14 Belgium integration tests pass

### G-Sprint: Polish (2026-02-07)

| Task | Status |
|------|--------|
| G1: Road names in turn-by-turn (754K named roads from `ways.raw`) | Done |
| G2: Polygon output stability (CCW rings, 5-decimal precision, ring closure) | Done |
| G3: Arrow streaming cancellation (AtomicBool cooperative cancellation on disconnect) | Done |

### F-Sprint: Tier 2 Features (2026-02-07)

| Task | Status |
|------|--------|
| F1: Reverse isochrone (`direction=arrive`, plain linear scan reverse PHAST) | Done |
| F2: TSP/trip optimization (`POST /trip`, nearest-neighbor + 2-opt + or-opt) | Done |
| F3: Elevation/DEM (`GET /height`, SRTM .hgt, bilinear interpolation) | Done |
| F4: Map matching | Deferred |

### E-Sprint: Tier 1 API Features (2026-02-07)

| Task | Status |
|------|--------|
| E1: Geometry encoding (polyline6 + GeoJSON) | Done |
| E2: Distance matrix (meters, not just time) | Done |
| E3: Nearest/snap endpoint | Done |
| E4: Turn-by-turn instructions | Done |
| E5: Alternative routes | Done |

### Trust Package

| Task | Status |
|------|--------|
| OSRM parity suite (10K routes, 0.98 correlation) | Done |
| Debug fields (`debug=true`) | Done |
| Duration units (all APIs use seconds) | Done |
| Snap radius fix (5km max) | Done |

### Performance Optimizations

| Optimization | Result |
|--------------|--------|
| Block-gated PHAST (C1) | 18x isochrone latency improvement (90ms -> 5ms p50) |
| Source-block outer loop (A1) | 1.56x matrix speedup |
| Thread-local PHAST state (B1) | 7% isochrone speedup |
| SoA bucket layout | 24% backward phase speedup |
| O(1) prefix-sum bucket lookup | 7% join speedup |
| Bound-aware join pruning | 41% fewer joins |
| 4-ary heap with DecreaseKey | 26% heap speedup (0% stale entries) |
| K-lane batching (K=8) | 2.24x matrix, 2.63x isochrone |

---

## Deferred Items

| Item | Reason |
|------|--------|
| Map matching (F4) | High complexity, needs HMM on CCH |
| Two-resolution isochrone mask (D8) | Current quality acceptable |
| Hybrid exact turn model | Incompatible with CCH separator quality (abandoned after testing) |
| Small-table fast path | Not worth complexity, Butterfly wins at scale |
