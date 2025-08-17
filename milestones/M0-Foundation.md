# M0 — Foundation (6 micro-milestones)

## M0.1 — Workspace & CI
**Why**: Reproducible 9-crate workspace with CI gates
**Artifacts**: All crates compile, clippy/rustfmt green, Axum+Utoipa skeleton
**Commit**: `"M0.1: workspace + CI + server skeleton"`

## M0.2 — Binary Formats Core  
**Why**: BFLY headers + chunked I/O foundation
**Artifacts**: 32-byte headers, zstd+TOC, CRC32+XXH3, read/write round-trips, 4KiB I/O alignment, `preadv/pwritev`, `madvise` hints (SEQUENTIAL/RANDOM), chunk size auditor (auto-adjust zstd level)
**I/O Target**: ≥1.5 GB/s aggregate zstd-3 write throughput on 16C/32T
**Commit**: `"M0.2: binary headers + chunked I/O + aligned writes"`

## M0.3 — External Sorter
**Why**: Spill/merge infrastructure for planet-scale builds
**Artifacts**: ExternalSorter trait, loser-tree k-way merge (low branch misprediction), grouped fsync (TOC+footer), RSS sampler with auto-throttle at 90% usable_mb
**Memory Enforcement**: Live RSS tracking via `/proc/self/statm` (250ms), token bucket worker admission control
**Commit**: `"M0.3: external sorter + memory throttling"`

## M0.4 — Autopilot Skeleton
**Why**: Memory planning + override scaffolding
**Artifacts**: `butterfly-plan` crate, fixed heuristics, env/config/CLI overrides, `--validate-plan`/`--debug-plan`, `--deterministic` mode
**Validation**: `--validate-plan` prints full inequality with numbers (workers × per_worker_mb + io_buffers_mb + merge_heaps_mb ≤ usable_mb)
**Deterministic**: Fixed zstd dicts disabled, fixed worker count, fixed run size/fan-in, no auto-throttle
**Commit**: `"M0.4: planner skeleton + validation + deterministic"`

## M0.5 — Geometry Traits (Stubs)
**Why**: API stability for streaming geometry pipeline
**Artifacts**: `ResampleArcLen`, `SimplifyNav`, `DeltaEncode` traits with naïve implementations
**Commit**: `"M0.5: geometry traits (stubs)"`

## M0.6 — Test Infrastructure
**Why**: Synthetic data + real-world corpus + micro-benchmarks
**Artifacts**: Shape generators, Monaco `-latest` fetch, seed problematic regions
**Commit**: `"M0.6: test corpora + micro benches"`