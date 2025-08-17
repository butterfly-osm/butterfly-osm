# M0 — Foundation (6 micro-milestones)

## ✅ M0.1 — Workspace & CI (COMPLETED)
**Why**: Reproducible 9-crate workspace with CI gates
**Artifacts**: All crates compile, clippy/rustfmt green, Axum+Utoipa skeleton
**Status**: ✅ **COMPLETED** - All 46 tests passing, CI working, 9-crate workspace established
**Achievements**:
- ✅ Created 9-crate workspace: butterfly-{common,dl,io,plan,extract,geometry,routing,serve,test}
- ✅ Moved butterfly-dl from tools/ to root level for consistency
- ✅ All crates compile with clippy/rustfmt green (zero warnings)
- ✅ GitHub Actions CI with formatting, clippy, build, test gates
- ✅ Basic Axum+Utoipa server skeleton in butterfly-serve with health endpoint
- ✅ Test infrastructure: 46 tests passing (37 butterfly-dl + 9 butterfly-common)
- ✅ Fixed integration test paths after workspace refactor
**Commits**: 
- `ddabbb7`: M0.1: workspace + CI + server skeleton
- `30b2c9c`: fix(tests): correct workspace root path in butterfly-dl integration tests

## 🔄 M0.2 — Binary Formats Core (IN PROGRESS)
**Why**: BFLY headers + chunked I/O foundation
**Artifacts**: 32-byte headers, zstd+TOC, CRC32+XXH3, read/write round-trips, 4KiB I/O alignment, `preadv/pwritev`, `madvise` hints (SEQUENTIAL/RANDOM), chunk size auditor (auto-adjust zstd level)
**I/O Target**: ≥1.5 GB/s aggregate zstd-3 write throughput on 16C/32T
**Status**: 🔄 **IN PROGRESS** - Basic structure in butterfly-io, needs full implementation
**Progress**:
- ✅ butterfly-io crate created with error types
- ✅ Basic BFLY header structure (32-byte layout)
- ✅ Stub AlignedIo with madvise hints API
- ❌ TODO: Implement actual I/O operations with preadv/pwritev
- ❌ TODO: Add CRC32 and XXH3 checksum implementations
- ❌ TODO: Implement chunk size auditor and zstd auto-adjustment
**Commit**: `"M0.2: binary headers + chunked I/O + aligned writes"`

## ⏳ M0.3 — External Sorter (PENDING)
**Why**: Spill/merge infrastructure for planet-scale builds
**Artifacts**: ExternalSorter trait, loser-tree k-way merge (low branch misprediction), grouped fsync (TOC+footer), RSS sampler with auto-throttle at 90% usable_mb
**Memory Enforcement**: Live RSS tracking via `/proc/self/statm` (250ms), token bucket worker admission control
**Status**: ⏳ **PENDING** - Awaiting M0.2 completion
**Commit**: `"M0.3: external sorter + memory throttling"`

## ⏳ M0.4 — Autopilot Skeleton (PENDING)
**Why**: Memory planning + override scaffolding
**Artifacts**: `butterfly-plan` crate, fixed heuristics, env/config/CLI overrides, `--validate-plan`/`--debug-plan`, `--deterministic` mode
**Validation**: `--validate-plan` prints full inequality with numbers (workers × per_worker_mb + io_buffers_mb + merge_heaps_mb ≤ usable_mb)
**Deterministic**: Fixed zstd dicts disabled, fixed worker count, fixed run size/fan-in, no auto-throttle
**Status**: ⏳ **PENDING** - Basic structure exists, needs CLI and validation implementation
**Progress**:
- ✅ butterfly-plan crate with config/memory/planner modules
- ✅ Basic MemoryBudget calculation (75-80% safety margin)
- ✅ PlanConfig with deterministic mode support
- ❌ TODO: CLI interface with --validate-plan/--debug-plan
- ❌ TODO: Environment variable and TOML config loading
- ❌ TODO: Detailed budget validation with numeric output
**Commit**: `"M0.4: planner skeleton + validation + deterministic"`

## ✅ M0.5 — Geometry Traits (COMPLETED)
**Why**: API stability for streaming geometry pipeline
**Artifacts**: `ResampleArcLen`, `SimplifyNav`, `DeltaEncode` traits with naïve implementations
**Status**: ✅ **COMPLETED** - All geometry traits implemented with stub behavior
**Achievements**:
- ✅ butterfly-geometry crate with 3-pass pipeline traits
- ✅ ResampleArcLen trait for Pass A (snap skeleton)
- ✅ SimplifyNav trait for Pass B (navigation grade)
- ✅ DeltaEncode trait for Pass C (full fidelity)
- ✅ Point2D structure and stub implementations
- ✅ All traits compile and have basic test coverage
**Commit**: `"M0.5: geometry traits (stubs)"`

## ⏳ M0.6 — Test Infrastructure (PENDING)
**Why**: Synthetic data + real-world corpus + micro-benchmarks
**Artifacts**: Shape generators, Monaco `-latest` fetch, seed problematic regions
**Status**: ⏳ **PENDING** - Basic structure exists, needs full implementation
**Progress**:
- ✅ butterfly-test crate with generators/corpus/benchmarks modules
- ✅ Basic synthetic linestring generator
- ✅ Monaco data fetch stub using butterfly-dl
- ✅ Simple benchmark runner with timing
- ❌ TODO: Shape generators for problematic regions
- ❌ TODO: Real Monaco data fetching and validation
- ❌ TODO: Micro-benchmark suite for geometry operations
**Commit**: `"M0.6: test corpora + micro benches"`

---

## M0 Foundation Progress Summary

**Overall Status**: 🔄 **2/6 COMPLETED** (33% complete)

### Completed ✅
- **M0.1**: Workspace & CI - Full 9-crate workspace with tests passing
- **M0.5**: Geometry Traits - Complete 3-pass pipeline trait definitions

### In Progress 🔄  
- **M0.2**: Binary Formats Core - Structure exists, needs I/O implementation

### Pending ⏳
- **M0.3**: External Sorter - Awaiting M0.2 foundation
- **M0.4**: Autopilot Skeleton - Basic structure exists, needs CLI
- **M0.6**: Test Infrastructure - Framework exists, needs full implementation

### Next Priority
**M0.2 Binary Formats Core** - Complete the I/O foundation that other milestones depend on.