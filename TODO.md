# butterfly-osm Master Plan

**Micro-milestone plan** for **butterfly-osm** with atomic, self-contained steps. Each milestone splits into focused sub-tasks with explicit artifacts, tests, and commit points.

## Core Architecture: 3-Pass Geometry + Dual Cores + Multi-Profile

### 3-Pass Geometry Pipeline
* **Pass A — `snap.skel`**: Arc-length resampling + 12–15° angle guards → universal snapping (always mapped)
* **Pass B — `nav.simpl`**: RDP + prefilter, 1.0–5.0m epsilon → turn-by-turn steps (lazy mapped)  
* **Pass C — `nav.full`**: Delta + 0.2–0.3m noise removal → debug/export (optional, lazy mapped)

### Dual Core Strategy
* **Time Graph**: CSR + weights + turns + Snap Layer (`snap.rtree` + `snap.skel`) — **no geometry**
* **Nav Graph**: Same CSR + `nav.simpl`/`nav.full` + `names.dict` — **lazy geometry mapping**

### Multi-Profile System (Car/Bike/Foot)
* **Profile Regression Suite (PRS)**: Mandatory after M4 for all milestones touching graph/weights/serve
* **Access isolation**: Car (driveable), Bike (cycleway preference), Foot (paths/footways)
* **Profile-aware snapping**: Correct edge selection per mode with ±35° heading tolerance

### Autopilot Planner
* **Compile-time cap**: `const BFLY_MAX_RAM_MB: u32 = 16_384`
* **Conservative budgeting**: 75-80% allocation + RSS tracking + fail-fast + auto-fallback
* **Override hierarchy**: Env vars > `butterfly.toml` > CLI flags + `--expert-mode`
* **Evolution path**: M0 fixed heuristics → M1+ telemetry-driven adaptive planning

### Production Readiness Timeline
* **M5**: Autopilot "production-ready for build system" 
* **M8**: Router core "production-ready" (time-only endpoints)
* **M12**: Full service ready (turn-by-turn steps)

**Architecture progression**: Monaco → Luxembourg → Belgium → France → Planet

---

## 🚀 Current Implementation Status

### M0 — Foundation (33% Complete - 2/6 milestones)
- ✅ **M0.1**: Workspace & CI - 9-crate workspace, all tests passing (46 tests), CI configured
- 🔄 **M0.2**: Binary Formats Core - Structure exists, needs I/O implementation 
- ⏳ **M0.3**: External Sorter - Awaiting M0.2 completion
- ⏳ **M0.4**: Autopilot Skeleton - Basic structure exists, needs CLI
- ✅ **M0.5**: Geometry Traits - Complete 3-pass pipeline trait definitions
- ⏳ **M0.6**: Test Infrastructure - Framework exists, needs full implementation

**Next Priority**: Complete M0.2 Binary Formats Core - the I/O foundation other milestones depend on

### M1-M20 — Advanced Milestones (Not Started)
All subsequent milestones await M0 foundation completion. See individual milestone files for detailed specifications.

---

# Performance Targets: Beat OSRM/Valhalla

## Query-Time SLAs (single socket, 16C/32T, NVMe)
* **p2p time-only (CCH+ALT, near)**: p50 **< 3 ms**, p95 **< 10 ms** per profile
* **p2p long-haul (CCH+ALT+TNR)**: p50 **< 2 ms**, p95 **< 6 ms**
* **Matrices (RPHAST, 1k×1k)**: **≤ 8 s** CPU; **≤ 2 s** GPU (M18)
* **Isochrone (PHAST, single origin 15-min)**: **≤ 60 ms** CPU; **≤ 15 ms** GPU (M17)
* **Turn-by-turn**: routing time identical to time-only; geometry unpack adds **≤ 1 ms** median
* **Snap performance**: p50 **< 1 ms**, p95 **< 4 ms** (warm)

## Serve RAM Budget (all three profiles loaded)
* **Time Graph + Snap Layer mapped**: **≤ 6–8 GB** RSS (no nav geometry mapped)
* **Nav geometry mapped (warm)**: add **~1–2 GB** for hot chunks; still **≤ cap**

## Planet Build SLAs (16C/32T, NVMe 2–3.5 GB/s, cap=16 GB)
* **Total planet build**: **≤ 10–12 h** (with `nav.full`), **≤ 8–10 h** (skip `nav.full`)
* **CCH order + customization (all profiles)**: **≤ 2 h** inside that budget
* **Rebuild with small diffs (M20)**: **≤ 60–120 min** (regional partial rebuild)

### Per-Phase Budgets (Planet)
* PBF sieve + telemetry: **≤ 60–90 min**
* Canonicalization + super-edges: **≤ 90–120 min**
* 3-pass geometry (A+B, optionally C): **≤ 3–5 h**
* R-tree + auxiliary tables: **≤ 20–40 min**
* Weights/turns (3 profiles): **≤ 30–60 min**
* CCH order/customize: **≤ 90–120 min**

## Memory Budget Enforcement
```
cap_mb = BFLY_MAX_RAM_MB           // compiled constant (e.g., 16384)
usable_mb = floor(0.78 * cap_mb)   // 75–80% safety margin

per_worker_mb = zstd_buf(2–3MB) + geom_scratch(≤2MB) + sorter_stage(2–6MB)
io_buffers_mb = #active_streams * 2 * chunk_target_mb
merge_heaps_mb = fan_in * (record_sz + heap_overhead)
fixed_overhead_mb = 256–512

Constraint: fixed_overhead + workers*per_worker + io_buffers + merge_heaps ≤ usable_mb
```
**Fail-fast** if live RSS exceeds `usable_mb * 1.10` (hidden allocations/fragmentation)

---

# Micro-Milestone Overview

Each milestone splits into **atomic sub-steps** with standalone artifacts and clear commit points. From **M4 onward**, the **Profile Regression Suite (PRS)** runs after every milestone touching extract/serve.

## Individual Milestone Files

The detailed micro-milestone breakdowns have been split into individual files for better organization:

### Foundation Phase (M0-M3)
- **[milestones/M0-Foundation.md](./milestones/M0-Foundation.md)** - Workspace, CI, binary formats, external sorter, autopilot skeleton, test infrastructure
- **[milestones/M1-Telemetry.md](./milestones/M1-Telemetry.md)** - PBF reader, density tiles, adaptive planning, telemetry endpoint
- **[milestones/M2-Coarsening.md](./milestones/M2-Coarsening.md)** - Semantic breakpoints, curvature analysis, canonicalization, policy smoothing
- **[milestones/M3-SuperEdges.md](./milestones/M3-SuperEdges.md)** - Canonical adjacency, degree-2 collapse, border reconciliation, debug APIs

### Core Routing Phase (M4-M8)
- **[milestones/M4-MultiProfile.md](./milestones/M4-MultiProfile.md)** - Multi-profile system with car/bike/foot support (🚨 PRS starts here)
- **[milestones/M5-GeometryDualCores.md](./milestones/M5-GeometryDualCores.md)** - 3-pass geometry pipeline + dual cores + distance routing
- **[milestones/M6-TimeRouting.md](./milestones/M6-TimeRouting.md)** - Weight compression, turn tables, time-cost routing
- **[milestones/M7-ParallelServing.md](./milestones/M7-ParallelServing.md)** - Thread architecture, sharded caching, load testing
- **[milestones/M8-ContractionHierarchies.md](./milestones/M8-ContractionHierarchies.md)** - CCH ordering, customization, bidirectional queries

### Advanced Features Phase (M9-M13)
- **[milestones/M9-ALTLandmarks.md](./milestones/M9-ALTLandmarks.md)** - ALT landmark system for heuristic search
- **[milestones/M10-PHASTIsochrones.md](./milestones/M10-PHASTIsochrones.md)** - PHAST isochrone computation
- **[milestones/M11-RPHASTMatrices.md](./milestones/M11-RPHASTMatrices.md)** - RPHAST matrix computation with GPU readiness
- **[milestones/M12-TurnByTurn.md](./milestones/M12-TurnByTurn.md)** - Turn-by-turn navigation instructions
- **[milestones/M13-TNROverlay.md](./milestones/M13-TNROverlay.md)** - Transit network representation overlay

### Production Phase (M14-M20)
- **[milestones/M14-PlanetHardening.md](./milestones/M14-PlanetHardening.md)** - Resume manifests, dataset validation, hot-swap, observability
- **[milestones/M15-PerformanceHarness.md](./milestones/M15-PerformanceHarness.md)** - SLA enforcement, performance validation, load testing
- **[milestones/M16-GPUScaffolding.md](./milestones/M16-GPUScaffolding.md)** - CUDA detection, GPU memory layout, basic operations
- **[milestones/M17-GPUPHAST.md](./milestones/M17-GPUPHAST.md)** - GPU-accelerated PHAST isochrones
- **[milestones/M18-GPURPHAST.md](./milestones/M18-GPURPHAST.md)** - GPU-accelerated RPHAST matrices
- **[milestones/M19-MixedWorkloadScheduler.md](./milestones/M19-MixedWorkloadScheduler.md)** - Queue management, tail-latency protection
- **[milestones/M20-PartialDistributedBuilds.md](./milestones/M20-PartialDistributedBuilds.md)** - Incremental updates, distributed building

### Testing Framework
- **[milestones/PRS-ProfileRegressionSuite.md](./milestones/PRS-ProfileRegressionSuite.md)** - Comprehensive multi-profile testing framework

For a complete overview of all milestones, see **[milestones/README.md](./milestones/README.md)**.

Each milestone follows the same atomic sub-milestone pattern with **mandatory PRS** testing after M4.

---

# Profile Regression Suite (PRS) - Mandatory After M4

Runs automatically after **every milestone** that touches extract, serve, graph, weights, or routing:

## Test Categories (All 3 Profiles: Car/Bike/Foot)

### Access Legality
* **Synthetic truth tables**: 100+ junction combinations per profile
* **No illegal edges**: Cars never on footways, bikes respect bicycle=no, foot respects access=private
* **Turn restrictions**: Profile-specific enforcement (car U-turn penalties vs bike/foot minimal)
* **Fail-fast reporter**: Print first illegal edge with tags + profile for immediate triage

### Snapping Quality  
* **Spatial recall**: ≥98% within 5m on 5k random points per region
* **Profile-appropriate selection**: Cycleway vs road vs footway based on mode
* **Heading tolerance**: ±35° GPS heading alignment where available

### Routing Legality & Plausibility
* **Route validation**: 50 curated routes per profile per region  
* **Legal edges only**: No profile violations in computed paths
* **ETA plausibility**: Bike never faster than car on motorway segments; foot times monotonic
* **Time parity**: |Time Graph ETA - Nav Graph ETA| ≤ 0.5s always

### Performance Regression
* **Build thresholds**: Time +25%, RSS +20%, spill volume +60% vs baseline
* **Serve thresholds**: p2p CCH +15%, matrices/isochrones +20% vs baseline  
* **Quality gates**: Hausdorff p95 ≤5m, zero turn restriction violations

## PRS Evolution
* **v1 (M4)**: Basic access + echo routing
* **v2 (M5)**: + Snap quality + geometry validation  
* **v3 (M6)**: + ETA plausibility + turn legality
* **v4 (M7)**: + Parallel scaling + cache efficiency
* **v5 (M8)**: + CCH correctness + performance SLA

**All PRS versions are cumulative** - later versions include all previous tests plus new ones.

---

# Ready for Micro-Milestone Split

This master plan now provides:
✅ **Atomic sub-steps** with clear artifacts and commit points
✅ **Mandatory PRS** integration after M4 for all profile-touching changes  
✅ **Production readiness gates** at M5/M8/M12 with specific criteria
✅ **Self-contained structure** ready to split into individual milestone files

Each micro-milestone can become a standalone Markdown file following the **Why → What → How → Concurrency → Invariants & Safety → Tests → Gate → Commit** template.