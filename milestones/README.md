# Butterfly-OSM Milestone Index

This directory contains the detailed micro-milestone breakdowns for the butterfly-osm project. Each milestone is split into atomic, self-contained sub-tasks with explicit artifacts, tests, and commit points.

## Architecture Overview

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

## Milestone Files

### Foundation Phase
- **[M0-Foundation.md](./M0-Foundation.md)** - Workspace, CI, binary formats, external sorter, autopilot skeleton
- **[M1-Telemetry.md](./M1-Telemetry.md)** - PBF reader, density tiles, adaptive planning
- **[M2-Coarsening.md](./M2-Coarsening.md)** - Semantic breakpoints, curvature analysis, canonicalization
- **[M3-SuperEdges.md](./M3-SuperEdges.md)** - Adjacency, degree-2 collapse, border reconciliation

### Core Routing Phase
- **[M4-MultiProfile.md](./M4-MultiProfile.md)** - Access tables, profile masking, weights (🚨 PRS starts here)
- **[M5-GeometryDualCores.md](./M5-GeometryDualCores.md)** - 3-pass geometry, dual cores, distance routing
- **[M6-TimeRouting.md](./M6-TimeRouting.md)** - Weight compression, turn tables, time-cost routing
- **[M7-ParallelServing.md](./M7-ParallelServing.md)** - Thread architecture, sharded caching, load testing
- **[M8-ContractionHierarchies.md](./M8-ContractionHierarchies.md)** - CCH ordering, customization, bidirectional queries

### Advanced Features Phase
- **[M9-ALTLandmarks.md](./M9-ALTLandmarks.md)** - Landmark selection, distance tables, ALT integration
- **[M10-PHASTIsochrones.md](./M10-PHASTIsochrones.md)** - Single/multi-origin PHAST, isochrone API
- **[M11-RPHASTMatrices.md](./M11-RPHASTMatrices.md)** - Blocked targets, GPU readiness, matrix API
- **[M12-TurnByTurn.md](./M12-TurnByTurn.md)** - Geometry unpacking, name dictionary, instruction generation
- **[M13-TNROverlay.md](./M13-TNROverlay.md)** - TNR structure, strategy selection, multi-modal routing

### Production Phase
- **[M14-PlanetHardening.md](./M14-PlanetHardening.md)** - Resume manifests, dataset validator, hot-swap, observability
- **[M15-PerformanceHarness.md](./M15-PerformanceHarness.md)** - SLA enforcement, nav-off/on split, load testing
- **[M16-GPUScaffolding.md](./M16-GPUScaffolding.md)** - CUDA detection, memory layout, basic operations
- **[M17-GPUPHAST.md](./M17-GPUPHAST.md)** - GPU isochrones, integration
- **[M18-GPURPHAST.md](./M18-GPURPHAST.md)** - GPU matrices, optimization
- **[M19-MixedWorkloadScheduler.md](./M19-MixedWorkloadScheduler.md)** - Queue management, tail-latency protection
- **[M20-PartialDistributedBuilds.md](./M20-PartialDistributedBuilds.md)** - Diff detection, CCH reuse, distributed building

### Testing Framework
- **[PRS-ProfileRegressionSuite.md](./PRS-ProfileRegressionSuite.md)** - Comprehensive multi-profile testing framework

## Production Readiness Timeline
* **M5**: Autopilot "production-ready for build system" 
* **M8**: Router core "production-ready" (time-only endpoints)
* **M12**: Full service ready (turn-by-turn steps)

## Performance Targets: Beat OSRM/Valhalla
* **p2p time-only (CCH+ALT, near)**: p50 **< 3 ms**, p95 **< 10 ms** per profile
* **p2p long-haul (CCH+ALT+TNR)**: p50 **< 2 ms**, p95 **< 6 ms**
* **Matrices (RPHAST, 1k×1k)**: **≤ 8 s** CPU; **≤ 2 s** GPU (M18)
* **Planet build**: **≤ 10–12 h** total with **≤ 16 GB** RAM cap

Each milestone follows atomic sub-milestone patterns with **mandatory PRS** testing after M4.