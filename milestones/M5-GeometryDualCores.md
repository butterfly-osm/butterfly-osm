# M5 — 3-Pass Geometry + Dual Cores (7 micro-milestones)

## M5.1 — R-tree Index
**Why**: Spatial index for universal snapping
**Artifacts**: Bulk-loaded R-tree from super-edge bboxes, `snap.rtree`
**Commit**: `"M5.1: snap R-tree"`

## M5.2 — Pass A (Snap Skeleton)
**Why**: Fast universal snapping geometry
**Artifacts**: Arc-length resampling + angle guards, `snap.skel` chunks, heading samples (1-byte signed delta every ~30-50m)
**Spacing**: Urban: min(5m, r_local); Rural: 20-30m; Force keep on semantic breaks
**Optional**: Heading deltas for heading-aware snapping without loading B/C
**Commit**: `"M5.2: Pass A (snap.skel) + heading samples"`

## M5.3 — Pass C (Full Fidelity)
**Why**: High-fidelity geometry for debug/export
**Artifacts**: Delta encoding + minimal noise removal, `nav.full` (optional - skip for 8-10h planet SLA)
**Build Strategy**: Optional for first planet builds to meet time SLA; keep code path
**Commit**: `"M5.3: Pass C (nav.full) - optional"`

## M5.4 — Pass B (Navigation Grade)
**Why**: Optimized geometry for turn-by-turn routing
**Artifacts**: RDP + curvature prefilter online, RDP post-segment on small vectors, `nav.simpl` chunks with anchors every max(512m, 2×r_local)
**Quality Gates**: Median Hausdorff ≤2m, p95 ≤5m vs C (or raw if C disabled)
**Fallback**: Auto-fallback to multi-pass only for tiles with split-rate >10%
**Commit**: `"M5.4: Pass B (nav.simpl) + quality gates"`

## M5.5 — Single-Pass Integration
**Why**: Memory-efficient streaming pipeline
**Artifacts**: A→C online, B post-segment processing, bounded memory per worker
**Commit**: `"M5.5: streaming A→B→C pipeline"`

## M5.6 — Dual Core Construction
**Why**: Separate Time vs Nav graph optimization
**Artifacts**: Time Graph (no geometry), Nav Graph (with geometry), XXH3 consistency digests (blocking on build failure, re-checked at server boot)
**Consistency**: Digest mismatches fail build; re-verification at startup mandatory
**Commit**: `"M5.6: dual cores + blocking consistency"`

## M5.7 — Distance Routing
**Why**: Basic routing validation
**Artifacts**: `/route` with Dijkstra distance-based routing per profile
**Commit**: `"M5.7: distance routing"`

**🔄 PRS v2**: Snap recall per profile + geometry quality + routing legality + cold-IO test (0.1% requests p95 <20ms on chunk miss) + **AUTOPILOT PRODUCTION-READY**