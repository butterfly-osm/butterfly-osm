# M2 — Adaptive Coarsening (5 micro-milestones)

## M2.1 — Semantic Breakpoints
**Why**: Preserve routing-critical topology changes
**Artifacts**: Name/ref/access/speed/layer/bridge/tunnel detection + turn restriction anchors
**Commit**: `"M2.1: semantic breakpoints"`

## M2.2 — Curvature Analysis
**Why**: Geometry-aware vertex retention
**Artifacts**: Local angle + cumulative bend windows, importance scoring, fast-path for <3° angles with arc-length guards
**Optimization**: Skip branches on straight segments (<3° + <A-spacing) for branch predictor efficiency
**Commit**: `"M2.2: curvature markers + fast-path"`

## M2.3 — Node Canonicalization
**Why**: Collision-safe coordinate merging
**Artifacts**: Grid hash + union-find, canonical ID mapping, illegal merge guards
**Commit**: `"M2.3: canonicalization"`

## M2.4 — Policy Smoothing
**Why**: Tile boundary consistency
**Artifacts**: 3×3 median smoothing, `coarsen.map`, `node_map.bin`, debug heatmaps
**Commit**: `"M2.4: coarsen artifacts"`

## M2.5 — `/probe/snap` API
**Why**: Early validation of canonical mapping
**Artifacts**: Snap probe endpoint for QA/debugging
**Commit**: `"M2.5: probe API"`