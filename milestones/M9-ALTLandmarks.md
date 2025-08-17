# M9 — ALT Landmarks (3 micro-milestones)

## M9.1 — Landmark Selection
**Why**: Efficient heuristic computation for CCH+ALT
**Artifacts**: 16-32 landmarks via farthest-point heuristic on core, landmark placement validation
**Commit**: `"M9.1: landmark selection"`

## M9.2 — Distance Tables
**Why**: Precomputed distances for admissible heuristics
**Artifacts**: Parallel landmark distance computation, compressed distance tables
**Commit**: `"M9.2: landmark tables"`

## M9.3 — ALT Integration
**Why**: Query-time potential functions
**Artifacts**: Admissible potentials, relaxation counter (target ≥60% reduction), query tracing
**Commit**: `"M9.3: ALT integration + tracing"`

**🔄 PRS v6**: ALT correctness + heuristic effectiveness + query performance