# M11 — RPHAST Matrices (3 micro-milestones)

## M11.1 — Blocked Targets
**Why**: Efficient matrix computation
**Artifacts**: Target blocking strategy, streaming matrix writer
**Performance**: 1k×1k matrix ≤8s CPU-only
**Commit**: `"M11.1: blocked RPHAST"`

## M11.2 — GPU Readiness
**Why**: Prepare for GPU acceleration
**Artifacts**: GPU readiness flag, device detection, memory layout preparation
**Target**: 1k×1k matrix ≤2s GPU (M18)
**Commit**: `"M11.2: GPU readiness"`

## M11.3 — Matrix API
**Why**: HTTP endpoint for matrix queries
**Artifacts**: `/matrix` endpoint, efficient response format
**Commit**: `"M11.3: matrix API"`

**🔄 PRS v8**: Matrix correctness + CPU performance SLA + GPU preparation