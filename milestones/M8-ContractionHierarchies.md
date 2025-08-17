# M8 — Contraction Hierarchies (3 micro-milestones)

## M8.1 — Graph Ordering
**Why**: Hierarchy for fast shortest-path queries
**Artifacts**: Nested dissection order, level computation, ordering watchdog (auto-coarsen separators if >2h wall-time)
**Safety**: Prevent "ages" stalls on planet by dropping to coarser min-cell size automatically
**Commit**: `"M8.1: CCH ordering + watchdog"`

## M8.2 — Profile Customization
**Why**: Mode-specific shortcut computation
**Artifacts**: Per-profile CCH customization, upward CSR with shortcuts
**Commit**: `"M8.2: CCH customization"`

## M8.3 — Bidirectional Queries
**Why**: High-performance exact routing
**Artifacts**: Bidir CCH implementation, performance validation
**Commit**: `"M8.3: bidirectional CCH"`

**🔄 PRS v5**: CCH correctness + performance SLA + **ROUTER CORE PRODUCTION-READY**