# M20 — Partial & Distributed Builds (3 micro-milestones)

## M20.1 — Diff Detection
**Why**: Incremental planet updates
**Artifacts**: Diff-PBF tile detector, impact analysis, selective rebuild
**Performance**: Regional partial rebuild ≤60-120min
**Commit**: `"M20.1: diff detection"`

## M20.2 — CCH Reuse
**Why**: Avoid full recomputation when possible
**Artifacts**: Topology delta analysis, order reuse logic, customize-only rebuilds
**Condition**: Reuse CCH order if topology delta <X%; customize per profile only
**Commit**: `"M20.2: CCH reuse"`

## M20.3 — Distributed Building
**Why**: Multi-node planet processing
**Artifacts**: Distributed macro-tile builds (2+ nodes), TOC merge, coordination
**Benefit**: Ensure planet builds never "take ages" after initial setup
**Commit**: `"M20.3: distributed builds"`

**🔄 PRS v17**: Incremental correctness + distributed consistency + rebuild performance