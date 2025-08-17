# M13 — TNR Overlay (3 micro-milestones)

## M13.1 — TNR Data Structure
**Why**: Transit network representation
**Artifacts**: TNR overlay graph, schedule integration
**Commit**: `"M13.1: TNR structure"`

## M13.2 — Strategy Selection
**Why**: Optimal routing strategy per query
**Artifacts**: Query classification, CCH vs TNR selection, fallback logic
**Coverage**: Target ≥95% TNR coverage for >100km trips; seamless CCH fallback
**Commit**: `"M13.2: strategy selection"`

## M13.3 — TNR Integration
**Why**: Multi-modal routing
**Artifacts**: TNR+CCH hybrid queries, schedule-aware routing
**Commit**: `"M13.3: TNR integration"`

**🔄 PRS v10**: TNR correctness + coverage metrics + fallback validation