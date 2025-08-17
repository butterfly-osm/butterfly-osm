# M3 — Super-Edge Construction (4 micro-milestones)

## M3.1 — Canonical Adjacency
**Why**: Build graph topology over canonical nodes
**Artifacts**: Adjacency lists, neighbor tracking
**Commit**: `"M3.1: canonical adjacency"`

## M3.2 — Degree-2 Collapse
**Why**: Reduce topology while preserving decision points
**Artifacts**: Policy-aware chain collapse, super-edge creation, segment guards (4,096 pts or >1km splits)
**Memory Safety**: Segment guards ensure M5 geometry passes stay bounded on OSM anomalies
**Commit**: `"M3.2: degree-2 collapse + segment guards"`

## M3.3 — Border Reconciliation  
**Why**: Consistent cross-tile processing
**Artifacts**: Tile boundary edge handling, global consistency
**Commit**: `"M3.3: border reconciliation"`

## M3.4 — Graph Debug APIs
**Why**: Inspection and validation tooling
**Artifacts**: `nodes.bin`, `super_edges.bin`, `geom.temp`, `/graph/stats`, `/graph/edge/{id}`
**Commit**: `"M3.4: graph artifacts + debug APIs"`