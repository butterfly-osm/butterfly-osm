# Butterfly-Route: Edge-Based CCH Implementation Plan

## Goal

Build a routing engine with **exact turn-aware isochrones** and **OSRM-class speed** using:
- Edge-based graph (state = directed edge ID)
- CCH preprocessing on edge-based graph
- Exact bounded Dijkstra on the hierarchy for all query types

**Key principle:** One graph, one hierarchy, one query engine. Routes, matrices, and isochrones use identical cost semantics.

---

## Completed Steps

### Step 1-6: Data Pipeline ✅

| Step | Output | Description |
|------|--------|-------------|
| 1 | `nodes.sa`, `nodes.si`, `ways.raw`, `relations.raw` | PBF ingest |
| 2 | `way_attrs.*.bin`, `turn_rules.*.bin` | Per-mode profiling (car/bike/foot) |
| 3 | `nbg.csr`, `nbg.geo`, `nbg.node_map` | Node-Based Graph (intermediate) |
| 4 | `ebg.nodes`, `ebg.csr`, `ebg.turn_table` | Edge-Based Graph (THE routing graph) |
| 5 | `w.*.u32`, `t.*.u32`, `mask.*.bitset` | Per-mode edge weights and turn penalties |
| 6 | `order.ebg` | CCH ordering on EBG via nested dissection |

---

## Step 7: CCH Contraction

### Objective

Build the CCH topology (shortcuts) using the EBG ordering.

### Inputs

- `ebg.csr`, `ebg.turn_table`
- `order.ebg`

### Outputs

- `cch.topo` — Shortcut topology (which shortcuts exist)
- `cch.topo.lock.json`

### Algorithm

Standard CCH contraction:
1. Process nodes in elimination order
2. For each contracted node, add shortcuts between remaining neighbors
3. Store shortcut topology (metric-independent)

---

## Step 8: CCH Customization

### Objective

Apply per-mode weights to the CCH shortcuts.

### Inputs

- `cch.topo`
- `w.*.u32`, `t.*.u32` (per-mode weights from Step 5)

### Outputs

- `cch.w.*.u32` — Per-mode shortcut weights (one file per mode)

### Algorithm

Bottom-up customization:
1. For each shortcut in contraction order
2. Compute shortcut weight = min over middle nodes

---

## Step 9: Query Engine

### Objective

Single query engine for all query types.

### Query Types

1. **P2P Routing**: Standard bidirectional CCH query
2. **One-to-Many / Matrix**: Same algorithm, multiple targets
3. **Isochrone (exact)**:
   - Exact bounded Dijkstra on CCH
   - Stop when `dist > T`
   - Collect settled states with `dist ≤ T`
   - Interpolate frontier on road geometry

### Key Properties

- All queries use edge-based state
- Turn restrictions exact
- Monotonic isochrones (T₁ < T₂ ⟹ iso(T₁) ⊂ iso(T₂))
- Consistent with routing (isochrone boundary = exactly reachable)

---

## What NOT to Do

- ❌ Use node-based graphs for routing/isochrones
- ❌ Approximate range queries
- ❌ Different backends for different query types
- ❌ Snap differently for different APIs
- ❌ PHAST until restriction correctness is proven

---

## CLI Commands (Target)

```bash
# Build pipeline (Steps 1-5 exist)
butterfly-route step1-ingest -i map.osm.pbf -o ./build/
butterfly-route step2-profile --ways ./build/ways.raw --relations ./build/relations.raw -o ./build/
butterfly-route step3-nbg ... -o ./build/
butterfly-route step4-ebg ... -o ./build/
butterfly-route step5-weights ... -o ./build/

# CCH pipeline (Steps 6-8 TODO)
butterfly-route step6-order --ebg-csr ./build/ebg.csr -o ./build/
butterfly-route step7-contract --ebg-csr ./build/ebg.csr --order ./build/order.ebg -o ./build/
butterfly-route step8-customize --cch-topo ./build/cch.topo --weights ./build/ -o ./build/

# Query server (Step 9 TODO)
butterfly-route serve --data ./build/ --port 8080
```

---

## Performance Targets

| Operation | Target |
|-----------|--------|
| Belgium ordering (~8M EBG nodes) | ≤ 2 minutes |
| Belgium contraction | ≤ 5 minutes |
| Belgium customization (per mode) | ≤ 30 seconds |
| P2P query | < 1ms |
| Isochrone (30 min) | < 100ms |
| Peak RSS (ordering) | ≤ 6 GB for Belgium |
