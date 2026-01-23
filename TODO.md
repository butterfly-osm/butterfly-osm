# Butterfly-Route: Edge-Based CCH Implementation Plan

## Goal

Build a routing engine with **exact turn-aware isochrones** and **OSRM-class speed** using:
- Edge-based graph (state = directed edge ID)
- CCH preprocessing on edge-based graph
- Exact bounded Dijkstra on the hierarchy for all query types

**Key principle:** One graph, one hierarchy, one query engine. Routes, matrices, and isochrones use identical cost semantics.

---

## Completed Steps

### Step 1-5: Data Pipeline ✅

| Step | Output | Description |
|------|--------|-------------|
| 1 | `nodes.sa`, `nodes.si`, `ways.raw`, `relations.raw` | PBF ingest |
| 2 | `way_attrs.*.bin`, `turn_rules.*.bin` | Per-mode profiling (car/bike/foot) |
| 3 | `nbg.csr`, `nbg.geo`, `nbg.node_map` | Node-Based Graph (intermediate) |
| 4 | `ebg.nodes`, `ebg.csr`, `ebg.turn_table` | Edge-Based Graph (THE routing graph) |
| 5 | `w.*.u32`, `t.*.u32`, `mask.*.bitset` | Per-mode edge weights and turn penalties |

---

## Step 6: CCH Ordering on EBG

### Objective

Compute a **single, high-quality elimination order** on the **EBG** (not NBG!) that is:
- Weight-independent (used by CCH for all modes)
- Deterministic (byte-for-byte reproducible)
- Good separators (balanced, small edge cuts)

### Inputs

- `ebg.csr` — Edge-based graph CSR
- `ebg.nodes` — Node metadata (for coordinates via NBG linkage)
- `nbg.geo` — Geometry (for inertial partitioning coordinates)

### Outputs

1. `order.ebg` — Permutation array: `perm[old_ebg_node] → rank`
2. `ebg.order.lock.json` — Lock file with SHA-256 and quality metrics

### Algorithm

Same nested dissection as before, but on EBG nodes:
1. Inertial partitioning via PCA on node coordinates
2. Histogram-based O(n) median selection
3. Boundary ring extraction + greedy minimum node cover
4. FM refinement on ring (1-2 passes)
5. Parallel recursion with Rayon (leaf threshold ~8k-16k)

### Lock Conditions

- A.1: `perm` is a valid permutation of `[0..n_ebg_nodes)`
- B.3: Balance ∈ [0.4, 0.6] for all non-leaf splits
- B.4: Separator size ≤ 12% for all splits
- B.5: Edge-cut efficiency cut_per_k ≤ 50 for 90% of splits

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
