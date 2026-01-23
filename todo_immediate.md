# Immediate TODO: Step 8 Fixes from AI Review

All issues identified by Gemini and Codex AI review on 2026-01-23.

---

## 1. CRITICAL: Wrong Processing Order (Gemini + Codex)

### Problem

Up shortcuts read `down_weights[u→m]` BEFORE any down edge from `u` is computed.

Current code processes UP edges before DOWN edges for each node:
```rust
for rank in 0..n_nodes {
    let u = inv_perm[rank];

    // BUG: Process UP edges first
    for i in up_start..up_end {
        if is_shortcut {
            let w_um = find_edge_weight(u, m, down_..., &down_weights); // READS UNCOMPUTED!
        }
    }

    // DOWN edges processed AFTER - too late!
    for i in down_start..down_end { ... }
}
```

**Result:** Every up shortcut gets `u32::MAX` for the u→m leg.

### Fix

Process DOWN edges BEFORE UP edges for each node.

---

## 2. CRITICAL: Down Edge Dependency Order (Gemini + Codex)

### Problem

Down shortcuts `u→v via m` require `down_weights[u→m]` and `up_weights[m→v]`.

For `down_weights[u→m]` to be available, node `m` must have been processed already. Since `rank(m) < rank(v) < rank(u)`, when processing node `u`, node `m` has been processed, so `down_weights` from `m` should exist.

BUT: The lookup is `find_edge_weight(u, m, down_offsets, down_targets, down_weights)` - this searches for edge `u→m` in `u`'s down adjacency. This edge's weight is being computed NOW, not earlier.

The actual dependency: `weight(u→m)` requires that the edge `u→m` exists and that if it's a shortcut via some `k`, then `weight(u→k)` and `weight(k→m)` are already computed.

### Fix

Process down edges in **increasing order of target rank** to ensure dependencies are satisfied. When computing `u→v`, all edges to lower-ranked targets have already been computed.

---

## 3. MEMORY: Duplicate Arc Lookup Structure (Codex)

### Problem

`build_arc_lookup()` creates a full copy of EBG adjacency:
```rust
fn build_arc_lookup(ebg_csr: &EbgCsr) -> Vec<Vec<(u32, usize)>> {
    let mut lookup: Vec<Vec<(u32, usize)>> = vec![Vec::new(); n_nodes];
    for u in 0..n_nodes {
        for arc_idx in start..end {
            lookup[u].push((v, arc_idx));
        }
        lookup[u].sort_unstable_by_key(|(v, _)| *v);
    }
    lookup
}
```

This duplicates ~60M entries for Belgium, wasting hundreds of MB.

### Fix

Remove `arc_lookup`. The EBG CSR already has edges sorted by target (heads array). Use direct binary search on `ebg_csr.heads[start..end]` to find arc index.

---

## 4. CPU: Repeated Binary Searches (Codex)

### Problem

For each shortcut, we do multiple binary searches:
- `find_edge_weight(u, m, ...)` - binary search in CCH
- `find_edge_weight(m, v, ...)` - binary search in CCH
- `find_arc_index(u, v)` - binary search in arc_lookup (for original edges)

### Fix

After removing arc_lookup, use direct index arithmetic where possible. The CCH topology stores edges in sorted order, so binary search is unavoidable for shortcuts, but original edges can compute arc_idx directly if we track the mapping.

---

## 5. PERFORMANCE: No Parallelism (Codex)

### Problem

Current implementation is single-threaded. CCH customization is embarrassingly parallel - nodes at the same rank can be processed independently.

### Fix

Add Rayon parallel iteration. Process nodes in batches by rank, parallelize within each batch.

**Note:** Implement AFTER correctness is fixed and verified.

---

## 6. CORRECTNESS: Original Edge Weight Computation (Review)

### Problem

Current code:
```rust
fn compute_original_weight(u, v, node_weights, turn_penalties, arc_lookup) -> u32 {
    let w_v = node_weights[v];  // Weight of TARGET node
    if w_v == 0 { return u32::MAX; }
    // ...
}
```

Is `node_weights[v]` correct? The EBG node weight represents traversal cost of that edge. For edge `u→v` in CCH:
- `u` and `v` are EBG node IDs (which are directed edge IDs from NBG)
- The weight should be the cost to traverse edge `v` plus turn penalty from `u` to `v`

This seems correct, but verify the semantics match what Step 5 produces.

### Fix

Verify and document that `w.*.u32[node_id]` = traversal cost of EBG node (directed edge).

---

## Checklist

- [x] 1. Fix processing order: DOWN edges before UP edges
- [x] 2. Process down edges sorted by target rank (increasing)
- [x] 3. Remove duplicate arc_lookup, use flat CSR with binary search
- [x] 4. Optimize arc index lookup for original edges (direct arc_idx indexing)
- [x] 5. Verify original edge weight semantics (node_weights[v] + turn_penalties[arc_idx])
- [x] 6. Re-run on Belgium for all modes (~5s each, well under 30s target)
- [x] 7. Sanity check implemented (reports original vs shortcut unreachable stats)
- [x] 8. Parallel preprocessing added (sorted EBG, down edge sorting); main loop sequential by design

## Results

**Performance:** ~5s per mode (target was ≤30s)

**Unreachable edge analysis:**
| Mode | Original | Shortcuts | Total |
|------|----------|-----------|-------|
| Car  | 51% | 75-84% | 73% |
| Bike | 4% | 16-30% | 20% |
| Foot | 1% | 7-17% | 10% |

The high unreachable percentage for car mode reflects real-world access restrictions (pedestrian paths, one-way streets, etc.). Shortcuts cascade unreachability: if either leg is unreachable, the shortcut is unreachable.

## Step 9 Requirement: Smart Snapping

The 51% car-inaccessible rate is correct preprocessing - it includes:
- Footways, cycleways, pedestrian paths (both directions blocked)
- One-way streets (reverse direction blocked)
- Private roads, no-access roads

**Query-time solution:** Snap to nearest **accessible** node:
1. Find K nearest EBG nodes using spatial index
2. Filter to nodes with `mask.*.bitset[node_id] == 1` (mode-accessible)
3. Snap to nearest accessible node

This ensures users always start from a car-accessible road segment.

---

## AI Reviewer Quotes

**Gemini:**
> "The computation of weights for the up-edges of a node `u` may depend on the weights of its down-edges. However, the current code calculates all up-edge weights for `u` *before* calculating any of its down-edge weights."

> "Down-edges need to be processed in increasing rank order of targets."

**Codex:**
> "Up shortcuts read `w_um` from `down_weights` before any down edge from `u` is customized (down pass runs afterwards), so every up shortcut gets `u32::MAX` for the u→m leg."

> "Down shortcuts may also have dependency issues if not processed in rank order."

> "`build_arc_lookup` duplicates EBG adjacency - memory inefficient."

> "Repeated binary searches per edge - CPU inefficient."

> "No parallelism used."
