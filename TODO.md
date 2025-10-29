Awesome—let’s lock down **Step 4: Edge-Based Graph (EBG, turn-expanded)** so it’s **shared by all modes**, compact, and provably correct.

---

# Step 4 — Turn-expanded, mode-agnostic core

## Objective

From the **node-based graph (NBG)** + Step-2 attributes, build a **single** edge-based topology that enumerates all legal **turn transitions** at intersections.

* **Nodes (state):** directed NBG edges (`u→v`).
* **Arcs (transition):** legal turns `(incoming edge-state u→v) → (v→w)`.
* Mode specificity is encoded as **bitmasks**; later steps apply per-mode weights/penalties.

---

## Inputs

* `nbg.csr`, `nbg.geo`, `nbg.node_map` (Step 3).
* `ways.raw` (for way ids along geometry chunks if needed).
* `way_attrs.{car,bike,foot}.bin` (Step 2).
* `turn_rules.{car,bike,foot}.bin` (Step 2) — includes `Ban` / `Only` / `Penalty` (time-dep flagged).

---

## Effective turn rules (unified view)

Before expansion, build a **canonical turn rule index**:

1. **Expand `via=way`** restrictions (those flagged `is_time_dep=2`):

   * Use NBG topology: find all nodes that lie on the `via` way; emit triples `(from_way, via_node, to_way)`.
   * This may create many triples—dedupe them.

2. **Merge per-mode rules** into a single **canonical table** keyed by `(via_node_id, from_way_id, to_way_id)`:

   * `mode_mask` (3 bits: car/bike/foot).
   * `kind` (Ban, Only, Penalty).
   * `penalty_ds[3]` (u32 each) — **store** but not applied here.
   * `has_time_dep` (true if any contributing relation was time-dependent).

3. For **ONLY turns** at `(via, from_way)`, compute the **allowed to_way set**; other to_ways at that `(via, from_way)` become **implicit bans** for relevant modes (recorded as `Ban` in the table to simplify the builder).

This yields a **mode-aware, static, turn rule table** we can query in O(log k) per intersection.

---

## Graph objects & on-disk artifacts

### A) EBG node table (directed NBG edges)

Each NBG undirected edge `(u—v)` becomes **two** EBG nodes: `u→v` and `v→u`.

```
File: ebg.nodes  (little-endian, mmap-able; fixed size per record)
Header (64B)
  magic        u32 = 0x4542474E   // "EBGN"
  version      u16 = 1
  reserved     u16 = 0
  n_nodes      u32                 // = 2 * nbg.geo.n_edges_und
  created_unix u64
  inputs_sha   [32]u8
Body (n_nodes records; id = index)
  u32  tail_nbg      // compact NBG node id
  u32  head_nbg      // compact NBG node id
  u32  geom_idx      // index into nbg.geo record
  u32  length_mm     // copy of nbg.geo.length_mm (same both directions)
  u32  class_bits    // inherited (ferry, bridge, tunnel, roundabout, ford, etc.)
  u32  primary_way   // lower 32 bits of first_osm_way_id (debug; full i64 optional in side table)
Footer
  body_crc64  u64
  file_crc64  u64
```

> `geom_idx` here satisfies your requested **geom_idx[]**.

### B) EBG adjacency (CSR over EBG nodes)

```
File: ebg.csr
Header (64B)
  magic        u32 = 0x45424743   // "EBGC"
  version      u16 = 1
  reserved     u16 = 0
  n_nodes      u32                 // must match ebg.nodes
  n_arcs       u64                 // number of turn transitions
  created_unix u64
  inputs_sha   [32]u8
Body
  offsets      u64[n_nodes + 1]    // CSR
  heads        u32[n_arcs]         // destination EBG node id
  turn_idx     u32[n_arcs]         // index into turn_table (see C)
Footer
  body_crc64   u64
  file_crc64   u64
```

> This realizes your **turn_idx[]** alongside CSR.

### C) Deduplicated turn table (mode mask + semantics)

```
File: ebg.turn_table
Header (40B)
  magic        u32 = 0x45424754   // "EBGT"
  version      u16 = 1
  reserved     u16 = 0
  n_entries    u32
  inputs_sha   [32]u8
Body (n_entries records)
  u8   mode_mask          // bit0=car, bit1=bike, bit2=foot
  u8   kind               // 1=Ban, 2=Only, 3=Penalty, 0=None (rare; should not be referenced)
  u8   has_time_dep       // 0/1
  u8   reserved
  u32  penalty_ds_car     // 0 if N/A
  u32  penalty_ds_bike
  u32  penalty_ds_foot
  u32  attrs_idx          // future use (e.g., turn classes); 0 for now
Footer
  body_crc64  u64
  file_crc64  u64
```

> Most arcs will map to a small set of table entries; expect **high reuse**.

---

## Build algorithm (streamed, low-RAM)

1. **Enumerate EBG nodes**

   * For each `nbg.geo` record `(u,v, geom_idx, length, class_bits)`:

     * Emit EBG node `idA` = `u→v`, `idB` = `v→u` into `ebg.nodes`.

2. **Pre-index adjacency candidates**

   * For each NBG node `x`, collect incoming EBG nodes (`?→x`) and outgoing EBG nodes (`x→?`).
   * These lists are derived from NBG CSR; keep them in **spillable** per-node buffers.

3. **Apply **turn rules** to produce transitions**
   For each intersection node `x`:

   * For each incoming `a = u→x`:

     * For each outgoing `b = x→w` with `w != u` (U-turn policy configurable later):

       * Determine **way ids** of `a` and `b` from `nbg.geo[geom_idx].first_osm_way_id` (or way set if needed).
       * Lookup canonical rule by key `(x_as_osm_node_id, from_way_id, to_way_id)`:

         * Combine explicit **Ban** / **Only** / **Penalty** rules; convert `Only` into **allowed set**; if `b` not in allowed set ⇒ treat as **Ban** for the relevant modes.
         * Build a **mode_mask** that is allowed (start with all three modes; clear bits that are banned at this turn).
         * Record any **per-mode penalty** (for later weights).
       * If `mode_mask == 0` ⇒ **skip** emitting this arc (no mode can use it).
       * Else:

         * Get / insert a **turn_table** entry for `(mode_mask, kind/penalties, has_time_dep)` → get `t_idx`.
         * Append `heads.push(b_id)` and `turn_idx.push(t_idx)` to adjacency for `a_id`.

   **Notes**

   * If no explicit rule applies, the arc is **allowed** with `mode_mask = union(access on a & b by mode)`.
     (Don’t allow a transition for a mode that cannot traverse either approach or exit segment in that mode.)
   * **U-turns**: respect global policy; often **forbidden for car**, allowed for foot, sometimes for bike. Encode via default rule or via special `turn_table` entry.

4. **Materialize CSR**

   * For EBG node ids in ascending order, flush their adjacency buffers; fill `offsets[]`, then concatenate `heads[]` and `turn_idx[]`.
   * Compute CRCs; fsync.

**Peak RSS target:** **≤ 6–8 GB** (planet), using chunked per-node adjacency and on-disk spill.

---

## What about `classes[]`?

You asked for `classes[]` (motorway, residential, path, etc.).

* We store **per-EBG node** `class_bits` in `ebg.nodes` (inherited from `nbg.geo` & Step-2 way classes).
* If you also want **per-arc** class flags (rare), keep a small `arc_flags[]` parallel to `heads[]` (not necessary for core routing).

---

## Validation & **Lock conditions**

You cannot proceed until **all** pass.

### A. Structural integrity

1. **Counts match**:

   * `ebg.nodes.n_nodes == 2 * nbg.geo.n_edges_und`.
   * `ebg.csr.n_nodes == ebg.nodes.n_nodes`.
   * `len(heads) == len(turn_idx) == ebg.csr.n_arcs`.
2. **CSR integrity**:

   * `offsets[0]==0`, `offsets[i] ≤ offsets[i+1]`, `offsets[n]==len(heads)`.
   * All `heads[k] < n_nodes`.
3. **Determinism**: identical inputs ⇒ identical SHA-256 for `ebg.nodes`, `ebg.csr`, `ebg.turn_table`.

### B. Topology semantics (turn expansion)

4. **Turn bans honored (hard lock)**:

   * Sample **10,000** banned triples from the canonical rule table (across all modes). For each, find all EBG arcs at that `(via, from_way)` and assert **no arc** exists to the banned `to_way` **for any mode whose bit is banned**.
   * If any exists ⇒ **fail** with diagnostics (via node, from/to way ids, ebg node ids).
5. **ONLY rules honored**:

   * For **1,000** random `(via, from_way)` pairs that have at least one `Only` rule per mode, assert that **exactly and only** the allowed `to_way` arcs exist for that mode; others are absent.
6. **Mode propagation**:

   * For **100k** random arcs, check: a mode bit is set **iff** that mode has access on **both** EBG nodes’ base segments **and** the turn rule does not ban it. (Derive mode access from Step-2 `way_attrs` on contributing ways.)
7. **No stray arcs**:

   * There must be no arc `(a→b)` where `tail(a).head_nbg != head(b).tail_nbg`. (All transitions meet at the same NBG node.)

### C. Roundabouts & complex junctions

8. **Hand-picked test set parity** (lock):

   * Maintain a curated list (e.g., 200 intersections worldwide) covering: roundabouts, multi-leg with slips, stacked junction layers, ferry terminals, bike-only connectors.
   * For each, compare EBG turn fan-out with **OSRM** on the same data (allowed/forbidden per mode). Must match **exactly** (ignoring time-dependent rules).
   * Any mismatch ⇒ fail with a diff (incoming/outgoing pairs by way id per mode).

### D. Geometry & indices

9. **Geom indices**: For 10k random EBG nodes, `nbg.geo[geom_idx]` must connect `tail_nbg` ↔ `head_nbg` and `length_mm` must equal that record’s length.
10. **Class bits stability**: For 100k random EBG nodes, `class_bits` must be the union of the contributing `nbg.geo.flags` and consistent with Step-2 `way_attrs` (`ferry` always set for ferry, etc.).

### E. Reachability sanity (per mode, ignoring weights)

11. **Graph connectivity mirrors access**:

* For each mode, build a **reachability filter**: keep EBG nodes whose base segments are accessible in that mode.
* On this filtered node set, arcs with `mode_mask` containing the mode must keep components connected exactly as expected:

  * For 1,000 random pairs in the largest component, BFS in EBG succeeds.
  * If an **Only** rule creates a dead end that OSRM also yields (validate against OSRM on the sample), it’s fine. Any drift ⇒ fail.

### F. Performance & resource bounds

12. **Peak RSS** ≤ **8 GB** during build (planet).
13. **Arc count** sanity: `n_arcs` within expected band (empirically: ~8–20 per EBG node on average, network-dependent). Huge deviations block the step.
14. **Build time** indicative threshold recorded; alert if regress >2× baseline.

**Lockfile**

```json
step4.lock.json {
  "inputs_sha256": "...",
  "ebg_nodes_sha256": "...",
  "ebg_csr_sha256": "...",
  "ebg_turn_table_sha256": "...",
  "n_nodes": N,
  "n_arcs": M,
  "rss_peak_bytes": ...,
  "ban_checks": {"sampled":10000,"violations":0},
  "only_checks": {"sampled":1000,"violations":0},
  "created_at_utc": "..."
}
```

---

## U-turn policy (set once; tested)

* Default: **car**: forbid at ordinary nodes; allow at dead-ends; **bike/foot**: allow unless banned.
* Implement via a small policy table resolved **before** rule lookup (so you can override per mode).
* Encode as special `turn_table` entries (so arc generation is uniform).

---

## Why this satisfies your goals

* **Single EBG** shared by all modes → minimal memory.
* **SoA + CSR** → compact, cache-friendly.
* **Turn bans & ONLY** applied at build → no drift later; isochrones and routes will respect restrictions identically.
* **Mode masks** keep it mode-agnostic now, mode-specific later (weights/customization).

If you want, I can follow with the **exact Rust record layouts + full builder outline** (copy-paste), or move on to Step 5 (per-mode weight layers & fast customization) with lock conditions.

