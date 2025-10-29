Great—here’s **Step 3: Node-based graph (NBG)** fully specified, with artifacts, build rules, and **lock conditions**.

---

# Step 3 — Node-based graph (topology backbone)

## Objective

Build a **mode-agnostic** road/ferry topology that preserves true intersections and geometry, to drive **ordering/partitioning** later.
No weights yet—**length only**.

---

## Inputs

* `nodes.sa` / `nodes.si` (Step 1) — OSM node id → (lat, lon).
* `ways.raw` (Step 1) — ways with node sequences + tag dicts.
* `way_attrs.{car,bike,foot}.bin` (Step 2) — to decide if a way is relevant (accessible by **any** mode).
* `profile_meta.json` (Step 2) — class bit positions, enums.

---

## Inclusion rules (what becomes topology)

A way is **in** the NBG if **any** mode has `access_fwd || access_rev` and it is a linear road/ferry segment:

* Include `highway=*` ways (except obvious non-linear/areas):

  * **Exclude**: `area=yes`, `highway=pedestrian + area=yes`, `highway=platform`, `highway=rest_area` (unless it has linear geometry and any access).
* Include `route=ferry` ways (linear).
* Exclude **non-navigable** geometries: `railway=*` (unless explicitly allowed by any mode), `aeroway=*`, `waterway=*` (unless ferry).
* Multi-polygon/area relations are ignored here.

> Mode-agnostic means: union of car/bike/foot access from Step 2 to decide inclusion; **but** the graph remains **undirected** for now.

---

## Topology rules (where to cut)

We **split** ways into NBG edges between **decision nodes**:

* **Ends of ways.**
* **OSM intersections**: a node shared by ≥2 included ways **at the same layer** (see below).
* **Tags forcing a split** (to stabilize geometry & later expansion):

  * `junction=roundabout` entry/exit nodes.
  * `bridge=*`, `tunnel=*`, `ford=yes` transition boundaries.
  * `route=ferry` endpoints.
  * **Layer changes**: split at first node where `layer` value changes along a way.
  * **Barriers**: `barrier=*` nodes if any mode allows passing only conditionally later (we mark but keep topology continuous here).

**Layer/level crossing**
Only connect ways at a shared node if their effective `layer` is equal (default 0).
Bridges/tunnels that **share a node** with surface links but different `layer` **do not** intersect.

---

## Geometry & metrics

* Coordinate unit: fixed-point 1e-7 deg from `nodes.sa`.
* Length per segment: **geodesic chord** in meters using **haversine** on WGS84 sphere (R=6,371,008.8 m), summed along the polyline between decision nodes.
* Store **u32 millimeters** (`length_mm`), saturated at `u32::MAX`.
* Optional `bearing_deci_deg` (0–3599) from first to last vertex of the collapsed segment (for snapping heuristics later).

---

## Artifacts (all little-endian, mmap-friendly)

### 1) `nbg.csr`

Compact CSR for **undirected** topology over **compacted graph nodes**.

```
Header (64B)
  magic        u32 = 0x4E424743   // "NBGC"
  version      u16 = 1
  reserved     u16 = 0
  n_nodes      u32
  n_edges_und  u64                 // number of undirected edges
  created_unix u64
  inputs_sha   [32]u8              // hash of nodes.sa + ways.raw + way_attrs.* + meta
Body
  offsets      u64[n_nodes + 1]    // adjacency offsets into heads[]
  heads        u32[2 * n_edges_und]// neighbor node ids (each undirected becomes two arcs)
  edge_idx     u64[2 * n_edges_und]// index into nbg.geo for each arc
Footer
  body_crc64   u64
  file_crc64   u64
```

* Node ids are **dense** `0..n_nodes-1`.
* `edge_idx` points into `nbg.geo` records (same for both directions).

### 2) `nbg.geo`

Per **undirected** edge geometry/metrics.

```
Header (64B)
  magic        u32 = 0x4E424747   // "NBGG"
  version      u16 = 1
  reserved     u16 = 0
  n_edges_und  u64
  poly_bytes   u64                 // size of poly blob section
Body (n_edges_und records)
  u32  u_node                     // tail compact node id (small copy for debugging)
  u32  v_node                     // head compact node id
  u32  length_mm                  // total length (mm)
  u16  bearing_deci_deg           // optional; 0..3599; 65535 if NA
  u16  n_poly_pts                 // number of intermediate vertices INCLUDING endpoints
  u64  poly_off                   // byte offset into poly blob
  i64  first_osm_way_id           // primary way id contributing
  u32  flags                      // bit0=ferry, bit1=bridge, bit2=tunnel, bit3=roundabout, bit4=ford, bit5=layer_boundary, ...
Poly blob (append-only)
  For each record:
    i32 lat_fxp[n_poly_pts]       // 1e-7 deg
    i32 lon_fxp[n_poly_pts]
Footer
  body_crc64   u64
  file_crc64   u64
```

### 3) `nbg.node_map`

Mapping between **OSM node ids** kept in NBG and **compact node ids**.

```
Header
  magic        u32 = 0x4E42474D   // "NBGM"
  version      u16 = 1
  reserved     u16 = 0
  count        u64                // number of kept nodes
Body (sorted by OSM node id)
  i64 osm_node_id
  u32 compact_id
Footer
  body_crc64   u64
  file_crc64   u64
```

> Minimal extra file, but **critical** downstream (edge-based expansion & snapping). Size ≈ 12 bytes/kept node.

---

## Build algorithm (streaming, low-RAM)

1. **Mark included ways**

   * Map `way_attrs.*`. A way is included if **any** mode has access in either direction.
2. **Collect candidate nodes**

   * First pass over included ways: count occurrences of each OSM node id (using external sort buckets to stay <2 GB).
   * Mark decision nodes:

     * endpoints, shared nodes (count ≥ 2 within the **same layer** bucket), and tag-driven split points (see rules).
3. **Compact node ids**

   * Build `nbg.node_map` with only decision nodes; assign dense ids 0..N-1 (external sort by OSM node id, then sequential assign).
4. **Emit edges**

   * For each included way, walk its node list:

     * Track current segment vertices until reaching the next **decision node**; if both endpoints are decision nodes, create one **undirected** edge:

       * Compute length via haversine summed across segment vertices.
       * Write a `nbg.geo` record (and append vertices to the poly blob).
       * Add two arcs into in-memory adjacency write buffers: `(u→v)` and `(v→u)` with `edge_idx`.
     * If a segment collapses to <2 distinct coordinates or zero length, **skip** (log counter).
5. **Assemble CSR**

   * Collect per-node adjacency buffers; sort neighbors by id; compute `offsets[]` and write `heads[]` + `edge_idx[]`.
6. **Finalize**

   * Fill headers, compute CRC-64, write `inputs_sha` as SHA-256 of all inputs (Step 1+2 artifacts), fsync.

**Memory target:** use chunked/partitioned external sorts and per-node spill files; keep **RSS ≤ 2–3 GB** for planet.

---

## Validation & **Lock conditions**

You cannot proceed until **all** pass.

### A. Structural

1. **Determinism**: Two runs with same inputs produce identical SHA-256 for `nbg.csr`, `nbg.geo`, `nbg.node_map`.
2. **Counts**:

   * `nbg.csr.n_nodes` equals `nbg.node_map.count`.
   * `2 * n_edges_und == len(heads) == len(edge_idx)`.
3. **CSR integrity**:

   * `offsets[0]==0`, `offsets[i] ≤ offsets[i+1]`, `offsets[n_nodes]==len(heads)`.
   * All `heads[k] < n_nodes`.
4. **Geo integrity**:

   * `nbg.geo.n_edges_und` equals the number of unique undirected edges referenced by `edge_idx`.
   * Every `poly_off + sizeof(record_poly)` is within `poly_bytes`.

### B. Topology semantics

5. **Layer correctness**: For every shared OSM node with **different** effective `layer`, ensure **no** adjacency between the corresponding compact nodes (only nodes with same layer connect).
6. **Symmetry**: For each undirected edge `(u,v)`, both arcs appear: `u→v` and `v→u` referencing the **same** `edge_idx`.
7. **No self-loops unless legitimate**:

   * Count arcs with `u==v`. Should be **0** except for degenerate closed geometries you explicitly allowed (target 0).
8. **Parallel edges allowed** (dual carriageways, ramps). No de-dup constraint.

### C. Metric correctness

9. **Length plausibility**:

   * For all edges: `1 m ≤ length_mm ≤ 500 km` (saturation allowed only for pathological ferries; log).
10. **Geometry sum parity** (**lock test**):

* Sample **1,000 random vertex pairs** `(u,v)` that are adjacent in NBG (i.e., an edge).
* Recompute length by summing haversine over the stored polyline vertices; must equal `length_mm` within **±1 m**.

11. **Way sum parity** (**lock test**):

* Sample **1,000 random OSM ways** used to generate edges.
* For each, concatenate the lengths of its emitted NBG segments; must equal the haversine sum along the original way (between decision nodes) within **±1 m** total.

### D. End-to-end reachability sanity

12. **Dijkstra parity on length** (**lock test**):

* Sample **1,000 random compact node pairs** `(s,t)` in the same connected component.
* Run Dijkstra **on NBG (edge lengths)**; then reconstruct the path’s polyline length by summing `nbg.geo` polylines along the returned edges.
* The two totals must match within **±1 m**.

13. **Component stats**:

* Report number and size distribution of connected components; largest contains most road km. Thresholds aren’t locks, but **drastic deviations** from previous build block the step.

### E. Performance & resource bounds

14. **Peak RSS** ≤ **3 GB** (planet target; enforce via cgroup).
15. **Throughput**: ≥ **2M edges/min** emission (indicative; alert if <50% of baseline).
16. **File sizes (indicative)**:

* `nbg.csr`: ~ (8*(n_nodes+1) + 12*2*n_edges_und) bytes.
* `nbg.geo`: ~ (24*n_edges_und + poly_bytes). On planet, expect a few GB total; Belgium: hundreds of MB.

### F. Failure handling

17. **Missing coordinates** for nodes referenced by included ways: count and **exclude** those segments; if >0.01% of segments, **fail** with diagnostics (list first 1,000 node ids).
18. **Zero/NaN lengths**: any NaN/Inf detected in length computation → **fail** with the offending edge id and coordinates.

**Lockfile**

```json
step3.lock.json {
  "inputs_sha256": "...",
  "nbg_csr_sha256": "...",
  "nbg_geo_sha256": "...",
  "nbg_node_map_sha256": "...",
  "n_nodes": N,
  "n_edges_und": M,
  "components": {"count": C, "largest_nodes": Ln, "largest_edges": Le},
  "rss_peak_bytes": ...,
  "created_at_utc": "..."
}
```

---

## CLI

```
osm-build step3-nbg \
  --nodes nodes.sa \
  --ways ways.raw \
  --way-attrs-car  way_attrs.car.bin \
  --way-attrs-bike way_attrs.bike.bin \
  --way-attrs-foot way_attrs.foot.bin \
  --outdir /data/osm/2025-10-29 \
  --threads 16
```

Exit **0** only if all **lock conditions** above pass and `step3.lock.json` is written.

---

## Notes

* NBG is **undirected** and **mode-agnostic** by design; directionality and turn costs arrive when we build the **edge-based graph** (Step 4).
* Keeping `nbg.node_map` now avoids expensive lookups later and guarantees **consistent IDs** across steps.
* Strict **±1 m** tolerances ensure geometry and length math are locked before relying on NBG for ordering/partitioning.

