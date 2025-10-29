Great—here’s **Step 5: Per-mode weight layers (no graph duplication)** with exact artifacts, math, and **lock conditions**.

---

# Step 5 — Per-mode weights & masks (car | bike | foot)

## Objective

Attach **mode-specific** traversal costs to the **shared EBG** without copying topology.

* **Node cost** (EBG node = directed base segment): `w.<mode>.u32`
* **Turn cost** (EBG arc = intersection transition): `t.<mode>.u32`
* **Node accessibility mask**: `mask.<mode>.bitset`

No topology changes; we only produce **arrays parallel** to `ebg.nodes` and `ebg.csr`.

---

## Inputs

* `ebg.nodes`, `ebg.csr`, `ebg.turn_table` (Step 4)
* `nbg.geo` (for segment length and flags)
* `way_attrs.{car,bike,foot}.bin` + `profile_meta.json` (Step 2)

---

## Units & integer math (fixed)

* **Lengths**: `length_mm` (millimeters) from `nbg.geo`
* **Speeds**: `base_speed_mmps` (millimeters/second) from `way_attrs.*`
* **Penalties**:

  * `const_penalty_ds` (deciseconds) from `way_attrs.*`
  * `per_km_penalty_ds` (deciseconds per kilometer) from `way_attrs.*`
  * **Turn penalties**: per-mode deciseconds from `ebg.turn_table` (Penalty kind)
* **Weights**: **u32 deciseconds** (0.1 s). Use **saturating** arithmetic; forbid 0 on traversable edges.

**Formulas (exact):**

```
travel_time_ds      = ceil( length_mm / base_speed_mmps * 10 )
                    = (length_mm * 10 + base_speed_mmps - 1) / base_speed_mmps

per_km_extra_ds     = ceil( length_km * per_km_penalty_ds )
                    = (length_mm * per_km_penalty_ds + 1_000_000 - 1) / 1_000_000

edge_weight_ds      = saturating_add( travel_time_ds,
                                      per_km_extra_ds,
                                      const_penalty_ds )

turn_penalty_ds     = (from ebg.turn_table for this arc & mode; 0 if none)
```

* If `base_speed_mmps == 0` (inaccessible): **mask=0**, **weight=0** (ignored by query).

**Ferry/duration handling (deterministic):**

* If `way_attrs` marked **ferry** and a parsed `duration` (in seconds) exists for the way → **override** `travel_time_ds = ceil(duration * 10)`. Still add `const_penalty_ds` and `per_km_extra_ds` (often 0).

---

## Outputs (per mode)

### 1) `w.<mode>.u32` — node weights (size = `ebg.nodes.n_nodes`)

```
Header (32B)
  magic     u32 = 0x574D4F44     // "WMOD"
  version   u16 = 1
  mode      u8  = {0=car,1=bike,2=foot}
  reserved  u8  = 0
  count     u32 = n_nodes
  inputs_sha[16]                  // truncated SHA-256 of all inputs for brevity
Body
  u32 weight_ds[count]
Footer
  body_crc64 u64
  file_crc64 u64
```

### 2) `t.<mode>.u32` — turn penalties (size = `ebg.csr.n_arcs`)

```
Header (as above, magic "TMOD")
Body
  u32 penalty_ds[n_arcs]   // 0 if no penalty or if mode not affected
```

> Arcs that a mode is **not allowed** to traverse remain present in EBG but are disallowed at query time via the **turn_table.mode_mask** bit (from Step 4). We do **not** need an arc-mask file.

### 3) `mask.<mode>.bitset` — node accessibility mask (size = ceil(n_nodes/8))

```
Header (24B)
  magic     u32 = 0x4D41534B     // "MASK"
  version   u16 = 1
  mode      u8  = {0,1,2}
  reserved  u8  = 0
  count     u32 = n_nodes
Body
  bits[ceil(count/8)]  // 1 = traversable in this mode; 0 = not traversable
Footer
  body_crc64 u64
  file_crc64 u64
```

---

## How to compute per-mode values

### Node (EBG node id `e`)

1. Get `geom_idx = ebg.nodes[e].geom_idx` → `length_mm = nbg.geo[geom_idx].length_mm`
2. Identify the **contributing OSM way id** (`first_osm_way_id`) and **direction**:

   * If `ebg.nodes[e]` is the **forward** state of that way segment, use **forward access** from `way_attrs`
   * If **reverse**, use **reverse access** considering mode-specific oneway rules
3. From the **mode’s** `way_attrs`, read:

   * `access_{fwd,rev}`, `base_speed_mmps`, `per_km_penalty_ds`, `const_penalty_ds`, `class_bits` (if you modulate speed by classes, that’s already baked into `base_speed_mmps`)
4. Set `mask[e]`:

   * `1` iff access in this direction is **true**
   * else `0` and **skip** weight compute (`w[e]=0`)
5. If `mask[e]=1`, compute `w[e] = edge_weight_ds` per formula above.

   * Enforce `w[e] ≥ 1` (no zero-cost traversals)

### Arc (index `k` across `ebg.csr`)

1. Read `turn_idx = ebg.csr.turn_idx[k]` → `tt = ebg.turn_table[turn_idx]`
2. If `tt.mode_mask` has this mode’s bit **off** → set `t[k]=0` (ignored; traversal is forbidden anyway)
3. Else:

   * If `tt.kind == Penalty` → `t[k] = tt.penalty_ds_<mode>`
   * If `Ban` or `Only` (allowed) → `t[k] = 0` (ban is enforced by the bitmask; allowed path has no implicit extra)

**Time-dependent rules:** entries flagged `has_time_dep=1` in turn_table were **ignored at expansion** (Step 4). Nothing to do here.

---

## Validation & **Lock conditions**

You cannot proceed until all pass.

### A. Structural

1. Sizes:

   * `len(w.<mode>) == ebg.nodes.n_nodes`
   * `len(t.<mode>) == ebg.csr.n_arcs`
   * `bitlen(mask.<mode>) == ebg.nodes.n_nodes`
2. CRC-64s verify; `inputs_sha` matches current build inputs.
3. Determinism: two runs → identical SHA-256 for all three artifacts per mode.

### B. Math parity (hard locks)

4. **100k node samples per mode**: recompute `edge_weight_ds` from
   `length_mm`, `base_speed_mmps`, `per_km_penalty_ds`, `const_penalty_ds`
   using the **integer formulas above**. Must equal `w[e]` bit-exact.
5. **Directionality & access**:

   * For 100k EBG nodes, resolve **directional access** from `way_attrs` and confirm `mask[e]` matches:

     * oneway handling (`oneway`, `oneway:<mode>`, `bicycle=dismount`, etc.)
     * ferries, footways, etc.
   * If access=false in that direction → `mask=0` and `w[e]=0`.
6. **Ferry duration rule**: For all edges tagged ferry with parsed `duration`, check that `travel_time_ds` equals `ceil(duration*10)` (±0 tolerance) before adding penalties.

### C. Arc/turn consistency

7. **Mode mask coherence**:

   * For 1M random arcs per mode: if the **mode bit is off** in `turn_table[turn_idx]`, traversal must be **forbidden** at query time (checked in Step 6/7; here we pre-check counts match):
     `count_arcs_allowed(mode)` equals the number of arcs with bit **on**.
8. **Penalty mapping**:

   * For 100k arcs whose turn_table kind is `Penalty`, confirm `t[k] == per-mode penalty` exactly; otherwise `t[k]==0`.

### D. Graph-level parity (length-only)

9. **Dijkstra on masked EBG** (weights = `w`, arcs allowed by mode bit & `mask`):

   * For 5k random OD pairs per mode, run Dijkstra (static, no heuristics).
   * The set of traversed **base segments** must be a subset of ways that have `access=true` in `way_attrs` for that mode and direction.
   * If any traversal uses a segment with `mask=0` or violates an `ONLY` rule, **fail** with the offending ids.

### E. Sanity & bounds

10. **Weight bounds** (for all `mask=1` nodes):

* car: `1 ≤ w[e] ≤ 10_000_000 ds` (≈ 11.5 days; ferries OK)
* bike: `1 ≤ w[e] ≤ 5_000_000 ds`
* foot: `1 ≤ w[e] ≤ 5_000_000 ds`

11. **Zero/NaN**: none allowed (integer math only).
12. **Resident memory** (planet guidance; informational): weights+turns+mask across 3 modes **≤ 8–10 GB**.

**Lockfile**

```json
step5.lock.json {
  "inputs_sha256": "...",
  "car":  {"w_sha256":"...","t_sha256":"...","mask_sha256":"..."},
  "bike": {"w_sha256":"...","t_sha256":"...","mask_sha256":"..."},
  "foot": {"w_sha256":"...","t_sha256":"...","mask_sha256":"..."},
  "node_count": N,
  "arc_count": M,
  "rss_peak_bytes": ...,
  "created_at_utc": "..."
}
```

---

## CLI

```
osm-build step5-weights \
  --ebg-nodes   ebg.nodes \
  --ebg-csr     ebg.csr \
  --turn-table  ebg.turn_table \
  --nbg-geo     nbg.geo \
  --way-attrs-car  way_attrs.car.bin \
  --way-attrs-bike way_attrs.bike.bin \
  --way-attrs-foot way_attrs.foot.bin \
  --outdir /data/osm/2025-10-29 \
  --threads 16
```

Exit **0** only if all **lock conditions** pass and `step5.lock.json` is written.

---

## Notes

* **No graph duplication**: all three modes share `ebg.*`; only arrays differ.
* **Query semantics** later: moving from arc `(a→b)` costs `t_mode[arc] + w_mode[b]` (plus source/target snapping costs), with traversal allowed iff `mask_mode[b]==1` **and** the arc’s `turn_table` mode bit is on.
* This guarantees **single-route/matrix/isochrone parity** because **the same weights and masks** are used everywhere.

