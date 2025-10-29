Here’s **Step 2 — Modal profiling (Rust-only)**, fully specified with artifacts and **lock conditions** so you can freeze it before Step 3.

---

# Step 2 — From raw tags to per-mode attributes

## Objective

From `ways.raw` and `relations.raw` (Step 1), compute **mode-specific, deterministic** attributes:

1. **Per-way attributes** (for car/bike/foot): access, oneway, base speed, class flags, per-way penalties.
2. **Turn rules** (per mode): bans/“only”-turns and explicit turn penalties derived from restriction relations (normalized, de-duplicated).
3. **Metadata** needed down the line (enumeration dictionaries, version info).

All authored and executed in **Rust** (profiles too).

---

## Inputs

* `nodes.sa` / `nodes.si` (from Step 1) — not required for tag logic, but available.
* `ways.raw` — lossless way nodes + tag dictionaries.
* `relations.raw` — restriction relations + dictionaries.
* `profile_car.rs`, `profile_bike.rs`, `profile_foot.rs` — **Rust** profiles implementing the ABI below.

---

## Rust profile ABI (stable, testable)

Each profile is a Rust crate compiled as a `cdylib` **or** to **WASM** (either is fine; same ABI). It exports:

```rust
#[repr(C)]
pub struct WayInput<'a> {
    pub kv_keys: &'a [u32],   // ids into ways.raw key dict
    pub kv_vals: &'a [u32],   // parallel array into ways.raw val dict
}

#[repr(C)]
pub struct WayOutput {
    pub access_fwd: bool,
    pub access_rev: bool,
    pub oneway: u8,           // 0=no,1=fwd,2=rev,3=both (rare), respects mode-specific oneway rules
    pub base_speed_mmps: u32, // integer mm/s, applies to traversable directions
    pub surface_class: u16,   // enum index (optional)
    pub highway_class: u16,   // enum index (required)
    pub class_bits: u32,      // bit flags (toll,tunnel,bridge,link,residential,track,cycleway,footway,ferry,…)
    pub per_km_penalty_ds: u16, // extra deciseconds per km (preference shaping; 0 if none)
    pub const_penalty_ds: u32,  // constant penalty per edge entry (e.g., traffic-calmed)
}

#[repr(C)]
pub struct TurnInput<'a> {
    pub tags_keys: &'a [u32],
    pub tags_vals: &'a [u32],
}

#[repr(C)]
pub enum TurnRuleKind { None=0, Ban=1, Only=2, Penalty=3 }

#[repr(C)]
pub struct TurnOutput {
    pub kind: TurnRuleKind,
    pub applies: u8,             // bitmask: bit0=car, bit1=bike, bit2=foot
    pub except_mask: u8,         // bitmask for exceptions (same encoding)
    pub penalty_ds: u32,         // for Penalty; 0 otherwise
    pub is_time_dependent: bool, // true if any :conditional encountered
}
```

**Exports:**

```rust
#[no_mangle] pub extern "C" fn profile_version() -> u32; // increment on ABI change
#[no_mangle] pub extern "C" fn process_way(input: WayInput) -> WayOutput;
#[no_mangle] pub extern "C" fn process_turn(input: TurnInput) -> TurnOutput;
```

> Profiles interpret **only tag semantics**. No geometry, no node lookups.

---

## Artifacts (on disk)

### 1) `way_attrs.<mode>.bin`  (one per mode)

Fixed-width, sorted by **way_id** (ascending). Little-endian, mmap-friendly.

```
Header (64B)
  magic        u32 = 0x57415941   // "WAYA"
  version      u16 = 1
  mode         u8  = {0=car,1=bike,2=foot}
  reserved     u8  = 0
  count        u64                 // number of ways present in ways.raw
  dict_k_sha   [32]u8             // sha256 of ways.raw key dict
  dict_v_sha   [32]u8             // sha256 of ways.raw val dict

Body (count records)
  way_id       i64
  flags        u32                 // bit0 access_fwd, bit1 access_rev, bits2.. enc oneway + feature flags
  base_speed_mmps u32              // mm/s, 0 if inaccessible both ways
  highway_class  u16
  surface_class  u16
  per_km_penalty_ds u16
  const_penalty_ds  u32

Footer (16B)
  body_crc64   u64
  file_crc64   u64
```

**Flag layout (u32 `flags`):**

* bit0: access_fwd, bit1: access_rev
* bits2..3: oneway (00=no, 01=fwd, 10=rev, 11=both)
* bits4..31: class bits (toll=4, ferry=5, tunnel=6, bridge=7, link=8, residential=9, track=10, cycleway=11, footway=12, living_street=13, service=14, construction=15, …) — enumerate in a JSON alongside.

### 2) `turn_rules.<mode>.bin`  (one per mode)

Normalized turn rules for this mode only; sorted by `(via_node_id, from_way_id, to_way_id)`.

```
Header (56B)
  magic         u32 = 0x5455524E   // "TURN"
  version       u16 = 1
  mode          u8  = {0,1,2}
  reserved      u8  = 0
  count         u64
  rel_dict_k_sha [32]u8           // from relations.raw key dict
  rel_dict_v_sha [32]u8

Body (count records)
  via_node_id   i64
  from_way_id   i64
  to_way_id     i64
  kind          u8    // 0=None,1=Ban,2=Only,3=Penalty
  penalty_ds    u32   // valid iff kind==Penalty
  is_time_dep   u8    // 0/1
  reserved[6]   u8

Footer
  body_crc64    u64
  file_crc64    u64
```

**Notes**

* **ONLY** rules materialize to multiple bans as needed at Step 4 (expansion); here we store the canonical triple.
* `is_time_dep=1` means **exclude** from baseline static graph later (but keep here for future TD support).

### 3) `profile_meta.json`

Contains:

* `abi_version`, `profile_version_car/bike/foot`
* enumerations for `highway_class`, `surface_class`, bit positions for `class_bits`
* unit constants and rounding policy
* sha256 of inputs and produced artifacts

---

## Pipeline

1. **Open dictionaries**

   * Map `ways.raw` and `relations.raw` dicts into memory. Compute SHA-256 for headers to pin.

2. **Per-way processing**

   * Stream `ways.raw` in **way_id order**. For each way:

     * Create `WayInput` using the tag ids (no string allocation).
     * Call `process_way` three times (car/bike/foot profiles).
     * Write one record per mode into the respective `way_attrs.<mode>.bin`.
   * Keep deterministic feature **class_bits** mapping (centralized enum).

3. **Turn relations processing**

   * Stream `relations.raw`. For each relation with `type=restriction`:

     * Build a `TurnInput` from relation tags.
     * Call `process_turn` **once per mode**.
     * If `TurnOutput.kind != None`, emit a record to that mode’s `turn_rules.<mode>.bin` for **each** concrete triple `(from, via, to)` implied by the relation:

       * If member roles are: `from`=way, `via`=node, `to`=way → single triple.
       * If `via` is a **way** (rare): **defer** expansion — store as *(from_way, via_way, to_way)* with `via_node_id = 0`; Step 4 will expand against geometry/topology. Mark such entries with `is_time_dep=2` (special “needs expansion” flag).
     * For `only_*` restrictions: store as kind `Only` (do **not** explode to bans here).
     * `:conditional` tags ⇒ set `is_time_dep=1`.

4. **Finalize**

   * Fill headers (counts, SHA-256 of dicts), compute CRC-64s, fsync.
   * Write `profile_meta.json`.

**Resource bounds**: Streaming; peak RSS **< 1.5 GB**.

---

## Determinism & Rounding (musts)

* **Integer speeds**: `base_speed_mmps = round(max(0, min(MAX, kmh * 1000.0 / 3.6)))`.
* **Penalty units**: deciseconds (ds) as integers; per-km penalty applied later in weight calc.
* **One-way logic** is mode-specific (e.g., `oneway:bicycle=no` overrides `oneway=yes` for bike).
* **Access resolution order (example)**: `access:*` → mode-specific (`motor_vehicle`, `bicycle`, `foot`) → `oneway:*` → exceptions (`vehicle=*`, `except=*`) — codify in profile tests.

---

## Validation & **Lock conditions**

You **cannot proceed** until all pass.

### A. Structural integrity

1. `way_attrs.<mode>.bin.header.count == ways.raw.count` for all modes.
2. CRC-64s in all produced files verify; header SHA-256 of dicts match inputs.
3. `turn_rules.<mode>.bin` sorted by `(via_node_id, from_way_id, to_way_id)` (single pass check).
4. Two identical runs → identical SHA-256 for each artifact (determinism).

### B. Profile semantics (unit tests baked into CI)

5. **Golden tag cases** (per mode, minimum set):

   * `highway=motorway` → access car fwd/rev true/false as per `oneway` rules; base speed derived from `maxspeed` or default table.
   * `highway=track + tracktype=grade4` (bike) → reduced base speed & possibly access=false if `bicycle=no`.
   * `highway=footway` (car) → access false.
   * `bicycle=dismount` (bike) → access true but base speed limited (e.g., foot speed).
   * `route=ferry` / `motor_vehicle=no` / `bicycle=yes` cases.
   * `maxspeed=signals` / `maxspeed:type=…` parsing, including `mph`.
   * `oneway:bicycle=no` overrides `oneway=yes`.
   * Barrier cases (`barrier=gate`, `access=private`, `destination`).
   * `*:conditional` flagged `is_time_dep=1`.

   **Lock** if every expected `WayOutput` field matches the golden table **exactly**.

6. **Enumeration stability**

   * `highway_class` and `surface_class` enum ids are **stable** (compare to snapshot JSON).
   * `class_bits` positions match `profile_meta.json`.

7. **Turn restriction correctness**

   * For a random sample of ≥10k `type=restriction` relations:

     * `process_turn` does **not** return `None` when a legal restriction tag is present.
     * Roles `from|via|to` captured correctly (member kinds respected).
     * `only_*` produce kind `Only`; `no_*` produce kind `Ban`.
     * `except=*` populates `except_mask` bits (via `process_turn`).
     * `:*:conditional` → `is_time_dep=1`.

   **Lock** if zero mismatches.

8. **Coverage parity**

   * Count of relations with restriction semantics in `relations.raw` equals the number of **distinct** `(rel_id)` that yielded a non-`None` `TurnOutput` in at least one mode (allowing time-dependent flag).
   * **Lock** if counts match.

### C. Cross-artifact consistency

9. **Access vs classes**

   * No way with `highway=motorway` yields `access=true` for foot or bike unless tags explicitly allow.
   * Ferry set (`route=ferry`) must set `class_bits.ferry` for all modes; access follows tags.
   * **Lock** if all checks pass on full dataset scan.

10. **Speed bounds**

* `base_speed_mmps` must be within `[walk_min, vmax_mode]` hard limits:

  * car: ≤ 60 m/s (216 km/h) unless an explicit higher `maxspeed` table allows it, still capped at 80 m/s.
  * bike: ≤ 16.7 m/s (60 km/h), ≥ 0 if access true.
  * foot: ≤ 2.8 m/s (10 km/h).
* **Lock** if all records satisfy bounds.

### D. Performance & resources

11. Peak RSS ≤ **1.5 GB**.
12. Throughput: ≥ **300k ways/s** and ≥ **50k relations/s** on a modern 16-core box (indicative).
13. File sizes (indicative; Belgium-scale):

* `way_attrs.car.bin` ~ 100–200 MB; bike/foot typically smaller.
* `turn_rules.<mode>.bin` ~ few–tens of MB.

### E. Failure handling

14. If a relation has `via=way`, mark `is_time_dep=2` and log **exact** rel_id count; do **not** drop it.
15. Unknown tags: ignored by profiles without panic; return consistent defaults.

**Lockfile (append fields to Step 1’s format):**

```json
step2.lock.json {
  "input_sha256": "...", 
  "ways_sha256": "...",
  "relations_sha256": "...",
  "way_attrs": {
    "car": {"sha256":"...", "count": Nw, "crc64":"..."},
    "bike":{"sha256":"...", "count": Nw, "crc64":"..."},
    "foot":{"sha256":"...", "count": Nw, "crc64":"..."}
  },
  "turn_rules": {
    "car":  {"sha256":"...", "count": Nr_car,  "crc64":"..."},
    "bike": {"sha256":"...", "count": Nr_bike, "crc64":"..."},
    "foot": {"sha256":"...", "count": Nr_foot, "crc64":"..."}
  },
  "profile_meta_sha256": "...",
  "created_at_utc": "..."
}
```

---

## CLI (Step 2)

```
osm-build step2-profile \
  --ways ways.raw \
  --rels relations.raw \
  --nodes nodes.sa \
  --profile-car   ./profiles/car/target/release/libcar_profile.so \
  --profile-bike  ./profiles/bike/target/release/libbike_profile.so \
  --profile-foot  ./profiles/foot/target/release/libfoot_profile.so \
  --outdir /data/osm/2025-10-29 \
  --threads 16
```

Exit **0** only if all **lock conditions A–E** pass and `step2.lock.json` is written.

---

## Notes

* This step is **pure tag semantics**; no topology yet.
* Time-dependent restrictions are preserved and flagged (`is_time_dep`), but **not** applied in the static baseline.
* Enumerations/bit positions are centralized in `profile_meta.json` to keep later steps consistent and drift-free.

If you want, I’ll write the **exact Rust structs & full writer/reader code** for `way_attrs.*` and `turn_rules.*` next—copy/paste ready.

