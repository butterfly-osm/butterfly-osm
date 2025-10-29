Here’s **Step 1: PBF ingest** — fully specified so you can implement it, run it, and **lock it as correct** before moving on.

# Step 1 — PBF Ingest (stream → immutable artifacts)

## Objective

Deterministically read an OSM **.pbf** and emit three immutable, compact artifacts:

1. `nodes.bin` — mapping of **OSM node id → (lat, lon)** in fixed-point.
2. `ways.raw` — per-way: ordered **node ids** + **minimal tag view** (lossless for what we need later).
3. `relations.raw` — **turn restrictions** and relevant relation info (lossless for restrictions).

All outputs must be **bit-for-bit deterministic** for the same inputs.

---

## Inputs

* `planet-latest.osm.pbf` (or region).
* Optional: **block allowlist** (to filter bbox during dev), not used in production.

---

## Constraints

* **RAM budget** during ingest: **< 2 GB** (planet).
* **Single pass** over PBF payloads (DenseNodes, Ways, Relations), streaming.
* **No tag interpretation** here (that’s Step 2). Keep tags raw/minimal-lossless.

---

## Data Representation (on disk)

### 1) `nodes.sa` + `nodes.si` (sorted array + sparse index)

* Purpose: random-access node coords by **OSM node id** with O(log N) lookup.
* **Rationale**: Sparse bitmap approach wastes gigabytes when OSM IDs span billions (99.5% empty for Belgium). Sorted array is simpler, smaller, and mmap-friendly.

#### `nodes.sa` (Sorted Array - little-endian, memory-mappable, fixed 16 bytes/record)

  ```
  Header (128 bytes):
    magic:        u32 = 0x4E4F4453      // "NODS"
    version:      u16 = 1
    reserved:     u16 = 0
    count:        u64                    // number of nodes stored
    scale:        u32 = 10_000_000       // 1e-7 deg units (WGS84)
    bbox_min_lat: i32                    // fixed-point (scale above)
    bbox_min_lon: i32
    bbox_max_lat: i32
    bbox_max_lon: i32
    created_unix: u64                    // timestamp
    input_sha256: [32]u8                 // SHA-256 of input PBF
    reserved2:    [60]u8 = 0

  Body (count records, sorted strictly ascending by id):
    For each node:
      id:         i64                    // OSM node id
      lat_fxp:    i32                    // degrees * scale
      lon_fxp:    i32                    // degrees * scale

  Footer (16 bytes):
    body_crc64:   u64                    // CRC-64-ISO of body section
    file_crc64:   u64                    // CRC-64-ISO of entire file except footer
  ```

#### `nodes.si` (Sparse Index - two-level for fast binary search bounds)

  ```
  Header (32 bytes):
    magic:        u32 = 0x4E4F4458      // "NODX"
    version:      u16 = 1
    reserved:     u16 = 0
    block_size:   u32 = 2048             // records per sample
    top_bits:     u8  = 16               // high-bit partition (2^16 buckets)
    reserved2:    [19]u8 = 0

  Level 1 (65536 entries for top_bits=16):
    For each bucket k in [0..65535]:
      start_idx:  u64                    // index into Level 2 for first sample
      end_idx:    u64                    // one past last (start=end if empty)

  Level 2 (M = ceil(count / block_size) samples):
    For each sample j in [0..M-1]:
      id_sample:  i64                    // id at record j*block_size in nodes.sa
      rec_index:  u64                    // = j*block_size
  ```

* **Size**: Belgium (70.1M nodes) → nodes.sa ≈ **1.12 GB**, nodes.si ≈ **1.6 MB**
* **Access**:
  1. Compute `hi = (id as u64) >> (64 - top_bits)`
  2. Read Level-1 bucket `[start_idx, end_idx)` → if empty, not found
  3. Binary search Level-2 samples to find candidate block j
  4. Binary search nodes.sa records `[j*block_size .. min((j+1)*block_size, count))` on id
  5. Return `(lat_fxp, lon_fxp)` if found
* **Complexity**: O(log(samples_in_bucket) + log(block_size)) ≈ 20-30 comparisons worst-case

### 2) `ways.raw` (append-only, sequential access)

* Purpose: preserve **way → ordered nodes** + **raw tags** needed later.
* Layout:

  ```
  Header:
    magic:      u32 = 0x57415953         // "WAYS"
    version:    u16 = 1
    reserved:   u16 = 0
    count:      u64                      // ways count
    kdict_off:  u64                      // offset to key dictionary (see below)
    vdict_off:  u64                      // offset to value dictionary
  Body:
    For each way (in ascending way id as seen in PBF):
      way_id:    i64
      n_nodes:   u32
      nodes:     i64[n_nodes]            // OSM node ids (unaltered)
      n_tags:    u16
      tags:      { k_id: u32, v_id: u32 } [n_tags]  // dictionary-coded
  Dictionaries:
    kdict: distinct tag keys sorted;   record: (k_id: u32, len: u16, bytes[len])
    vdict: distinct tag values sorted; record: (v_id: u32, len: u16, bytes[len])
  Footer:
    ways_crc64: u64
    file_crc64: u64
  ```
* Note: keep **all tags** for ways (lossless) — you will filter/interpret in Step 2.

### 3) `relations.raw` (restriction-focused)

* Purpose: capture **turn restrictions** precisely, plus minimal extras used later.
* Layout:

  ```
  Header:
    magic:       u32 = 0x52454C53        // "RELS"
    version:     u16 = 1
    reserved:    u16 = 0
    count:       u64
    kdict_off:   u64
    vdict_off:   u64
  Body:
    For each relation of interest (type=restriction or with keys we care about):
      rel_id:    i64
      n_members: u16
      members:
        role_id: u16        // dictionary-coded role ("from","via","to")
        kind:    u8         // 0=node,1=way,2=relation (we store only node/way here)
        reserved:u8=0
        ref:     i64        // OSM id
      n_tags:   u16
      tags:     { k_id: u32, v_id: u32 } [n_tags]
  Dictionaries:
    key/value dictionaries as in ways.raw; roles share the same vdict
  Footer:
    rels_crc64: u64
    file_crc64: u64
  ```
* Filtering: include **only** relations with `type=restriction` (any mode) OR relations whose tags intersect a small allowlist used later (e.g., route=ferry). Everything else can be ignored for Step 1.

---

## Parsing & Pipeline (Rust)

### Core approach

* Streaming parse PBF blocks; process **DenseNodes**, **Ways**, **Relations** separately.
* **No inter-block buffering of full datasets.** Write to disk incrementally.
* **Order of emission** must be stable (ascending ids within each file).

### Recommended crates / techniques

* Use an efficient PBF reader (e.g., implement with Prost over OSM PBF schema or a zero-copy PBF decoder).
* Use **rayon** for CPU-bound transforms **within** a block when safe; keep write order deterministic via per-type output buffers flushed in id order.

### Steps

1. **Nodes pass**

   * For each DenseNodes:

     * Maintain running deltas (OSM PBF stores lat/lon as scaled ints with deltas).
     * Convert to **i32 fixed-point** with `scale = 1e7`.
     * Track global **min/max lat/lon** and **min/max node id**.
     * Append presence bits and coords into **chunked buffers** (e.g., 4–16 MiB) and flush to a temp file.
   * After full pass:

     * Build final `nodes.bin`: write header with `id_base = min_id`, `id_stride = max_id - min_id + 1`.
     * Re-emit presence bitmap (composed during pass) and packed coords in **id order**.
     * Compute **CRC-64** fields; fsync.

2. **Ways pass**

   * For each Way:

     * Collect node refs (i64) and tags (string k/v).
     * Insert keys/values into **deduplicated dictionaries** (hash set), but **store tag pairs temporarily** as string ids (u32) mapped later to sorted ids.
     * Append a record into a temp file (way_id, nodes, tag_khash, tag_vhash).
   * After full pass:

     * Sort **dictionaries lexicographically**, assign sequential ids.
     * One migration pass: rewrite temp records into final `ways.raw` with dictionary ids.
     * Compute CRC-64s; fsync.

3. **Relations pass**

   * For each Relation:

     * Read tags; **keep only** if `type=restriction` or tag keys intersect allowlist (e.g., `restriction:*`, `except`, `vehicle`, `bicycle`, `foot`, `ferry`).
     * Normalize members: store kind (node/way), role (string) → role dictionary.
     * As with ways: build dictionaries, then rewrite to final `relations.raw`.
   * Compute CRC-64s; fsync.

---

## Determinism & Reproducibility

* Sort all items by **OSM id ascending** before final write (nodes implicit via `id_stride`; ways/relations explicit).
* Dictionaries: **lexicographic UTF-8** order; stable sort.
* Hash-based collections must use **seeded deterministic hash** (e.g., `ahash` with fixed seed or a simple FNV-1a).
* All integers are **little-endian** on disk; specify explicitly in writer/reader.
* Record lengths are implied by counts; no varints in our files (fixed sizes for speed).

---

## Concurrency

* Parse blocks sequentially to respect streaming order; inside a block, you may parallelize:

  * DenseNodes: delta decode is sequential; post-decode coord packing can use small threads but keep ordering.
  * Ways/Relations: parallel tag dictionary insertion using sharded maps; commit to per-shard temporary buffers and merge deterministically.
* Disk I/O: use **O_DIRECT/O_SYNC** only at final fsync; otherwise buffered I/O with large writes.

---

## Validation (Lock Conditions)

You **only proceed to Step 2** when **all** conditions pass.

### A. Structural integrity

1. **Counts**

   * `nodes.bin.count` equals number of **present bits** in bitmap.
   * `ways.raw.count` equals the number of PBF ways encountered.
   * `relations.raw.count` equals filtered relation count.
   * **Lock** if counts match and are non-zero for realistic extracts.

2. **Checksums**

   * Re-open each file, recompute CRC-64 sections; must equal footers.
   * **Lock** if all CRCs match.

3. **Determinism**

   * Two runs on identical input produce **identical byte-for-byte files** (compare SHA-256).
   * **Lock** if SHA-256 is identical.

### B. Semantics

4. **Coordinate accuracy**

   * For a random sample (e.g., 1M nodes): reconstruct lat/lon in degrees; compare to PBF-decoded doubles directly; error ≤ **5e-8 deg** (due to fixed-point).
   * **Lock** if max error ≤ threshold.

5. **Way continuity**

   * For a random sample (e.g., 100k ways): verify each consecutive node pair exists in `nodes.bin` presence bitmap.
   * **Lock** if missing ratio = **0** (or ≤ known OSM anomalies threshold, typically ~0; log if any).

6. **Relation coverage (restrictions)**

   * Count of `type=restriction` relations in PBF equals count retained in `relations.raw` (including subtypes like `restriction:conditional`, `no_left_turn`, `only_right_turn`, etc.).
   * **Lock** if counts equal; **and** for a random sample of 10k restrictions, roles `from|via|to` are present and member kinds (node/way) match PBF.

7. **Dictionary losslessness**

   * Reconstruct string tags for a 100k sample and compare to PBF source exactly (byte-equal UTF-8).
   * **Lock** if all equal.

### C. Performance & resource bounds

8. **Peak RSS** ≤ **2 GB** (measure under cgroup limit).
9. **Throughput** ≥ **X MB/s** (set target based on disk: e.g., ≥ 150 MB/s on NVMe; not a hard lock, but investigate if below half of target).
10. **File sizes (planet)** indicative (not strict locks, but alerts):

    * `nodes.bin` ≈ 4–6 GB
    * `ways.raw` ≈ 6–9 GB
    * `relations.raw` ≈ 0.1–0.3 GB

### D. Fuzz & robustness

11. **Fuzzed PBF inputs** (mutated headers, truncated blobs): parser must **fail fast with clear error** and **no panics/UB**; exit code ≠ 0.
12. **Graceful skip** of unknown tags and extra relation members; no crashes.

If **any** fails, fix before proceeding. Keep a **LOCKFILE**:

```
step1.lock.json {
  input_sha256,
  nodes_sha256,
  ways_sha256,
  relations_sha256,
  counts: {nodes, ways, relations},
  bbox: {min_lat, min_lon, max_lat, max_lon},
  created_at_utc
}
```

---

## CLI Specification (for Step 1)

```
osm-build step1-ingest \
  --input planet.osm.pbf \
  --outdir /data/osm/2025-10-29 \
  --scale 10000000 \
  --threads 8 \
  --filter-relations restriction \
  --bbox ""                       # empty = no filter
```

* Exit code 0 only if **all lock conditions** (A–D) pass and `step1.lock.json` is written.
* `--verify-only` re-checks CRCs and determinism (no write).

---

## Error Handling (musts)

* On malformed PBF: print exact block, offset, entity type; do **not** continue.
* On missing nodes referenced by ways: log first N, count total, **but do not drop the way** (needed later to diagnose data issues).
* On dictionary overflow (unlikely): escalate to 64-bit ids; fail with actionable message.

---

## Metrics to export (for CI)

* `ingest.nodes.total`, `ingest.ways.total`, `ingest.rels.total`
* `ingest.bytes_read`, `ingest.seconds`, `ingest.rss_peak_bytes`
* `ingest.determinism.sha256` (strings)
* `ingest.crc_mismatch` (0/1)

---

## Why Step 1 is “locked”

* Files are **immutable, checksummed, deterministic**.
* They contain **all** raw information required for Step 2 (profiles) without committing to any cost model.
* Validation ensures **no drift** vs original PBF content, setting a clean foundation for correctness downstream.

If you want next, I’ll specify **Step 2 (Rust profiles → per-mode edge attributes)** with its file formats and lock conditions.

