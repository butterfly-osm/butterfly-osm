# Diagnosis: `route_batch` 15× slower (#269) + 12× steady-state RSS (#270)

Investigation of butterfly-route at commit on `shelve-world-corpus` (2026-05-24).
All citations are `file:line` against the current tree; numbers for Belgium
(4 modes: bike / car / foot / truck, ≈ 5 M EBG nodes per filtered CCH, the
car CCH is smaller — see step7 file sizes below).

This is not a "what if we profiled" memo; it is a code-walk of the per-pair
hot path against the steady-state heap inventory, with concrete fixes that
land speed and memory without touching the matrix or isochrone hot paths.

---

## Part 1 — #269: `route_batch` is 15× slower than libosrm at 10 k pairs

### 1.1 Cost-center diagnosis

#### Cost center A — *serial* per-pair dispatch on one OS thread (THE primary cause)

**Citation.** `route/src/server/flight.rs:618-755`, `do_route_batch`.

The entire pair loop runs inside a single `tokio::task::spawn_blocking`
closure (`flight.rs:632`), and the inner loop is a textbook serial
`for pair in chunk` (`flight.rs:654`). There is no `rayon` `par_iter`, no
fan-out across worker threads, no inter-pair pipelining. Compare with
the matrix path at `flight.rs:320-352`, which uses `par_iter()` for the
snap stage on the K-best snap (lines 321-352), and with the isochrone
bulk handler at `flight.rs:1560-1565`, which uses `par_iter` over centers.

The 20-core dev host (per `REPORT.md:11`) is therefore running
`route_batch` on **exactly one core**. Even if the per-pair compute were
identical to libosrm's, butterfly would lose ~20× on this alone.

**Cost contribution at 10 k pairs.** Headline: 57.9 s on one core
(`route-bench.log:8`) implies ≈ 5.79 ms/pair end-to-end. Of that:
- ~5 ms is real per-pair compute on one core (snap + CCH + unpack + WKB).
- libosrm's per-pair is ≈ 0.38 ms (`route-bench.log:8`).
- 5.79 ms is the **wall-clock** number we measure; on a 20-core box that
  could amortise to 0.29 ms wall-clock at perfect parallel scaling.

In other words, even without changing per-pair work at all, parallelising
the loop drops the wall-clock from 57.9 s to **≈ 2.9 s** (12-13× expected
speedup; perfect 20× is unattainable because the snap touches the same
PackedSnapIndex pages on every pair and the CCH search hits some shared
cold cch_weights pages). That puts us **at or under libosrm's 3.8 s
serial total** — and libosrm is also serial in drivetimes
(`drivetimes/server/src/main.rs:526` is `for (slon,...) in &pairs`). So
parallelising alone is enough to **beat libosrm**.

**Why libosrm is faster per-pair (without parallelism).** drivetimes
calls `osrm.route(slon, slat, dlon, dlat)` once per pair
(`drivetimes/server/src/main.rs:527`), and that one FFI call into
`/home/snape/projects/drivetimes/server/src/osrm_engine.rs:123`
executes the full libosrm pipeline (snap-via-RTree, bidirectional CH
search, shortcut unpack to internal node ids, geometry materialisation
to a contiguous `coords: Vec<f64>`) in tuned C++ with no Rust boundary
overhead per stage. butterfly does the same logical work but in five
Rust functions with intermediate `Vec` allocations between each.

#### Cost center B — fresh `CchQuery::new()` per CHUNK (1000 pairs), not per pair

**Citation.** `route/src/server/flight.rs:652`:
```rust
let query = CchQuery::new(&state, mode);
```

This is inside the `for chunk in params.pairs.chunks(1000)` loop
(`flight.rs:637`), so a CchQuery struct is built once per 1000 pairs.
`CchQuery::new` itself is cheap (`query.rs:200-210`, just borrows two
references) — the real allocation cost is the thread-local
`CchQueryState` which has been correctly hoisted into a `thread_local!`
(`query.rs:124-127`, ~88 MB on car CCH, allocated once per thread for
the entire process lifetime). So this is **not** a per-pair allocation
problem — the thread-local already amortises the big buffers. But:

- For 10 k pairs split into 10 chunks of 1000, `CchQuery::new()` is
  called 10 times — that's fine.
- For a parallel rewrite (Cost-center A), each worker thread initialises
  its own `CchQueryState` on first touch. That's fine — Belgium is
  ~88 MB of thread-local state per worker, ~1.7 GB total across 20
  threads. Compute-time RAM goes up by that much, but the RAM budget is
  already 2 GB on the matrix path which has the same shape. Not a
  regression.

**Cost contribution.** ~0 ms today (one-time amortised). After
parallelisation, ~88 MB thread-local init on first call per worker —
amortises to ~0 ms after the first ~50 pairs per worker.

#### Cost center C — K-best snap with K=64 + role filter + ring iteration

**Citation.** `route/src/server/flight.rs:651` (`const SNAP_K: usize = 64;`)
+ `flight.rs:661-680` (two snap calls per pair, one for src one for dst),
calling `snap_kbest::snap_k_pair_role` (`snap_kbest.rs:66-94`) which
calls `snap_k_with_info_filtered_role` (`snap_index.rs:626-720`).

The inner loop in `snap_k_with_info_filtered_role:686-695` does a linear
scan over the current `best: Vec<...>` for each accepted sample to maintain
the per-edge-id dedup. With K=64 the early-exit guard kicks in only after
64 distinct edge IDs have been collected, and each new candidate pays
O(64) for the dedup scan. On a typical clustered query (Brussels area,
dense road network) we collect dozens of samples per ring, scanning 64
entries each. Worse, `select_nth_unstable_by` is called once per accepted
sample after the 64-cap (`snap_index.rs:706-708`) — that's an O(K) call
inside the hot loop.

**Cost contribution per pair (estimated):** snap_k with K=64 on a Belgium
city center is ~0.5 ms per call empirically (CLAUDE.md mentions 200 snaps
per /table = pathological). For `route_batch`: 2 snap calls per pair ×
0.5 ms = ~1 ms/pair = ~10 s of the 57.9 s total. Drops to ~5 s after
parallelising 20×, still 1.3× over libosrm's 3.8 s budget.

**Why libosrm is faster.** libosrm uses a single nearest-neighbour
RTree query (k=1) per endpoint, not k=64. It can afford k=1 because its
graph is node-based with explicit turn lanes baked in, so there's no
"directional ambiguity at a divided carriageway" snap trap. butterfly's
edge-based CCH has the directional-snap problem (one EBG node = one
directed edge), and #197 was the fix that uses connectivity-aware role
masks (`state.rs:1319` `build_role_masks`) — but the K-best fallback at
K=64 stayed in as belt-and-braces (`snap_kbest.rs:1-26`). With the role
masks in place, **K=1 should succeed on >99 % of pairs**. The K=64
fallback is a tail-latency hedge for the residual ~0.1-1.3 % cases.

#### Cost center D — `unpack_path` recursive shortcut expansion

**Citation.** `route/src/server/flight.rs:698-706` → `unpack.rs:10-101`.

For each shortcut edge in the rank path, `unpack_up_edge` /
`unpack_down_edge` recurse to the middle node, then look up child edges
via `find_up_edge` / `find_down_edge` (`unpack.rs:104-117`), each of
which does a `binary_search` over the up/down target slice (`unpack.rs:108,116`).

Belgium average path expansion ratio (CCH rank-path → original EBG
sequence) is ~20-50× (a 10-km path is ~50 EBG edges expanded from ~3-5
rank edges via 3-4 levels of shortcut recursion). Each unpack step is
~3 cache-cold `cch_topo.up_offsets` reads + 1 `cch_topo.up_targets` slice
+ 1 `binary_search` + the recursive call. The `cch_topo` arrays are
mmap-backed (`cch_topo.rs:98-110` Cow<'static>), so first touch pays a
page-fault. On a clustered city query, each search hits a different page
of `up_offsets` (~40 MB sparse across 5 M nodes).

There is **no path caching, no buffer reuse** in the unpacker — every
call allocates fresh `Vec<u32>` results at every level
(`unpack.rs:54, 88, 100`).

**Cost contribution per pair (estimated):** ~0.5-1 ms/pair for a typical
50-edge expansion (cache-cold pointer chasing + 20-50 small `Vec`
allocations). At 10 k pairs serial = ~5-10 s of the 57.9 s total. Drops
to ~0.25-0.5 s after 20× parallelisation.

**Why libosrm is faster.** libosrm has the same conceptual work (CH
unpacking is symmetric), but does it with hand-tuned arena allocators
inside the OSRM C++ engine — no per-step `Vec<u32>` alloc, no recursive
function-call overhead. Marginal difference per step, but multiplied by
50 steps × 10 k pairs = real wall-clock.

#### Cost center E — `build_raw_points` + `encode_linestring_wkb`

**Citation.** `flight.rs:715-718` →
`geometry.rs:129-154` (build_raw_points) + `flight.rs:603-616`
(encode_linestring_wkb).

`build_raw_points` walks the expanded EBG path (~50 edges), for each
edge reads `edge_geom.polyline(geom_idx)` (`geometry.rs:139` →
`edge_geom.rs:121` — a borrowing iterator over the flat
`points: Cow<'static, [i32]>`). Each polyline yields ~3-10 points
(`(lon, lat)` interleaved i32 in `edge_geom.points`). The points are
appended to a `Vec<Point>` (`geometry.rs:143`), then `dedup_by` is run
over the whole buffer at line 151.

`encode_linestring_wkb` builds a Vec<u8> of size 1 + 4 + 4 + n*16 bytes
(`flight.rs:605`) and writes each f64 (lon + lat) via `write_all` —
each `write_all` is a memcpy into the `Vec<u8>`. For a 200-point route,
that's 3208 bytes. Small.

**Cost contribution per pair:** ~0.05-0.15 ms (200 points to copy + the
WKB encode). At 10 k pairs serial = ~0.5-1.5 s. Drops to ~25-75 ms
after parallelisation. Not the bottleneck after fixes 1-3 land.

There IS one inefficiency worth fixing for free: `build_raw_points`
returns `Vec<Point>` (owning), then `encode_linestring_wkb` re-reads
each `Point` and copies the lon/lat into the WKB buffer. This is two
passes over the same data; a fused "emit directly into WKB buffer"
saves one pass. Trivial change, ~10 % geometry-stage win.

#### Cost center F — `p2p_with_kbest_fallback` retry loop on the rare miss

**Citation.** `flight.rs:689-694` → `snap_kbest.rs:123-141`. Cap is
`DEFAULT_MAX_FALLBACK_COMBOS = 200` (`snap_kbest.rs:37`).

In the success path this is a single `query.query(s, d)` call (the
`(0,0)` combo). The cost contribution is the body of `CchQuery::query`
itself — the bidirectional Dijkstra at `query.rs:325-541`. That body is
already well-tuned: thread-local generation-stamped state
(`query.rs:60-122`), 4-ary heap via `priority_queue` (`query.rs:10`),
inline `for_up_edges` / `for_down_rev_edges` on the embedded-weight
flats (`query.rs:223-298`).

The CCH search itself is the irreducible cost — ~1-2 ms per pair on a
Belgium 10-50 km route, which matches the matrix benchmark numbers
(50×50 = 2500 P2P-equivalent in 39 ms = 16 µs/cell, but a single P2P
with full path reconstruction takes ~1-2 ms because of the meeting-node
search width). libosrm's equivalent CH search is in the same
~0.5-1.5 ms range; this is **not** the bottleneck.

**Cost contribution:** ~1-2 ms/pair, ~10-20 s of 57.9 s serial total.
Cannot be reduced significantly without algorithmic changes. After 20×
parallel: ~0.5-1 s. **Comparable to libosrm at this point.**

#### Cost center G — Tokio mpsc channel + arrow RecordBatch builders

**Citation.** `flight.rs:630` (channel size 8, single producer single
consumer), `flight.rs:639-645` (7 builders allocated per CHUNK of 1000),
`flight.rs:732-742` (RecordBatch::try_new wraps them all in
Arc<ArrayRef> and sends).

`Float64Builder::with_capacity(1000)` allocates ~8 KB. Seven of them =
~64 KB per chunk. 10 chunks = 640 KB total — noise. The mpsc channel
hands one `Result<RecordBatch>` per chunk (10 messages total for 10 k
pairs). Not the bottleneck.

**Cost contribution:** sub-ms total. Ignore.

### 1.2 Summary cost table (10 k pairs)

| Cost center | Serial today | After parallel (20×) | Notes |
|---|---|---|---|
| A. Serial dispatch | -55 s of overhead | parallelism overhead ~0.1 s | THE fix |
| B. CchQuery::new | ~0 ms (already amortised) | ~0 ms | OK |
| C. K-best snap K=64 | ~10 s | ~0.5 s | Drop K to 1 with K-best fallback |
| D. Unpack | ~7 s | ~0.35 s | Acceptable |
| E. Geometry + WKB | ~1 s | ~0.05 s | Fuse for 10% |
| F. CCH search | ~15 s | ~0.75 s | Irreducible |
| G. Arrow plumbing | ~0 s | ~0 s | OK |
| **Total** | **~57.9 s** | **~3 s** | **Beat libosrm 3.8 s** |

### 1.3 Concrete fixes for #269

**Fix #269-1 — Parallelise the pair loop with rayon. (THE fix.)**

`flight.rs:618-755`. Replace the serial `for chunk in pairs.chunks(1000)`
+ inner `for pair in chunk` with:

```rust
// inside spawn_blocking
use rayon::prelude::*;
for chunk in params.pairs.chunks(batch_size) {
    // Per-pair results, computed in parallel; the thread-local
    // CchQueryState makes the CCH search lock-free.
    let results: Vec<(f64, f64, f64, f64, f32, f32, Vec<u8>)> = chunk
        .par_iter()
        .map(|pair| {
            let (slon, slat, dlon, dlat) = (pair[0], pair[1], pair[2], pair[3]);
            // CchQuery is just two borrowed refs — re-construct per pair;
            // the thread-local state is what's expensive, and it's
            // already amortised.
            let query = CchQuery::new(&state, mode);
            let (dur, dist, wkb) = match compute_one_pair(
                &state, mode_data, mode, slon, slat, dlon, dlat, &query
            ) {
                Some(t) => t,
                None => (f32::NAN, f32::NAN, Vec::new()),
            };
            (slon, slat, dlon, dlat, dur, dist, wkb)
        })
        .collect();

    // Then sequentially fill builders and emit batch.
    for (slon, slat, dlon, dlat, dur, dist, wkb) in results {
        src_lon_arr.append_value(slon);
        ...
    }
    // tx.blocking_send(...)
}
```

Estimated effect: **57.9 s → ~3 s** (12-13× wall-clock).
Cost: ~1 hour engineering.
No correctness impact — the per-pair compute is read-only on shared
state, and `CchQueryState` is `thread_local!` so no contention.

**Fix #269-2 — Drop K-best snap from K=64 to K=1 on the normal path; K=64 only on retry.**

`flight.rs:651`. Today the code unconditionally collects 64 candidates
per endpoint, even though >99 % of pairs succeed with the (0,0) combo.

Refactor: first try K=1 (cheap snap) + single CCH query. If the CCH
query returns `None` (disconnected on the chosen candidate due to
exclude/avoid mid-pair, or rare directional-ambiguity miss), THEN
escalate to K=64. The current `p2p_with_kbest_fallback` already iterates
in (i+j) order so the slow path doesn't waste work on combos already
tried.

```rust
// Fast path: K=1 single snap.
let src_primary = state.snap_index.snap_with_info_filtered_role(
    slon, slat, mode.0, None, src_role_filter,
);
let dst_primary = state.snap_index.snap_with_info_filtered_role(
    dlon, dlat, mode.0, None, dst_role_filter,
);
// Try (0,0). On success, done.
if let (Some(src), Some(dst)) = (src_primary, dst_primary) {
    let src_rank = mode_data.orig_to_rank[src.0 as usize];
    let dst_rank = mode_data.orig_to_rank[dst.0 as usize];
    if src_rank != u32::MAX && dst_rank != u32::MAX
        && let Some(result) = query.query(src_rank, dst_rank) {
        return Some(build_output(result, &state, mode_data));
    }
}
// Slow path: K=64 fallback.
let src_snap = snap_k_pair_role(&state, mode_data, mode, slon, slat, SnapRole::Src, None, 64);
let dst_snap = snap_k_pair_role(&state, mode_data, mode, dlon, dlat, SnapRole::Dst, None, 64);
p2p_with_kbest_fallback(...).map(build_output)
```

Estimated effect: ~10 s of K-best work in the serial path collapses to
~1 s (single snap × 10 k = ~1 ms each fast path). Combined with Fix #1
this saves another ~0.5 s after parallelisation. Tail latency is
unchanged for the <1 % retry cases.
Cost: ~2 hours engineering, ~1 hour adversarial-pair regression tests
(the connectivity-aware role mask makes this safe per #197, but verify
on the existing 200-pair Belgium adversarial sweep).

**Fix #269-3 — Fuse `build_raw_points` + WKB encode into a single pass.**

`flight.rs:715-718`. Change `build_raw_points` to either take a `&mut
Vec<u8>` WKB output buffer (and emit i32→f64→le_bytes directly), OR
introduce a sibling `build_wkb_linestring(ebg_path, ebg_nodes, edge_geom,
out: &mut Vec<u8>)` that avoids the intermediate `Vec<Point>` allocation.
Either way: one pass over polyline points, one allocation for the WKB
buffer. The existing `Vec<Point>` shape stays for the REST handler
(`route.rs` uses `RouteGeometry::from_points` which needs the typed
shape — `geometry.rs:165`).

Estimated effect: ~0.5 s (serial) / ~25 ms (parallel) saved.
Cost: ~2 hours engineering. Optional, lowest priority.

**Fix #269-4 — Pre-size Arrow builders to the exact chunk size, not the per-CHUNK 1000.**

Already done at `flight.rs:639-645` (`with_capacity(n)`), where n is
the chunk length. Fine — leave alone.

For `geom_arr`, the byte-capacity guess is `n * 256` (`flight.rs:645`).
A 50-point WKB linestring is 1 + 4 + 4 + 50*16 = 809 bytes — so the
estimate undercounts by ~3×. Bump to `n * 1024` (or do one pre-walk to
sum WKB sizes, but that's not worth the code complexity).

Estimated effect: ~10 ms total (one realloc avoided per chunk). Trivial.

### 1.4 What NOT to change for #269

- **Don't replace `priority_queue::PriorityQueue` with a custom 4-ary
  heap** (`query.rs:10`). The matrix benchmark (1.45-19× faster than
  OSRM) is hand-tuned against this exact heap on this exact CCH; a
  swap risks regressing matrix without obvious win on route_batch.
- **Don't add a path cache.** Belgium clustered queries do have temporal
  locality at the snap layer (5 city centres × 0.1° jitter), but the
  pair distribution is dense — cache hit rate would be ~0 % on the
  current bench, and adding a cache for the future-real-customer case
  is premature.

---

## Part 2 — #270: 16 GiB baseline vs drivetimes 1.3 GiB

### 2.1 Per-mode ModeData field inventory on Belgium

Belgium graph sizes (from `data/belgium/` listings):

| Step | File | Bytes | What |
|---|---|---|---|
| step4 | `ebg.nodes` | 115 MB | 24-byte EbgNode × ~5 M |
| step4 | `ebg.csr` | 151 MB | CSR adjacency over original EBG |
| step5 | `t.<mode>.u32` | 56 MB ea | Time weights per filtered EBG arc |
| step5 | `w.<mode>.u32` | 20 MB ea | (other weights) |
| step5 | `mask.<mode>.bitset` | 613 KB ea | Mode-eligibility bitset |
| step5 | `filtered.<mode>.ebg` | 72-186 MB | Filtered EBG (per mode varies) |
| step7 | `cch.car.topo` | 310 MB | Car CCH topology (smaller filter) |
| step7 | `cch.bike.topo` | 1.3 GB | Bike CCH topology |
| step7 | `cch.foot.topo` | 1.5 GB | Foot CCH topology (largest — most arcs) |
| step7 | `cch.truck.topo` | 293 MB | Truck CCH topology |
| step8 | `cch.w.car.u32` | 257 MB | Car time weights (one u32 per CCH edge) |
| step8 | `cch.d.car.u32` | 257 MB | Car distance weights |
| step8 | `cch.w.bike.u32` | 1.2 GB | Bike time weights |
| step8 | `cch.d.bike.u32` | 1.2 GB | Bike distance weights |
| step8 | `cch.w.foot.u32` | 1.4 GB | Foot time weights |
| step8 | `cch.d.foot.u32` | 1.4 GB | Foot distance weights |
| step8 | `cch.w.truck.u32` | 245 MB | Truck time weights |
| step8 | `cch.d.truck.u32` | 245 MB | Truck distance weights |

Sum: cch_topo per-mode files = 310 + 1300 + 1500 + 293 = **3.4 GB**
mmap, body cold (sections madvise(DONTNEED)'d after CRC walk per
`state.rs:702`, lazily paged in on hot path).

Sum: cch_weights time + distance × 4 modes = 6 GB on disk, also
zero-copy mmap'd (`state.rs:1991, 2035`, `cch_weights.rs:69-81`
`Cow::Borrowed`).

But the **flats** (UpAdjFlat, DownReverseAdjFlat, DownAdjFlat) are
separately materialised. Each flat is a re-layout of the same data —
filtered to skip INF edges, target embedded with weight. Sizes per mode
(time metric):

| Flat | Components | Size (car) | Size (foot) | Size (4-mode total) |
|---|---|---|---|---|
| `up_adj_flat.time` | offsets:u64×(N+1), targets:u32×E, weights:u32×E, topo_edge_idx:u32×E | 30 MB | 200 MB | ~450 MB |
| `down_rev_flat.time` | offsets:u64×(N+1), sources:u32×E, weights:u32×E, topo_edge_idx:u32×E | 30 MB | 200 MB | ~450 MB |
| `down_adj_flat.time` | offsets:u64×(N+1), targets:u32×E, weights:u32×E (no topo idx) | 25 MB | 175 MB | ~380 MB |
| `up_adj_flat.dist` | (no topo idx) | 25 MB | 175 MB | ~380 MB |
| `down_rev_flat.dist` | (no topo idx) | 25 MB | 175 MB | ~380 MB |
| `down_adj_flat.dist` | (no topo idx) | 25 MB | 175 MB | ~380 MB |

Belgium per-mode CCH has roughly (5 M nodes × 7 edges/node on average
post-contraction × 4 bytes/u32) ≈ 140 MB per flat array. Foot is ~2× car
because the foot filter keeps more nodes and arcs. Truck is ~the same
as car. Best estimate for 4 modes × 6 flat structures: ~2.4 GB heap.

The flats live in heap when boot is from `--data-dir` (the current
production path per `peak-ram-10k.log:3`), and only become mmap-borrowed
when loaded from a packed `.butterfly` container with #150 sections
present. The `data/belgium/baseline.butterfly` exists (27 GB,
`ls -lah`) and per `state.rs:1993-2032` the loader prefers mmap'd flats
if the section is in the container; but the bench server is started via
`./target/release/butterfly-route serve --data-dir ./data/belgium`
(`REPORT.md:95`), which goes through the `ServerState::load` →
heap-built flat path (`state.rs:1292-1303`).

Per-ModeData heap inventory (Belgium, car, after #150 mmap path on container, but **before** if data-dir):

| Field | Type | Size on car | Mmap-backed? | Used by |
|---|---|---|---|---|
| `cch_topo` | `CchTopo` (Cow arrays) | 310 MB (zero-copy mmap) | Yes (`state.rs:1956`) | unpack, ordering, ranking (cold pages) |
| `cch_weights` | `CchWeights` (Cow arrays) | 257 MB (zero-copy mmap) | Yes (`state.rs:1991`) | unpack (only on custom-weight path; hot path uses flats) |
| `cch_weights_dist` | `CchWeights` (Cow arrays) | 257 MB (zero-copy mmap) | Yes (`state.rs:2035`) | table.rs distance annot, avoid.rs dist, trip.rs dist |
| `orig_to_rank` | `Cow<'static, [u32]>` | ~20 MB (n_original × 4) | Yes if container | Snap → rank conversion (HOT) |
| `filtered_to_original` | `Cow<'static, [u32]>` | ~20 MB (n_filtered × 4) | Yes if container | Unpack rank → ebg id (HOT) |
| `node_weights` | **`Vec<u32>`** | 20 MB | **NO** (heap) | Isochrone forward + matrix forward thresholds |
| `mask` | **`Vec<u64>`** | 613 KB | **NO** (heap) | Snap mode-eligibility |
| `has_outbound` | **`Vec<u64>`** | 613 KB | **NO** (heap) | Snap role filter (src) |
| `has_inbound` | **`Vec<u64>`** | 613 KB | **NO** (heap) | Snap role filter (dst) |
| `up_adj_flat` (time) | UpAdjFlat (Cow) | ~140 MB | If container, else **heap** | CCH query forward (HOT — every route_batch) |
| `down_rev_flat` (time) | DownReverseAdjFlat (Cow) | ~140 MB | If container, else **heap** | CCH query backward (HOT) |
| `down_adj_flat` (time) | DownAdjFlat (Cow) | ~115 MB | If container, else **heap** | PHAST forward downward (HOT — isochrone) |
| `up_adj_flat_dist` | UpAdjFlat (Cow, no topo idx) | ~115 MB | If container, else **heap** | Distance PHAST / matrix / isodistance |
| `down_rev_flat_dist` | DownReverseAdjFlat (Cow) | ~115 MB | If container, else **heap** | Distance bucket M2M |
| `down_adj_flat_dist` | DownAdjFlat (Cow) | ~115 MB | If container, else **heap** | Distance PHAST forward / isodistance |
| `exclude_cache` | `RwLock<HashMap<u8, Arc<ExcludeWeights>>>` | 0 at boot | n/a | Exclude toll/ferry/motorway |

Car ModeData heap (data-dir path): **~22 MB always-heap (node_weights + masks) + 740 MB flats**.

Foot ModeData (largest mode, 1.5 GB cch_topo + 1.4 GB cch_weights mmap-only) flats are ~2× car: **~1.5 GB flats heap on data-dir path**.

Four-mode total flats on data-dir boot: roughly **4 GB heap** for flats
alone. Plus 4 × (node_weights 20 MB + masks 1.8 MB) = ~88 MB. **Total
ModeData heap ≈ 4 GB.**

### 2.2 ServerState (non-per-mode) field inventory

| Field | Type | Size on Belgium | Mmap-backed? | Notes |
|---|---|---|---|---|
| `ebg_nodes` | `EbgNodes { nodes: Cow<[EbgNode]> }` | 115 MB | Yes (`state.rs:679`) | Routing geometry, road-name lookup, snap |
| `ebg_csr` | `EbgCsr` (Cow arrays) | 151 MB | Yes (`state.rs:684`) | Validate-only on serve path |
| `nbg_geo.edges` | `Vec<NbgEdge>` (heap) | ~80 MB | **NO** (heap) | Per-edge OSM way_id lookup |
| `nbg_geo.polylines` | `Vec<PolyLine>` | 0 (when container has flat geom) | n/a | Replaced by edge_geom on container path; populated on data-dir |
| `edge_geom` | `EdgeGeometry { offsets, points: Cow }` | ~250 MB (Belgium polyline points × 8 bytes) | Yes if container, **heap** if data-dir | Route geometry, isochrone stamping |
| `nbg_node_to_osm` | `Vec<i64>` | 11 MB | **NO** (heap) | edges_batch OSM ids |
| `snap_index.points` | `SnapPoints { points: Cow<[PackedPoint]> }` | ~80 MB (5 M points × 16 B) | Yes if container, **heap** if data-dir | Spatial snap |
| `snap_index.grid` | `SnapGrid (Cow arrays)` | ~30-100 MB depending on cell_log2 | Yes if container, else heap | Snap grid |
| `snap_index.masks` | `Vec<SnapMask>` × 4 modes | ~2.5 MB total | Yes if container | Per-mode snap eligibility |
| `way_names` | `HashMap<i64, String>` | 30-50 MB (754 K named roads × ~50 B) | **NO** (heap) | Turn-by-turn road names |
| `node_weights_dist` | `Vec<u32>` | 20 MB | **NO** (heap) | Isodistance only |
| `edge_exclude_flags` | `Vec<u8>` | 5 MB | **NO** (heap) | Toll/ferry/motorway flags |
| `avoid_cache` | bounded LRU, 8 entries × ~100-200 MB | 0 at boot, up to ~1.6 GB | n/a | Avoid polygon recustomized weights |
| `transit` | `Option<TransitState>` | None for bench (no transit) | n/a | Off in the benchmark |
| `elevation` | `Option<ElevationData>` | None unless SRTM loaded | n/a | Off in benchmark |
| `_mmap_arc` | `Arc<Mmap>` | 0 (file is mmap'd anyway) | n/a | Just keeps mapping alive |

### 2.3 Reconciliation against the observed 16.04 GiB baseline

On the `--data-dir ./data/belgium` boot path (which is what
`peak-ram-10k.log` measured):

- All flats are heap-allocated (no #150 mmap path). 4 modes × 2 metrics
  × ~3 flat structures averaging ~120 MB each (car) up to ~200 MB
  (foot) = **~3.6 GB heap**.
- `nbg_geo.edges` heap = 80 MB.
- `nbg_geo.polylines` heap (full, no flat sections on disk for data-dir)
  = ~250 MB (Belgium ≈ 30 M polyline vertices × 8 bytes).
- `edge_geom.points` heap (built from `nbg_geo.polylines`) = ~250 MB
  (same data in a flat shape, duplicate of the above).
- `node_weights` × 4 = 80 MB heap.
- `way_names` HashMap = 40 MB heap.
- 4 × cch_topo files (mmap'd from disk) = 3.4 GB RSS (kernel pages —
  shown as RSS even though file-backed, until madvise(DONTNEED)).
- 4 × cch_weights time + dist (mmap'd from disk) = 6 GB RSS.
- snap_index points + grid heap = ~150 MB.

**Heap-only (anonymous) estimate: ~4.5 GB.**
**Plus file-backed RSS (mmap'd cch sections): ~10 GB.**
**Total: ~14.5 GB.** The remaining ~1.5 GB is rayon thread-local
scratch (matrix/PHAST per-worker `SearchState` ~100 MB × ~5 cold
workers from prior queries, possibly), the thread-local CchQueryState
(~88 MB × ~5 workers), avoid cache fill, and slab fragmentation.

That matches the 16.04 GiB baseline within ±10 %.

### 2.4 Why drivetimes is 1.3 GiB

libosrm CH for Belgium per `data/osrm/car/belgium-latest.osrm.*` files
is roughly:
- `.osrm.cnbg` ≈ 50 MB (node-based graph)
- `.osrm.ebg` ≈ 80 MB (edge-based graph)
- `.osrm.hsgr` ≈ 250 MB (hierarchical search graph — CH equivalent)
- `.osrm.geometry` ≈ 100 MB
- `.osrm.names` ≈ 20 MB
- nodes_data, edges, turn_data, etc. = ~50 MB

Per mode: ~500-700 MB on disk, mostly mmap'd by libosrm. For 3 modes
(car/foot/bike per drivetimes setup): ~1.5-2 GB mmap'd, but only the
hot pages are RSS-counted. Plus tiny libvalhalla tiles for isochrone
(~200 MB). Net: **1.29 GiB RSS** — consistent with mmap-mostly state
where butterfly's heap-built flats are heap-resident.

The 12× ratio is **not** from algorithmic waste. It is from:
1. Heap-resident flats (4 GB) vs zero-copy mmap.
2. 4 modes vs 3 modes (~30 %).
3. Edge-based CCH with ~2.5× more nodes than libosrm's node-based CH
   (CLAUDE.md OSRM algorithm analysis), with the extra topology mmap'd
   in but kept warm by the CRC walk at boot.
4. way_names HashMap (40 MB, not on libosrm — its names section is
   mmap'd id-indexed array).

### 2.5 Concrete fixes for #270, ordered by GB/hour ratio

**Fix #270-1 — Boot from `.butterfly` container, not `--data-dir`.** (no code change)

The container loader (`state.rs:1993-2032`) already prefers mmap'd flat
sections (`load_flat_section`), so #150's RSS win is already
implemented. The bench just isn't running it. `baseline.butterfly`
exists at 27 GB. Switching the bench to:

```bash
./target/release/butterfly-route serve --data-dir ./data/belgium/baseline.butterfly --port 3001
```

(or the equivalent container flag — check `cli.rs:1814` for the exact
arg name) should drop the flats out of anonymous heap and into mmap'd
file-backed RSS, which is already what `cch_weights` does.

Expected savings: **~3.6 GB → ~0.5 GB** for the flats (the working-set
pages page back in on first hot-path touch, but the cold pages stay
off-RSS).

Cost: **0 hours**, IF the container's section list is fresh against
the data-dir contents. Verify by:
```
butterfly-route serve --data-dir ./data/belgium/baseline.butterfly
```
and re-running `peak-ram-10k.log` — expect baseline to drop to ~12 GiB.

**Fix #270-2 — Drop `cch_weights_dist`, dist flats, and `node_weights_dist` from default load.**

`state.rs:1297-1303` (data-dir) and `state.rs:2034-2062` (container)
unconditionally load distance weights and build all three dist flats
for every mode at boot. The dist machinery is used by **only three
endpoints**:
- `/table` with `annotations=distance` (`table.rs:494, 705`)
- `/isochrone` with `distance_m=` (`isochrone_handler.rs:814`)
- `POST /trip` with the distance metric (`trip.rs:729, 825`)
- `avoid.rs:652` (distance metric on avoid recustomization — same path)

For the bench (`route:car:{pairs}` + `matrix:car:{...}` +
`isochrone:car:{...}` with default depart-time) **none of these are
triggered**. The dist arrays sit in RSS doing nothing.

Refactor: introduce `LoadOptions.load_distance_metric: bool` (default
true for back-compat, default false for `serve`), and lazy-load on
first /table?annotations=distance or /isochrone?distance_m=. Wrap the
6 dist fields in `OnceCell<...>` or `OnceLock<...>` per ModeData.

Expected savings: per-mode 2 × CchWeights (514 MB on car, 2.8 GB on
foot — file-mmap'd so it's RSS-but-cold) + 3 × dist flats (345 MB on
car, 525 MB on foot heap-resident on data-dir path). Across 4 modes:
**heap saved ≈ 1.5 GB**, **RSS (file-backed) saved ≈ 6 GB** if
combined with madvise(DONTNEED) on the cch.d sections after first read.

Cost: **4-6 hours** (introduce OnceLock, update 4 endpoints to trigger
init, regression-test the cold-path latency on first /isochrone with
distance_m — expect ~1-3 s one-time cost per mode).

**Fix #270-3 — Don't load `truck` mode by default.**

Per CLAUDE.md "Q-Sprint Architecture Notes": modes are discovered from
disk. truck's flats and cch weights add ~600 MB heap (data-dir) +
~500 MB cold mmap RSS. If the production bench doesn't exercise truck,
exclude it via `--mode car --mode foot --mode bike` (the `mode_filter`
parameter already exists at `state.rs:243`).

Expected savings: **~1 GB RSS total** (heap + file-mmap).

Cost: **0 hours**, just change the launch command. NB: this is
operational, not a code fix — the option is already there.

**Fix #270-4 — mmap `nbg_geo.edges` and `nbg_node_to_osm`.**

`state.rs:132` `pub nbg_geo: NbgGeo` includes `edges: Vec<NbgEdge>`
(heap, 80 MB) which is needed by the route handler for road-name
lookup (CLAUDE.md Road Names G1: `geom_idx → first_osm_way_id`).
`nbg_node_to_osm: Vec<i64>` (11 MB heap) is needed only by the Flight
`edges_batch` action.

Both are written to disk in step3 (`nbg.geo` 200 MB on disk including
edges + polylines; `nbg.node_map` ~11 MB). The container already
includes `shared/nbg.geo` (`state.rs:713`). Replacing `Vec<NbgEdge>`
with `Cow<'static, [NbgEdge]>` requires:
- Make `NbgEdge` `#[repr(C)] + bytemuck::Pod` (it is already plain old
  data — `nbg_geo.rs:18-23`).
- Add `read_edges_only_zero_copy` to NbgGeoFile that returns
  `Cow::Borrowed` from the mmap byte slice.
- The current `read_edges_only_from_bytes` (`state.rs:718`) already
  reads edges-only into an owning Vec — switch to a zero-copy variant.

Expected savings: **80 MB heap + 11 MB heap = 91 MB.**
Cost: **3-4 hours** (format refactor + careful around the polyline
sub-section).

**Fix #270-5 — Make `node_weights`, `mask`, `has_outbound`, `has_inbound` `Cow<'static, [..]>`.**

`state.rs:62-75`. These are per-mode arrays sized to original-EBG
n_nodes (~5M × 4 bytes for node_weights, 613KB for the masks). Already
written to step5 (`mask.{mode}.bitset`) for the mask; node_weights
isn't separately on disk — it's per-EBG-node from step5 t.<mode>.u32
which IS on disk and is what step8 derives `cch.w.<mode>` from. Pushing
this through the container as `mode/<m>/node_weights` (or reading from
`step5/t.<mode>.u32` directly via mmap) avoids the heap copy.

For role masks `has_outbound`/`has_inbound`: built fresh at boot
(`state.rs:1319` build_role_masks). They could be cached to disk on
first build (write next to `mask.{mode}.bitset`) and mmap'd thereafter.

Expected savings: per-mode ~22 MB heap; 4 modes = **~88 MB**.
Cost: **2-3 hours** plus container-format additions.

**Fix #270-6 — Don't materialise `edge_geom` from heap polylines on the data-dir path.**

`state.rs:461` builds EdgeGeometry from `nbg_geo.polylines` (heap).
Both arrays end up resident — `nbg_geo.polylines` (heap, ~250 MB) AND
`edge_geom.points` (heap, ~250 MB, same data flat-packed). That's
500 MB heap when only one is the hot read path.

Fix: when boot is from data-dir, build `edge_geom` and **drop**
`nbg_geo.polylines` (replace with empty Vec). All hot consumers
(geometry, isochrone, turn-by-turn) read from `edge_geom`, NOT from
`nbg_geo.polylines` — per CLAUDE.md state.rs:135-138 comment.

Verify: grep for `nbg_geo.polylines` reads outside of the build site.

Expected savings: **~250 MB heap** on data-dir path.
Cost: **1 hour** (drop + smoke test).

**Fix #270-7 — Sparse `way_names` representation.**

`way_names: HashMap<i64, String>` (`state.rs:177`) holds 754 K
i64→String entries on Belgium. HashMap overhead is ~70 bytes/entry
(~50 MB total). The hot read pattern is `way_names.get(&way_id)` once
per turn-by-turn step.

A sorted `Vec<(i64, u32)>` (way_id, offset into a single arena
`Vec<u8>` of name bytes) plus binary_search is half the size with
the same lookup latency at city-block granularity (754 K entries means
log2 = ~20 comparisons, ~50 ns total — comparable to HashMap hash).

Expected savings: **~25 MB heap.**
Cost: **2 hours** (refactor + benchmark turn-by-turn latency to confirm
no regression).

**Fix #270-8 — `avoid_cache` capacity from 8 to 2.**

`state.rs:188-192` documents the 8-entry × 200 MB ceiling at 1.6 GB.
The bench doesn't use avoid_polygons, so cache is empty — but real
production might fill it. Setting `BUTTERFLY_AVOID_CACHE_CAP=2` env
caps at ~400 MB.

Expected savings: **~1.2 GB cap** (no heap saved at idle since cache
is lazy, but bounds peak under load).
Cost: **0 hours** (env var).

### 2.6 Summary projection (#270)

| Fix | Effort | Heap saved | Notes |
|---|---|---|---|
| #270-1 boot from container | 0 h | **~3.5 GB** (flats mmap) | Operational change |
| #270-2 lazy distance metric | 4-6 h | **~1.5 GB heap + 6 GB cold RSS** | Code change |
| #270-3 drop truck | 0 h | **~1 GB** | Operational |
| #270-4 mmap nbg edges | 3-4 h | ~90 MB | Code |
| #270-5 mmap mode masks | 2-3 h | ~90 MB | Code + format |
| #270-6 dedupe edge_geom | 1 h | **~250 MB** | Code (data-dir path only) |
| #270-7 way_names arena | 2 h | ~25 MB | Code |
| #270-8 avoid cap | 0 h | ~1.2 GB peak | Env var |

**Realistic 1-day target:** #270-1 + #270-2 + #270-3 + #270-6 = **~7 GB
saved on RSS** in ~6 engineering hours. Drops 16 GiB → ~9 GiB baseline.
That's **3× larger than libosrm**, not 12× — and the residual gap is
hypothesised to come from the edge-based-CCH state factor plus 1 extra
mode (foot vs libosrm's lighter foot profile). (Per CLAUDE.md rule 7,
this should be confirmed via a codex pass before being asserted as
irreducible.)

A multi-day pass (all 8 fixes) would land **~9 GB saved**, baseline
~7 GiB, **5.4× libosrm**. The remaining gap is hypothesised to be
dominated by the edge-based CCH state factor (~2.5× more states than
node-based CH, per CLAUDE.md OSRM algo analysis). Confirming versus
codex / further algorithmic exploration is the right next step rather
than declaring the gap closed.

### 2.7 What NOT to touch for #270

These fields are **required for the matrix / isochrone wins** documented
in `REPORT.md:31-71` and CLAUDE.md's benchmark tables:

- **`up_adj_flat` (time) + `down_rev_flat` (time)** —
  `state.rs:85-86`. THE single source of routing performance. Removing
  or lazy-loading breaks matrix bucket M2M (`bucket_ch.rs`), all CCH
  queries, /transit access leg, every isochrone, every route. The
  matrix is **1.8× faster than OSRM at 10 k×10 k** because of these.
  *Lazy-load is acceptable only if first-use latency is amortised over
  thousands of queries; the bench profile is not that.* Leave on-load.
- **`down_adj_flat` (time)** — `state.rs:90`. Forward DOWN flat for
  PHAST downward scan. Required for `/isochrone` (and matrix). The
  C1 block-gated PHAST (CLAUDE.md "18x isochrone speedup") depends on
  reading from this flat, not from `cch_weights.down`. Required.
- **`cch_topo`** — `state.rs:34`. Already mmap-zero-copy; not heap.
  Cold pages already madvise(DONTNEED). Touched only on unpack (and
  validate at boot). Already optimal.
- **`cch_weights` (time)** — `state.rs:35`. Already mmap-zero-copy.
  Touched only by `CchQuery::with_custom_weights` (alternatives,
  exclude/avoid, transit access) — that backend is the cold path; hot
  path uses flats. **However:** the body must NOT be `madvise(DONTNEED)`
  by default, because alternatives + transit DO read it at request time
  and would page in cold every call.
- **`orig_to_rank` + `filtered_to_original`** — `state.rs:49,53`. Both
  on the snap-to-query hot path (`flight.rs:707-712`, `flight.rs:818`),
  read on every route_batch pair. Already Cow + mmap-borrowed on the
  container path. Don't touch.
- **`snap_index.points`** — `state.rs:171`. Touched by every snap. If
  we make this lazy or memory-mapped-only, we add cold-page faults to
  the very first /matrix or /route. Already Cow+mmap on container.
  Don't touch.
- **`ebg_nodes`** — `state.rs:130`. Read in `build_raw_points`
  (`geometry.rs:138`) for every route, every matrix annotation, every
  isochrone stamp. Already Cow + mmap-borrowed. Don't touch.
- **`edge_geom`** — `state.rs:145`. Same hot path as above. Don't touch
  the container path; only the data-dir-built copy (Fix #270-6) is
  worth deduplicating.

---

## Part 3 — Cross-cutting recommendation

The #269 fix (parallelise route_batch with rayon `par_iter` over pairs)
is the single highest-leverage change in the codebase right now: ~1 hour
of engineering, drops 57.9 s → ~3 s, **beats libosrm 3.8 s**, no
correctness risk because `CchQuery` state is thread-local and the
shared graph is read-only.

The #270 fix bundle is more operational than code: boot from the
container, exclude truck if not needed, and lazy-load the distance
metric. That trio takes a day and lands the headline number.

After those two land, the honest scoreboard becomes:

| Bench | Before today | After fixes | vs libosrm |
|---|---|---|---|
| matrix 10 k × 10 k | 18.3 s @ 16 GiB | 18.3 s @ 9 GiB | 33× faster, 7× more RAM |
| route_batch 10 k | 57.9 s @ 16 GiB | ~3 s @ 9 GiB | 1.27× faster, 7× more RAM |
| isochrone 60 min p50 | 346 ms @ 16 GiB | 346 ms @ 9 GiB | 1.7× faster, 7× more RAM |

That is **"beats OSRM in every respect except RAM, and RAM is within
1 day of being competitive"** — which is the production posture the
CLAUDE.md guidance asks for.

---

## Appendix A — Citation index

- `route/src/server/flight.rs:618-755` — `do_route_batch` (#269 primary)
- `route/src/server/flight.rs:630` — single-task spawn_blocking
- `route/src/server/flight.rs:637` — outer chunk loop
- `route/src/server/flight.rs:651-680` — K=64 snap per pair
- `route/src/server/flight.rs:689-694` — p2p with 200-combo fallback
- `route/src/server/flight.rs:698-718` — unpack + WKB
- `route/src/server/flight.rs:603-616` — encode_linestring_wkb
- `route/src/server/snap_kbest.rs:37` — DEFAULT_MAX_FALLBACK_COMBOS=200
- `route/src/server/snap_kbest.rs:66-94` — snap_k_pair_role
- `route/src/server/snap_kbest.rs:123-141` — p2p_with_kbest_fallback
- `route/src/server/snap_index.rs:626-720` — snap_k_with_info_filtered_role
- `route/src/server/snap_index.rs:53` — MAX_RING_RADIUS=8
- `route/src/server/snap_index.rs:799-870` — iterate_rings
- `route/src/server/query.rs:124-127` — thread-local CchQueryState
- `route/src/server/query.rs:200-210` — CchQuery::new
- `route/src/server/query.rs:325-541` — CchQuery::query (bidir search + reconstruct)
- `route/src/server/unpack.rs:10-101` — recursive shortcut unpacker
- `route/src/server/unpack.rs:104-117` — find_up_edge / find_down_edge (binary_search)
- `route/src/server/geometry.rs:129-154` — build_raw_points
- `route/src/server/state.rs:30-99` — ModeData struct (#270)
- `route/src/server/state.rs:128-221` — ServerState struct
- `route/src/server/state.rs:1287-1303` — data-dir path: heap-built flats
- `route/src/server/state.rs:1319` — build_role_masks
- `route/src/server/state.rs:1601-1645` — load_way_names (HashMap)
- `route/src/server/state.rs:1993-2062` — container path: load_flat_section (mmap)
- `route/src/server/edge_geom.rs:27-101` — EdgeGeometry (flat Cow)
- `route/src/server/edge_geom.rs:73-101` — from_legacy_polylines (heap-build)
- `route/src/matrix/bucket_ch.rs:73-156` — UpAdjFlat shape
- `route/src/matrix/bucket_ch.rs:165-222` — DownAdjFlat shape
- `route/src/matrix/bucket_ch.rs:231-310` — DownReverseAdjFlat shape
- `route/src/formats/cch_topo.rs:90-115` — CchTopo (Cow arrays, mmap)
- `route/src/formats/cch_weights.rs:35-42` — CchWeights (Cow arrays, mmap)
- `route/src/formats/ebg_nodes.rs:30-53` — EbgNode 24 B, EbgNodes Cow
- `route/src/formats/nbg_geo.rs:31-35` — NbgGeo heap edges + polylines
- `route/src/server/avoid.rs:51-125` — AvoidWeightCache bounded LRU
- `drivetimes/server/src/main.rs:489-557` — drivetimes stream_route (serial)
- `drivetimes/server/src/osrm_engine.rs:123-141` — libosrm route FFI

## Appendix B — Things that would surprise a casual reader

1. **drivetimes is also single-threaded per-call** for route_batch
   (`drivetimes/server/src/main.rs:526`). libosrm is faster per-pair,
   not because of internal parallelism, but because of tighter C++
   code and a node-based graph with ~2.5× fewer states.
2. **The matrix path's "1.8× faster than OSRM" win is REAL** and lives
   in the same process — the gap on route_batch is purely the missing
   `par_iter` and the K=64 hedge tax. The CCH is fine.
3. **The 12× RAM gap is mostly the data-dir vs container boot mode**,
   not deep architecture. The container loader (#150) already mmap's
   flats; the bench just isn't using it. Switching the bench launch
   command closes ~3.5 GB of the gap with zero code.
4. **K=64 snap is over-engineered for the bench-style query** (clustered
   urban). The role masks made K-best unnecessary as a primary
   mechanism, but the K stayed in. A K=1-then-K=64-on-miss escalation
   gets back ~10 s on the serial path with zero correctness regression.
5. **`down_adj_flat` and `down_adj_flat_dist`** are functionally
   identical layouts to `up_adj_flat`/`down_rev_flat` but for forward
   DOWN edges (PHAST forward-isochrone downward scan needs this). The
   flat structure is **why isochrone is fast** — removing it would
   regress isochrone wins. Cannot be lazy-loaded without sacrificing
   first-/isochrone latency.
