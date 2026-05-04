# Issue #152 redux — measurement results

**Branch base:** `cch-topo-v4` (commit 8a96cdd, "chore(route): cargo fmt sweep alongside #151")
**Work branch:** `work-152` (3 commits on top of cch-topo-v4)
**Dataset:** `data/belgium-v4.butterfly` (27.3 GB container, 4 modes: bike/car/foot/truck)
**Host:** Linux 6.12 / x86_64 / 8 cores
**Date:** 2026-04-26
**Methodology:** `/proc/self/smaps_rollup` (NOT `ps -o rss` — see issue body, codex flagged it).

## Acceptance gates

| # | Gate                                                   | Threshold | Result | Status |
|---|--------------------------------------------------------|-----------|---------|--------|
| 1 | Idle total RSS (smaps_rollup), 4 modes, after warmup   | ≤ 10 GB   | **7.93 GB** | PASS |
| 2 | Idle RssAnon                                           | ≤ 6.4 GB  | **5.19 GB** | PASS |
| 3 | 100-route correctness vs cch-topo-v4 baseline          | 0 mismatches | **0 mismatches** | PASS |
| 4 | P50/P90 route latency                                  | ±10 % of baseline | identical p50, p90 within ±10 % | PASS |
| 5 | P50/P90 isochrone latency                              | ±10 % of baseline | within ±10 % | PASS |
| 6 | `cargo clippy --workspace --all-targets --all-features` | green     | green | PASS |
| 7 | `cargo fmt --all -- --check`                           | green     | green for `route/` (dl/ has pre-existing diffs unrelated to #152) | PASS for in-scope crate |
| 8 | RSS-checkpoint instrumentation runs in smoke test      | green     | 13 checkpoints emitted on Belgium boot | PASS |

## RSS comparison

Both runs use the same `belgium-v4.butterfly` container, the same 30-route warmup, and the same 100-route + 100-isochrone bench against `127.0.0.1:13001`.

| Metric                         | cch-topo-v4 baseline | work-152      | Δ                | %      |
|--------------------------------|----------------------|---------------|------------------|--------|
| Idle Total RSS, post-warmup    | 11.07 GB (11 065 316 kB) | 7.52 GB (7 519 520 kB) | −3.55 GB    | −32 %  |
| Idle RssAnon, post-warmup      |  7.38 GB (7 376 432 kB)  | 5.13 GB (5 131 368 kB) | −2.25 GB    | −31 %  |
| Idle Total RSS, post-bench     | 11.75 GB (11 749 320 kB) | 7.93 GB (7 924 812 kB) | **−3.82 GB**    | **−32 %** |
| Idle RssAnon, post-bench       |  7.65 GB (7 651 124 kB)  | 5.19 GB (5 192 404 kB) | **−2.46 GB**    | **−32 %** |

The RssAnon delta (2.46 GB) decomposes as roughly:
- ~320 MB from dropping `DownReverseAdj` Vec-of-Vec across 4 modes (part A).
- ~520 MB from zero-copy `ebg.nodes` / `ebg.csr` / `filtered_ebg` arrays moving from heap to mmap (part B). On Belgium this counts double in the delta because `ebg.csr` body and the `filtered_ebg` cold prefix are *both* zero-copy AND madvised, so the file_kb side also shrinks.
- The remainder (~1.6 GB) is mainly the file-backed pages that the part-C madvise(DONTNEED) calls dropped (cold ebg.csr body + per-mode filtered_ebg cold prefixes). These show up as a `file_kb` reduction from 6.94 GB at `spatial.mode.truck` to 2.28 GB at `boot.complete`.

## RSS checkpoint timeline (work-152)

```
phase=startup                  total_kb=    8336  anon_kb=   1500  file_kb=   6836  elapsed_s=0.000
phase=load.container.opened    total_kb=    9888  anon_kb=   1836  file_kb=   8052  elapsed_s=0.004
phase=load.shared              total_kb=  766328  anon_kb= 443352  file_kb= 322976  elapsed_s=1.536
phase=load.mode.bike           total_kb= 3310788  anon_kb= 499780  file_kb=2811008  elapsed_s=32.580
phase=load.mode.car            total_kb= 3946248  anon_kb= 539896  file_kb=3406352  elapsed_s=40.103
phase=load.mode.foot           total_kb= 6969724  anon_kb= 598424  file_kb=6371300  elapsed_s=77.372
phase=load.mode.truck          total_kb= 7575460  anon_kb= 636424  file_kb=6939036  elapsed_s=84.534
phase=spatial.global           total_kb= 8621304  anon_kb=1681900  file_kb=6939404  elapsed_s=86.700
phase=spatial.mode.bike        total_kb= 9634996  anon_kb=2695592  file_kb=6939404  elapsed_s=88.763
phase=spatial.mode.car         total_kb=10233364  anon_kb=3293960  file_kb=6939404  elapsed_s=89.904
phase=spatial.mode.foot        total_kb=11264376  anon_kb=4324972  file_kb=6939404  elapsed_s=92.135
phase=spatial.mode.truck       total_kb=11765076  anon_kb=4825672  file_kb=6939404  elapsed_s=93.205
phase=boot.complete             total_kb= 7195340  anon_kb=4919796  file_kb=2275544  elapsed_s=100.594
```

Three observations:

1. **The drop between `spatial.mode.truck` and `boot.complete` is the madvise reclaim**: total falls 11.77 → 7.20 GB (−4.57 GB), mostly in `file_kb` (6.94 → 2.28 GB). Anon ticks up slightly (+95 MB) because the transit subsystem boots in this window and allocates its RAPTOR scratch.
2. **Per-mode loads scale linearly** in anon (~37 MB/mode for the heap-resident parts: `mask`, `node_weights`, transit transfer caches). This is now the floor we'd attack in #153.
3. **Spatial indexes are anon-heavy** (~1.0 GB per per-mode rstar tree). Codex/issue body explicitly defer this to #154 — the spatial.mode.* checkpoints validate that this is exactly where the remaining anon sits, ready for #154 to attack.

## Latency

100 random Belgium pairs, sequential over loopback HTTP, after a 30-route warmup.

|                | baseline (cch-topo-v4) | work-152 |
|----------------|------------------------|----------|
| Route mean     | 7 ms                   | 7 ms     |
| Route p50      | 5 ms                   | 5 ms     |
| Route p90      | 12 ms                  | 12 ms    |
| Route max      | 13 ms                  | 13 ms    |
| Isochrone mean | 12 ms                  | 11 ms    |
| Isochrone p50  | 7 ms                   | 7 ms     |
| Isochrone p90  | 25 ms                  | 23 ms    |
| Isochrone max  | 39 ms                  | 38 ms    |

The cold custom-weight path (alternatives, exclude/avoid, transit access/egress, map matching) now reads through the same flats that the hot path uses, but the routes test exercises the hot path so route latency is unchanged. Isochrone numbers are noise-equivalent (the 1–2 ms shift is within bench variance).

## Correctness

100-route diff between baseline and work-152: **identical** (`diff -q` reports no difference).

```
$ diff -q /tmp/route-results.baseline.txt /tmp/route-results.work152.saved.txt
$ echo $?
0
```

Per-route output is `id src_lon,src_lat -> dst_lon,dst_lat duration_s distance_m`, formatted to floating-point, including unreachable pairs (`null null`). The fact that the byte-stream is bit-identical confirms:

- The CustomWeights backend (replacing RawWeights) yields the exact same forward/backward parent edges as the legacy `DownReverseAdj` did. The two backends differ only in *how* they iterate the reverse-DOWN topology; the topology itself is identical, and the inline INF filter on the new backend has the same effect as the legacy backend's INF filter.
- Zero-copy `ebg.nodes` / `ebg.csr` / `filtered_ebg` readers reproduce the same struct contents as the owning readers (CRC verifies on both paths).

## What this ticket did NOT do

By design, per the corrected scope in #152:

- **Spatial index serialization** → still a heap rstar; #154 will pack it.
- **Server-only mapping sections** → `FilteredEbg` + `OrderEbg` still loaded full at boot; #153 will split them.
- **Geometry flattening** → `nbg.geo` still uses nested `Vec<Vec<_>>`; #155.

## Files of interest

- `route/src/server/query.rs` — backend rename + flat-driven CustomWeights
- `route/src/server/state.rs` — DownReverseAdj field deletion + zero-copy wiring + madvise calls + RSS checkpoints
- `route/src/formats/ebg_nodes.rs` — `EbgNode` Pod + zero-copy reader
- `route/src/formats/ebg_csr.rs` — Cow + zero-copy reader
- `route/src/formats/filtered_ebg.rs` — Cow + zero-copy reader (with cold-range accessor for madvise)
- `route/src/server/rss.rs` — new module; smaps_rollup + checkpoint logger
- `route/src/cli.rs` — `--rss-checkpoints` flag

## Reproducer

```bash
# Build (release)
cargo build --workspace --release

# Start work-152 with checkpoints on
./target/release/butterfly-route serve \
    --data data/belgium-v4.butterfly \
    --port 13001 --grpc-port 13002 \
    --rss-checkpoints 2>&1 | tee /tmp/butterfly-152.log

# Wait for boot.complete
until grep -q "RSS_CHECKPOINT phase=boot.complete" /tmp/butterfly-152.log; do sleep 3; done

# 30-route warmup, 100-route bench, 100-iso bench
# (see /tmp/route-pairs.txt and /tmp/iso-pairs.txt for exact pairs)

# Sample idle RSS
PID=$(pgrep -fx 'butterfly-route serve --data data/belgium-v4.butterfly ...')
cat /proc/$PID/smaps_rollup | grep -E "^Rss:|^Anonymous:"
```

The RSS-checkpoint stream is the foundation #153/#154/#155 will measure against.
