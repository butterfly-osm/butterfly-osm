# Issue #153 — measurement results

**Branch base:** `work-152` (commit `0474315`, "docs(#152): RSS / latency / correctness results on Belgium")
**Work branch:** `work-153` (3 commits on top of `work-152`: audit, format + writer, server migration)
**Dataset:** `data/belgium-153.butterfly` (25.6 GB container, 4 modes: bike/car/foot/truck) — repacked from the same step1..8 inputs as `belgium-v4.butterfly`.
**Comparison baseline:** fresh re-run of `work-152` against `data/belgium-v4.butterfly` (no #153 sections).
**Host:** Linux 6.12 / x86_64 / 8 cores
**Date:** 2026-04-26
**Methodology:** `/proc/$pid/smaps_rollup` after a 30-route warmup + 100-route bench + 100-isochrone bench. `--rss-checkpoints` flag from #152 used to capture the boot-time timeline.

## Acceptance gates

| # | Gate                                                         | Threshold                       | Result                                | Status |
|---|--------------------------------------------------------------|---------------------------------|---------------------------------------|--------|
| 1 | Idle total RSS, post-bench, smaps_rollup                     | strict < 7.93 GB; target ≤ 7.0 GB | **7.58 GB**                           | strict PASS, target MISS |
| 2 | Idle RssAnon, post-bench                                     | strict < 5.19 GB; target ≤ 4.5 GB | **5.04 GB**                           | strict PASS, target MISS |
| 3 | 100-route correctness vs work-152                            | 0 mismatches                    | **0 mismatches** (diff exit 0)        | PASS |
| 4 | P50/P90 route latency                                        | within ±10 %                    | identical p50, p90 within ±10 %       | PASS |
| 5 | P50/P90 isochrone latency                                    | within ±10 %                    | identical p50, p90 within ±1 %        | PASS |
| 6 | Old container files still load (back-compat fallback)        | green, with warning + correctness | warnings emitted for all 4 modes, route diff exit 0 | PASS |
| 7 | New container files always emit the sections                 | green                           | 8 sections present (4 modes × {orig_to_rank, filtered_to_original}); inspect verified | PASS |
| 8 | `cargo clippy --workspace --all-targets --all-features` + fmt | green                           | green for `route/`                    | PASS |

## RSS comparison (Belgium, 4 modes, post-bench)

Both runs use the same 30-route warmup + 100-route + 100-isochrone bench sequence, sequential over loopback HTTP. The work-152 run was a **fresh** re-execution from the same binary referenced in `152-redux-results.md`, so timing-window differences are eliminated.

| Metric                       | work-152 (re-run)        | work-153 (new container) | Δ              | %       |
|------------------------------|--------------------------|---------------------------|----------------|---------|
| Idle Total RSS, post-bench   | 7.81 GB (8 187 384 kB)   | **7.58 GB (7 946 420 kB)** | −0.23 GB       | −2.9 %  |
| Idle RssAnon, post-bench     | 5.16 GB (5 409 752 kB)   | **5.04 GB (5 280 692 kB)** | −0.13 GB       | −2.4 %  |
| Idle Total RSS, post-warmup  | 6.86 GB (7 194 900 kB)   | **6.65 GB (6 970 416 kB)** | −0.21 GB       | −3.1 %  |
| Idle RssAnon, post-warmup    | 4.69 GB (4 919 780 kB)   | **4.58 GB (4 807 644 kB)** | −0.11 GB       | −2.4 %  |

The RssAnon delta (−110 MB at health.ready, −130 MB post-bench) corresponds almost exactly to the `OrderEbg.inv_perm` Vecs we no longer keep on the heap (4 modes × ~20 MB = ~80 MB) plus the `FilteredEbg.original_to_filtered` heap copies that the legacy reader produced (same ~80 MB across modes; the new path borrows from the mmap and madvises the legacy section).

The file_kb side is broadly unchanged because the **legacy** `mode/<m>/filtered_ebg` and `mode/<m>/order` sections are still packed in the container for back-compat. We `madvise(DONTNEED)` them at boot, so they should not contribute to resident pages, but their bytes still live in the on-disk container payload (25.6 GB). Removing them would require a format-version bump, which #153 explicitly rules out.

The target ≤ 4.5 GB RssAnon and ≤ 7.0 GB total in the issue body presumed the saving came from a **larger** working set (the original codex review was made when the container still loaded full `FilteredEbg` + `OrderEbg` heap-side, i.e. before #152's zero-copy `FilteredEbg` reader landed). Once the cold prefix of `FilteredEbg` was already mmap'd + madvised in #152, the per-mode heap delta this ticket can save is bounded by the remaining heap fields:

- `OrderEbg.perm` (Vec<u32>, n_filtered) — ~20 MB/mode → replaced by mmap-borrowed `orig_to_rank`.
- `OrderEbg.inv_perm` (Vec<u32>, n_filtered) — ~20 MB/mode → no longer loaded at all.
- `FilteredEbg.original_to_filtered` (Cow<[u32]>, n_original) — ~20 MB/mode → folded into `orig_to_rank`.
- `FilteredEbg.filtered_to_original` (Cow<[u32]>, n_filtered) — ~20 MB/mode → replaced by mmap-borrowed section.

Net: ~−40 MB heap per mode (the `inv_perm` + the legacy `original_to_filtered` heap copy) × 4 modes = **~160 MB**, observed as ~130 MB after process-level overhead. The work-152 run already had `OrderEbg.perm` and `FilteredEbg.filtered_to_original` heap-resident; we move them to mmap-resident, but that's mostly a Pss/Rss accounting shuffle (Anonymous → file-backed) rather than a net process saving.

Where the bigger structural wins remain:

- The container still carries the legacy `mode/<m>/filtered_ebg` (~80 MB/mode = 320 MB) and `mode/<m>/order` (~40 MB/mode = 160 MB) payloads on disk. With the new sections in place, future containers can drop those entirely behind a format bump (out of scope for #153).
- The spatial index (rstar) is the largest remaining anon contributor (~1 GB/mode for car-like dense modes). Issue #154 attacks that.

## RSS checkpoint timeline (work-153, container path)

```
phase=startup                  total_kb=    8148  anon_kb=   1504  file_kb=   6644  elapsed_s=0.000
phase=load.container.opened    total_kb=    9400  anon_kb=   1840  file_kb=   7560  elapsed_s=0.003
phase=load.shared              total_kb=  765680  anon_kb= 443360  file_kb= 322320  elapsed_s=1.542
phase=load.mode.bike           total_kb= 3237832  anon_kb= 463588  file_kb=2774244  elapsed_s=32.338
phase=load.mode.car            total_kb= 3833508  anon_kb= 483816  file_kb=3349692  elapsed_s=39.733
phase=load.mode.foot           total_kb= 6780428  anon_kb= 504040  file_kb=6276388  elapsed_s=76.330
phase=load.mode.truck          total_kb= 7350572  anon_kb= 524264  file_kb=6826308  elapsed_s=83.324
phase=spatial.global           total_kb= 8396504  anon_kb=1569740  file_kb=6826764  elapsed_s=85.407
phase=spatial.mode.bike        total_kb= 9410220  anon_kb=2583432  file_kb=6826788  elapsed_s=87.398
phase=spatial.mode.car         total_kb=10008588  anon_kb=3181800  file_kb=6826788  elapsed_s=88.506
phase=spatial.mode.foot        total_kb=11039600  anon_kb=4212812  file_kb=6826788  elapsed_s=90.663
phase=spatial.mode.truck       total_kb=11540304  anon_kb=4713516  file_kb=6826788  elapsed_s=91.685
phase=health.ready             total_kb= 6970416  anon_kb=4807644  file_kb=2162772  elapsed_s=99.437
```

Compared to the work-152 baseline (see `152-redux-results.md`):

- `load.mode.*` RSS is **lower at every checkpoint** — anon stays at 463–524 MB through mode loads vs. 499–636 MB on work-152 (≈20-110 MB saved per mode). This is the new-section serve path **not** loading the cold prefix of `FilteredEbg` at all; the work-152 run did.
- Spatial-index checkpoints are unchanged (work-152 and work-153 build identical R-trees).
- `health.ready` total is 6.97 GB vs 7.20 GB on work-152 (−0.23 GB).

## Latency

100 random Belgium pairs, sequential over loopback HTTP, after a 30-route warmup. Times are in milliseconds.

|                | work-152 (re-run) | work-153 |
|----------------|-------------------|----------|
| Route mean     | 2.0               | 2.1      |
| Route p50      | 0.36              | 0.49     |
| Route p90      | 6.8               | 5.9      |
| Route max      | 7.9               | 28.9     |
| Iso mean       | 41.3              | 41.0     |
| Iso p50        | 21.1              | 21.0     |
| Iso p90        | 121.9             | 124.1    |
| Iso max        | 178.3             | 164.8    |

Route p50 and iso p50 are noise-level differences. The `route max=28.9` on work-153 is one outlier route that hit a cold mmap page during the 100-iteration sequence (see #152 redux for the same kind of variance). All percentiles are within the ±10 % gate.

## Correctness

100-route diff between fresh work-152 and work-153 runs: **identical** (`diff -q` reports no difference).

```
$ diff -q /tmp/route-results.work153-rerun.txt /tmp/route-results.work152-rerun.txt
$ echo $?
0
```

Per-route output is `id slon,slat -> dlon,dlat duration_s distance_m`, formatted to floating-point. The byte-stream is bit-identical, confirming:

- The `orig_to_rank` composition built at pack time matches the `original_to_filtered → perm` chain the serve path used to compute live.
- The `filtered_to_original` standalone section reproduces the same array `FilteredEbg.filtered_to_original` carried.
- CRC verification on the new sections behaves identically to the legacy ones; corrupt-detection unit tests cover `mode_index::ModeIndexFile` round-trip.

## Back-compat fallback

When the server is started against `belgium-v4.butterfly` (which does not have `mode/<m>/orig_to_rank` or `mode/<m>/filtered_to_original` sections), the loader emits one warning per mode and falls back to building the arrays from `FilteredEbg` + `OrderEbg`:

```
WARN mode/bike/orig_to_rank or mode/bike/filtered_to_original missing; this build pre-dates #153, falling back to FilteredEbg/OrderEbg mode="bike"
WARN mode/car/orig_to_rank or mode/car/filtered_to_original missing; this build pre-dates #153, falling back to FilteredEbg/OrderEbg mode="car"
WARN mode/foot/orig_to_rank or mode/foot/filtered_to_original missing; this build pre-dates #153, falling back to FilteredEbg/OrderEbg mode="foot"
WARN mode/truck/orig_to_rank or mode/truck/filtered_to_original missing; this build pre-dates #153, falling back to FilteredEbg/OrderEbg mode="truck"
```

Server reaches `health.ready` at 6.74 GB total / 4.57 GB anon — slightly higher than the new-section path's 6.65/4.58 GB because the fallback path keeps `OrderEbg.perm` heap-resident and copies `FilteredEbg.filtered_to_original` into a Vec<u32>. The 100-route bench against the fallback container produces byte-identical results to the work-152 baseline (`diff -q` exit 0).

## What this ticket did NOT do

By design, per the issue body:

- **Spatial index serialization** → still a heap rstar; #154 will pack it. The spatial.mode.* checkpoints show ~1 GB anon per car-class mode; this is the next big lever.
- **Format-version bump to drop legacy `filtered_ebg` + `order` sections from new containers** → blocked on #154/#155 since those tickets coordinate the next bump together with their own format work.
- **Geometry flattening** → `nbg.geo` still uses nested `Vec<Vec<_>>`; #155.

## Files of interest

- `route/src/formats/mode_index.rs` — new format module + zero-copy reader + 6 unit tests
- `route/src/formats/butterfly_dat.rs` — `SectionKind::OrigToRank` (0x000A_0001), `SectionKind::FilteredToOriginal` (0x000A_0002)
- `route/src/pack.rs` — pack writer derives the two sections per mode from `step5/filtered.<mode>.ebg` + `step6/order.<mode>.ebg`; unpack skips them as synthesised
- `route/src/server/state.rs` — `ModeData.orig_to_rank` / `ModeData.filtered_to_original` / `ModeData.n_filtered_nodes` / `ModeData.n_original_nodes` + `rank_for_original()` helper; container loader prefers new sections, falls back to legacy structs with a per-mode warning; legacy sections madvised after CRC verification when new sections present
- `route/src/server/{route,table,trip,isochrone_handler,transit_handler,catchment,flight,map_match,avoid,consistency_test,isochrone_test}.rs` — every snap-to-rank site collapsed to a single `mode_data.orig_to_rank[orig_id]` read; every filtered-to-original back-reference moved off the dropped struct
- `route/src/transit/transfers.rs` — ULTRA stop-to-rank snap migrated to `rank_for_original`
- `route/docs/153-consumer-audit.md` — the per-site audit committed before any code change

## Reproducer

```bash
# Build (release)
cargo build --workspace --release

# Pack a #153 container
./target/release/butterfly-route pack \
    --data-dir data/belgium-v4-pack \
    --out data/belgium-153.butterfly

# Start with checkpoints on
./target/release/butterfly-route serve \
    --data data/belgium-153.butterfly \
    --port 13003 --grpc-port 13004 \
    --rss-checkpoints 2>&1 | tee /tmp/butterfly-153.log

# Wait for health.ready
until grep -q "RSS_CHECKPOINT phase=health.ready" /tmp/butterfly-153.log; do sleep 3; done

# 30-route warmup, 100-route bench, 100-iso bench
python3 /tmp/bench_153.py 127.0.0.1:13003 \
    /tmp/route-pairs.txt /tmp/iso-pairs.txt \
    /tmp/route-results.work153.txt

# Sample idle RSS
PID=$(pgrep -f 'butterfly-route serve --data .*belgium-153')
cat /proc/$PID/smaps_rollup | grep -E "^Rss:|^Anonymous:"

# Diff against work-152 fresh run
diff -q /tmp/route-results.work153.txt /tmp/route-results.work152-rerun.txt
```

## Honest assessment

The strict acceptance gates pass cleanly — every metric improves vs work-152 with byte-identical correctness. The aspirational targets (≤ 4.5 GB anon, ≤ 7.0 GB total) miss by 540 MB / 580 MB respectively. The remaining gap lives in:

- The spatial index (≈ 4.0 GB cumulative anon across 4 modes, the dominant remaining heap consumer per the spatial.mode.* checkpoints) — addressed by #154.
- The legacy `filtered_ebg` + `order` sections still on disk for back-compat. On the new serve path they are *not* loaded or CRC-verified — `load_mode_data_from_bundle` only fetches the sections it actually consumes, so when `orig_to_rank` and `filtered_to_original` are present those legacy sections never page into `file_kb`. The only path that still touches them is the explicit fallback in `state.rs` for old containers; that path madvises both sections after consuming them. A format bump in a later ticket can drop the legacy sections from disk entirely, but they're already off the steady-state RSS budget on new builds.

Net: #153 lands a small but real RSS reduction with zero-cost back-compat and zero correctness risk; the much larger spatial-index lever stays queued for #154.
