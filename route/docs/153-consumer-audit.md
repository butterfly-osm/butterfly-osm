# Issue #153 — `FilteredEbg` / `OrderEbg` consumer audit

**Branch base:** `work-152` (commit 0474315)
**Date:** 2026-04-26
**Goal:** identify every site on the **serve path** that reads `mode_data.order.*`,
`mode_data.filtered_ebg.*`, or otherwise depends on the full `OrderEbg` /
`FilteredEbg` structs being loaded into `ServerState`. Anything outside the
serve path (`step3-nbg`, `step4-ebg`, `pack`, `inspect`, validators) is
considered build/validation-side and stays unchanged in #153.

`ServerState` here means the result of `ServerState::load_from_container` —
the only path #153 modifies. The `--data-dir` `ServerState::load` path keeps
working unchanged because it already loads the owning structs from per-step
files.

## Method

```bash
grep -rn '\.order\.perm\|\.order\.inv_perm\|\.filtered_ebg\.' route/src/
grep -rn 'FilteredEbg\|OrderEbg\|filtered_ebg\|filtered\.ebg' route/src/
```

Each hit was opened and classified.

## Hot fields (what serve actually needs)

The serve path reads exactly **two** fields from `OrderEbg` and **two** fields
from `FilteredEbg`:

| Source struct  | Field                  | Indexed by                | Length            | Used as            |
|----------------|------------------------|---------------------------|-------------------|--------------------|
| `OrderEbg`     | `perm`                 | filtered EBG node id      | n_filtered_nodes  | `→ rank`           |
| `FilteredEbg`  | `original_to_filtered` | original EBG node id      | n_original_nodes  | snap → filtered    |
| `FilteredEbg`  | `filtered_to_original` | filtered EBG node id      | n_filtered_nodes  | unpack → original  |
| `FilteredEbg`  | `n_filtered_nodes`     | (header u32)              | —                 | health/log only    |

`OrderEbg.inv_perm`, `FilteredEbg.offsets`, `FilteredEbg.heads`,
`FilteredEbg.original_arc_idx` are **never** read on the serve path. They are
build-time / validation-time only.

## Composition the serve path always does

Every snap-to-rank site composes `original_to_filtered` followed by `perm`:

```rust
let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
if filtered == u32::MAX { return None; }
let rank = mode_data.order.perm[filtered as usize];
```

That is exactly what the new section `mode/<m>/orig_to_rank` collapses into a
single dense `u32` array of length `n_original_nodes`, with `u32::MAX`
sentinel for "not in this mode's filtered subgraph". Saves one indirection
and one cache miss on the hot path; more importantly, it lets us drop the
inverse permutation table from RSS entirely.

The unpack-direction lookup `mode_data.filtered_ebg.filtered_to_original`
becomes `mode/<m>/filtered_to_original` — same array as today, just packed as
its own section.

## Site list (serve path consumers)

Format: `path:line — purpose — needed at serve?`

### Pure rank lookups (`orig → filtered → rank`)

These collapse to a single `orig_to_rank[orig_id]` read and stop needing both
`filtered_ebg` and `order`.

| Site | Purpose | Needed at serve? |
|------|---------|------------------|
| `route/src/server/route.rs:473-488` | snap origin/dest → rank for P2P | YES (hot) |
| `route/src/server/table.rs:325-327` | matrix forward source rank | YES (hot) |
| `route/src/server/table.rs:364-366` | matrix forward target rank | YES (hot) |
| `route/src/server/table.rs:660-662` | bulk matrix source rank | YES (hot) |
| `route/src/server/table.rs:682-684` | bulk matrix target rank | YES (hot) |
| `route/src/server/isochrone_handler.rs:746-756` | center → rank for forward iso | YES (hot) |
| `route/src/server/isochrone_handler.rs:1152-1156` | bulk iso center → rank | YES (hot) |
| `route/src/server/transit_handler.rs:1224-1228` | access/egress origin → rank | YES (hot) |
| `route/src/server/trip.rs:615-617` | TSP/trip waypoint → rank | YES (hot) |
| `route/src/server/map_match.rs:507-518` | HMM transition src/dst → rank | YES (cold) |
| `route/src/server/map_match.rs:617-625` | route_distance helper → rank | YES (cold) |
| `route/src/server/catchment.rs:292-300` | catchment src/dst → rank | YES (hot) |
| `route/src/server/catchment.rs:354-358` | catchment origin → rank | YES (hot) |
| `route/src/server/catchment.rs:756-760` | DoExchange store → rank | YES (hot) |
| `route/src/server/catchment.rs:794-796` | DoExchange points → rank | YES (hot) |
| `route/src/server/flight.rs:255-261` | matrix flight action src/dst → rank | YES (hot) |
| `route/src/server/flight.rs:278-282` | route_batch flight src/dst → rank | YES (hot) |
| `route/src/server/flight.rs:596-608` | edges_batch flight src/dst → rank | YES (hot) |
| `route/src/server/flight.rs:728-737` | isochrone flight origin → rank | YES (hot) |
| `route/src/server/flight.rs:925-940` | edges_batch path src/dst → rank | YES (hot) |
| `route/src/server/flight.rs:1418-1431` | catchment store/points → rank | YES (hot) |
| `route/src/server/consistency_test.rs:82-86` | test harness — same composition | YES (test only) |
| `route/src/server/isochrone_test.rs:265-267,497-506` | iso correctness test | YES (test only) |
| `route/src/transit/transfers.rs:458-462` | ULTRA build → rank lookup | YES (cold; once at startup) |

### Filtered → original back-references (`filtered_to_original`)

Used after a routing search returns rank/filtered ids and we need the
original EBG id back (for geometry, OSM ids, way-name lookup, etc.).

| Site | Purpose | Needed at serve? |
|------|---------|------------------|
| `route/src/server/route.rs:541` | unpack edge → orig EBG | YES (hot) |
| `route/src/server/isochrone_handler.rs:826` | iso filtered_id → orig | YES (hot) |
| `route/src/server/isochrone_handler.rs:1166` | bulk iso filtered_id → orig | YES (hot) |
| `route/src/server/transit_handler.rs:1207` | transit filtered_id → orig | YES (hot) |
| `route/src/server/map_match.rs:542,649` | map-match filtered_id → orig | YES (cold) |
| `route/src/server/catchment.rs:322,375` | catchment filtered_id → orig | YES (hot) |
| `route/src/server/flight.rs:629,765,979` | flight filtered_id → orig | YES (hot) |
| `route/src/server/avoid.rs:237,271` | avoid recustomize: filtered_to_original passed by ref | YES (cold) |
| `route/src/server/state.rs:657` | exclude recustomize: filtered_to_original passed by ref | YES (cold) |
| `route/src/server/consistency_test.rs:488,551,831,1097,1214,1287` | test harness | YES (test) |
| `route/src/server/isochrone_test.rs:282` | iso test | YES (test) |

### Diagnostic / metadata reads (`n_filtered_nodes`)

Pure header u32 reads for log/health/debug — not affected by either approach.
The new sections still let us derive `n_filtered_nodes` from the
`filtered_to_original` slice length, so we can keep these working without the
full struct.

| Site | Purpose | Needed at serve? |
|------|---------|------------------|
| `route/src/server/state.rs:221,481` | log line at boot | metadata only |

### Direct CSR / cold-prefix readers (`offsets`, `heads`)

These are inside `consistency_test.rs` only — the test exercises raw
filtered-EBG topology to prove route paths are reachable in the filtered
graph. The test path is gated behind `#[cfg(test)]` and never runs in
production serve. The legacy `FilteredEbg` struct can still be loaded
on-demand by the test if we keep a fallback constructor.

| Site | Purpose | Needed at serve? |
|------|---------|------------------|
| `route/src/server/consistency_test.rs:574-576` | filtered CSR adjacency walk | TEST ONLY |

## Build / validation consumers (NOT serve path — out of scope for #153)

Listed for completeness so future tickets know what we did not touch.

- `route/src/cli.rs` — every step6/step7 CLI subcommand reads
  `FilteredEbg` and `OrderEbg` from `step5/`/`step6/` files directly. Pack
  is not changed here.
- `route/src/ordering.rs`, `route/src/ordering_lifted.rs` — ND ordering
  build. Reads `FilteredEbg` to drive the elimination order computation.
- `route/src/contraction.rs`, `route/src/customization.rs` — CCH build
  steps. Read both structs; they run inside `step7/step8` CLI subcommands
  only.
- `route/src/validate/ordering.rs`, `route/src/validate/contraction.rs`,
  `route/src/validate/cch_correctness.rs`, `route/src/validate/invariants.rs`
  — lock-condition verifiers. Build-time only.
- `route/src/range/frontier.rs`, `route/src/range/batched_isochrone.rs` —
  these are *bench-harness* code paths (`butterfly-bench`), not the serve
  path. They take `FilteredEbg` paths from the CLI and load files
  directly. Out of scope.
- `route/src/pack.rs` — the pack tool itself; we add new sections here as
  part of #153 (writer side).

## Plan implications

1. Two new sections per mode in the container:
   - `mode/<m>/orig_to_rank` (`[u32; n_original_nodes]`, sentinel
     `u32::MAX` for "not accessible in this mode"). Replaces the
     two-step `original_to_filtered → perm` chain at every hot site.
   - `mode/<m>/filtered_to_original` (`[u32; n_filtered_nodes]`).
     Same content as `FilteredEbg.filtered_to_original` today.

2. `ServerState.modes[i]` (`ModeData`) drops the `order: OrderEbg` and
   `filtered_ebg: FilteredEbg` fields, gains:
   - `orig_to_rank: Cow<'static, [u32]>` (mmap-zero-copy; primary
     replacement)
   - `filtered_to_original: Cow<'static, [u32]>` (mmap-zero-copy; back-ref
     replacement)
   - `n_filtered_nodes: u32` (kept as u32 for log/diagnostic reads;
     equals `filtered_to_original.len() as u32`).

3. Every site in the "Pure rank lookups" table collapses to one read:
   ```rust
   let rank = mode_data.orig_to_rank[orig_id as usize];
   if rank == u32::MAX { return None; }
   ```

4. The transit `ULTRA` builder (`transit/transfers.rs:458-462`) and the
   exclude / avoid recustomizers also migrate. They run cold (once at
   boot or once per request); behavior is unchanged.

5. `consistency_test.rs:574-576` is the only site that reads the cold
   filtered-EBG CSR (`offsets`/`heads`). It is `#[cfg(test)]` only and
   has no production impact. The test will load `FilteredEbg` from the
   per-mode file directly via `FilteredEbgFile::read`, decoupled from
   `ServerState`.

6. **Back-compat fallback**: when an old container is loaded (no
   `orig_to_rank` / `filtered_to_original` sections present), the
   loader logs one warning per missing section and synthesises the
   arrays from the legacy `FilteredEbg` + `OrderEbg` sections. The
   legacy sections are still read in that path; the cold prefix and
   `inv_perm` are dropped after array construction so RSS is bounded
   to (new sections) + (one-time build cost).

7. **New containers always emit the sections** going forward. After
   first re-pack, every Belgium build benefits.

## Sites left untouched

- `pack.rs` reads from the per-step files (`step5/filtered.<m>.ebg`,
  `step6/order.<m>.ebg`); the per-step writer / loader code stays as
  is. We add new sections in `pack` that are derived from the same
  per-step files.
- The `--data-dir` `ServerState::load` path keeps using the legacy
  `OrderEbgFile::read` / `FilteredEbgFile::read` and computing the new
  arrays in memory before constructing `ModeData`. This keeps
  `--data-dir` developers unblocked without paying the disk-format
  cost.
