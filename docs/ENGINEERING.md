# Engineering guide

What a new engineer needs to build, run, query, calibrate and test the engine.
History is not repeated here — it lives in `CHANGELOG.md` and in closed issues.

## 1. Build & run

```bash
cargo build --workspace [--release]                       # all three crates
cargo test  --workspace                                   # unit tests, no data needed
cargo clippy --workspace --all-targets --all-features     # warnings are errors
cargo fmt --all
cargo build --release --features bench --bin butterfly-bench  # benchmark harness (bench feature)
./target/release/butterfly-bench bucket-m2m --data-dir ./data/belgium --sizes 10,25,50,100

docker build -t butterfly-route .                   # serving image (default target)
docker build --target tools -t butterfly-tools .    # pipeline / fetch image
docker run -d --name butterfly -p 3001:8080 -p 3002:8081 \
  -v "${PWD}/data/belgium:/data" butterfly-route && curl localhost:3001/health
```

There is no protobuf build dependency: the GTFS-Realtime bindings are generated
ahead of time and committed (`route/src/transit/gtfs_realtime.rs`, #574).
`protoc` is needed only by `scripts/gen-gtfs-rt.sh`, run by hand when the
(frozen) spec moves. The toolchain is pinned by `rust-version` in the root
`Cargo.toml` (edition 2024).

One multi-target image manifest (#573): `Dockerfile` holds a shared `builder`
stage plus a `tools` and a `runtime` stage, `runtime` last so a bare
`docker build .` still produces the serving image. `Dockerfile.tools` is a
deprecated generated shim (`scripts/gen-dockerfile-tools.sh`, checked by CI)
kept for one release. Release binaries are stripped
(`[profile.release] strip = "symbols"`); build `--profile release-debug` when
you need named backtrace frames. Multi-stage image
(`rust:1.95-trixie` → `debian:trixie-slim`, non-root); default
`CMD` is `serve --data-dir /data --port 8080 --log-format json` — REST on 8080,
Arrow Flight on 8081. `serve` flags that matter:

| Flag | Meaning |
|---|---|
| `--data <f.butterfly>` / `--data-dir <dir>` | one packed container (mmap) vs a directory of containers / step outputs |
| `--port`, `--grpc-port`, `--transport rest\|grpc\|both` | listeners; gRPC defaults to REST port + 1 |
| `--modes car,bike,foot,truck`, `--transit on\|off` | load a subset (each mode costs RAM); skip all feed and transfer-graph work |
| `--regions BE,LU`, `--eager-regions` | multi-region selection; loading is lazy by default |
| `--rss-budget-gb`, `--idle-compact-secs` | LRU region eviction, per-worker scratch compaction |
| `--eager-verify` / `--warmup-on-boot` | section CRC verification at boot vs in background |
| `--overlay <file>`, `--log-format text\|json` | inter-region P2P overlay; structured logging |

Env overrides worth knowing: `RUST_LOG`, `BUTTERFLY_TRANSIT`,
`BUTTERFLY_RSS_BUDGET_GB`, `BUTTERFLY_IDLE_COMPACT_SECS`, `BUTTERFLY_REFS_DIR`,
`BUTTERFLY_MODELS_DIR`, `BUTTERFLY_MATRIX_ALGO=bucket|phast` (read ONCE at the
first matrix call and then frozen for the life of the process — set it before
`serve` starts, it cannot be flipped on a live server; #546, #591),
`BUTTERFLY_BOOT_RECUSTOMIZE=off` (uncalibrated free-flow car).

## 2. Workspace map and data flow

```
butterfly-common/  shared errors and small utilities
dl/                butterfly-dl — streaming OSM downloader (<1 GB RAM, any size)
route/             butterfly-route — the pipeline, the router, the transit engine
bench/             post-deploy gate + competitor harnesses
models/            per-mode *.model.json cost profiles

--- data flow, run once per region ---
region.pbf
  step1-ingest    nodes.sa nodes.si ways.raw relations.raw
  step2-profile   way_attrs.<mode>.bin  turn_rules.<mode>.bin  (+ density class)
  step3-nbg       node-based graph — BUILD-TIME INTERMEDIATE ONLY
  step4-ebg       ebg.nodes ebg.csr ebg.turn_table              <- the routing graph
  step5-weights   w.<mode>.u32  t.<mode>.u32  mask.<mode>.bitset
  step6-lifted    CCH order (nested dissection on NBG, lifted to EBG)
  step7-contract  cch.<mode>.topo
  step8-customize cch.w.<mode>.u32  cch.d.<mode>.u32
  pack            <region>.butterfly  (one mmap'd container, CRC per section)
  serve           REST + Flight
```

Every step writes a `stepN.lock.json` with SHA-256 checksums; `pack` verifies
sections and by default prunes the `step*/` trees.

**One graph.** The edge-based graph is the single source of truth: routing state
is a directed edge id, turn cost is a transition `cost(e_in -> e_out)`. Routes,
matrices and isochrones therefore cannot disagree. The NBG exists only so nested
dissection sees the physical topology; it is never queried. Query engines:

- **PHAST** for isochrones — an upward priority-queue search, then one linear
  rank-order scan of the DOWN edges, block-gated so unreachable rank blocks are
  skipped entirely. **K-lane batching** (K=8) amortises that scan across 8
  sources for bulk matrices and bulk isochrones.
- **Bucket many-to-many** for matrices — explores only towards requested
  targets. `d(s->t) = min_m d(s->m) + d(m->t)`; the target phase is a **reverse**
  search, because `d(t->m) != d(m->t)` on a directed graph.
- **Lopsided PHAST fields** when one side is much smaller — a forward field for
  many-to-one, a reverse field for one-to-many. The choice between bucket and
  PHAST is made by a **router with measured cost constants** (an EWMA of real
  sweep and scan wall times on this host), not a hardcoded size threshold.

Every endpoint snaps through **phantom endpoints**: both directed twins of up to
3 near-equidistant physical edges, with exact partial-edge costs. Never
reintroduce a single directed-edge commit, and never expand seeds at the API
layer — both have been measured, both are much worse.

**Transit** is a RAPTOR engine sharing the process and the foot CCH: `origin ->
access CCH 1-to-N -> RAPTOR rounds over a merged timetable -> egress CCH 1-to-N`.
GTFS and NeTEx-EPIP feeds merge into one `Timetable` with namespaced stop ids;
transfers are ULTRA-preprocessed at startup and cached behind a provenance hash.

## 3. Query surface

Two transports, no mixing: **REST stays JSON, Flight stays Arrow.** REST speaks
human units — seconds and metres, suffixed `_s` / `_m` (`duration_s`,
`distance_m`, `time_s`, `interval_s`). Flight speaks machine units: `duration_ms`
(u32 ms) and `distance_m`, plus `source_idx` / `target_idx` / `query_idx`. REST:

| Endpoint | Purpose |
|---|---|
| `GET /route` | point-to-point: geometry, per-edge annotations, turn-by-turn steps, alternatives |
| `GET /nearest` | snap a coordinate to nearby road segments |
| `POST /table` | many-to-many duration / distance matrix |
| `GET /isochrone` | one reachability polygon (GeoJSON or WKB), `direction=depart\|arrive`, multiple `contours` |
| `POST /isochrone/bulk` | batch isochrones as a WKB stream |
| `POST /trip` | TSP / trip optimisation (nearest-neighbour + 2-opt + or-opt) |
| `POST /match` | map-match a GPS trace |
| `POST /catchment` | catchment hulls around stores from client points |
| `GET /transit`, `POST /transit/bulk` | multimodal journeys, single and batched |
| `GET /health`, `/version`, `/regions`, `/metrics` | liveness, build, loaded regions, Prometheus |
| `GET /height`, `/swagger-ui`, `/api-docs/openapi.json` | elevation (only mounted when DEM tiles are present); interactive docs |

Flight gRPC, ticket `action:profile:params_json`. `DoGet`: `matrix`,
`route_batch`, `isochrone`, `transit_bulk`, `edges_batch` (unnested per-edge
output keyed by OSM node ids, for flow analytics). `DoExchange`: `catchment`,
`edges_flow` — both take an input table. Call the engine from code over Flight;
REST is for humans and single queries.

Both matrix surfaces report the plan the shape-aware router (#526/#527)
actually ran — `bucket`, `phast_fwd`, `phast_rev`, or `mixed` when a tiled
Flight request straddled two. `/table` sets the `x-butterfly-matrix-plan`
response header; the Flight `matrix` completeness trailer carries the same
value under `plan`. The value is a literal inside the engine that ran, threaded
out untouched, so it is the branch taken and not a re-derivation — which is why
the post-deploy gate asserts on it instead of on wall clock (#594).

`uncertainty=bands` on `/table`, `/isochrone` and `/trip` adds best and worst
alongside the typical answer (`durations_best` / `durations_worst`, extra `best`
/ `worst` contour features, `duration_best` / `duration_worst` totals). Car-only,
JSON-only, incompatible with `exclude` / `avoid_polygons`, and an error when the
loaded speed table carries no band columns. Product rules, non-negotiable:

- **An isochrone is ONE simple polygon** — the origin's connected component, no
  holes, never a MultiPolygon — and it **contains its own snapped origin**. The
  gate fails any other shape.
- **PHAST labels are head arrivals.** For `direction=depart` the frontier is
  built from the *unreached successors* of settled edges; for `direction=arrive`
  the settled edge itself is used and there is no successor frontier. Mixing the
  two yields confetti or over-large polygons.
- **An arrive field is seeded like a destination.** Reaching the snapped edge
  pays its full weight but the journey stops at the snap, so the seed refunds
  that suffix as `shift − part_time` and the pipeline removes the `shift` from
  every label — the same convention `/route` and the many-to-one matrix field
  use. Seeding it any other way shifts the WHOLE field by a constant: the
  polygon then serves a smaller threshold than the one asked for (#544).

## 4. Calibration seam

The engine ships legal-limit weights and calibrates itself at boot from an
**`edge_speeds.parquet`** placed beside the container (per-region, or one at the
data-dir root) by the private deploy tooling — the engine never fetches it.

| Column | Meaning |
|---|---|
| `osm_node_from`, `osm_node_to` (i64) | the directed edge, keyed by the engine's own OSM node pair — `butterfly-route export-edges` emits the list to match against, so alignment holds by construction |
| `speed_ratio` (f64) | observed / free-flow ratio, typical profile; optional `speed_ratio_best` / `speed_ratio_worst` enable `uncertainty=bands` |

Parquet **key-value metadata** carries the global level: `time_scale`, and
optionally `time_scale_best` / `time_scale_worst` (each sanity-bounded to
`[0.5, 2.0]`). Boot recustomization scales **link weights AND turn penalties**
by it. Never fold a level anchor back into the per-edge ratios instead: only
part of it propagates (turn costs stay unscaled) and rank correlation erodes.

The level is **anchored on time-stamped reference trips** — origin, destination
and observed travel time at a known hour — staged under `$BUTTERFLY_REFS_DIR` as
`<prefix>_{typical,best,worst}.csv`. Two rules govern the anchor:

1. **Better too slow than too fast.** The accepted band on the median
   engine/reference ratio is asymmetric: never more than ~2 % fast, with real
   slack on the slow side. A fast engine promises arrivals it cannot keep.
2. **Like-for-like.** Compare the engine's route against the reference's route
   for the same origin-destination pair at the same hour — never a free-flow
   reference against a traffic-aware engine, never a typical band against a
   best-band reference set.

`BUTTERFLY_BOOT_RECUSTOMIZE=off` serves the uncalibrated free-flow car — the
fastest way to tell whether a regression is in the graph or in the speed table.

## 5. Testing and the post-deploy gate

```bash
cargo test --workspace                     # ~860 tests (unit + integration + doc), no data
cargo test --workspace --all-features      # + the `feature = "bench"` tests (#556)
bash scripts/check-upstream-clean.sh       # public-repo leak guard
BUTTERFLY_REFS_DIR=/data/reference-trips python3 bench/postdeploy_gate.py --base http://localhost:3001 [--quick] [--no-flight] [--list-gates]
```

Local pre-push and CI run ONE step list, `scripts/ci-steps.sh` — print it with
`bash scripts/ci-steps.sh --list`. In order: the upstream-clean leak guard,
`cargo fmt --check`, clippy with warnings as errors (all targets, all features),
`cargo test --workspace`, `cargo test --workspace --all-features` (#556), a count
guard proving the all-features test list is a strict superset of the default one,
the gate's `py_compile` + `--list-gates` smoke, and the gate's own offline unit
tests (`bench/test_postdeploy_gate.py`). There is no separate
`cargo build` step: `cargo test --workspace` already links `butterfly-route` and
`butterfly-dl`, and `butterfly-bench` is a `--all-features` bin (#591).
`scripts/hooks/install.sh` installs the hook. Skip it with
`BUTTERFLY_NO_VERIFY=1` in emergencies only. Belgium is the only test dataset —
do not add fixtures for other regions.

The post-deploy gate runs against a live server and blocks promotion. `--quick`
runs the invariants only (~30 s) and skips the reference-trip ground truth;
`--trips` and `--refs-prefix` point at CSVs under `$BUTTERFLY_REFS_DIR`. It is
**invariant-based** and holds no measured-then-pasted constants: a pasted
constant only asserts "the server returns what it returned yesterday", which a
regression walks straight through. It checks properties instead — detour bounded
against crow-fly distance, plausible mean speed, distance ≡ polyline ≡ Σ
annotations, `/route` and `/table` agreeing within 3 s, A→B vs B→A symmetry,
isochrone topology and snapped-origin containment, `edges_batch` sums vs live
`/route`, matrix completeness and sparse-output consistency.

The suite asserts **no wall-clock number**. Where it once inferred the matrix
plan from a scaling ratio — flaky on a loaded runner, so it had been downgraded
to a warning — it now reads the plan the server reports for the very request it
made, and fails on the wrong one (#594). `BUTTERFLY_MATRIX_ALGO=bucket|phast`
cannot help here: it is read once at the first matrix call and frozen, so a
live server cannot be flipped; the gate exercises both branches by SHAPE
instead (1×N and N×1 must be `phast_*`, a balanced N×N must be `bucket`).

**One invariant per user ticket.** `gate_ticket_invariants` names every
user-reported failure class and either checks it directly or points at the gate
that already is that invariant. When you fix a user-visible bug, add its
invariant there — that is how a failure class stops being able to ship twice.

## 6. Repo boundaries

This repository is **public and holds the engine only**: the pipeline, the CCH,
the query server, the calibration mathematics, the benchmark harnesses. Three
things live in private repositories and must never appear here: deploy tooling
(build, precompute, artifact publishing, image tags); infrastructure manifests
(orchestration, volumes, environments, hostnames); the licensed observed-speed
pipeline (feeds, provider names, segment matching, publication).

**The only things that cross the boundary are data artefacts with documented
contracts**: `edge_speeds.parquet` (§4) and the reference-trip CSVs
(`route_id,long_1,lat_1,long_2,lat_2,ref_min,ref_km`). Both are defined by their
columns, not by where they come from. No private repository name, data provider,
storage path, host, IP address or client name belongs in engine code, comments,
docs or commit messages; for provenance and cache invalidation use
container-internal identity (section CRCs, input hashes), never a deploy-side
identifier. `scripts/check-upstream-clean.sh` enforces this over **tracked files
and commit messages** in the pre-push range, and runs first in CI. If it fires,
reword generically or move the content — do not add an exemption.

## 7. Development principles

- **KISS** — the minimal abstraction that is correct. **Test first** — the
  failing test, then the implementation.
- **Atomic conventional commits**, one logical change each: `feat(matrix): …`,
  `fix(isochrone): …`, `docs: …`.
- **Warnings are errors.** `[workspace.lints]` denies warnings, dead code,
  unused items and `unsafe_code`. The sole `unsafe` carveout is the
  memory-mapping surface in `route/src/formats/mmap.rs`, with SAFETY blocks.
- **No placeholders** — no stubs, no "implement later". If it cannot be done
  correctly, stop and ask.
- **Prove it.** Run the pipeline on Belgium, check the lock conditions, run the
  gate. Never assume a code path is live — verify the served artefact.
  **Profile before optimising**, and prefer a better algorithm to more threads.

## 8. Key file paths

```
route/src/cli.rs                    every subcommand and its flags
route/src/ebg/                      edge-based graph construction (the routing graph)
route/src/ordering_lifted.rs        step 6 nested dissection, lifted NBG -> EBG
route/src/{contraction,customization}.rs   steps 7 and 8
route/src/calibrate.rs              speed-table reading, time_scale, calibration math
route/src/pack.rs                   .butterfly container + export-edges
route/src/formats/                  binary formats, CRC, mmap (sole unsafe carveout)
route/src/matrix/bucket_ch.rs       bucket M2M + the measured-cost matrix router
route/src/range/phast.rs            single-source PHAST (batched_phast.rs = K-lane)
route/src/range/sparse_contour.rs   isochrone topology (one simple polygon)
route/src/server/api.rs             REST router; handlers sit beside it
route/src/server/{state,flight}.rs  server state + boot recustomization; Flight actions
route/src/transit/                  timetable, loaders, RAPTOR, ULTRA transfers
bench/postdeploy_gate.py            the invariant gate
```
