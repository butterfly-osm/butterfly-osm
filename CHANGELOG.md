# Butterfly-OSM Ecosystem Changelog

All notable changes to the butterfly-osm ecosystem will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For detailed tool-specific changes, see individual tool changelogs:
- [butterfly-dl](./tools/butterfly-dl/CHANGELOG.md) - OSM data downloader

## [Unreleased]

### 2026-09-04 — Arrive isochrones reach as far as they should (#544)

An arrive (`direction=arrive`) isochrone was seeded at `w(edge) − part_time`
with no shift. That is the destination convention (`shift − part_time`, the
caller subtracts the shift) plus one full seed-edge weight, so **every**
reverse label came out `w(snapped edge)` too large and the polygon served
the `T − w(snapped edge)` isochrone. On a rural snap onto a long fast edge
that is tens of seconds of missing road at the boundary — up to ~9 % of the
threshold.

- The arrive field now uses the SAME seeding as `/route` and the many-to-one
  matrix field (`PhantomEnd::query_seeds_and_shift`): one convention, no
  second copy. The pipeline bounds the field at `T + shift` and normalises
  every label by `− shift`, so a settled label means the same thing in both
  directions.
- `arrive_reach` is now the ONE named definition of the arrive direction's
  reach, the dual of `depart_frontier`. No predecessor scan and no
  reverse-UP adjacency: a depart label is an arrival at the HEAD, so the
  partial edges are UNREACHED successors (no labels — hence the scan), while
  an arrive label is the cost FROM the head to the snap, so every edge with a
  reachable point already HAS a label. The partial edges are the settled ones.
- Depart is untouched (shift 0, same stamp, same frontier).
- Proof, data-free: `arrive_reach` against a brute-force reference at every
  threshold on the six-edge synthetic hierarchy, and — on the synthetic
  lattice with a real steps 6/7/8 contraction — the arrive field against BOTH
  the many-to-one table and an independent reverse Dijkstra written in the
  test. Served-polygon coverage on a 400 m / 60 s lattice: **0.00 %** of the
  reachable network more than 150 m outside (0/292 at 300 s, 0/1348 at
  600 s), where the pre-fix field left **15.75 %** and **7.72 %** out, worst
  257 m. No over-reach: 0 unreachable points sit deeper than 150 m inside.

### 2026-09-04 — One query context for the handlers that never cross a region (#577)

`RegionsState` was the axum state of a dozen handlers, but only `/route` and
`/match` ever *solve* a cross-region query. Every other surface opened with the
same prologue — start instant, `dispatch_*`, keep `(state, region_id)`, reject
mixed-region input — and closed by recording the per-region metric with that
instant. `QueryContext { state, region_id, region_idx, started }` is that
prologue once, built by `from_point` / `from_pair` / `from_points`, with
`record(endpoint)` as the epilogue.

Moved to the context: `/nearest`, `/isochrone`, `/isochrone/bulk`, `/table`,
`/trip`, `/catchment`, `/transit`, `/transit/bulk`, and the Flight actions
(`matrix`, `route_batch`, `isochrone`, `edges_batch`, both DoExchange entries).
`/route` and `/match` keep the multi-region state — they hand cross-region
input to the overlay coordinator instead of rejecting it — and so does
`/transit/bulk`, which confirms `queries[1..]` against every region's bbox and
now reads the winning index off the context.

The dispatcher's surface shrinks to its four entry points plus the
cross-region pair: `dispatch_single_id` / `dispatch_p2p_id` /
`dispatch_p2p_with_idx` collapse into `dispatch_single` / `dispatch_p2p`, both
returning `(state, region id, region index)` like `dispatch_many`.

Unchanged on purpose, and why:

- **Not an axum extractor.** Every handler validates something before
  dispatching, and the dispatch inputs differ per surface. An extractor would
  snap first and change which error a malformed request gets. The constructors
  are called exactly where the prologue sat.
- **Error text, status and evaluation order are byte-identical** on every
  surface. Two inline renderings became named functions so the wording is
  testable without a loaded container: `DispatchError::into_code_message_parts`
  (`/trip` and `/match` each open-coded it) and `/transit/bulk`'s
  `bulk_out_of_region` / `bulk_query_dispatch_error`.
- **`/metrics` label set is byte-identical.** `QueryContext::record` forwards
  verbatim to the same `record_query`, with the dispatcher's own region id and
  the same endpoint literal at every call site. Pinned by a capturing
  `metrics::Recorder` installed per-thread, including that endpoint labels
  outside `ENDPOINTS` (`isochrone_bulk`, `catchment`) still reach the exporter.

**If you read a latency graph:** two histograms shift by roughly a
microsecond. `/transit` and `/transit/bulk` used to take their start instant
before computing the access mode / before the batch-size checks; the context
takes it immediately before the dispatch. Same labels, same series — the
recorded value simply no longer includes that sliver of validation. Every other
endpoint's instant is taken at the same point as before.

### 2026-09-04 — Retire the baked traffic-variant loader (#599)

**Breaking for a legacy container: its baked traffic variants are no longer
served.** They were never part of the public profile set — Belgium ships ONE
public car profile — and no artifact that could carry one was found.

#582 removed everything that could WRITE a traffic profile (the step-8
`--traffic` bake, the offline fit subcommand, the sample profile). What
survived was the read half: a loader that discovered `<base>_<variant>` weight
sets in a container or a step8 tree, parsed the profile out of the sibling
provenance section, synthesised a `<base>_<variant>` mode, and served it under
`?traffic=<name>`. No build could produce that input, so the open question was
artifact compatibility, not dead code.

**The evidence.** Every `*.butterfly` container on the build host — six of
them, 2026-06-11 through 2026-08-31, spanning the oldest kept precompute and
the newest multi-region artifact — carries exactly 115 sections, four base
modes (`bike`, `car`, `car_nodir`, `foot`), and ZERO
`mode/<m>/_variant/<v>/...` or traffic-profile-provenance sections. The
variant shape is already absent from artifacts that predate the
one-public-car-profile decision. The loader has never had an input and, with
every producer gone, could not acquire one.

Removed:

- `crate::traffic` — the profile schema, parser and validation (535 lines).
- `customization::apply_traffic_to_node_weights_in_memory`, whose only caller
  was this loader.
- Container variant registration (`server/state/modes.rs`) and step8-tree
  variant discovery (`server/state.rs`).
- `Container::list_traffic_variants` and its use in the multi-region mode
  catalogue.
- `pack`'s scan for `cch.w.<mode>_<variant>.u32` + sibling `.traffic.json`,
  and both section emissions.
- `SectionKind::TrafficProfileJson` (`0x0008_0004`). The discriminant is left
  documented and unused so nobody reuses it.

Kept, deliberately:

- **`?traffic=<v>` still answers 400** rather than degrading into an ignored
  unknown query parameter. A caller who asked for variant weights must not be
  handed the base mode's silently. The message now says the parameter is
  retired.
- **`unpack` skips `mode/<m>/_variant/<v>/...`** instead of aborting on a
  section it can no longer map to a step file, so a legacy artifact stays
  extractable. Covered by `unpack_skips_a_legacy_traffic_variant_pair`.
- **A legacy container still opens.** The retired section kind now reads as
  `Unknown`, exactly like `shared/region_tiles` and `shared/manifest.json`
  already do; `inspect` lists it and the loader ignores it.
- **The `density_class` byte in `way_attrs` v2**, now with no consumer at all.
  Its module docs say so. Removing it is an on-disk format decision, not a
  cleanup.

**Unchanged:** the live speed recalibration path — the directed per-edge
`edge_speeds.parquet` contract, its `time_scale*` level anchors, the
best/typical/worst bands and the serve-boot recustomization that consumes
them. None of it went through the profile schema: `car_freeflow` and the band
weight sets are built in `server/state/recustomize.rs` from the edge table,
never from a variant section.

### 2026-09-04 — One matrix batch builder, one seed split, one pair driver (#580)

Three consolidations in `route/src/server/flight.rs`. No behaviour change on
any surface. The engine half of the file loses 74 code lines (3611 → 3537
non-comment, non-blank) and gains the doc comments that explain what is now
single, plus 5 tests: three invariants that survived only as comments now fail
a test instead.

- **One matrix batch builder.** The small (whole-grid) and streamed (tiled)
  matrix paths emitted the same four columns with the same sparse and
  radius-mask semantics, differing only in which index vector they walked and
  in the tiled copy hardcoding `distance_m = u32::MAX`. That divergence is not
  hypothetical — it shipped (#534) and large-request clients read whole tiles
  as unreachable; the fix at the time was a comment plus a values-only
  cross-path test. Both call sites now enter one body: the small branch hands
  it the whole valid grid with an unbounded threshold, the tiled branch one
  source block plus the bound. `small_and_tile_emit_identical_schema` asserts
  the two shapings emit the same field names, types, nullability and order,
  dense and sparse, with and without a lat matrix.
- **One seed split per role, K-best rescue by position.** The "walk the primary
  snaps, push what snapped into four parallel vectors" loop was written twice
  (origins, destinations), each copy CLONING the endpoint's phantom seed set
  although the snap vector is dead on the next line. The per-cell K=64 rescue
  was the same shape a third time and kept its answers in two
  `HashMap<usize, Vec<u32>>` keyed by original input index — so every cell of
  the fallback join hashed a `usize` and needed an `&empty` sentinel, while
  every lookup it ever did was by POSITION. Now `split_primary_snaps` (seeds
  moved, not cloned) and `kbest_ranks_by_position` (a position-indexed
  `Vec<Option<Vec<u32>>>`, selected by a `Vec<bool>`).
- **One per-pair driver.** `route_batch` unbounded, `route_batch` under
  `max_meters`, and `edges_batch` each carried a private copy of the whole
  per-pair stack: the phantom-seeded fast path three times, the K=64
  closest-sum-first escalation three times. That fast path is exactly what
  makes `/route`, `route_batch` and `edges_batch` agree on the route for a
  pair, and hand-copies of it are how they drifted apart before. Now
  `phantom_pair` + `escalate_route` (combo cap as an argument) run under a
  named `PairPlan`, and `drive_pair` hands the `QueryResult` to the one thing
  the surfaces actually differ on — a WKB route, that route's length under a
  bound, or per-edge OSM rows. Four guards pin it, including a source scan
  that the phantom-seeded query exists exactly once.
- **Two differences kept and named rather than traded away.** `edges_batch`
  tries a direct K=1 snap before the K=64 collect and `route_batch` does not
  (enabling it there would change which of several equal-cost geometries a
  phantom-miss pair returns); and `edges_batch` caps its escalation at 16
  combos where #548 set every other surface to 400 precisely so two surfaces
  could not disagree on whether a pair is routable at all. The second is a
  real residual disagreement — a pair `edges_batch` calls unreachable can be
  routable on `/route` — now a named constant with a compile-time assertion
  that it is the narrower one, instead of a literal buried in a third copy.
- **Not done: the shared-forward win the ticket expected.** #580 assumed
  `edges_batch` groups pairs by source and `route_batch` does not, so one
  driver would hand `route_batch` the #438 grouping. It does not: `do_edges_batch`
  builds an EMPTY group list and routes every pair through the per-pair path
  (`let _ = group_pairs;`), because #506 found the group machinery keys on
  single K=1 ranks and emits pre-phantom detour paths. The grouping is live
  only in `compute_edges_grouped`, i.e. the bench and the equivalence oracle.
  Unifying the drivers therefore gives `route_batch` no grouping; restoring it
  needs the seeded meet-group tracked on #506.

### 2026-09-04 — One atomic file per cached recustomization pass (#571)

The boot recustomization cache was ONE file grown by append, and nearly all
of its machinery existed only to make a concurrent or interrupted append
survivable: a frame scan, torn-tail truncation before every write, a
maximum-section bound, a key-seeded section CRC, and realignment on the
declared frame boundary after a bad parse.

- **One file per section**, `recustomize.car.edge.<key>.<base>.<id>.bin`,
  written to a scratch name and `rename`d into place. POSIX rename is atomic,
  so a reader sees a whole section or no file — a torn section is
  structurally impossible, and the five mechanisms above go with it (−230
  lines of framing). Superseded keys, their orphan scratch files and the
  pre-#571 append file are unlinked once per boot.
- **Every guard kept.** The payload CRC stays (rename says nothing about
  bit-rot on a network volume) and now covers the header too; `ensure_fits`
  stays and is strictly tighter, bounding allocations by the file's REAL size
  instead of a length the file declares about itself; the #552 storage width
  still round-trips; and the #563 rule that a section is served only to a
  pass whose base weights are byte-identical is now enforced by the file NAME
  *and* the CRC-covered header rather than by seeding the section checksum.
  Any problem reading a section still means "recompute", never a failure.
- **Two properties the append design made hard to test** are now unit tests:
  five writers racing on the same three sections all succeed and all read
  back (`concurrent_writers_do_not_make_each_other_recompute`), and a killed
  writer's half-file is never readable as a section and is swept when the key
  moves on (`an_interrupted_write_leaves_no_half_file`). The cold-boot ==
  warm-boot pipeline test, its cache-hit counter and the band-ordering test
  are unchanged.
- **Cache tag bumped** `recustomize-car-edge-v7` → `v8`: the key derivation
  changed shape, so the first boot after this change recomputes.

### 2026-09-04 — One weight plan for the six query surfaces (#566, #561)

`/route`, `/table`, `/trip`, `/isochrone`, `/isochrone/bulk` and `/match` each
carried a copy of the same four steps: compute (or cache-fetch) the
`avoid_polygons` recustomization, build the snap mask, pick the `exclude=`
weight set when avoid is absent, then re-derive the avoid-over-exclude priority
at every use site. Six copies, and they had drifted.

- **#566 — one `avoid::resolve_weights()`.** It returns a `WeightPlan` holding
  the four answers, and `WeightPlan::weights()` is the single place the
  priority rule (avoid wins, because its recustomization already folds the
  exclude flags in) lives. `build_avoid_mask` and `compute_avoid_weights` are
  now private to `avoid.rs` and `compute_avoid_weights_time_only` — a pure
  alias — is gone, so the compiler, not a reviewer, is what stops a seventh
  copy. 335 lines of handler for 229 in one place.
- **#561 — the 99 % path stops copying the edge bitset.** `WeightPlan.snap_mask`
  is `Cow::Borrowed(&mode_data.mask)` when neither option is present. `/table`
  and `/isochrone/bulk` cloned the whole mask on every request — one bit per
  EBG node, 80 639 words = **630 KiB** on Belgium's 5 160 848 — on the two
  highest-throughput REST surfaces, while `/route` already borrowed it. (The
  ticket estimated ~1.5 MB; measured against the artifact it is 630 KiB, and
  the 1.5 MB section is the snap index's own per-mode mask, not this one.)
- **Two divergences found between the copies, kept rather than traded away.**
  `/trip` and `/match` resolve their polygon with a bare `.ok()` inside
  `spawn_blocking`, so an avoid polygon that covers no edge degrades to a plain
  query there while the other four answer `400 no edges found inside avoid
  polygon(s)`; `resolve_weights_lenient_avoid()` keeps that and names it. And
  `/route` resolved its exclude weights ~400 lines below the mask, after
  several early returns, so a cold-cache recustomization never ran on those
  paths — the plan resolves them on FIRST USE (`OnceLock`), which keeps
  `/route` exactly as lazy as it was and makes the other five lazy too.
- **Guards.** `snap_mask_is_borrowed_only_when_no_option_filters_it` covers all
  four option combinations without the artifact;
  `weight_plan_borrows_the_mode_mask_on_the_common_path` asserts pointer
  identity with `mode_data.mask`; `every_surface_resolves_the_same_plan` drives
  the six surfaces as table rows across four parameter combinations; and
  `only_trip_and_match_ignore_an_unusable_avoid_polygon` pins the divergence
  above, error string included.

### 2026-09-04 — Build, images and the dependency graph (#565, #573, #574, #586)

Four KISS-audit tickets that all landed on the same surface: what it costs to
build this repo, and what ships in the images.

- **#565 — a failed dependency layer can no longer look green.** Both
  Dockerfiles pre-built dependencies with
  `cargo build --release … 2>/dev/null || true`. The `|| true` swallowed the
  failure, so a broken cache passed and only blew up later, in the real build,
  with a confusing error — or, worse, succeeded against stale deps. The
  dummy-source trick is gone entirely: BuildKit cache mounts over the cargo
  registry, the vendored checkouts and `target/` do the caching, and nothing
  swallows an error.
- **#573 — one multi-target `Dockerfile`.** A shared `builder` stage feeds a
  `tools` stage and a `runtime` stage; `runtime` is last, so a bare
  `docker build .` still produces the serving image and
  `docker build --target tools .` produces the pipeline image. Building both
  now compiles the workspace ONCE instead of twice. `Dockerfile.tools`
  survives one more release as a *generated* shim
  (`scripts/gen-dockerfile-tools.sh`, verified by CI) so external tooling that
  still passes `-f Dockerfile.tools` keeps working and cannot drift.
- **#573 — stripped release binary, without losing the backtrace.**
  `[profile.release] strip = "symbols"` takes `butterfly-route` from 55.6 MB
  to 48.0 MB and `butterfly-dl` from 12.6 MB to 10.3 MB. Panic location,
  panic message and anyhow context chains are unaffected — they are static
  string data, not symbols; only symbol NAMES in a `RUST_BACKTRACE` dump are,
  so `[profile.release-debug]` (same optimisation, symbols kept) is there when
  you need them. `--version` now exists on the CLI, because a stripped binary
  needs a way to say which build it is.
- **#573 — no `curl` in the serving image, and the healthcheck stays.** curl
  existed for exactly one reason: the Dockerfile `HEALTHCHECK`. It is replaced
  by `butterfly-route healthcheck [--url] [--timeout-secs]`, a
  dependency-free HTTP/1.1 probe over `TcpStream` that exits 0 on 2xx.
  `docker run`, Compose and Swarm keep the liveness signal they had;
  Kubernetes keeps ignoring it in favour of its own `httpGet` probe; libcurl
  and ~10 transitive libraries — and their CVE stream — leave the image.
- **#574 — the GTFS-RT protobuf toolchain is vendored, not built.**
  `gtfs-rt 0.5` dragged a whole second protobuf stack — prost 0.11,
  prost-derive, prost-types, prost-build, prettyplease 0.1, heck 0.4,
  itertools 0.10, petgraph, multimap, fixedbitset — alongside the prost 0.14
  already in the graph, and `prost-build` was the ONLY reason `protoc` was a
  build dependency (a CI step plus `protobuf-compiler` in both images). The
  bindings are now generated once with prost 0.14 and committed next to the
  `.proto` (`route/src/transit/gtfs_realtime.rs`, `scripts/gen-gtfs-rt.sh`).
  11 crates leave `Cargo.lock`, none join, and no protobuf compiler is needed
  to build anything. `prost` is now single-version in the graph; `syn 1.0`
  survives, but no longer for a protobuf reason — its one remaining consumer
  is `gtfs-structures 0.47 -> derivative 2.2`, which upstream dropped in
  gtfs-structures 0.49 (a separate bump, out of scope here). The guard is a
  round-trip test over a recorded
  `FeedMessage` fixture whose bytes were produced by the implementation being
  replaced — an independent witness of the wire format: it decodes every
  field and re-encodes byte-for-byte.
- **#586 — the raw correctness-sweep JSONL is out of the tree.** 8.7 MB of the
  9.5 MB `bench/` archive was three `results-*.jsonl` files every clone paid
  for forever. The summaries, the top-disagreement tables and `REPORT.md` are
  the conclusions and stay; `REPORT.md` now names the commit that holds the
  raw data, and the command to get it back.
### 2026-09-04 — A committed GTFS fixture unblocks the static-loader dependency bump (#598)

Every feed-backed transit test needs a real feed staged on disk, so on a bare
runner they all skip and nothing checked the GTFS static parse at all. That is
not a safety net, and it had a concrete cost: a dependency clean-up was ready
and could not be landed, because the same bump moves the hashing and archive
crates used inside the loader and nothing could prove the parse output was
unchanged.

- **`route/tests/fixtures/gtfs_mini/`** — an invented six-stop feed, plain CSV,
  parsed through the real loader in `route/tests/gtfs_static_parse.rs`. No
  artifact, no network, no licensed feed; the file runs in well under a second.
  It pins the cases the loader special-cases: id-sorted stop registration (the
  transfer-graph cache keys on `StopIdx`), a parent station with two platforms,
  `calendar_dates` deleting the weekday service and adding a never-scheduled one
  on the same date, stop-times written out of `stop_sequence` order, a trip
  running past 86400 s, missing arrival/departure fallbacks, a one-stop-time
  trip that must be dropped, trips re-sorted by first departure, empty route
  names with a `direction_id` headsign fallback, and a two-feed namespaced
  merge. Beyond the explicit assertions the compiled timetable is rendered
  canonically and pinned by digest. The fixture is also zipped at test time
  (stored entries, fixed timestamps ⇒ reproducible bytes) and parsed through the
  archive path, with the feed sha256 pinned — so swapping the archive or hashing
  crate underneath either reproduces exactly or fails loudly.
- **`gtfs-structures` 0.47 → 0.49** — validated on that fixture: zero source
  changes, identical timetable digest and identical feed sha256 either side of
  the bump. `derivative` is gone, so the GTFS static loader no longer pulls
  syn 1.0; the remaining syn 1.0 edge is the realtime protobuf chain
  (prost 0.11) alone. 0.50 is deliberately not taken — it swaps chrono for jiff
  in the public calendar types.

### 2026-09-04 — Post-deploy gate consolidation + CI runs the integration tests (#550, #555)

The gate (`bench/postdeploy_gate.py`) no longer passes by accident:

- **Fail-loud preflight**: pyarrow/pandas missing is a FAIL, not eleven silent
  `except ImportError: return True` skips; `--no-flight` is the explicit opt-out
  (Flight gates then print SKIP). An unreachable Flight endpoint is a FAIL too.
- **One `flight_client(base)`** (REST port + 1, `--flight-base` override) for
  every Flight gate — `gate_bands` used to derive the port by string-replacing
  a port number, so it silently probed the wrong instance.
- **One shared helper block** (haversine, point-in-ring, ring area, WKB
  polygon/multipolygon, polyline6, distance-to-ring); the dead `_pip` and the
  subset `_wkb_polygon_rings` are gone.
- **`iso_bundle(...)`** memoises each isochrone origin's WKB / JSON contours /
  `include=network` / GeoJSON / snap, shared by the five isochrone gates, and
  the reference trips are routed ONCE for both `gate_bands` and
  `gate_ground_truth`.
- **`gate_consistency` merged into one route≡table agreement gate** with a
  uniform and a close-pair sampler, both run on a 16-way thread pool; symmetry,
  graph-holes and one-way routability are parallel too.
- **Errors are counted, not skipped**: more than `max_errors` transport failures
  fails the gate (an HTTP 400/404 "no route" is counted separately).
- **One `THRESHOLDS` table** for every tolerance, and `--list-gates` prints the
  gate registry (CI smoke).
- Product rule unchanged: an isochrone is ONE simple polygon — no MultiPolygon,
  no holes.

CI (`.github/workflows/ci.yml`) and `scripts/hooks/pre-push` now run the SAME
step list, `scripts/ci-steps.sh`: `cargo test --workspace` (route/tests/*.rs ran
nowhere before) plus `py_compile` and `--list-gates` on the gate. Dead
benchmark scripts that posted to the streaming table endpoint, and a frozen
parity output file, were removed; the duplicated `geo` dev-dependency in
`route/Cargo.toml` (already a normal dependency, same version) was dropped.

### 2026-07-23 — Uncertainty bands: single car profile + opt-in Q1/Q3 (#521, dev)

ONE public car profile (the demand-weighted median). `uncertainty=bands` on
`/route`, `POST /table`, `GET /isochrone`, `POST /trip` adds diurnal TIME
quantiles (q25 optimistic / q75 pessimistic) computed on two HIDDEN weight
sets registered at boot from optional `speed_ratio_q25/q75` columns of
`edge_speeds.parquet` (same clean base, same #481 turn correction, same
#524 time_scale; slots absent from `mode_lookup`, so no `?mode=` reaches
them). Default responses are byte-identical to before — bands are an
explicit opt-in because they cost real compute (2 extra passes). `/route`
and `/trip` band numbers are full re-queries (the band's world may
legitimately reroute); isochrones return nested tagged contour features.
Validated on Belgium: 0 monotonicity violations over routes/table/trip,
optimistic ⊇ median ⊇ pessimistic isochrone nesting, full gate PASS with
defaults untouched. Dev-only until the diurnal uplift models fatten with
survey accumulation (band width is honest but narrow off-corridor —
uplift spatial-CV R² 0.37/0.25). Flight band columns: follow-up on #521.

### 2026-07-23 — Route geometry closure, engine-keyed calibration, global time_scale (#521–#524)

- **Foot/route consistency (#522/#523)**: `/route` now clips the polyline,
  per-edge annotations, and steps at BOTH phantom endpoints (previously only
  the origin side was clipped on seeded meets), and orients the first edge's
  geometry against the next edge's nearer endpoint. `distance_m` ≡ polyline
  length ≡ Σ annotations within 0.01 % over 46.5 km; near-identical foot
  pairs now return coherent 1.33–1.37 m/s speeds.
- **`export-edges` tool (#524)**: dumps the NBG edge list
  (`u_osm, v_osm, way_id, length_m`) and, with `--segments`, the raw OSM
  segment chains behind every directed edge (1:1 with served polyline
  vertices) — the engine-side half of an exact external-data alignment; the
  private speeds pipeline resolves its graph onto these segments (98.9 % of
  edges) and emits `edge_speeds.parquet` keyed to the engine by construction.
- **Global `time_scale` (#524)**: `edge_speeds.parquet` may carry a
  `time_scale` KV-metadata scalar; boot recustomization scales link weights
  AND turn penalties by it (forbidden-turn sentinels preserved), so a
  producer-measured end-to-end level anchor lands exactly in one step.
  Ratio-side anchoring only propagated ~55 % per pass (turns unscaled) and
  eroded rank correlation. Recustomize cache tags bumped to v3.
- **Post-deploy gate rewritten to invariants**: sentinel fixtures now check
  bounded detour vs crow-fly, plausible mean speed, and
  distance ≡ polyline ≡ Σ annotations — no measured-then-pasted constants
  anywhere; `edges_batch` sums are checked against the live `/route`.
- **Validation (Belgium, 1000 independently observed reference trips)**:
  duration p50 0.992 (was 1.020), corr 0.994 (was 0.963), MAPE 4.0 %
  (was 10.6 %); distance outliers 39 (max 80). Full gate PASS.

### 2026-07-17 — Phantom endpoints, close-pair correctness, in-engine seeded matrices (#502–#517)

The largest correctness campaign since the CCH landed, deployed fleet-wide
and locked in by a new post-deploy gate:

- **Phantom endpoints (#502/#503/#504–#508)**: snapping commits to BOTH
  directed twins of up to 3 near-equidistant physical edges with exact
  partial-edge costs, on every surface — `/route`, `/table`, Flight
  `matrix` (small + streamed), `route_batch` (unbounded + `max_meters`),
  `/trip`, REST + Flight isochrones (+ exact snapped contour anchor, #497),
  `/isochrone/bulk`, catchment, `edges_batch`. Kills 2–4× wrong-way
  detours on long rural edges; field-validated (Berloz/Heers/Robertville).
- **Close-pair correctness (#509/#510)**: fixed 0-second `/table` answers
  (12 % of close pairs), a legacy same-edge `/route` shortcut, secondary-
  candidate zero-cost conflation, and seed-label domination in the seeded
  bidirectional search (ALT-label meets).
- **In-engine multi-seed bucket M2M (#511)**: phantom seeds now initialise
  the engine directly (super-source forward, shift-trick backward,
  pure-meet guard with in-join same-edge directs). Replaces the API-layer
  expansion that cost 12–15× on matrices; measured at parity with the
  pre-phantom engine (1000×1000: 492 ms seeded vs 509 ms legacy).
- **edges_batch (#512)**: per-edge paths now match `/route` exactly
  (fixture sums 334 s → 163 s).
- **Turn-charge label correction (#515)**: boot recustomization subtracts
  the engine's own expected junction charge from observed door-to-door
  edge slowdowns (zero fitted parameters).
- **`GET /version` (#516)** and **post-deploy gate** (`bench/postdeploy_gate.py`,
  #505/#513/#517): ticket fixtures, fwd/rev symmetry, route==table,
  close-pair sweep, isochrone snapped-origin containment, edges_batch
  sums, and a 1 000-trip reference ground truth with independently
  observed travel times (dur p50 1.02, dist p50 1.004, thresholds
  ratcheted); `deploy.sh staging` refuses to promote on FAIL.
- **OSRM re-validation (post-phantom)**: tied at 200×200, 2.7× faster at
  500×500, 4.8× faster at 1000×1000 (same-host interleaved HTTP).


### 2026-06-10 — Traffic profiles: (highway_class × density) modifier matrix (#428)

Traffic profiles (`traffic/*.traffic.json`) may now carry an opt-in 2-D
`matrix` section refining the per-density speed-factor vector with
per-`(highway_class × density)` cells:

- **Schema**: `"matrix": {"<highway_class_code>": {"<density>": factor, ...}}`.
  Outer keys are the numeric `highway_class` codes stored per way in
  `way_attrs.<mode>.bin` (assigned by the build model's `highway_class`
  table — model-defined, so the code rather than a name is the exact value
  available at customization time). Rows may be partial: a missing
  `(highway, density)` cell falls back to the per-density `speed_factors`
  vector, which stays required and complete. Factors validated in
  `[0.1, 1.5]`; unknown keys, non-canonical codes, empty matrices/rows
  rejected. Vector-only profiles are unchanged and round-trip
  byte-for-byte (the `matrix` key is omitted when absent).
- **Application**: step 8 `--traffic` and the serve-boot car
  recustomization both resolve `factor_for_cell(highway, density)` —
  identical to the pre-#428 behavior when no matrix is present.
- **Calibration**: `calibrate-traffic --matrix` fits the matrix from the
  same observed table — per-cell sample-count-weighted median, same
  clamp band, cells emitted only above `--min-samples` (omitted cells
  fall back cell → density-marginal → global). Deterministic output.

Closed #371/#372 — the matrix endpoints (`/table`, `/trip`, Flight)
now report distance values that belong to the SAME path as the
duration, matching what `/route` reports for the same coordinate pair.
The fix combined a new on-disk weight (`cch.lat.<mode>.u32`,
length-along-time-shortest per CCH edge), a 2-channel bucket-M2M
algorithm, and a bound-pruned CAS loop on a packed `AtomicU64` for
the parallel backward join. Net effect: drivetime APIs are
semantically consistent AND faster than the broken legacy.

### Belgium /table 1000×1000 dur+dist (HTTP wall)

| state | latency | distance metric |
|---|---:|---|
| Pre-#372 legacy 2-pass | 549 ms | wrong (distance-shortest CCH, different geometric path) |
| **Shipped: 2-channel + target-owned local columns** | **379 ms** | **correct (matches /route to within 0.45 % u32 rounding)** |
| OSRM CH reference (HTTP wall) | 684 ms | — |

Butterfly is now **1.81× faster than OSRM** at 1000×1000 with the
correct drivetime distance metric.

### Correctness sweep (4 Belgium pairs)

| pair | /route distance | /table distance | gap |
|---|---:|---:|---:|
| Brussels–Antwerp | 57 693 m | 57 678 m | 0.026 % |
| Aalst–Charleroi | 161 545 m | 160 826 m | 0.45 % |
| Liège–Gent | 166 871 m | 166 861 m | 0.006 % |
| Bruges–Namur | 236 950 m | 236 909 m | 0.017 % |

All durations match EXACTLY. The residual u32 rounding gap comes
from `EbgNode.length_m` being u32-rounded vs `/route`'s polyline
geometry sum.

### Pipeline / on-disk

- step8 customize emits `cch.lat.<mode>.u32` alongside `cch.d.<mode>.u32`
  via the new `bottom_up_with_external_middles` helper — for each
  shortcut, sum the physical edge lengths along the time-optimal
  middle's two halves, recursive bottom-up using the post-relax time
  middles. Belgium car: +0.48 s in step8.
- pack.rs bundles `cch.lat.<mode>.u32` into the container as a new
  `CchWeightsLat = 0x0008_0003` section. Belgium container 12.87 GiB
  → **15.4 GiB** (+1.5 GiB for cch.lat across 3 modes).
- `ServerState.ModeData` gains `cch_weights_len_along_time:
  Option<CchWeights>` plus `up_adj_flat_len_along_time` /
  `down_rev_flat_len_along_time`. Old containers boot with `None`
  and fall through to the legacy 2-pass.

### Algorithm (matrix/bucket_ch.rs)

- `SearchState2` — `NodeEntry` + parallel `Vec<u32> lats`; `relax()`
  takes `(node, dist, lat)` and updates both when `dist` improves;
  `pop()` returns `(dist, lat, node)`.
- `Bucket2Entry` — 12 bytes `(dist, lat, source_idx)`. SoA layout
  proven slower for this access pattern than AoS; AoS-only.
- `PrefixSumBuckets2` — same prefix-sum stamping as the single-channel
  buckets, AoS-only.
- `forward_fill_buckets_flat_len_along_time` — reads time from
  `up_adj_flat.weights` and lat from `up_adj_flat_len_along_time.
  weights` at the same flat index. Same topology, parallel arrays.
- `backward_join_parallel_prefix_len_along_time` — per-cell update via
  **bound-pruned CAS loop** on packed `AtomicU64`:

  ```rust
  let mut cur = packed_matrix[cell].load(Relaxed);
  loop {
      let cur_time = (cur >> 32) as u32;
      if cur_time <= entry.dist { break; }            // can't improve via this entry
      let total_time = entry.dist.saturating_add(d);
      if total_time >= cur_time { break; }
      let next = ((total_time as u64) << 32) | total_lat as u64;
      match packed_matrix[cell].compare_exchange_weak(cur, next, Relaxed, Relaxed) {
          Ok(_) => break,
          Err(observed) => cur = observed,
      }
  }
  ```

  Unconditional `fetch_min` was the dominant cost on contended cells;
  load-and-check skips the locked RMW when the current value already
  beats this bucket's entry.

### Consumers

- `/table` and `/trip` dispatch to the 2-channel function when:
  duration+distance both requested, no exclude/avoid, AND
  `cch_weights_len_along_time` is loaded. Otherwise falls back to
  the legacy 2-pass single-channel path (distance-shortest CCH).
- Flight `route_batch` / `edges_batch` already correct (per-cell
  unpack from the time CCH); Flight `matrix` returns `u32::MAX` for
  distance (unchanged).

### Removed

- `/isochrone?distance_m=` parameter (PR #373). Was the only endpoint
  that ran PHAST on the separate distance-shortest CCH; reachability
  was reported for a path geometry different from every other
  endpoint. Requests now return 400.

### PRs

- #373 fix(isochrone): #371 remove `distance_m` (isodistance) parameter
- #377 feat(customize): #371/#372 emit `cch.lat.<mode>.u32` alongside `cch.d`
- #378 feat(state): #372 load `cch.lat.<mode>.u32` into `ModeData.cch_weights_len_along_time`
- #379 feat(pack): #372 bundle `cch.lat.<mode>.u32` into container as `CchWeightsLat` section
- #380 feat(state): #372 build `up_adj_flat_len_along_time` / `down_rev_flat_len_along_time` at boot
- #381 feat(matrix,table): #372 2-channel bucket-M2M (time + length-along-time)
- #383 perf(matrix): #372 target-owned local columns — eliminate `AtomicU64` in 2-channel backward

### Known follow-up

- `cch_weights_dist` and the dist flats are still loaded — the legacy
  2-pass fallback uses them when custom weights (exclude/avoid) are
  in play. Once the exclude/avoid recustomiser also computes
  length-along-time, drop the dist plumbing entirely.

### 2026-05-26 — Lazy snap escalation + isodistance removal

Closed the OSRM gap on the headline `/route` endpoint and pushed `/table`
ahead of OSRM on the HTTP wall, all by deferring the K=64 candidate
fetch in every snapping handler. Also removed isodistance from
`/isochrone` as part of the drivetime-distance-consistency cleanup
(#371). Six PRs landed in one day on top of the codec sprint below.

### Performance — lazy K=64 snap escalation across all snapping endpoints

The pre-patch pattern: every endpoint paid the K=64 candidate fetch
upfront for every source/destination (~2.14 ms each on Belgium per
the `iterate_rings` + linear-scan-update-best loop), even though
98.7% of pairs route on (0, 0) (#197 sweep). After: K=1 primary
upfront, K=64 escalation only for src/tgt indices that produce an
INF cell or where the primary CCH query returns None.

| endpoint | size | before | after | Δ |
|---|---|---:|---:|---:|
| `/route` Brussels→Antwerp HTTP wall | apples-to-apples | 12 ms p50 | **9 ms p50** | **−25%** |
| `/route` tail | 30-run max | 13 ms | **16 ms** | within noise |
| `/table` HTTP wall | 100×100 | 75 ms | **47 ms** | **−37%** |
| `/table` HTTP wall | 1000×1000 | ~740 ms | **549 ms** | **−26%** |
| OSRM CH `/table` HTTP wall reference | 1000×1000 | 684 ms | — | Butterfly is now **1.25× faster than OSRM** |

`/route` now ties OSRM at p50 (9 ms vs 9 ms apples-to-apples) and
beats it on the tail (16 ms vs OSRM 33 ms max).

### Added

- **butterfly-route**: `snap_kbest::snap_primary_role` helper
  (PR #375). K=1 primary with a valid CCH rank; transparently
  escalates to K=64 if the geometrically-closest candidate has
  `orig_to_rank == u32::MAX` (rare `role_filter` / `orig_to_rank`
  disagreement edge case). Used by `/route`, `/catchment`, Flight
  `matrix`, Flight `edges_batch`, Flight `catchment`.

### Changed

- **butterfly-route**: `/route` lazy snap escalation (PR #368).
  Snap K=1 primary first; only escalate to K=64 + #197 combo
  enumeration on primary CCH query failure (~1.3% of Belgium pairs).
  snap_src 2140 µs → 127 µs, snap_dst 717 µs → 23 µs, handler total
  6850 µs → 4180 µs.
- **butterfly-route**: `/table` lazy snap (PR #370). Same pattern,
  K=64 only for src/tgt indices that have at least one failed cell
  after bucket-M2M. Healthy 1000×1000 matrices snap K=64 for zero
  indices.
- **butterfly-route**: `/trip` lazy snap (PR #374). K=1 per waypoint
  upfront, K=64 only for waypoints whose row/column has an INF cell.
- **butterfly-route**: Flight `matrix` / `route_batch` / `edges_batch`
  + Flight `catchment` DoExchange + REST `/catchment` all share the
  same lazy pattern (PR #375).

### Removed

- **butterfly-route**: `/isochrone?distance_m=…` (isodistance) removed
  entirely (#371, PR #373). Isodistance was the only endpoint that
  ran PHAST on the separate distance-shortest CCH (`cch_weights_dist`),
  reporting reachability for a geometric path different from every
  other drivetime endpoint in the engine. Requests now return 400
  `Provide exactly one of: time_s or contours`. The `cch_weights_dist`
  storage stays for now — still consumed by `/table`, `/trip`, and
  Flight matrix endpoints; #372 tracks the 2-channel bucket-M2M
  migration that retires it from those endpoints too.

### Subsequently fixed (#372, see top of [Unreleased])

- The matrix endpoints' divergent distance metric (`/table`, `/trip`,
  Flight `matrix` / `route_batch` / `edges_batch` reporting from the
  separate distance-shortest CCH instead of length-along-time) was the
  reason the 2-channel bucket-M2M work in #372 shipped. With that
  work landed, all matrix endpoints now report distance consistent
  with `/route` within u32 rounding (≤ 0.45 % on the 4-pair Belgium
  sweep).

### Internal

- Clippy + fmt drift cleanup (PR #369). 21 files reformatted under
  edition-2024 rustfmt; 4 `needless_option_as_deref` warnings
  collapsed in `way_names_idx` test code.

### 2026-05-26 — Disk/RAM codec sprint

End-to-end disk + RAM reduction sweep landed across nine PRs. Belgium
packed container shrank from 16.06 GiB to 12.87 GiB (**−20%**) with
no query-latency regression. Cumulative Europe-scale projection at
10 regions: ~20-30 GiB on-disk savings.

### Added

- **butterfly-route**: Format v5 width-picked CCH middles (#352,
  PR #357). `cch.topo` packs `up_middle`/`down_middle` at u16/u24/u32
  depending on rank range. Belgium savings: 272 MB. `WeightArray`
  reuse keeps `u32::MAX` "no middle" sentinel semantics across all
  three widths.
- **butterfly-route**: zstd-compressed cold sections (#347, PR #358).
  `shared/way_names_idx` 19.81 → 6.61 MiB (67% saved) +
  `shared/snap_grid` 179 → 77 KiB (57% saved). Section-internal
  transparent magic-prefix sniff — pre-#347 containers load
  unchanged.
- **butterfly-route**: Split flat-adjacency format (#345, PR #360).
  Per-(mode × direction) `FlatTopo` section shared across time and
  dist metric variants; per-(mode × direction × metric) `FlatWeights`
  sections carry only the weight bytes. Saves ~1 GiB on Belgium.
  Pack-side topology divergence guard catches the unexpected case
  loudly.
- **butterfly-route**: Cold `CchMiddles` SectionKind (#359, PR #362).
  Pack splits `cch.topo` middles out into a dedicated cold section;
  server boot loads both, then `madvise(DONTNEED)` on the middles
  range after CRC walk. Matrix / isochrone / bucket-M2M never touch
  middles, so the kernel reclaims their pages and route-unpack pages
  them back on demand. Codex estimate: ~300-420 MB RSS per Belgium
  mode under 24-thread matrix load.
- **butterfly-route**: Transit_bulk preflight bbox-tier confirm
  (#343, PR #361). `RegionsState::confirm_in_region` replaces per-
  query full snap with bbox + tile check, falling back to full snap
  only for bbox-overlap zones. Projected 100k same-region batch:
  1 s → <50 ms.

### Changed

- **butterfly-route**: u32 offsets in flat adjacencies when n_edges
  fits u32 (#350, PR #355). Belgium-class containers gain another
  ~300 MB.
- **butterfly-route**: u24 absolute targets in flat adjacencies
  (#351, PR #356). Codex re-consult on rank-delta concluded absolute
  u24 is the right first step (rank-delta deferred — bench math
  showed it would regress on hot-loop edge reads). 652 MB saved on
  Belgium.
- **butterfly-route**: u16/u24 weights propagation to flats (#349,
  PR #354). 970 MB compressed across the four flat-adjacency
  variants on Belgium.
- **butterfly-route**: Auto-prune step1..step8 after pack (#344,
  PR #348). `pack` now defaults to deleting the per-step intermediate
  trees after CRC-verifying the packed container — typically 30-60%
  of a region's footprint. `--keep-intermediates` opts out for
  iterative dev.
- **butterfly-route**: Lean default pack drops `shared/nbg.csr`
  (#346, PR #353). Belgium container shrank by another ~190 MB; the
  per-edge geometry index in `shared/edge_geom_*` (#155) supplants
  the unused NBG CSR for serve-time geometry lookups.

### Tested

- Multi-region serve (BE + LU) verified end-to-end: 19/19 REST PASS,
  10/11 Flight PASS (only `transit_bulk` fails — transit subsystem
  not loaded, expected for a no-transit-feed setup).
- /route Brussels→Antwerp byte-identical across all 9 merges.
  12 ms p50 latency unchanged.
- Matrix bench 1000×1000 mean: 244.9 ms (was 249 ms pre-codec —
  noise-band but trending faster).
- e2e-isochrone bench: 4.11 ms mean / 11.5 ms p99 / 243 iso/sec
  single-threaded.
- 600 lib tests pass.

### Removed

- ~365 GB of stale build artifacts (geocode/nominatim docker volume,
  pre-codec Belgium snapshots, abandoned step experiments).

### Internal

- 0 clippy errors on butterfly-route — `chore(clippy)` sweep
  (PR #363, #364) collapses 13 lints into idiomatic forms with no
  behaviour change.

### 2026-05-23

### Added

- **butterfly-route**: Incremental `avoid_polygons` customization
  ([#240](https://github.com/butterfly-osm/butterfly-osm/issues/240),
  [#249](https://github.com/butterfly-osm/butterfly-osm/pull/249)). The
  recustomization pass now walks an explicit BFS frontier seeded from
  the edges that intersect the avoid polygons, instead of re-running a
  whole-graph triangle relaxation. A 1 km rural polygon on Belgium went
  from 37 s to ~780 ms end-to-end (47× speedup); the larger E19
  motorway-corridor polygon settles at 1.16 s. Cold `/route` requests
  that previously dominated the response now spend the bulk of their
  time in I/O and snap, not in customization.
- **butterfly-route**: LRU avoid-polygon cache with operational
  visibility ([#242](https://github.com/butterfly-osm/butterfly-osm/issues/242),
  [#243](https://github.com/butterfly-osm/butterfly-osm/issues/243),
  [#246](https://github.com/butterfly-osm/butterfly-osm/pull/246),
  [#247](https://github.com/butterfly-osm/butterfly-osm/pull/247)).
  Cache hit rate, entry count, and eviction counters are now surfaced
  on `GET /health` and exported as four Prometheus gauges on
  `GET /metrics`. Polygon inputs are canonicalized before hashing so
  semantically equivalent JSON inputs (rotation, whitespace, ring
  closure) collide on the same cache entry. Booth's algorithm
  ([#250](https://github.com/butterfly-osm/butterfly-osm/pull/250))
  replaces the quadratic rotation search used in the first cut of
  canonicalization.
- **belgium-latest container** ([#236](https://github.com/butterfly-osm/butterfly-osm/issues/236)):
  refreshed Belgium build deployed with 5.13M EBG nodes, 14.98M edges,
  769K named roads, and 4 modes (bike, car, foot, truck). Used as the
  reference dataset for every benchmark in this release.

### Changed

- **butterfly-route**: Avoid cache now returns `Arc<AvoidEntry>` rather
  than cloning the customized weight set per request
  ([#241](https://github.com/butterfly-osm/butterfly-osm/issues/241),
  [#245](https://github.com/butterfly-osm/butterfly-osm/pull/245)).
  `/table` warm-hit latency dropped from 366 ms to 22 ms, matching the
  baseline `/table` cost on un-avoided queries.
- **butterfly-route**: `POST /table/stream` now borrows the flat
  adjacency arrays from the cached `AvoidEntry` instead of cloning
  them ([#248](https://github.com/butterfly-osm/butterfly-osm/pull/248)).
  Eliminates a 100–200 MB per-request clone on Belgium-sized inputs;
  visible as a flat memory profile under sustained streaming load.

### Fixed

- **butterfly-route**: Matrix gap closed
  ([#197](https://github.com/butterfly-osm/butterfly-osm/issues/197),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  K-best snap and SCC-aware role masks are now applied at every snap
  site — `/route`, `/nearest`, `/table`, `/matrix`, `/isochrone`,
  `/trip`, and the Flight gRPC equivalents — instead of only `/route`.
  A 200-pair Belgium `/route` ↔ `/table` correlation sweep now reports
  100% agreement, up from a ~9% gap where `/table` would return
  unreachable for pairs `/route` resolved successfully.
- **butterfly-route**: Small-N matrix dispatch fast-path
  ([#191](https://github.com/butterfly-osm/butterfly-osm/issues/191),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  10×10 and 25×25 matrices no longer fall through to the bulk
  scheduler — rayon thread-dispatch overhead at those sizes outweighed
  the parallelism win.
- **butterfly-route**: Sparse triangle correctness for avoid polygons
  ([#235](https://github.com/butterfly-osm/butterfly-osm/issues/235),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  `/route` and `/table` durations now match exactly on avoided
  queries; the previous implementation had an 8% disagreement caused
  by the sparse pass touching a different node set than the dense
  baseline.
- **butterfly-route**: Stale unpacked geometry in serve-time triangle
  relaxation ([#239](https://github.com/butterfly-osm/butterfly-osm/issues/239),
  [#244](https://github.com/butterfly-osm/butterfly-osm/pull/244)).
  When the relax loop replaced a shortcut's middle node, the unpacking
  arrays still pointed at the original topology middle, producing
  polylines that crossed the avoid polygon even though the duration
  number was correct. `up_middle` and `down_middle` are now updated in
  lockstep with the weight.
- **butterfly-route**: Additional correctness and review fixes for
  the incremental avoid path
  ([#233](https://github.com/butterfly-osm/butterfly-osm/issues/233),
  [#234](https://github.com/butterfly-osm/butterfly-osm/issues/234),
  [#248](https://github.com/butterfly-osm/butterfly-osm/pull/248),
  [#251](https://github.com/butterfly-osm/butterfly-osm/pull/251),
  [#252](https://github.com/butterfly-osm/butterfly-osm/pull/252)).

### Removed

- **butterfly-geocode**: Crate shelved
  ([#253](https://github.com/butterfly-osm/butterfly-osm/issues/253),
  [#254](https://github.com/butterfly-osm/butterfly-osm/pull/254)).
  The full geocoder work tree is preserved under the git tag
  `geocode-shelved-2026-05-23` and can be restored at any time; it is
  removed from the workspace to keep CI and release artifacts focused
  on the routing engine.

### Documentation

- New top-level `docs/` directory with a quickstart guide, REST + gRPC
  API reference, deployment guide, architecture overview, and
  troubleshooting notes.
- README rewritten to reflect the current state of the workspace
  (route engine production-ready, geocoder shelved, downloader stable).
- Stale "sparse triangle" comments across `route/src/server/exclude.rs`
  and adjacent modules updated to "incremental BFS"
  ([#251](https://github.com/butterfly-osm/butterfly-osm/pull/251),
  [#252](https://github.com/butterfly-osm/butterfly-osm/pull/252)) so
  the code matches the algorithm that actually runs.

### Performance reference (Belgium, 2026-05-23)

- 10k×10k distance matrix: **18.3 s**, 1.8× faster than OSRM CH on the
  same dataset.
- 50k×50k Flight gRPC matrix: **9.61 min**, at parity with the
  historical `/table/stream` baseline and well outside what OSRM can
  serve at all (URL-length limits, no streaming).
- `/route` with `avoid_polygons`, warm cache hit: **11 ms**.
- `/route` with `avoid_polygons`, cold miss: **~780 ms** for a 1 km
  rural polygon (was 37 s); **1.16 s** for the E19 motorway corridor.
- `/table` with `avoid_polygons`, warm cache hit: **22 ms** (was
  366 ms before the `Arc<AvoidEntry>` return).

### 2026-04-14

### Changed

- **License**: relicensed from MIT to AGPL-3.0-or-later. See
  [#99](https://github.com/butterfly-osm/butterfly-osm/issues/99) for the
  full rationale. Every workspace crate (`butterfly-common`,
  `butterfly-dl`, `butterfly-route`) now ships under
  AGPL-3.0-or-later. Network-deployed forks must publish source per the
  AGPL §13 requirement. The `LICENSE` file now carries the canonical FSF
  AGPL-3.0 text byte-for-byte. `CONTRIBUTING.md` documents the
  submission-implies-agreement contributor grant.

### Removed
- **butterfly-route**: Experimental PHAST routing implementation and related routing tools
- **benchmarks/**: Deprecated benchmark infrastructure
- **scripts/**: Deprecated utility scripts
- **Planned tool scaffolds**: Removed placeholder directories for butterfly-shrink, butterfly-extract, and butterfly-serve to focus on core functionality first

### Changed
- **Workspace structure**: Simplified to focus on production-ready butterfly-dl and butterfly-common foundation
- **Development focus**: Concentrating on core data acquisition tools before expanding to additional planned tools

## [2.0.0] - 2025-06-27

### 🌟 Major Milestone: Ecosystem Foundation

**Transformation from single-tool to ecosystem workspace**

### Added
- **🏗️ Workspace Architecture**: Multi-tool Rust workspace with shared components
- **📚 butterfly-common**: Shared library for error handling, geographic algorithms, and utilities
- **🤖 Automated Release Process**: Modern GitHub Actions with multi-platform builds (5 platforms)
- **🔒 Security**: Automatic checksums and integrity verification for all releases
- **📋 Tool Templates**: Standardized structure for future butterfly tools
- **🌍 Enhanced Geographic Intelligence**: Advanced fuzzy matching with semantic understanding
- **🎯 Project Roadmap**: Comprehensive development plan for ecosystem expansion
- **📊 CI Badge**: Added build status badge to README for transparency

### Changed
- **Repository Structure**: Organized as multi-tool workspace
- **Release Process**: Fully automated from tag push to published release (~4 minutes)
- **Performance**: Improved build times while maintaining runtime performance
- **Documentation**: Ecosystem-focused with tool-specific documentation

### Maintained
- **100% Backward Compatibility**: All v1.x APIs and CLI usage preserved
- **Performance**: Same runtime characteristics and memory efficiency
- **Features**: All existing functionality retained

### Performance
- **Build Efficiency**: Shared dependencies across tools
- **Release Speed**: 4-minute automated releases vs 30+ minute manual process
- **Platform Coverage**: 5 platforms (Linux x86_64/ARM64, macOS Intel/Apple Silicon, Windows x86_64)

---

## butterfly-dl Evolution (1.0.0 → 2.0.0)

*For detailed version history, see [butterfly-dl CHANGELOG](./tools/butterfly-dl/CHANGELOG.md)*

### Key Milestones

#### 🚀 **Performance Era** (1.4.x)
- Hurricane-fast downloads: **79% faster** than aria2, **3x faster** than curl
- Memory efficiency: **<1GB RAM** for any file size (including 81GB planet)
- Network resilience with intelligent retry and resume
- Beautiful progress displays with tqdm-style formatting

#### 🧠 **Intelligence Era** (1.2.x - 1.3.x)  
- Geographic-aware fuzzy matching: knows Belgium is in Europe, not Antarctica
- Dynamic source discovery from Geofabrik API
- Semantic error correction: "austrailia" → "australia-oceania" (not "austria")
- Real-time source updates, no hardcoded lists

#### 🏗️ **Architecture Era** (1.0.x - 1.1.x)
- Library + CLI architecture with C FFI bindings
- HTTP-only design for security and simplicity
- Smart connection scaling based on file size
- Comprehensive benchmarking against industry standards

#### 🛠️ **Foundation Era** (0.1.x)
- Multi-connection parallel downloads
- Docker-first development
- Convention over configuration approach
- Production-ready Geofabrik downloader

### Performance Achievements

| Metric | Achievement | Comparison |
|--------|-------------|------------|
| **Speed** | 14.07 MB/s | 79% faster than aria2 |
| **Memory** | <1GB fixed | 4-16x less than alternatives |
| **Reliability** | Smart resume | Network resilience with retry |
| **Intelligence** | Geographic fuzzy matching | Semantic understanding |

---

## Upcoming Tools

### 🔄 **Development Roadmap**

#### **Phase 2: Geometric Operations** 
- **butterfly-shrink**: Polygon-based extraction with GEOS integration
- Target: **10x faster** than osmium extract
- Memory limit: **<2GB** for planet-scale operations

#### **Phase 3: Data Transformation**
- **butterfly-extract**: Advanced filtering and transformation engine  
- Target: **5-10x faster** than osmosis
- Memory limit: **<1GB** for streaming operations

#### **Phase 4: High-Performance Serving**
- **butterfly-serve**: HTTP tile server with intelligent caching
- Target: **10-50x faster** QPS than existing solutions
- Memory limit: **<500MB** baseline + configurable caching

### 🎯 **Ecosystem Goals**

- **10x Performance**: Across all operations vs state-of-the-art
- **Minimal Memory**: Fixed memory usage regardless of data size  
- **Modern Architecture**: Rust's safety + async performance
- **Composable Design**: Unix philosophy applied to OSM processing

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for ecosystem development guidelines.

### Performance Standards
- All performance claims must be benchmarked
- Memory usage must be predictable and bounded
- Tools must compose via standard streams and formats

### Tool Development
- Each tool has a single, well-defined responsibility
- Shared functionality goes in butterfly-common
- Comprehensive test coverage including performance tests

---

**butterfly-osm** - Hurricane-fast OSM processing for the modern era.