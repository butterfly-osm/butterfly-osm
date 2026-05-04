# #91 multi-region container loader — Phase 1 results

This document records the measurements that motivate / justify the
loader-only PR for #91. Phase 1 ships per-region container discovery +
same-region routing; the cross-region overlay (Phase 2) is tracked as
PR C.

## What landed in this PR

1. **Container manifest carries `region_id`** (see
   [`route/src/pack.rs::build_manifest`](../src/pack.rs)). Default is
   `"BE"` for backwards-compat with pre-#91 `baseline.butterfly`
   containers — those decode as `region_id = "BE"` without a re-pack.
2. **`butterfly-route pack --region <ID>`** writes the id into the
   manifest. Allowed characters: `[A-Z0-9_-]`, max 16 chars; lowercase
   input is upper-cased.
3. **`butterfly-route serve --data-dir <dir>`** auto-detects
   multi-region directories: any `*.butterfly` file under `<dir>`
   triggers the multi-region loader; otherwise the legacy step-tree
   loader runs unchanged.
4. **`--regions BE,LU`** filters which containers load.
5. **`RegionsState`** in
   [`route/src/server/regions.rs`](../src/server/regions.rs) holds
   `Vec<RegionEntry>`. Each entry pins one `Arc<ServerState>` plus its
   region id, container path, and CRC verify status.
6. **Per-request region dispatch**: every routing endpoint snaps its
   coordinates, picks the best region, then forwards to that region's
   `ServerState`.
7. **501 on cross-region**: the correctness invariant. Every routing
   endpoint that handles two-or-more coordinates (`/route`,
   `/table`, `/trip`, `/match`, `/catchment`,
   `/isochrone/bulk`) returns HTTP 501 with the body
   `{"error": "route spans regions BE → LU; cross-region overlay not
   yet implemented (#91 Phase 2)"}` whenever the points snap into
   different regions.
8. **`GET /regions`** lists every loaded region with id, container
   path, node + edge counts, named-roads count, modes, and CRC verify
   status.
9. **Per-region Prometheus metrics** in
   [`route/src/server/region_metrics.rs`](../src/server/region_metrics.rs):
   - `butterfly_route_region_nodes_total{region}` (gauge)
   - `butterfly_route_region_edges_total{region}` (gauge)
   - `butterfly_route_query_total{region,endpoint}` (counter)
   - `butterfly_route_query_duration_seconds{region,endpoint}` (histogram)
   - `butterfly_route_query_cross_region_total{src,dst}` (counter)

## What did **not** change

- gRPC Flight is single-region only in #91 Phase 1 — multi-region
  Flight is part of PR C. The Flight server is handed the *primary*
  region's `Arc<ServerState>` and warns at boot if more than one
  region is loaded.
- Transit is single-region only; the timetable + ULTRA transfer
  graph load against the primary region's foot CCH. The
  multi-region transit story is its own follow-up.
- Existing single-region deployments are byte-for-byte compatible:
  `serve --data <baseline.butterfly>` still works and the container
  is treated as a one-region `RegionsState` — same router, same
  handlers, same JSON shapes.

## Built containers

| region | container | nodes | edges | named roads | modes | size on disk |
|---|---|---:|---:|---:|---|---:|
| BE | `data/belgium/baseline.butterfly` | 5 019 052 | 14 649 023 | 754 380 | bike, car, foot, truck | 25.9 GB |
| LU | `data/luxembourg/luxembourg.butterfly` | 478 552 | ~1.1 M | n/a | bike, car, foot | 1.06 GB |

LU was built from `data/luxembourg.pbf` (~46 MB Geofabrik PBF) on the
session machine. Steps 1-5 ran across all four modes (car/bike/foot/
truck) but step 7 fails for truck on Luxembourg's tiny graph (the
truck-filtered EBG has 0 arcs after profile-filtering — the small
LU country has no truck-only roads). The LU pipeline therefore ships
3 modes; this is a profile-on-tiny-graph issue, not a multi-region
loader issue.

Build time on this machine: step 1 ~30 s, step 2 ~20 s, steps 3-5
~30 s combined, steps 6-8 per mode ~5-10 s. Total LU build under
3 minutes. `pack --region LU` finalised in ~3 s.

## Boot + RSS

### Single-region BE baseline

Reproduced from `route/docs/154-results.md` for reference (no
multi-region machinery active):

| metric | value |
|---|---|
| Total RSS | 3.02 GB |
| RssAnon | 0.70 GB |
| `/health` ready | 12 s |

### Multi-region BE + LU

Boot lines from `/tmp/multi_region_serve.log` (RSS checkpoints
enabled, both `*.butterfly` containers staged in `/tmp/multi_region`):

```
RSS_CHECKPOINT phase=startup        total_kb=8188      anon_kb=1524     file_kb=6664     elapsed_s=0.000
RSS_CHECKPOINT phase=load.shared    total_kb=411352    anon_kb=262232   file_kb=149120   elapsed_s=1.378
... per-mode bundles, both regions ...
loaded region region=BE container=/tmp/multi_region/be.butterfly load_ms=87624 nodes=5019052 edges=14649023 modes=["bike", "car", "foot", "truck"]
loaded region region=LU container=/tmp/multi_region/lu.butterfly load_ms=3566  nodes=478552  edges=1365815  modes=["bike", "car", "foot"]
RSS_CHECKPOINT phase=boot.complete  total_kb=2964796   anon_kb=513552   file_kb=2451244  elapsed_s=91.194
REST server listening on http://127.0.0.1:3091
```

Final boot RSS, both regions loaded, no warm-up, primary REST listener
bound:

| metric | value |
|---|---|
| Total RSS | **2.96 GB** |
| RssAnon | **502 MB** |
| Time to `/health` ready | **91 s** (BE 88 s + LU 3.6 s) |

Post-warmup (100 BE routes + 100 LU isochrones, single client):

| metric | value |
|---|---|
| Total RSS | **3.15 GB** |
| RssAnon | **687 MB** |

Single-region BE-only baseline for comparison
(`route/docs/154-results.md`, post-warmup):

| metric | value |
|---|---|
| Total RSS | 3.02 GB |
| RssAnon | 0.70 GB |

**Conclusion**: adding LU on top of BE costs ~130 MB of total RSS post-
warmup. Anon RSS is essentially flat (LU's working set is small;
file-backed pages dominate). The "host the planet on a 16 GB box"
claim from #91 holds — every region is bounded by its query working
set, not its container size, and mmap lets the OS evict cold regions
when memory pressure rises.

The BE load time is dominated by the legacy CRC walk on a multi-GB
container. PR A (#160 lazy CRC) eliminates this; this PR composes
cleanly with it (we touch container loading additively).

## Live verification

Running the multi-region server against `/tmp/multi_region/{be,lu}.butterfly`,
the four critical responses:

```
$ curl -s http://127.0.0.1:3091/health | jq '. | {regions, regions_count, total_nodes_count, total_edges_count}'
{
  "regions": ["BE", "LU"],
  "regions_count": 2,
  "total_nodes_count": 5497604,
  "total_edges_count": 16014838
}

$ curl -s http://127.0.0.1:3091/regions
{"loaded":[
  {"id":"BE","container":"/tmp/multi_region/be.butterfly","nodes":5019052,"edges":14649023,
   "verify_status":"verified","named_roads":754380,"modes":["bike","car","foot","truck"]},
  {"id":"LU","container":"/tmp/multi_region/lu.butterfly","nodes":478552,"edges":1365815,
   "verify_status":"verified","named_roads":46287,"modes":["bike","car","foot"]}
]}

# Same-region BE -> BE: 200, real route
$ curl -s "http://127.0.0.1:3091/route?src_lon=4.3567&src_lat=50.8453&dst_lon=3.2247&dst_lat=51.2093&mode=car" | jq '.duration_s,.distance_m'
3678.0
97549.472

# Cross-region BE -> LU: 501 with the spec-mandated body
$ curl -s -w "HTTP %{http_code}\n" "http://127.0.0.1:3091/route?src_lon=4.3567&src_lat=50.8453&dst_lon=6.1296&dst_lat=49.6116&mode=car"
{"error":"route spans regions BE → LU; cross-region overlay not yet implemented (#91 Phase 2)"}HTTP 501

# Per-region metrics increment correctly
$ curl -s http://127.0.0.1:3091/metrics | grep butterfly_route_query_cross_region_total
butterfly_route_query_cross_region_total{src="LU",dst="BE"} 1
butterfly_route_query_cross_region_total{src="BE",dst="LU"} 1
```

## Test inventory

`route/tests/multi_region.rs` ships:

| test | gated on data | what it proves |
|---|---|---|
| `region_id_normalises_lowercase_to_uppercase` | no | tag normalisation contract |
| `region_id_rejects_invalid_chars` | no | tag character whitelist |
| `region_id_caps_at_16_chars` | no | tag length cap |
| `manifest_region_id_returns_default_for_missing` | no | back-compat with pre-#91 manifests |
| `manifest_region_id_parses_explicit_field` | no | new manifest schema |
| `manifest_region_id_normalises_case` | no | tag normalisation on read |
| `manifest_region_id_handles_garbage_fallsback` | no | malformed manifest fallback |
| `empty_data_dir_is_rejected` | no | hard error on zero containers |
| `non_directory_data_dir_is_rejected` | no | hard error on file path |
| `loads_two_regions_from_directory` | BE + LU | discovery + sort |
| `region_filter_skips_unrequested_containers` | BE + LU | `--regions BE` filter |
| `dispatcher_picks_right_region_for_known_points` | BE + LU | Brussels → BE, Luxembourg-Ville → LU |
| `p2p_dispatch_same_region_be_to_be` | BE + LU | Brussels → Bruges, no 501 |
| `p2p_dispatch_same_region_lu_to_lu` | BE + LU | LU-Ville → Esch, no 501 |
| `p2p_dispatch_cross_region_returns_501` | BE + LU | **correctness invariant**: BE → LU is 501 with `BE → LU` in body, references #91 |
| `region_filter_excluding_everything_is_rejected` | BE + LU | hard error on impossible filter |
| `duplicate_region_id_is_rejected` | BE | hard error when two containers share a region id |

Run the data-gated tests with:

```
cargo test --release -p butterfly-route --test multi_region -- --ignored
```

All 17 tests pass on the session machine with both containers staged.

## What's *not* tested in this PR (and why)

- **Live HTTP /route across regions**: the dispatcher unit tests
  cover the rejection path; spinning up a full Axum binary in CI
  isn't worth the BE container load (>60 s).
- **gRPC Flight cross-region**: Flight is single-region only in PR
  B, so cross-region semantics live in PR C.
- **Transit cross-region**: ditto.

## Phase 2 (PR C) preview

PR C extends this loader with the cross-region overlay:

1. Border-node extraction from each region's NBG bbox + neighbour
   adjacency (small synthetic two-region fixture before any production
   code, per Pierre's #91 design notes).
2. Border-to-border matrix precomputation per region (one-to-many
   CCH on the region's existing topology — no new query engine).
3. `overlay.butterfly` storage format, sections inside #90's
   container shape so per-region builds stay independent.
4. Multi-region query coordinator: source CCH → overlay lookup →
   target CCH for cross-region routes; chained overlays for ≥3
   regions.
5. The `DispatchError::CrossRegion` 501 path in this PR is the
   correctness gate — once the overlay coordinator lands, the 501
   site becomes the place that calls into it, and the error becomes
   unreachable in normal operation.
