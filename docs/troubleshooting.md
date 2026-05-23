# Troubleshooting

FAQ-style. Each entry: what you see, why it happens, how to fix it.

If you are new to the server, read [Quickstart](quickstart.md) first. For endpoint shapes see the [API reference](api.md), for boot and ops see [Deployment](deployment.md), for the algorithms see [Architecture](architecture.md).

---

## Symptom: `/route` returns 400 `Could not snap source to road network`

The exact strings — `Could not snap source to road network`, `Could not snap destination to road network`, and (multi-region) `No road found within snap distance for source (lon, lat) mode=X` — all come out of the same root cause.

**Diagnosis**

- Coordinate is geometrically far from any sampled road vertex. The packed snap index walks an 8-cell ring around the query point with a hard `MAX_SNAP_DISTANCE_M = 5000.0` m cap (`route/src/server/snap_index.rs`). Points in water, dense forest, or on a building rooftop that is more than 5 km from any sampled vertex will not snap.
- Wrong region pack loaded. In multi-region mode every loaded region tries its snap; if none succeed you get `No road found within snap distance ...` from `regions.rs` — `DispatchError::NoRegion`. This usually means the container for that country was not mounted.
- Role-aware snap rejected the candidate for the requested role (#197). `/route` source needs an EBG node with at least one mode-valid outbound arc (`SnapRole::Src` → `has_outbound` bitset). Destination needs inbound. A point near a one-way slip road can be rejected as a source but accepted as a destination, or vice versa.
- Mode mask filters it out: a foot-only path is invisible to `mode=car`.

**Fix**

1. Hit `/health` and confirm the region you expect is loaded (`regions` list, `nodes_count`, `modes`). Also hit `/regions` for the per-region container path.
2. Try `/nearest?lon=...&lat=...&mode=car&number=5&radius=5000`. If `/nearest` returns nothing inside 5 km, the point is genuinely off-graph — the snap index will not find it no matter what you do.
3. Move the coordinate to the actual road centreline. Coordinates from address geocoders often land on building centroids; offset by ~20 m towards the street.
4. If `/nearest` succeeds but `/route` 400s, the snap candidate was rejected for the role. Try the same coordinate as the *other* endpoint (swap src and dst) — if that works, you are hitting role asymmetry. Use a coordinate closer to a two-way segment.

---

## Symptom: `/table` returns `null` for some pairs

The matrix endpoints return `null` (or `u32::MAX` in Flight) when no path exists between a pair.

**Diagnosis**

- Pre-#197 was 9% null rate on random Belgium-bbox coordinates: most of those were snap-traps in micro-SCCs (a dead-end driveway sampled into the index and the routing core unreachable from there). Post-#197 the K-best snap with connectivity-aware role masks dropped this to under 0.1%.
- True graph disconnect. Truck mode with truck-restricted base edges, or any mode whose mask carves the graph into multiple components, will legitimately return `null` between components — there is no route.
- Combo fallback gave up. `/route` and the matrix endpoints try up to `SNAP_K=64` source candidates × 64 destination candidates capped at 400 combos. If all 400 fail, the pair is null.

**Fix**

1. Sanity check with `mode=car` on the same pair. Car has the densest connectivity in the EBG; if car routes and truck does not, the truck base-edge mask is the cause. This is expected and correct.
2. Move the offending coordinate ~50 m towards a major road. Micro-SCC traps are a property of the snap *index*, not the road network.
3. Verify both coords pass `/nearest` independently. A null `/nearest` is a hard "off-graph" signal — no amount of K-best will rescue it.

---

## Symptom: First `/route?avoid_polygons=...` is slow (~1 second)

**Diagnosis**

This is expected. Avoid is implemented by sparse CCH recustomization (`route/src/server/avoid.rs`):

1. R-tree spatial query finds EBG edges whose midpoints fall inside the polygon.
2. Bottom-up + sparse triangle relaxation rebuilds the affected shortcut weights.
3. The recustomized weight set is cached keyed by `(mode, polygon_hash, exclude_mask)`.

Cache MISS on `/route` is ~780 ms for a tiny polygon (only the recustomization fixed cost) and ~1.2 s for a polygon covering a major motorway corridor like E19. Cache HIT is ~22 ms on `/table` and ~11 ms on `/route` — the recustomized weights are already in the LRU and reused across `/route`, `/table`, `/isochrone`, `/trip`.

**Fix**

- Don't fight it: subsequent calls with byte-identical polygon JSON hit the cache. JSON is canonicalised before hashing (vertex quantisation to 6 decimals, lex-minimum cyclic rotation per ring, polygon sort) so equivalent polygons hash to the same key — see `hash_polygon_json` in `avoid.rs`.
- Resize the cache via `BUTTERFLY_AVOID_CACHE_CAP` env var. Default is 8 entries, each ~100-200 MB on Belgium, so default ceiling is ~1.6 GB.
- `/health` surfaces `avoid_cache.hits`, `misses`, `hit_rate`, `size`, `capacity` per region. Watch hit_rate during canary; below 0.5 means callers are sending non-canonical or genuinely varied polygons.

---

## Symptom: `/route?avoid_polygons=...` takes 30+ seconds

**Diagnosis**

You are running pre-#240 code. The original avoid implementation did a full Belgium-wide bottom-up + parallel triangle relaxation on every request (~28 s on Belgium even for a tiny polygon — recorded in CLAUDE.md memory under "P-Sprint Architecture Notes"). The 16-30 s version reflects the original 28 s relaxation cost plus the cache populate.

The cache + sparse-relaxation work (#240, #242, #243) brought MISS down to ~780 ms — ~1.2 s. If you still see 30 s, you do not have those changes.

**Fix**

1. `git log --oneline route/src/server/avoid.rs` — confirm #240+ commits are present.
2. Rebuild the Docker image. Sparse relaxation is a code-path change, not a runtime flag.
3. If you are on current code and *still* seeing 30 s, the polygon hash table is thrashing — confirm `/health` shows non-zero `avoid_cache.size` after one call. Zero size means the insert is being immediately evicted (capacity 1 race), which is impossible with `DEFAULT_AVOID_CACHE_CAP=8` unless `BUTTERFLY_AVOID_CACHE_CAP=1` was set.

There is no design path where a small polygon takes 30 s on current code. If you reproduce it, it is a bug.

---

## Symptom: `/table/stream` takes minutes for 50k × 50k

**Diagnosis**

That's the design. On Belgium the May-22 bench measured 9.61 min wall for a 50 000 × 50 000 matrix on `/table/stream`. This is parity with the pre-Flight `/table/stream` numbers documented in CLAUDE.md ("Arrow Streaming - Large Scale"). The bench result is 4.4M distances/sec sustained at this scale.

A 50k × 50k matrix is 2.5 billion cells. At the documented throughput, anything below ~10 minutes would imply a regression somewhere else.

**Fix**

- Use the gRPC Flight `matrix` action instead of REST `/table/stream` if you are building bulk pipelines. Same algorithm, lower transport overhead (no JSON envelope, no HTTP/1 chunking). See [API reference](api.md) for the ticket shape.
- If wall time is materially worse than 10 min for 50k × 50k Belgium, profile RSS — eviction-thrashing during the streaming write is the usual culprit. The matrix algorithm itself is L3-tiled (#190); transport buffering is the only knob left.

---

## Symptom: 501 response with `route spans regions X → Y; cross-region overlay not yet implemented (#91 Phase 2)`

**Diagnosis**

The two endpoints snapped into *different* loaded regions, and the cross-region overlay is not yet built for this deployment. The HTTP status is actually **501 Not Implemented**, not 400 — `DispatchError::CrossRegion::into_response_parts` in `regions.rs`.

**Fix**

- If you only need one country, load only that container. The dispatcher cannot return `CrossRegion` from a single-region state.
- If you need multi-country routes, you need the cross-region overlay (`overlay.rs`). It is an opt-in feature; the loader sets `RegionsState::overlay = Some(...)`. Without it, every cross-region pair returns 501.
- Hit `/regions` first to confirm which region each coordinate snaps into. The dispatcher only ever returns 501 between pairs of region ids that appear in `/regions` — useful for narrowing down which side is mis-located.

---

## Symptom: 400 `No road found within snap distance for source (lon, lat) mode=X`

**Diagnosis**

The dispatcher tried every loaded region's snap for the requested mode and none returned a candidate. This is the multi-region equivalent of `Could not snap source to road network`, with the loaded-region list folded in.

**Fix**

- Hit `/regions` and check that the bbox you care about is actually covered. If you expected `LU` (Luxembourg) to be loaded and it isn't in the list, the container failed to load — check the boot logs.
- Verify the mode is loaded in *some* region: `/health` shows `modes` for the primary region, `/regions` shows `modes` per region. If your mode is missing everywhere you get `InvalidMode` with the available list — that's a different 400.
- Multi-region: add the missing container under your `--data-dir`. The discovery pass picks up any `*.butterfly` file in the directory. See [Deployment](deployment.md).

---

## Symptom: 400 `Invalid mode 'X'. Available across loaded regions: ...`

**Diagnosis**

The mode string is not the name of any loaded mode. Modes are discovered from the container's per-mode files; the lookup is case-insensitive (`s_lower` in `parse_mode` and `resolve_mode`).

**Fix**

- Use one of the modes in the error message. Standard packs ship `car`, `bike`, `foot`. Truck and traffic-variant modes (`car_rush_hour`, `car_offpeak`, `car_freeflow`) are present only when those weights were built into the container.
- The mode list is dynamic. Adding `truck` requires the build pipeline to have produced `way_attrs.truck.bin` and the rest of the per-mode files. See [Architecture](architecture.md) on the declarative model system.

---

## Symptom: `/transit` returns 404 `no transit journey found`

**Diagnosis**

Five candidate causes, in rough order of frequency:

1. Origin or destination is more than the per-mode access radius from any stop. Defaults (`default_access_params` in `transit_handler.rs`): foot 2 km / 20 stops, bike 8 km / 60 stops, car 30 km / 500 stops. Caller can override via `max_access_m`, `max_egress_m`, `max_access_stops`; legacy `max_walk_m` still works for foot. The error message identifies which side: `no transit stops within max_access_m (... m, mode=foot) of origin`.
2. Origin or destination snapped to the road network fine, but RAPTOR found no journey before the access/egress walk caps. Error: `no access stops reachable within walking time` or `no egress stops reachable within walking time`. Usually means the depart time is outside service hours.
3. Active calendar is empty for the request date. NeTEx-EPIP publications (STIB) can be weeks stale; the loader remaps today to the same weekday in the latest published period (`netex_epip::compute_active_day_types`) so weekday/weekend semantics survive, but if a feed has *no* active period at all, that operator's services vanish. GTFS is filtered by `ServiceFilter` and behaves the same way.
4. Feed not loaded. Check the boot log for `Loaded feed ...` lines. If `transit/transfers.bin` is missing, the server rebuilds the transfer graph (3 min cold on Belgium). If it exists but is stale (CCH fingerprint mismatch, feed hash mismatch, algo version drift) it is rejected and rebuilt.
5. Foot CCH not loaded. RAPTOR access and egress legs require the foot mode for transfers. If the container has no foot weights, the transit subsystem refuses to load.

**Fix**

1. Loosen `max_access_m` / `max_egress_m`. Foot to 5000 is reasonable for first-mile-last-mile experiments.
2. Try a depart time mid-morning on a weekday (`depart=08:00:00`) — eliminates calendar weirdness.
3. `ls data/<region>/transit/` should show a `gtfs/` directory with the feeds, optionally `netex/`, and `transfers.bin` after first boot.
4. If `transfers.bin` is constantly rebuilding, check that the CCH topology and foot weights are not changing across boots. The cache provenance hash includes them.

---

## Symptom: 503 `transit not loaded` or `timetable has zero stops`

**Diagnosis**

`/transit` and `/transit/bulk` return 503 when the server booted without transit (no `transit/` directory under `--data-dir`, or every feed failed to load). `timetable has zero stops` is a stronger variant — the feeds parsed but produced an empty timetable, which is almost always a calendar / service-id mismatch.

**Fix**

- Inspect the boot log. Each feed loader prints success / failure with the feed id.
- For NeTEx-EPIP, the most common failure is the streaming parser hitting an unexpected element. The file must be raw XML, not a zip wrapper.
- Empty timetable on a non-empty feed usually means `ServiceFilter` rejected every trip. Confirm the system clock — if the container thinks it's 2099, no service date matches.

---

## Symptom: Out of memory at boot

**Diagnosis**

Steady-state RSS on Belgium is ~24 GB across the road graph (5 M EBG nodes, named-roads HashMap, spatial index) and the transit subsystem (timetable + ULTRA transfer graph + R-tree stop index). If the host has less than ~32 GB total, there is no headroom for the avoid cache, request buffers, and OS page cache.

Boot peaks above steady-state: container open + per-section verify + ULTRA build all hold transient buffers.

**Fix**

- Drop `BUTTERFLY_AVOID_CACHE_CAP=4` to halve the avoid cache ceiling (8 → 4 entries, ~800 MB → ~400 MB).
- Drop transit feeds you don't need. The `transit/transfers.bin` build is the largest transient allocation; fewer feeds means a smaller graph.
- Skip transit entirely by removing the `data/<region>/transit/` directory. Boot drops from ~3 min to ~30 s and RSS drops by several gigabytes.
- For the bench tool only, `--src-tile-size` shrinks the matrix tiles; this does not apply to the server.

---

## Symptom: gRPC Flight client gets `"Ticket format: action:profile:params_json"`

**Diagnosis**

The Flight ticket parser in `flight.rs::parse_ticket` requires exactly two colons separating three fields:

```
action:profile:params_json
```

Common failure modes:

- Only one colon: `matrix:{"sources":...}` — missing the profile field. The parser returns `Status::invalid_argument("Ticket format: action:profile:params_json")` at the *first* colon search if there is no colon at all, or at the *second* colon search if there is exactly one.
- Three or more colons unescaped *outside* the JSON payload: rare in practice because the JSON body is matched as everything after the second colon, so colons inside JSON are fine.
- Bytes are not UTF-8: returns `Status::invalid_argument("Ticket must be UTF-8")`.

**Fix**

- Format the ticket as `matrix:car:{...}` — three fields, two colons, JSON last.
- Verify with a known-good action: `route_batch:car:{"pairs":[[src_lon,src_lat,dst_lon,dst_lat],...]}`. If that works, your `matrix` request is malformed; if it also fails, your colon count is wrong.

---

## Symptom: gRPC Flight client errors with `Connection refused`

**Diagnosis**

You are pointing the Flight client at the REST port. REST is **3001** (Axum), Flight is **3002** (tonic). They are separate listeners on separate ports — there is no transport mixing by design.

**Fix**

- Flight URI: `grpc://localhost:3002`. Not `grpc://localhost:3001`, not `http://localhost:3002`.
- `Server never sent a data message` from your gRPC client usually means the host is wrong (typo, missing port). Verify with `grpcurl -plaintext localhost:3002 list` — should list `arrow.flight.protocol.FlightService`.

---

## Symptom: gRPC Flight ticket returns `Unknown profile 'X'`

**Diagnosis**

`resolve_mode` in `flight.rs` does a case-insensitive lookup against `state.mode_lookup`. Same dynamic mode discovery as REST. The error message includes the sorted list of available modes.

**Fix**

- Use one of the modes from the error message.
- Profile is the second field of the ticket, not a header or query param. `matrix::{...}` (empty profile) will fail mode lookup with empty available list mismatch.

---

## Symptom: `/health` shows `verify_status=degraded`

**Diagnosis**

One or more per-section CRC verifications failed during lazy verification (`health_handler.rs`, #160). The `verify.failed` array lists the offending sections with `region`, `name`, `reason`. Boot-time eager-CRC failures cause the region to refuse to load entirely — `degraded` means a section was verified *after* the region was up, and that section's CRC did not match the manifest.

**Fix**

- Identify the failing section from the JSON. Reverify the source `.butterfly` container with `sha256sum` against your build records.
- The container is corrupted. Re-pack it from the step1-step8 directory tree, or re-fetch from your build artifact store.
- Hot-swap is not supported — restart the server with the rebuilt container.
- The Prometheus metric `butterfly_route_section_verify_failed_total{section=...}` counts these incidents. Alert on it.

---

## Symptom: `/health` shows `verify_status=pending` and never moves to `verified`

**Diagnosis**

Lazy verification only verifies sections as they are touched on the serve path. A section that no request has touched stays `Unverified`. Cold-boot `/health` will show `pending` until enough traffic has exercised every section.

**Fix**

- Run a small synthetic load that exercises every mode and every endpoint family (`/route`, `/table`, `/isochrone`, `/nearest`).
- Or accept that `pending` is a normal cold-start state and only alert on `degraded`.

---

## Symptom: Container boot takes 3+ minutes

**Diagnosis**

Transit transfer graph rebuild dominates. ULTRA preprocessing runs bounded multi-source Dijkstra over the foot CCH for every transit stop (66 512 stops on Belgium, ~668 k transfer edges), produces `transit/transfers.bin`, and verifies the provenance hash (CCH fingerprint + feed hash + algo version).

Once `transfers.bin` exists, subsequent boots reuse it: ~30 s instead of ~3 min.

**Fix**

- This is one-time cost; the cache is on disk. Don't blow it away.
- If you are iterating on the foot CCH (Step 6-8 weights changed), the provenance hash mismatches and ULTRA rebuilds. Expected.
- To bypass entirely, remove `data/<region>/transit/`. The server runs road-only and skips ULTRA. Boot is ~30 s.

---

## Symptom: `cargo build` fails with workspace error mentioning `geocode`

**Diagnosis**

The geocoder crate was shelved (#254). If your local checkout has a stale `Cargo.lock` referencing the removed crate, or you have a worktree pointing at an old branch, the workspace resolver fails on the missing member.

**Fix**

```bash
git pull origin main
cargo clean
cargo build --workspace
```

`cargo clean` is required because the lockfile-driven incremental cache holds onto the old member graph.

---

## Symptom: Health endpoint shows `avoid_cache.hit_rate` near zero

**Diagnosis**

Either callers are sending genuinely different polygons (every request unique), or polygon JSON is subtly varying (different coordinate precision, different vertex order, polygons in different orders). The canonicaliser in `hash_polygon_json` handles vertex quantisation to 6 decimals, Booth's lex-min cyclic rotation per ring, and polygon-set sort — but it does *not* paper over reversed winding or extra vertices.

**Fix**

- Have clients pin a stable polygon serialisation. Same vertex count, same winding, same JSON whitespace doesn't matter (the parser normalises) but the *vertex set* must match.
- Increase `BUTTERFLY_AVOID_CACHE_CAP` if the working set genuinely exceeds 8 distinct polygons. Memory cost is ~100-200 MB per entry.
- Per-region hit rate is broken out in the `/health` JSON — if one region is 0.95 and another is 0.05, the bad client is hitting only one region.

---

## Symptom: Logs are JSON and unreadable in `docker logs`

**Diagnosis**

Default log format is JSON for production. Configured via `--log-format` (see CLAUDE.md "Production Hardening").

**Fix**

```bash
docker run ... butterfly-route serve --data-dir /data --port 8080 --log-format text
```

---

## Still stuck?

- `RUST_LOG=debug` on the server, reproduce the failing request, and the structured log line will name the handler, the region (in multi-region mode), and any snap candidate counts.
- Compare against [API reference](api.md) for the exact expected request shape.
- For algorithm-level questions (why does this matrix cell return null, why is this isochrone shape weird) see [Architecture](architecture.md).
