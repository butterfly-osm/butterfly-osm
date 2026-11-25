# Query surface

(Part of the [engineering guide](../ENGINEERING.md).)

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
| (`POST /isochrone/bulk`) | removed in #624 — batches are the Flight `isochrone` action |
| `POST /trip` | TSP / trip optimisation (nearest-neighbour + 2-opt + or-opt) |
| `POST /match` | map-match a GPS trace |
| `POST /catchment` | catchment hulls around stores from client points |
| `GET /transit` | multimodal journeys, single (batches: Flight `transit_bulk`; the REST `/transit/bulk` was removed in #624) |
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
