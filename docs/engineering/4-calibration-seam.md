# Calibration seam

(Part of the [engineering guide](../ENGINEERING.md).)

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

**Graph stamp.** The table is keyed by OSM node pairs, so it would "load" onto
any graph and calibrate only the edges that survive. The producer stamps the
graph it was built on into the KV metadata (`graph_<key>`, e.g. `graph_pbf_sha`,
plus `gate=passed`); the operator names the loaded graph's identity in a
`key=value` text file (`BUTTERFLY_GRAPH_IDENTITY_FILE`). Every compared key
(`BUTTERFLY_SPEEDS_STAMP_KEYS`, default `pbf_sha,lu_pbf_sha`) the graph carries
must match, or the table is refused (clean base car, an error naming both
identities). `/health` reports `speeds` (`ok`, `mismatched`, `unstamped`,
`ungated`, `error`, `failed`, `unverified`, `unchecked`, `disabled`, `none`),
`speeds_detail` and `speeds_matched_fraction`; the `speeds_stamp` gate fails on
anything but `ok`, `none` and `disabled`. `BUTTERFLY_SPEEDS_STAMP_CHECK=off`
skips the check (tables whose producer has no notion of the graph).
