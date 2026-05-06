# Traffic recustomization smoke test (issue #84)

Belgium, 2026-05-06. Validates the step-8 traffic recustomization end-to-end:
density-class assigned at step 2 → speed factors applied at step 8 →
synthetic mode `<base>_<variant>` discovered at server boot → routing query
returns appropriately scaled durations.

## Setup

- Pipeline: existing step1-7 from `data/belgium/`, step2 regenerated as v2
  way_attrs (`data/belgium/step2-traffic/`), step8 traffic variants emitted
  to `data/belgium/step8-traffic/` and copied alongside the baseline into
  `data/belgium-traffic/step8/`.
- Profiles: `traffic/freeflow.traffic.json`, `traffic/offpeak.traffic.json`,
  `traffic/rush_hour.traffic.json`.
- Server: `butterfly-route serve --data-dir data/belgium-traffic --port 18800
  --transport rest --modes car`.

## Recustomization wall time (TIME-only metric, includes triangle relax)

| Profile     | Wall time |
|-------------|-----------|
| freeflow    | 43.4 s    |
| rush_hour   | 34.8 s    |
| offpeak     | 40.7 s    |

Baseline freeflow step 8 (TIME + DIST + relax for both): 61.9 s. Traffic
recustomization is ~30-40% faster because it skips the distance metric
(distance is physical, not affected by traffic).

The bottom-up customization itself runs in 0.5 s; the rest is triangle
relaxation (~30 s) and load+sanity. Triangle relaxation is **correctness-
critical**: skipping it produced a 5583 s / 77 km route for Brussels-Antwerp
instead of the correct 1947 s / 45 km — empirically validated and gated
behind a hidden `--skip-triangle-relax` development flag.

## Brussels → Antwerp (4.3517,50.8503 → 4.4017,51.2194)

| Mode / variant                | duration   | distance | Δ vs freeflow |
|-------------------------------|------------|----------|---------------|
| `mode=car` (baseline)         | 1946.7 s   | 45 513 m | —             |
| `mode=car&traffic=freeflow`   | 1946.7 s   | 45 513 m | 0.0%          |
| `mode=car_freeflow` (direct)  | 1946.7 s   | 45 513 m | 0.0%          |
| `mode=car&traffic=offpeak`    | 2081.7 s   | 45 897 m | +6.9%         |
| `mode=car_offpeak` (direct)   | 2081.7 s   | 45 897 m | +6.9%         |
| `mode=car&traffic=rush_hour`  | 2574.7 s   | 45 899 m | +32.3%        |
| `mode=car_rush_hour` (direct) | 2574.7 s   | 45 899 m | +32.3%        |

Validation:

- Rush-hour duration is **+32.3% longer than freeflow** — within the spec
  target of "30-50% longer".
- Both lookup paths (`?traffic=X` query parameter vs `mode=<base>_<variant>`)
  return identical durations and distances.
- Freeflow profile (factors all 1.0) reproduces the baseline duration and
  distance exactly — confirms zero-overhead correctness when factors are
  unity.
- Freeflow weight file is **byte-identical** (md5sum match) to the baseline
  `cch.w.car.u32`.
- Unknown traffic variant returns 400 with a clear error message:
  `{"error":"Unknown traffic variant 'does_not_exist' for mode 'car'. Build
  it with `step8-customize --traffic`."}`

## Files

- `brussels-antwerp.json` — raw smoke test output (durations + distances).

## How to reproduce

```bash
# 1. Build with density classifier
mkdir -p data/belgium/step2-traffic
butterfly-route step2-profile \
  --ways data/belgium/step1/ways.raw \
  --relations data/belgium/step1/relations.raw \
  --models-dir models \
  --outdir data/belgium/step2-traffic
# (--density-classifier osm-tag is the default)

# 2. Recustomize for each profile
mkdir -p data/belgium/step8-traffic
for prof in freeflow offpeak rush_hour; do
  butterfly-route step8-customize \
    --cch-topo    data/belgium/step7/cch.car.topo \
    --filtered-ebg data/belgium/step5/filtered.car.ebg \
    --order       data/belgium/step6/order.car.ebg \
    --weights     data/belgium/step5/w.car.u32 \
    --turns       data/belgium/step5/t.car.u32 \
    --ebg-nodes   data/belgium/step4/ebg.nodes \
    --way-attrs   data/belgium/step2-traffic/way_attrs.car.bin \
    --nbg-geo     data/belgium/step3/nbg.geo \
    --traffic     traffic/$prof.traffic.json \
    --mode        car \
    --outdir      data/belgium/step8-traffic
done

# 3. Boot the server with the unified data dir (baseline + variants in one step8/)
butterfly-route serve --data-dir data/belgium-traffic --port 18800 \
  --transport rest --modes car

# 4. Hit the routing endpoint
curl 'http://localhost:18800/route?src_lon=4.3517&src_lat=50.8503&dst_lon=4.4017&dst_lat=51.2194&mode=car&traffic=rush_hour'
```
