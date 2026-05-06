# Issue #146 — Empirical sharing analysis (Belgium, 2026-05-06)

## TL;DR

**Honest finding from the structural analysis: only one mode pair (`car + truck`) is a
plausible candidate for topology bundling.** Every other pair fails even under an
optimistic linear-scaling cost model — and the linear model is known to under-predict
real bundle cost (issue #90 measured +7.3 GB for the 4-way bundle vs my linear model
predicting only +3.4 GB).

| pair        | arc Jaccard | node Jaccard | baseline | predicted bundled | predicted ratio | predicted disk pass? |
|-------------|------------:|-------------:|---------:|------------------:|----------------:|:---------------------|
| car + truck |      0.8954 |       0.9995 |  1.57 GB |          1.31 GB  |          0.833  | yes (likely true)    |
| bike + foot |      0.6244 |       0.9627 |  7.75 GB |          6.97 GB  |          0.900  | yes (marginal)       |
| bike + car  |      0.4148 |       0.5181 |  4.34 GB |          5.89 GB  |          1.358  | no                   |
| bike + truck|      0.3739 |       0.5178 |  4.30 GB |          5.87 GB  |          1.367  | no                   |
| car + foot  |      0.2652 |       0.5029 |  5.02 GB |          6.99 GB  |          1.392  | no                   |
| foot + truck|      0.2373 |       0.5026 |  4.98 GB |          6.99 GB  |          1.403  | no                   |

The 4-way `{bike, car, foot, truck}` bundle (the original #90 shape) is predicted at
+3.4 GB regression by the same model; #90's actual measurement was +7.3 GB. The
predicted ratio is therefore an **optimistic lower bound** on bundled cost. Pairs
with predicted ratio ≥ 1.0 reject; pairs with predicted ratio < 1.0 *may* pay off
but require a real rebuild to confirm.

## Methodology

Run `butterfly-route topology-diff --path <container>` against a packed Belgium
container. The tool loads, for each mode in the container:

1. The per-original-EBG-node accessibility mask (`mode/<m>/mask`, ~600 KB / mode).
2. The per-mode filtered EBG (`mode/<m>/filtered_ebg`, 75 MB – 194 MB / mode). Its
   `original_arc_idx` array enumerates the original-EBG arcs the mode can traverse.
3. The CCH topology header for the mode (first 80 bytes of `mode/<m>/topo`) for
   `n_nodes` / `n_shortcuts` / `n_original_arcs` and the section size.
4. The per-mode customised weight section sizes (`mode/<m>/weights.time` +
   `mode/<m>/weights.dist`).

For each unordered mode pair `(A, B)`:

- **Node Jaccard** = `popcount(mask_A & mask_B) / popcount(mask_A | mask_B)`.
- **Arc Jaccard** = `|S_A ∩ S_B| / |S_A ∪ S_B|`, where `S_X` is the set of unique
  values in `X`'s `original_arc_idx`.
- **Predicted bundled bytes**: with `large = mode-with-larger-arc-set` and
  `scale = arc_union / arc_count(large)`, the bundle holds
  `topology(scale * large.topo_bytes) + 4 * (scale * large.single_metric_weight_bytes)`
  (one shared topology + time + dist weights for each of the two bundled modes).
- **Predicted ratio** = `bundled / baseline`. < 1.0 ⇒ "predicted disk pass".

The linear-scaling assumption is documented as **optimistic** because:

1. CCH topology size is dominated by shortcut count, which is super-linear in graph
   size when ordering quality degrades. Larger union ⇒ worse ordering ⇒ more
   shortcuts than the linear projection.
2. Issue #90's empirical 4-way regression (+7.3 GB) is 2.1× worse than this model
   predicts (+3.4 GB), so the "+1 dB safety factor" is well-established.

## Raw output (Belgium, 2026-05-06)

```
$ ./target/release/butterfly-route topology-diff \
    --path data/belgium/baseline.butterfly

mode A         mode B             node J      arc J    baseline GB     bundled GB      ratio
bike           car                0.5181     0.4148          4.336          5.889     1.3583
bike           foot               0.9627     0.6244          7.750          6.972     0.8997 *
bike           truck              0.5178     0.3739          4.296          5.873     1.3672
car            foot               0.5029     0.2652          5.023          6.992     1.3920
car            truck              0.9995     0.8954          1.569          1.306     0.8328 *
foot           truck              0.5026     0.2373          4.983          6.990     1.4027

(* = predicted disk-acceptance pass under linear scaling model)
```

The full JSON report (per-pair node counts, arc counts, topology header values, and
predicted figures) is emitted to stdout. See `topology-diff-belgium.json` (committed
alongside this document).

## Belgium per-mode totals (for context)

| mode  | filt nodes | filt arcs | topo MB | w.time MB | w.dist MB | total MB |
|-------|-----------:|----------:|--------:|----------:|----------:|---------:|
| car   |   2 545 589|  3 911 326|     324 |       269 |       269 |      862 |
| truck |   2 274 368|  3 502 162|     306 |       257 |       257 |      820 |
| bike  |   4 633 460|  9 041 792|   1 338 |     1 226 |     1 226 |    3 790 |
| foot  |   4 902 334| 14 389 749|   1 589 |     1 468 |     1 468 |    4 525 |
| **sum**|         — |         —|   3 557 |     3 220 |     3 220 |   10 000 |

Observations:

- `car` and `truck` are essentially the same vehicle accessibility: 99.95% of car-
  reachable nodes are also truck-reachable and vice versa, and 89.5% of the per-
  mode arcs are shared (the 10% delta is exactly the truck-restriction edges —
  width/weight/height limits, residential streets, etc).
- `bike` and `foot` share 96% of nodes (both are pedestrian-style and reach the
  whole road network minus motorway-only segments) but only 62% of arcs. The arc
  asymmetry is real: bike-allowed cycle lanes and one-way streets that foot
  ignores; foot-allowed pedestrian areas, footways, stairs that bike refuses.
- `car` × `foot` and `car` × `bike` overlap heavily on the *node* set (≈ 50% — most
  drivable nodes are also pedestrian-reachable) but the *arc* set diverges sharply
  (≈ 27% / 41%). This is expected: turn restrictions and one-way rules are mode-
  specific. Sharing topology across these pairs would double-count most of foot's
  edge graph in car's contraction without recovering the cost.

## Per-pair acceptance prediction vs the four #146 conditions

Issue #146 mandates four acceptance criteria:

1. **Total disk smaller than baseline** ← this tool's `predicted_passes_disk_acceptance`
   field projects this. **Only `car+truck` and `bike+foot` predict pass; #146 mandates
   the actual rebuild for confirmation.**
2. **Step 6 + 7 wall-clock no slower than baseline** ← cannot be measured without an
   actual rebuild. Out of scope for this tool.
3. **No material query-latency regression (P50/P90 within ±5%)** ← cannot be
   measured without an actual rebuild + serve. Out of scope.
4. **No reconstruction step that defeats mmap** ← architectural; depends on how the
   pack/load path is plumbed. The container schema *already* supports it via the
   `bundles` field in `shared/manifest.json`, which today maps every mode to a
   singleton bundle (one mode per bundle id). A multi-mode bundle would emit the
   shared topology under a `bundle/<id>/topo` section and reference it from each
   bundled mode's manifest entry; the server would mmap the bundle's topology
   bytes once and pass them to every member mode's `ModeData`.

## Recommendation

Per #146's spirit ("ship the bundling only if the empirical numbers say yes; close
as falsified otherwise"), the structural analysis recommends the following next
steps **if** someone wants to push the experiment forward:

1. **Build a `car+truck` bundle by hand**. Run step5 with the union accessibility
   mask (car ∪ truck), then step6/7 once on the union, then step8 twice (one
   customisation pass per mode against the union topology). Pack as
   `bundle/car_truck/topo` + `mode/car/weights.{time,dist}` +
   `mode/truck/weights.{time,dist}` and update the manifest's `bundles` field to
   `{"car_truck": ["car", "truck"]}`. Re-run `topology-diff` on the rebuilt
   container; compare the actual size of `bundle/car_truck/topo + 4 weights` against
   the per-mode baseline of `1.57 GB`.
2. **If `car+truck` passes (1) — then run the full acceptance harness (2)+(3)+(4).**
   These require a working serve path that mmap-shares a bundle topology across
   member modes, which is itself a non-trivial change to `route/src/server/state.rs`
   that the current PR cannot scope.
3. **Skip `bike+foot`.** The predicted ratio is 0.90, only 10% under the per-mode
   baseline, and the linear model is known to be 2× optimistic on the 4-way bundle.
   The actual ratio for `bike+foot` is almost certainly ≥ 1.0 and ships as a disk
   regression. Don't run this rebuild.
4. **Skip every other pair.** All four predicted to fail at ratios 1.36–1.40, well
   above the 1.0 break-even, and the model is optimistic.

## What this PR ships

- `butterfly-route topology-diff` subcommand — the empirical analysis tool above.
  Reads the existing `mode/<m>/{mask,filtered_ebg,topo,weights.time,weights.dist}`
  sections; no new container schema, no rebuild.
- The `shared/manifest.json` `bundles` field is already in the on-disk schema (#90).
  Each mode currently maps to a singleton bundle. The bundle-build path that would
  emit a multi-mode `bundle/<id>/...` group lives in steps 5/6/7 of the build
  pipeline (`route/src/weights.rs`, `route/src/ordering.rs`,
  `route/src/contraction.rs`, `route/src/customization.rs`) — strictly out of scope
  for this PR's territory rules.
- A `manifest_bundles()` parser in `route/src/pack.rs` to read the `bundles` field
  back from a packed manifest. Forward-compatibility tested in
  `route/tests/topology_grouping.rs`.

## What this PR explicitly does NOT ship

- An actual `--topology-grouping shared` flag on `pack`. Implementing it
  honestly requires rebuilding step5/6/7 against the union mask and packing the
  result under `bundle/<id>/...`. That code lives in `weights.rs`/`ordering.rs`/
  `contraction.rs`/`customization.rs` — explicitly outside the territory rules
  for this branch.
- A shared-topology server load path. Same reason.
- Any RSS measurement on a "shared-topology Belgium container". The container
  doesn't exist yet, and shipping a fake one would make the RSS number meaningless.

The honest deliverable is the empirical evidence + a clear "yes for one pair, no
for everything else" recommendation. A follow-up PR with the right territory
(weights/ordering/contraction/customization) can pick up the `car+truck` rebuild
and run the four acceptance criteria for real.
