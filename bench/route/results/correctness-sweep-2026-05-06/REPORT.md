# Route Correctness Sweep -- Belgium, butterfly-route vs OSRM

**Date:** 2026-05-06

**Engines under test:**
- butterfly-route (post #189 / d5faa99 main, edge-based CCH, single-region BE container)
- OSRM v5.26.0 CH (latest osrm-backend docker image)

**Dataset:** Belgium, single region, baseline.butterfly
**Pairs:** 10,000 randomly generated origin-destination pairs.
**Modes:** car, bike, foot

**Pair generation:**
- Both endpoints sampled uniformly from BE bounding box [49.55, 51.45]N x [2.65, 6.35]E.
- Both endpoints snapped via butterfly's `/nearest?mode=car`. Pairs rejected when snap distance > 100m.
- Great-circle distance bounded to [500m, 250km] (rejects degenerate-short and out-of-region pairs).
- Same 10,000 snapped pairs used for all three modes (`bench/route/results/correctness-sweep-2026-05-06/be-pairs.tsv`).

**Tolerance per pair:**
- Distance: within 0.5%, or 50m absolute (whichever is larger)
- Duration: within 1%,   or 5s absolute (whichever is larger)

## Methodology caveat -- speed-profile bias

OSRM and butterfly use different speed profiles by design.

| road class | OSRM `car.lua` | butterfly `models/car.model.json` |
|------------|----------------|-----------------------------------|
| motorway   | 90 km/h * 0.8 = **72** | **110** |
| trunk      | 85 * 0.8 = **68** | **90** |
| primary    | 65 * 0.8 = **52** | **70** |
| secondary  | 55 * 0.8 = **44** | **60** |

(OSRM `car.lua` applies a global `speed_reduction = 0.8` multiplier on top of the table speeds, modelling traffic.)

This produces a systematic per-mode duration bias that no routing algorithm can close -- it is a profile-tuning question, not a correctness question. We therefore report **two** duration metrics:

1. **Raw duration disagreement** -- engines compared with their default profiles. Useful for production-truth comparisons but dominated by speed-profile choice.
2. **Bias-corrected duration disagreement** -- butterfly durations rescaled by the per-mode aggregate ratio `sum(OSRM_duration) / sum(butterfly_duration)`. Isolates **do they agree on which roads to take** from **do they use the same speed table**. The bias-corrected number is the true correctness signal for the routing algorithm.

Distance disagreement is profile-independent and is reported as-is.

## Per-mode summary (raw, default profiles)

| mode | n_pairs_both_valid | dist_p50 % | dist_p95 % | dist_p99 % | dist_max % | dur_p50 % | dur_p95 % | dur_p99 % | flagged_dist>5% | flagged_dur>5% |
|------|-------------------:|-----------:|-----------:|-----------:|-----------:|----------:|----------:|----------:|----------------:|----------------:|
| car | 8437 | 1.88 | 13.18 | 24.88 | 229.39 | 14.71 | 21.78 | 26.46 | 1911 | 8082 |
| bike | 10000 | 0.95 | 4.46 | 9.87 | 183.09 | 20.32 | 24.46 | 26.70 | 399 | 9991 |
| foot | 10000 | 1.56 | 4.33 | 8.88 | 82.94 | 10.48 | 13.36 | 16.72 | 340 | 9926 |

## Per-mode summary (bias-corrected -- true correctness signal)

Butterfly durations rescaled by per-mode aggregate ratio so total duration matches OSRM.

| mode | speed_bias (OSRM/BF) | dist_p50 % | dist+corr_dur within tol | dur_corr_p50 % | dur_corr_p95 % | dur_corr_p99 % |
|------|---------------------:|-----------:|-------------------------:|---------------:|---------------:|---------------:|
| car | 1.1660 | 1.88 | 5.12% | 3.02 | 12.64 | 21.32 |
| bike | 1.2542 | 0.95 | 6.82% | 2.36 | 6.58 | 11.08 |
| foot | 0.9050 | 1.56 | 5.07% | 1.02 | 3.55 | 6.94 |

## Distance disagreement distribution (cumulative)

Percentage of pairs whose distance disagreement is at most X% of OSRM distance.

| mode | <=0.5% | <=1% | <=2% | <=5% | <=10% | <=25% | <=50% | <=100% |
|------|-------:|-----:|-----:|-----:|------:|------:|------:|-------:|
| car | 21.29% | 35.04% | 51.52% | 77.35% | 91.73% | 99.00% | 99.94% | 99.96% |
| bike | 29.37% | 51.93% | 78.49% | 96.01% | 99.01% | 99.77% | 99.94% | 99.97% |
| foot | 12.76% | 28.74% | 64.56% | 96.60% | 99.20% | 99.89% | 99.96% | 100.00% |

## Mode `car` -- full breakdown

- pairs total: **10000**
- both engines succeeded: **8437** (84.37%)
- OSRM-only success: 1563
- butterfly-only success: 0
- both failed: 0

**Distance disagreement** (% of OSRM):
- p50 1.879 %  |  p95 13.180 %  |  p99 24.877 %  |  max 229.394 %  |  mean 3.687 %

**Duration disagreement, raw** (% of OSRM):
- p50 14.711 %  |  p95 21.784 %  |  p99 26.458 %  |  max 131.533 %  |  mean 14.486 %

**Duration disagreement, bias-corrected** (% of OSRM):
- p50 3.023 %  |  p95 12.644 %  |  p99 21.322 %  |  max 169.978 %  |  mean 4.340 %

**Tolerance pass-rate**:
- distance only (within 0.5% / 50m):                 1796/8437 (21.29%)
- duration raw only (within 1% / 5s):                49/8437 (0.58%)
- duration bias-corrected only (within 1% / 5s):     1571/8437 (18.62%)
- BOTH raw distance and bias-corrected duration:     5.12%
- BOTH raw distance and raw duration:                0.07%

**Aggregate totals (sanity check):**
- OSRM:      1048169 km, 13349 h
- butterfly: 1039087 km, 11448 h
- bias (butterfly relative to OSRM): distance -0.866%, duration -14.240%

## Mode `bike` -- full breakdown

- pairs total: **10000**
- both engines succeeded: **10000** (100.00%)
- OSRM-only success: 0
- butterfly-only success: 0
- both failed: 0

**Distance disagreement** (% of OSRM):
- p50 0.950 %  |  p95 4.462 %  |  p99 9.866 %  |  max 183.088 %  |  mean 1.555 %

**Duration disagreement, raw** (% of OSRM):
- p50 20.320 %  |  p95 24.458 %  |  p99 26.701 %  |  max 97.655 %  |  mean 20.282 %

**Duration disagreement, bias-corrected** (% of OSRM):
- p50 2.360 %  |  p95 6.578 %  |  p99 11.083 %  |  max 147.904 %  |  mean 2.824 %

**Tolerance pass-rate**:
- distance only (within 0.5% / 50m):                 2937/10000 (29.37%)
- duration raw only (within 1% / 5s):                0/10000 (0.00%)
- duration bias-corrected only (within 1% / 5s):     2022/10000 (20.22%)
- BOTH raw distance and bias-corrected duration:     6.82%
- BOTH raw distance and raw duration:                0.00%

**Aggregate totals (sanity check):**
- OSRM:      1110882 km, 76423 h
- butterfly: 1116472 km, 60933 h
- bias (butterfly relative to OSRM): distance +0.503%, duration -20.270%

## Mode `foot` -- full breakdown

- pairs total: **10000**
- both engines succeeded: **10000** (100.00%)
- OSRM-only success: 0
- butterfly-only success: 0
- both failed: 0

**Distance disagreement** (% of OSRM):
- p50 1.562 %  |  p95 4.328 %  |  p99 8.881 %  |  max 82.941 %  |  mean 1.910 %

**Duration disagreement, raw** (% of OSRM):
- p50 10.479 %  |  p95 13.361 %  |  p99 16.720 %  |  max 88.601 %  |  mean 10.470 %

**Duration disagreement, bias-corrected** (% of OSRM):
- p50 1.017 %  |  p95 3.546 %  |  p99 6.937 %  |  max 70.692 %  |  mean 1.372 %

**Tolerance pass-rate**:
- distance only (within 0.5% / 50m):                 1276/10000 (12.76%)
- duration raw only (within 1% / 5s):                13/10000 (0.13%)
- duration bias-corrected only (within 1% / 5s):     4950/10000 (49.50%)
- BOTH raw distance and bias-corrected duration:     5.07%
- BOTH raw distance and raw duration:                0.00%

**Aggregate totals (sanity check):**
- OSRM:      1089676 km, 218387 h
- butterfly: 1106219 km, 241300 h
- bias (butterfly relative to OSRM): distance +1.518%, duration +10.492%

## Top-20 disagreement cases (per mode)

Ranked by max(distance_pct_diff, duration_pct_diff). Use the lat/lon to manually inspect on https://www.openstreetmap.org or another reference engine.

### car

| rank | src (lon,lat) | dst (lon,lat) | OSRM d (m) | BF d (m) | dist % | OSRM t (s) | BF t (s) | dur % |
|-----:|---------------|---------------|-----------:|---------:|-------:|-----------:|---------:|------:|
| 1 | 6.16108,50.22786 | 6.14064,50.20259 | 3638 | 11983 | 229.39 | 426 | 857 | 101.08 |
| 2 | 4.92691,50.15777 | 4.91485,50.15182 | 1108 | 3138 | 183.09 | 80 | 184 | 131.53 |
| 3 | 3.77009,51.09573 | 3.71416,51.11008 | 5813 | 11807 | 103.13 | 797 | 771 | 3.29 |
| 4 | 5.19465,51.12336 | 5.12716,51.12360 | 5877 | 9740 | 65.75 | 744 | 755 | 1.42 |
| 5 | 5.64513,51.17507 | 5.57810,51.16346 | 6422 | 10023 | 56.07 | 697 | 726 | 4.12 |
| 6 | 5.64964,50.88816 | 5.54619,50.87792 | 9860 | 14664 | 48.72 | 854 | 858 | 0.50 |
| 7 | 2.84349,50.77260 | 2.80000,50.78875 | 4672 | 4903 | 4.95 | 553 | 293 | 47.01 |
| 8 | 4.29572,50.21828 | 4.29005,50.22307 | 1574 | 2292 | 45.69 | 232 | 247 | 6.60 |
| 9 | 4.25893,50.78785 | 4.40880,50.86617 | 19397 | 17866 | 7.89 | 1838 | 1057 | 42.50 |
| 10 | 6.04401,50.63526 | 6.00830,50.33929 | 42820 | 60378 | 41.01 | 2544 | 2457 | 3.40 |
| 11 | 4.86838,51.11832 | 4.83527,51.12949 | 3525 | 4930 | 39.87 | 367 | 432 | 17.74 |
| 12 | 5.01509,50.38508 | 6.25531,50.26589 | 118923 | 165153 | 38.87 | 6771 | 6058 | 10.52 |
| 13 | 4.32896,50.84095 | 4.15936,51.02105 | 31743 | 29530 | 6.97 | 2687 | 1661 | 38.19 |
| 14 | 2.81966,51.09806 | 3.14701,50.92181 | 63590 | 39452 | 37.96 | 2693 | 2084 | 22.61 |
| 15 | 3.91275,50.32980 | 4.07317,50.94403 | 83000 | 114464 | 37.91 | 5330 | 4394 | 17.56 |
| 16 | 5.65455,50.67753 | 5.12816,50.28664 | 74228 | 102344 | 37.88 | 3943 | 3614 | 8.35 |
| 17 | 4.97184,50.39658 | 5.58159,50.57197 | 68082 | 93607 | 37.49 | 3681 | 3370 | 8.45 |
| 18 | 4.88654,49.85704 | 5.66519,50.60895 | 124063 | 170382 | 37.33 | 7054 | 6839 | 3.04 |
| 19 | 5.37246,50.53896 | 5.58372,50.55434 | 21286 | 29225 | 37.30 | 1539 | 1670 | 8.53 |
| 20 | 2.66379,51.06550 | 3.15482,51.01408 | 64468 | 40479 | 37.21 | 2607 | 2166 | 16.91 |

### bike

| rank | src (lon,lat) | dst (lon,lat) | OSRM d (m) | BF d (m) | dist % | OSRM t (s) | BF t (s) | dur % |
|-----:|---------------|---------------|-----------:|---------:|-------:|-----------:|---------:|------:|
| 1 | 4.92691,50.15777 | 4.91485,50.15182 | 1108 | 3138 | 183.09 | 269 | 531 | 97.66 |
| 2 | 4.29572,50.21828 | 4.29005,50.22307 | 945 | 2292 | 142.64 | 254 | 408 | 60.84 |
| 3 | 4.23310,51.15940 | 4.36205,51.14450 | 13064 | 26155 | 100.20 | 3439 | 5031 | 46.30 |
| 4 | 6.16108,50.22786 | 6.14064,50.20259 | 3638 | 7010 | 92.69 | 881 | 1411 | 60.27 |
| 5 | 3.77009,51.09573 | 3.71416,51.11008 | 5629 | 10019 | 78.00 | 1522 | 2004 | 31.67 |
| 6 | 3.89198,50.63751 | 3.87395,50.65102 | 2328 | 3521 | 51.24 | 576 | 707 | 22.67 |
| 7 | 5.22053,51.04735 | 5.21076,51.05560 | 1276 | 1795 | 40.70 | 341 | 396 | 16.12 |
| 8 | 5.19465,51.12336 | 5.12716,51.12360 | 5725 | 7843 | 37.01 | 1384 | 1606 | 16.07 |
| 9 | 4.39903,50.68915 | 4.37416,50.67995 | 2801 | 2855 | 1.94 | 839 | 529 | 36.92 |
| 10 | 4.07498,50.52912 | 4.08645,50.55170 | 3096 | 4194 | 35.45 | 753 | 835 | 10.89 |
| 11 | 5.09269,51.21479 | 5.07890,51.26284 | 8338 | 6841 | 17.95 | 2074 | 1346 | 35.09 |
| 12 | 5.38094,51.07263 | 5.27783,51.10469 | 14438 | 10464 | 27.52 | 3566 | 2316 | 35.05 |
| 13 | 4.71636,51.38553 | 4.28807,51.31141 | 46015 | 38623 | 16.06 | 11394 | 7476 | 34.38 |
| 14 | 4.28848,50.84622 | 4.41870,50.89874 | 12848 | 12198 | 5.06 | 3630 | 2412 | 33.54 |
| 15 | 5.58387,49.72636 | 5.63569,49.69639 | 6054 | 8038 | 32.75 | 1473 | 1205 | 18.20 |
| 16 | 5.70996,50.69164 | 5.48826,50.65862 | 23322 | 20543 | 11.91 | 6034 | 4091 | 32.21 |
| 17 | 5.22215,50.67978 | 5.13521,50.67288 | 6785 | 8958 | 32.01 | 1650 | 1804 | 9.31 |
| 18 | 4.62553,51.21177 | 4.36722,51.18980 | 21744 | 21113 | 2.90 | 5661 | 3913 | 30.88 |
| 19 | 4.35610,50.95112 | 4.38265,50.70653 | 32358 | 31719 | 1.98 | 8722 | 6040 | 30.75 |
| 20 | 4.33158,50.68891 | 4.40369,50.74222 | 10313 | 9811 | 4.87 | 2798 | 1941 | 30.64 |

### foot

| rank | src (lon,lat) | dst (lon,lat) | OSRM d (m) | BF d (m) | dist % | OSRM t (s) | BF t (s) | dur % |
|-----:|---------------|---------------|-----------:|---------:|-------:|-----------:|---------:|------:|
| 1 | 4.23310,51.15940 | 4.36205,51.14450 | 12364 | 22618 | 82.94 | 8922 | 16828 | 88.60 |
| 2 | 4.29572,50.21828 | 4.29005,50.22307 | 945 | 1666 | 76.33 | 680 | 1052 | 54.72 |
| 3 | 4.92691,50.15777 | 4.91485,50.15182 | 1108 | 1870 | 68.70 | 798 | 998 | 25.06 |
| 4 | 6.16108,50.22786 | 6.14064,50.20259 | 3638 | 5900 | 62.17 | 2620 | 3857 | 47.22 |
| 5 | 5.22053,51.04735 | 5.21076,51.05560 | 1264 | 1780 | 40.80 | 914 | 1418 | 55.06 |
| 6 | 3.86659,51.14732 | 4.24679,51.38500 | 57414 | 71534 | 24.59 | 41369 | 56446 | 36.45 |
| 7 | 5.58387,49.72636 | 5.63569,49.69639 | 5999 | 8031 | 33.88 | 4319 | 4681 | 8.39 |
| 8 | 4.31355,51.16398 | 4.30671,50.98528 | 23462 | 29902 | 27.45 | 16962 | 22640 | 33.47 |
| 9 | 3.89198,50.63751 | 3.87395,50.65102 | 2328 | 2968 | 27.48 | 1677 | 2213 | 31.98 |
| 10 | 4.68783,50.94566 | 4.30245,51.13560 | 40426 | 48653 | 20.35 | 29117 | 38101 | 30.85 |
| 11 | 3.66918,50.87303 | 4.28240,51.33362 | 77806 | 92750 | 19.21 | 56068 | 73352 | 30.83 |
| 12 | 4.07498,50.52912 | 4.08645,50.55170 | 3104 | 3704 | 19.34 | 2235 | 2903 | 29.90 |
| 13 | 4.18308,50.41766 | 4.20671,50.43232 | 3023 | 3551 | 17.48 | 2177 | 2822 | 29.66 |
| 14 | 4.10454,50.79717 | 4.15638,50.88769 | 12323 | 14872 | 20.69 | 8889 | 11484 | 29.19 |
| 15 | 4.21776,50.86786 | 4.18133,51.05763 | 25245 | 29978 | 18.75 | 18203 | 23460 | 28.88 |
| 16 | 3.48918,50.62383 | 3.45717,50.66833 | 8071 | 10306 | 27.69 | 5812 | 6529 | 12.33 |
| 17 | 5.41555,50.10696 | 6.15543,50.39831 | 71346 | 83826 | 17.49 | 51586 | 65528 | 27.03 |
| 18 | 3.73337,50.66907 | 3.71449,50.70220 | 4697 | 5966 | 27.02 | 3382 | 4032 | 19.20 |
| 19 | 4.26764,51.14714 | 4.30338,51.04796 | 14736 | 18497 | 25.52 | 10617 | 13459 | 26.77 |
| 20 | 4.73537,50.61110 | 4.69959,50.64123 | 5745 | 6976 | 21.44 | 4136 | 5235 | 26.56 |

## Findings

### F1. Speed-profile divergence dominates raw duration agreement

Default OSRM v5.26 `car.lua` and butterfly `models/car.model.json` differ by ~17% on aggregate duration for car (OSRM/BF = 1.166), ~25% for bike (1.254), and butterfly is slower than OSRM for foot (OSRM/BF = 0.905). This is a TUNABLE parameter, not an algorithmic property. After bias correction the engines agree on bike at p50=2.36% and foot at p50=1.02%. Recommend either:
- Tune `models/car.model.json` to match OSRM defaults for compatibility-mode workloads.
- Document the divergence and offer a `--profile osrm-compat` switch.

### F2. butterfly returns 'no route' on 15.6% of car pairs that OSRM successfully routes

On the car sweep, 1,563 of 10,000 pairs returned 404 'No route found' from butterfly while OSRM successfully routed both directions. Investigation:

- Sampled 50 unique unroutable destinations -- only 2 (4%) are isolated from a Brussels-centred probe (true small-SCC). The remaining 96% are routable in **at least one direction** to/from Brussels.
- Concretely: pair `(4.4579,51.2697) -> (5.7803,49.7259)` returns 404 from butterfly. Butterfly successfully routes `dst -> brussels` (192km, 200 OK) but `brussels -> dst` returns 404. This is **directional asymmetry**: the destination snap point is reachable backward but not forward, or vice versa.
- The bike (0/10000) and foot (0/10000) sweeps have ZERO no-route failures on the same coordinates, because their graphs include reverse-undirected pedestrian/bike paths that bypass the issue.

**Conclusion:** butterfly's `/nearest` snaps to the geometrically closest EBG node without considering whether that node is FORWARD-reachable (for src) or BACKWARD-reachable (for dst). OSRM's snapping handles both endpoints of an undirected edge and picks the side matching the role.

This is a genuine snap-quality bug worth filing as a GitHub issue. It costs ~15.6% of long-distance car pairs.

### F3. Distance agreement on routable pairs is reasonable but not tight

Among pairs where both engines route successfully, distance disagreement distributions:
- `car` p50/p95/p99/max: 1.88% / 13.18% / 24.88% / 229.39%
- `bike` p50/p95/p99/max: 0.95% / 4.46% / 9.87% / 183.09%
- `foot` p50/p95/p99/max: 1.56% / 4.33% / 8.88% / 82.94%

p50 distance differences of 1-2% indicate the engines pick **subtly different routes** on roughly half the queries -- typically choosing different motorway exits, alternates, or via roads. This is partly due to the speed-profile differences (favouring different road classes) and partly due to turn-cost model differences. Not a correctness defect; a tuning gap.

Long-tail distance differences (>50%, ~0.06% of car pairs) correspond to genuine routing-graph divergences, often where one engine takes a far longer detour. These rare cases are worth inspecting individually (see Top-20 tables above).

## Honest characterisation per mode

### `car`

- **5.12%** of pairs agree on both distance (within 0.5% / 50m) AND bias-corrected duration (within 1% / 5s).
- Raw raw-tolerance match: 0.07% (skewed by speed-profile bias).
- Speed-profile bias (OSRM/butterfly aggregate-duration ratio): **1.166**.
- Distance bias (butterfly total / OSRM total - 1): -0.866%.
- butterfly returned 'no route' on **1563/10000 (15.63%)** pairs that OSRM successfully routed.
- Distance disagreement: p50=1.88%, p95=13.18%, p99=24.88%, max=229.39%.
- Bias-corrected duration disagreement: p50=3.02%, p95=12.64%, p99=21.32%, max=169.98%.

### `bike`

- **6.82%** of pairs agree on both distance (within 0.5% / 50m) AND bias-corrected duration (within 1% / 5s).
- Raw raw-tolerance match: 0.00% (skewed by speed-profile bias).
- Speed-profile bias (OSRM/butterfly aggregate-duration ratio): **1.254**.
- Distance bias (butterfly total / OSRM total - 1): +0.503%.
- butterfly returned 'no route' on **0/10000 (0.00%)** pairs that OSRM successfully routed.
- Distance disagreement: p50=0.95%, p95=4.46%, p99=9.87%, max=183.09%.
- Bias-corrected duration disagreement: p50=2.36%, p95=6.58%, p99=11.08%, max=147.90%.

### `foot`

- **5.07%** of pairs agree on both distance (within 0.5% / 50m) AND bias-corrected duration (within 1% / 5s).
- Raw raw-tolerance match: 0.00% (skewed by speed-profile bias).
- Speed-profile bias (OSRM/butterfly aggregate-duration ratio): **0.905**.
- Distance bias (butterfly total / OSRM total - 1): +1.518%.
- butterfly returned 'no route' on **0/10000 (0.00%)** pairs that OSRM successfully routed.
- Distance disagreement: p50=1.56%, p95=4.33%, p99=8.88%, max=82.94%.
- Bias-corrected duration disagreement: p50=1.02%, p95=3.55%, p99=6.94%, max=70.69%.

## Verdict

**FAIL.** No mode meets the 99.5% within-tolerance floor when both distance (0.5% / 50m) AND bias-corrected duration (1% / 5s) are required simultaneously.

Root causes (in order of impact):

1. **Speed-profile divergence** (F1) -- accounts for the 14-25% raw duration drift. Tunable, not algorithmic.
2. **Snap directional asymmetry** (F2) -- accounts for 15.6% car pairs returning 404. **Genuine bug**, file as GitHub issue.
3. **Routing-decision divergence on long routes** (F3) -- p50 ~1-2% distance gap; partly explained by F1 (different road-class preferences), partly by turn-cost model differences. Investigate the Top-20 tables.

**However, after isolating routing-decision agreement from profile choice and snap quality:**
- `car` bias-corrected duration agreement (alone): **18.62%** within 1%/5s
- `bike` bias-corrected duration agreement (alone): **20.22%** within 1%/5s
- `foot` bias-corrected duration agreement (alone): **49.50%** within 1%/5s

These bias-corrected duration agreement rates are the meaningful correctness numbers for the routing algorithm itself.
