# Profile Regression Suite (PRS) - Mandatory After M4

Runs automatically after **every milestone** that touches extract, serve, graph, weights, or routing:

## Test Categories (All 3 Profiles: Car/Bike/Foot)

### Access Legality
* **Synthetic truth tables**: 100+ junction combinations per profile
* **No illegal edges**: Cars never on footways, bikes respect bicycle=no, foot respects access=private
* **Turn restrictions**: Profile-specific enforcement (car U-turn penalties vs bike/foot minimal)
* **Fail-fast reporter**: Print first illegal edge with tags + profile for immediate triage

### Snapping Quality  
* **Spatial recall**: ≥98% within 5m on 5k random points per region
* **Profile-appropriate selection**: Cycleway vs road vs footway based on mode
* **Heading tolerance**: ±35° GPS heading alignment where available

### Routing Legality & Plausibility
* **Route validation**: 50 curated routes per profile per region  
* **Legal edges only**: No profile violations in computed paths
* **ETA plausibility**: Bike never faster than car on motorway segments; foot times monotonic
* **Time parity**: |Time Graph ETA - Nav Graph ETA| ≤ 0.5s always

### Performance Regression
* **Build thresholds**: Time +25%, RSS +20%, spill volume +60% vs baseline
* **Serve thresholds**: p2p CCH +15%, matrices/isochrones +20% vs baseline  
* **Quality gates**: Hausdorff p95 ≤5m, zero turn restriction violations

## PRS Evolution
* **v1 (M4)**: Basic access + echo routing + forbidden-edge reporter
* **v2 (M5)**: + Snap quality + geometry validation + cold-IO test
* **v3 (M6)**: + ETA plausibility + turn legality
* **v4 (M7)**: + Parallel scaling + cache efficiency
* **v5 (M8)**: + CCH correctness + performance SLA
* **v6+ (M9-M20)**: + Feature-specific correctness and performance validation

**All PRS versions are cumulative** - later versions include all previous tests plus new ones.