# M1 — Telemetry & Adaptive Planning (5 micro-milestones)

## M1.1 — PBF Reader & Tag Sieve
**Why**: OSM streaming with routing-relevant filtering
**Artifacts**: PBF parser, tag truth tables, sieve unit tests
**Commit**: `"M1.1: PBF streaming + sieve"`

## M1.2 — Density Tiles
**Why**: Spatial metrics for adaptive planning
**Artifacts**: 125m tile grid, junction/length/curvature metrics with percentiles (P15/P50/P85/P99), `telemetry.json`
**Usage**: M5 geometry passes consume percentiles for epsilon spacing
**Commit**: `"M1.2: tile telemetry + percentiles"`

## M1.3 — `/telemetry` Endpoint
**Why**: Debugging spatial density distribution
**Artifacts**: REST API with bbox filtering, schema validation
**Commit**: `"M1.3: /telemetry"`

## M1.4 — Telemetry → BuildPlan
**Why**: Upgrade from fixed to adaptive planning
**Artifacts**: Urban/suburban/rural density classes, telemetry-driven parameter derivation
**Commit**: `"M1.4: telemetry-driven plan"`

## M1.5 — Plan Fuzzing & Validation
**Why**: Stress-test autopilot with edge cases
**Artifacts**: Synthetic histogram fuzzing, planet-like distributions (long tails + hot urban tiles), invariant checking (never exceed cap)
**Safety**: Fuzz must assert plan never breaches compile-time cap under any distribution
**Commit**: `"M1.5: planner fuzzing + planet simulation"`