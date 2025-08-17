# M4 — Multi-Profile System (5 micro-milestones)
**🚨 PRS (Profile Regression Suite) starts here - runs after EVERY later milestone**

## M4.1 — Access Truth Tables
**Why**: Legal routing rules per transportation mode
**Artifacts**: Car/bike/foot access rules, 100+ synthetic junction tests
**Commit**: `"M4.1: access truth tables"`

## M4.2 — Profile Masking
**Why**: Mode-specific graph pruning
**Artifacts**: Three pruned graphs (car/bike/foot), mask validation
**Commit**: `"M4.2: profile masks"`

## M4.3 — Component Analysis
**Why**: Remove disconnected islands per profile
**Artifacts**: Profile-aware component pruning, legitimate island preservation
**Commit**: `"M4.3: component pruning"`

## M4.4 — Speed & Time Weights
**Why**: Mode-specific travel time calculation with adaptive grade penalties
**Artifacts**: Highway/surface speed tables, u16 quantization, overflow handling, **adaptive grade-aware penalties** auto-scaled from telemetry data:
- **Bike**: Exponential uphill penalty (`factor = exp(α * grade)`) with α auto-solved from 95th percentile grade distribution
- **Foot**: Naismith-style time penalties (`t_ascent = k_up * Δh`) with k_up scaled from 90th percentile ascent data  
- **Car**: Gentle linear/logistic penalty bounded by engine limits, auto-scaled from grade telemetry
- **Surface modulation**: Grade penalties amplified on gravel/dirt/sand for bike/foot
- **Model validation**: Tests verify monotonicity and model-consistent slowdowns (not hard-coded thresholds)
**Monitoring**: Log quantization tick distribution per block + grade penalty parameters in meta.json.plan
**Commit**: `"M4.4: adaptive weights + telemetry-driven grade penalties"`

## M4.5 — Multi-Profile Loader
**Why**: Server support for all transportation modes
**Artifacts**: Profile-specific loading, `/route` echo per mode
**Commit**: `"M4.5: loader + echo"`

**🔄 PRS v1**: Access legality + basic routing smoke tests per profile + forbidden-edge reporter (first offending edge with tags)