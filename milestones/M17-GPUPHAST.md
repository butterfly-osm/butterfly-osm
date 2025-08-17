# M17 — GPU PHAST (2 micro-milestones)

## M17.1 — GPU Isochrones
**Why**: GPU-accelerated isochrone computation
**Artifacts**: GPU PHAST kernels, memory management
**Performance**: 15-min isochrone ≤15ms GPU target
**Commit**: `"M17.1: GPU isochrones"`

## M17.2 — GPU Integration
**Why**: Seamless CPU/GPU fallback
**Artifacts**: Device selection logic, performance monitoring
**Commit**: `"M17.2: GPU integration"`

**🔄 PRS v14**: GPU isochrone correctness + performance SLA + CPU parity ≤0.5s