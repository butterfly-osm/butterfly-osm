# M16 — GPU Scaffolding (3 micro-milestones)

## M16.1 — CUDA Detection
**Why**: GPU capability detection and initialization
**Artifacts**: CUDA device detection, memory allocation, error handling
**Constraint**: Time Graph representation only for GPU
**Commit**: `"M16.1: CUDA scaffolding"`

## M16.2 — Memory Layout
**Why**: GPU-optimized data structures
**Artifacts**: GPU memory layout, bf16 weight representation (experimental), transfer optimization
**Commit**: `"M16.2: GPU memory layout"`

## M16.3 — Basic Operations
**Why**: Foundation GPU operations
**Artifacts**: Basic GPU kernels, performance baselines
**Commit**: `"M16.3: GPU operations"`

**🔄 PRS v13**: GPU capability + memory safety + basic operation correctness