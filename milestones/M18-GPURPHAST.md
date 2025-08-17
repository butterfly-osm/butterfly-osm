# M18 — GPU RPHAST (2 micro-milestones)

## M18.1 — GPU Matrices
**Why**: GPU-accelerated matrix computation
**Artifacts**: GPU RPHAST kernels, blocked computation
**Performance**: 1k×1k matrix ≤2s GPU target
**Commit**: `"M18.1: GPU matrices"`

## M18.2 — GPU Optimization
**Why**: Production GPU performance
**Artifacts**: Memory coalescing, kernel optimization, throughput maximization
**Commit**: `"M18.2: GPU optimization"`

**🔄 PRS v15**: GPU matrix correctness + performance SLA + CPU parity ≤0.5s