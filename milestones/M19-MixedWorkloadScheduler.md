# M19 — Mixed Workload Scheduler (3 micro-milestones)

## M19.1 — Queue Management
**Why**: Intelligent request prioritization
**Artifacts**: Priority queues, request classification, admission control
**Commit**: `"M19.1: queue management"`

## M19.2 — Tail-Latency Protection
**Why**: Prevent performance degradation under load
**Artifacts**: Tail-latency protector, GPU shunting for matrices/isochrones when queue depth >N
**Performance**: p95 inflation ≤1.2× under 95/5 p2p/matrix mix
**Commit**: `"M19.2: tail-latency protection"`

## M19.3 — Load Balancing
**Why**: Optimal resource utilization
**Artifacts**: Dynamic load balancing, CPU/GPU work distribution
**Commit**: `"M19.3: load balancing"`

**🔄 PRS v16**: Scheduler correctness + tail-latency SLA + mixed workload performance