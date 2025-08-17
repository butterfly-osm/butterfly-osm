# M7 — Parallel Serving (3 micro-milestones)

## M7.1 — Thread Architecture
**Why**: NUMA-aware high-performance serving
**Artifacts**: Per-thread arenas, NUMA pinning, lock-free hot paths
**Commit**: `"M7.1: thread architecture"`

## M7.2 — Sharded Caching
**Why**: Concurrent cache access without contention
**Artifacts**: 64-shard LRU caches for turn tables, proper eviction, NUMA-aware allocation after thread pinning, mmap interleave policy
**Rebalancing**: Auto-rebalance turn vs geom cache by 5% steps if hit-rate gap ≥12% for ≥60s
**Commit**: `"M7.2: sharded caches + NUMA + rebalancing"`

## M7.3 — Load Testing
**Why**: Concurrent multi-profile serving validation
**Artifacts**: Mixed-profile load tests, throughput scaling, Axum streaming
**Commit**: `"M7.3: parallel serving"`

**🔄 PRS v4**: Parallel scaling + profile concurrency + cache efficiency