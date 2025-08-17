# M15 — Performance Harness (3 micro-milestones)

## M15.1 — SLA Enforcement
**Why**: Automated performance regression detection
**Artifacts**: Performance test suite, SLA gates, regression alerts
**Commit**: `"M15.1: SLA enforcement"`

## M15.2 — Nav-Off vs Nav-On Split
**Why**: Prove geometry-free routing advantage
**Artifacts**: Performance comparison harness, `/route?steps=false` vs `/route?steps=true` benchmarks
**Proof**: Time-only never touches geometry and beats engines that always load shapes
**Commit**: `"M15.2: geometry performance split"`

## M15.3 — Load Testing
**Why**: Production-scale performance validation
**Artifacts**: Multi-profile load tests, mixed workload scenarios
**Commit**: `"M15.3: load testing"`

**🔄 PRS v12**: Performance SLA compliance + geometry-free validation + load testing