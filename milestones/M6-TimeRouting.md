# M6 — Time-Based Routing (3 micro-milestones)

## M6.1 — Weight Compression
**Why**: Efficient time-cost storage
**Artifacts**: u16 quantization per 131k-edge block, tick calculation, overflow tables
**Commit**: `"M6.1: weight compression"`

## M6.2 — Turn Restriction Tables
**Why**: Legal and realistic turn handling
**Artifacts**: Profile-specific turn rules, penalty matrices, restriction enforcement, fixed 256-512 junctions/block sharding
**Performance**: Target >97% shard hit-rate warm; track shard miss % metric per profile
**Commit**: `"M6.2: turn tables + sharding"`

## M6.3 — Time-Cost Routing
**Why**: Realistic travel time routing
**Artifacts**: `/route` time-based Dijkstra per profile
**Commit**: `"M6.3: time-cost routing"`

**🔄 PRS v3**: ETA plausibility + turn legality + time parity validation