# Baseline Performance: A* with Turn Restrictions

**Date:** 2025-10-25
**Version:** v0.5
**Algorithm:** A* with turn restrictions, R-tree spatial index
**Dataset:** Belgium (636MB PBF → 1.1GB graph with restrictions)
**Graph Details:**
- Nodes: 7,960,000 highway nodes
- Turn restrictions: 7,052 loaded
- Graph file: `belgium-restrictions.graph` (1.1GB)

---

## Test Configuration

**Hardware:** (to be documented)
**Binary:** `target/release/butterfly-route` (release build)
**Graph:** `belgium-restrictions.graph`

---

## Baseline Query Performance

### Test Route 1: Brussels → Antwerp (29.2km)

```
From: 50.8503, 4.3517 (Brussels central)
To:   51.2194, 4.4025 (Antwerp central)

Results:
├─ Total query time: 0.927s
├─ Distance: 29,190m (29.2km)
├─ Time: 32.4 minutes
└─ Nodes visited: 928

Detailed Breakdown:
├─ R-tree lookups (×2):     0.00001s  (0.0%)
├─ Heuristic calculations:  0.279s    (30.1%)
├─ Restriction checks:      0.367s    (39.6%)
└─ Heap operations:         0.301s    (32.5%)

A* Statistics:
├─ Iterations: 574,075
├─ Edges explored: 1,201,898
├─ Restrictions checked: 1,201,896
├─ Restrictions blocked: 353 (0.03% hit rate)
├─ Heuristic calls: 576,869
└─ Heap operations: 1,150,944
```

**Key Insight:** 99.97% of restriction checks find no restriction!

---

### Test Route 2: Brussels → Ghent (31.7km)

```
From: 50.8503, 4.3517 (Brussels central)
To:   51.0543, 3.7174 (Ghent central)

Results:
├─ Total query time: 0.674s
├─ Distance: 31,679m (31.7km)
├─ Time: 35.2 minutes
└─ Nodes visited: 792

Detailed Breakdown:
├─ R-tree lookups (×2):     0.00001s  (0.0%)
├─ Heuristic calculations:  0.208s    (30.9%)
├─ Restriction checks:      0.273s    (40.5%)
└─ Heap operations:         0.225s    (33.3%)

A* Statistics:
├─ Iterations: 429,463
├─ Edges explored: 902,117
├─ Restrictions checked: 902,115
├─ Restrictions blocked: 264 (0.03% hit rate)
├─ Heuristic calls: 432,034
└─ Heap operations: 861,497
```

---

### Test Route 3: Brussels → Namur (35.8km)

```
From: 50.8503, 4.3517 (Brussels central)
To:   50.4674, 4.8720 (Namur central)

Results:
├─ Total query time: 0.736s
├─ Distance: 35,752m (35.8km)
├─ Time: 39.7 minutes
└─ Nodes visited: 1,027

Detailed Breakdown:
├─ R-tree lookups (×2):     0.00001s  (0.0%)
├─ Heuristic calculations:  0.235s    (31.9%)
├─ Restriction checks:      0.309s    (41.9%)
└─ Heap operations:         0.252s    (34.2%)

A* Statistics:
├─ Iterations: 532,630
├─ Edges explored: 1,124,148
├─ Restrictions checked: 1,124,146
├─ Restrictions blocked: 276 (0.02% hit rate)
├─ Heuristic calls: 535,269
└─ Heap operations: 1,067,899
```

---

## Performance Summary

| Metric | Brussels→Antwerp | Brussels→Ghent | Brussels→Namur | Average |
|--------|------------------|----------------|----------------|---------|
| **Distance** | 29.2 km | 31.7 km | 35.8 km | 32.2 km |
| **Query Time** | 0.927s | 0.674s | 0.736s | **0.779s** |
| **Nodes Visited** | 928 | 792 | 1,027 | 916 |
| **Iterations** | 574,075 | 429,463 | 532,630 | 512,056 |
| **Edges Explored** | 1,201,898 | 902,117 | 1,124,148 | 1,076,054 |

### Time Breakdown (Average)

```
Component               Time      Percentage
─────────────────────────────────────────────
R-tree lookups         0.00001s     0.0%
Heuristic              0.241s      30.9%
Restriction checks     0.316s      40.7%  ⚠️ BOTTLENECK
Heap operations        0.259s      33.3%
Path reconstruction    0.000s       0.0%
─────────────────────────────────────────────
TOTAL                  0.779s     100.0%
```

---

## Critical Findings

### 1. Restriction Checking is the Biggest Bottleneck

**Problem:**
- ~40% of query time spent checking turn restrictions
- ~1.1M restriction checks per query
- Only ~300 restrictions actually block (0.03% hit rate)
- **99.97% of restriction checks are wasted work!**

**Why it's expensive:**
Each restriction check requires:
1. `edge_to_way.get(&prev_edge)` - HashMap lookup
2. `edge_to_way.get(&current_edge)` - HashMap lookup
3. `graph.node_weight(current_node)` - Graph lookup
4. `restrictions.get(&(from_way, via_node))` - HashMap lookup
5. `restricted_ways.contains(&to_way)` - HashSet check

= **5 lookups per edge** × 1.1M edges = 5.5M lookups!

### 2. Massive Search Space

**Compared to target (CH):**
- Current A*: Explores ~500k iterations per query
- Target CH: Should explore ~100-500 iterations
- **1000x more iterations than needed!**

This is why A* can never be fast enough - it explores too much.

### 3. R-tree is Excellent

- R-tree lookups: <0.01ms (negligible)
- O(log n) performance confirmed
- Not a bottleneck at all ✓

### 4. Memory Usage

- Graph file: 1.1GB (includes R-tree + restrictions + shortcuts metadata)
- Estimated RAM: ~1.5-2GB when loaded
- For Belgium (8M nodes): Very reasonable
- But: Planet scale (1B nodes) would need ~150GB+ with this approach ❌

---

## Comparison to Target (CH Performance)

| Metric | Current A* | Target CH | Speedup Needed |
|--------|-----------|-----------|----------------|
| **Query Time** | 779ms | ~10-50ms | 15-75x faster |
| **Iterations** | 512,056 | 100-500 | 1000x fewer |
| **Edges Explored** | 1,076,054 | 200-1,000 | 1000x fewer |
| **Restriction Checks** | 1,076,000 | 0 (preprocessing) | ∞ |

---

## Why CH Will Be Faster

### 1. Preprocessing Eliminates Runtime Restriction Checks

**Current A*:**
- Check restrictions at query time
- 1.1M checks × 5 lookups = 5.5M operations per query
- 40% of query time wasted

**CH:**
- Apply restrictions during preprocessing (one-time cost)
- Remove illegal edges or mark with infinite weight
- Restrictions baked into shortcuts
- **Query time: 0 restriction checks** ✓

### 2. Hierarchy Reduces Search Space

**Current A*:**
- Searches "outward" from origin in all directions
- Explores residential streets, local roads, highways equally
- Must visit ~500k nodes to find path

**CH:**
- Quickly "climbs" to highway level
- Only searches high-importance nodes
- "Descends" to destination
- Visits ~100-500 nodes total

**Visual:**
```
A* Search Pattern:          CH Search Pattern:

    ●●●●●●●●●●●●               ─────●─────
  ●●●●●●●●●●●●●●              ─────┼─────
 ●●●●●●●●●●●●●●●●            ●    │    ●
●●●●●●A●●●●●●●●●●●           │    │    │
 ●●●●●●●●●●●B●●●●            A    │    B
  ●●●●●●●●●●●●●●                  │
    ●●●●●●●●●●●                   │

Explores ~500k nodes      Explores ~200 nodes
```

### 3. Contraction Creates Shortcuts

**Example:**
```
Before:  A ─→ X ─→ B  (2 edges, must visit X)
After:   A ───────→ B  (shortcut, X contracted out)
```

For a Belgium-scale graph:
- Original: 8M nodes, 20M edges
- After CH: 8M nodes + ranks, 26M edges (30% more due to shortcuts)
- But: Search only visits nodes "above" current level
- Result: Tiny search space despite larger graph

---

## Expected CH Performance (First Implementation)

### Conservative Estimates

```
Preprocessing (one-time):
├─ Node ordering: 2-3 minutes
├─ Node contraction: 3-5 minutes
├─ Shortcut creation: 1-2 minutes
└─ Total: ~5-10 minutes (acceptable for one-time cost)

Query Performance:
├─ Brussels → Antwerp: 50-100ms (vs 927ms = 9-18x faster)
├─ Brussels → Ghent: 30-70ms (vs 674ms = 9-22x faster)
└─ Brussels → Namur: 50-100ms (vs 736ms = 7-14x faster)

Graph Size:
└─ 1.1GB → ~1.5GB (+30% for shortcuts)
```

### After Optimization (Phase 4)

```
Query Performance (with optimized CH):
├─ Brussels → Antwerp: 10-20ms (vs 927ms = 46-92x faster)
├─ Brussels → Ghent: 10-20ms (vs 674ms = 33-67x faster)
└─ Brussels → Namur: 10-20ms (vs 736ms = 36-73x faster)
```

---

## Success Criteria for CH Implementation

**Phase 1: Basic CH Working**
- ✅ Queries: <100ms (10x faster than current)
- ✅ Iterations: <1,000 (1000x fewer)
- ✅ Edges explored: <2,000 (500x fewer)
- ✅ Correctness: Routes within 5% of A* distances
- ✅ No runtime restriction checks

**Phase 2: Optimized CH**
- ✅ Queries: <20ms (40-80x faster than current)
- ✅ Iterations: <500 (1000x fewer)
- ✅ Approaching OSRM performance (6ms)

**Phase 3: Multi-level PHAST**
- ✅ Queries: <5ms (150x faster than current)
- ✅ Beating OSRM by 2x (target: 3ms)

---

## Baseline Test Routes (for CH comparison)

Use these exact same routes to compare CH performance:

```bash
# Route 1: Brussels → Antwerp
./target/release/butterfly-route route <graph> \
  --from 50.8503,4.3517 --to 51.2194,4.4025

# Route 2: Brussels → Ghent
./target/release/butterfly-route route <graph> \
  --from 50.8503,4.3517 --to 51.0543,3.7174

# Route 3: Brussels → Namur
./target/release/butterfly-route route <graph> \
  --from 50.8503,4.3517 --to 50.4674,4.8720
```

**Important:** Use SAME coordinates to ensure fair comparison!

---

## Next Steps

1. ✅ **Baseline recorded** - This document
2. ⏳ **Implement CH** - Start with node ordering
3. ⏳ **Compare** - Run same routes, measure improvement
4. ⏳ **Optimize** - Iterate until <20ms queries
5. ⏳ **PHAST** - Multi-level architecture for <5ms

---

**Baseline established:** 2025-10-25
**Ready for CH implementation** ✓
