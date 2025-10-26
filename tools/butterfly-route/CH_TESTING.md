# Testing Contraction Hierarchies Implementation

**Status:** Ready to build and test!
**Date:** 2025-10-25

---

## What Was Implemented

Complete CH implementation in **~600 lines of Rust:**

```
src/ch.rs (533 lines):
├─ CHGraph structure
├─ Node ordering (edge difference heuristic)
├─ Node contraction with shortcut creation
├─ Bidirectional CH query (upward search)
├─ Serialization (save/load via bincode)
└─ Nearest node search

src/main.rs:
├─ build-ch command (preprocess graph)
└─ route-ch command (query CH graph)
```

**Key Features:**
- ✅ Full node ordering and contraction
- ✅ Shortcut creation and metadata storage
- ✅ Bidirectional upward search
- ✅ Graph serialization (save/load)
- ✅ Complete CLI integration
- ⚠️ Turn restrictions NOT yet integrated (pending)

---

## Build Instructions

### Step 1: Build the Binary

```bash
cd /home/snape/projects/butterfly-osm
cargo build --release --bin butterfly-route
```

**Expected:** 2-3 minutes compile time

**Output:**
```
   Compiling butterfly-route v0.5.0
    Finished release [optimized] target(s) in 2m 15s
```

**Binary location:** `./target/release/butterfly-route`

---

## CH Preprocessing

### Step 2: Build CH Graph from Existing Graph

```bash
./target/release/butterfly-route build-ch \
  belgium-restrictions.graph \
  belgium-ch.graph
```

**Expected output:**
```
Loading graph from belgium-restrictions.graph...
Graph loaded in X.XXs

Building Contraction Hierarchies...
Starting Contraction Hierarchies preprocessing...
Cloned graph: 7960000 nodes, XXXXX edges

[1/2] Computing node ordering...
Node ordering computed in X.XXs

[2/2] Contracting nodes and creating shortcuts...
Contracted 100000 / 7960000 nodes (1.3%), shortcuts so far: XXXX
Contracted 200000 / 7960000 nodes (2.5%), shortcuts so far: XXXX
...
Contracted 7960000 / 7960000 nodes (100.0%), shortcuts so far: XXXX
Contraction completed in X.XXs
Created XXXXX shortcuts
Final graph: 7960000 nodes, XXXXX edges (XX% increase)

✓ CH preprocessing completed in X.XXs

Saving CH graph to belgium-ch.graph...
✓ CH graph saved in X.XXs
✓ CH preprocessing complete!
```

**Performance expectations:**
- Preprocessing time: **5-10 minutes** for Belgium (7.96M nodes)
- Graph size: **1.5-2GB** (30-50% larger due to shortcuts)
- Memory usage during preprocessing: **~4-6GB**

**⚠️ Note:** First implementation may be slower. This is normal!

---

## CH Query Testing

### Step 3: Run Test Queries

**Test Route 1: Brussels → Antwerp (29.2km)**
```bash
./target/release/butterfly-route route-ch belgium-ch.graph \
  --from 50.8503,4.3517 --to 51.2194,4.4025
```

**Expected output:**
```
Loading CH graph from belgium-ch.graph...
CH graph loaded in X.XXs

Finding nearest nodes...
Routing from node 9449924585 to node 6400826601
CH query from 9449924585 to 6400826601
CH query completed in 0.XXXs
Iterations: XXX, Best distance: XXXX.Xm

=== CH Query Results ===
Total query time: 0.XXXs
Distance: XXXXXm (XX.X km)
Time: XX.X minutes
Nodes in path: XXX
========================
```

**Test Route 2: Brussels → Ghent (31.7km)**
```bash
./target/release/butterfly-route route-ch belgium-ch.graph \
  --from 50.8503,4.3517 --to 51.0543,3.7174
```

**Test Route 3: Brussels → Namur (35.8km)**
```bash
./target/release/butterfly-route route-ch belgium-ch.graph \
  --from 50.8503,4.3517 --to 50.4674,4.8720
```

---

## Success Criteria

### ✅ CH Preprocessing

- [ ] Completes without errors
- [ ] Time: 5-15 minutes (acceptable range)
- [ ] Creates shortcuts (30-50% graph increase)
- [ ] Saves CH graph successfully
- [ ] File size: 1.5-2GB

### ✅ CH Query Performance (First Iteration)

| Metric | Target | Baseline (A*) | Improvement |
|--------|--------|---------------|-------------|
| **Query Time** | <100ms | 779ms | >7x faster |
| **Iterations** | <1,000 | 512,000 | >500x fewer |
| **Distance Accuracy** | ±5% | exact | acceptable |

### ✅ Correctness

- [ ] Routes are reasonable (not wildly different from A*)
- [ ] Distances within 5% of A* results
- [ ] No crashes or errors
- [ ] Can load saved CH graph successfully

---

## Comparison with Baseline

### Baseline Performance (from BASELINE.md)

```
A* with turn restrictions:
├─ Brussels → Antwerp: 0.927s (928 nodes in path, 29.2km)
├─ Brussels → Ghent:   0.674s (792 nodes in path, 31.7km)
└─ Brussels → Namur:   0.736s (1027 nodes in path, 35.8km)

Average: 0.779s per query
Iterations: ~512k per query
```

### Expected CH Performance (First Iteration)

```
CH (first implementation):
├─ Brussels → Antwerp: 50-100ms (XX nodes in path, ~29km)
├─ Brussels → Ghent:   50-100ms (XX nodes in path, ~32km)
└─ Brussels → Namur:   50-100ms (XX nodes in path, ~36km)

Average: 50-100ms per query
Iterations: ~100-1000 per query
```

**Expected speedup: 7-15x faster!**

---

## Recording Results

Create a comparison table:

```bash
# Run A* baseline
./target/release/butterfly-route route belgium-restrictions.graph \
  --from 50.8503,4.3517 --to 51.2194,4.4025 > astar_antwerp.txt

# Run CH version
./target/release/butterfly-route route-ch belgium-ch.graph \
  --from 50.8503,4.3517 --to 51.2194,4.4025 > ch_antwerp.txt

# Compare
diff -u astar_antwerp.txt ch_antwerp.txt
```

**Record in BASELINE.md:**

| Route | A* Time | CH Time | Speedup | A* Iterations | CH Iterations | Improvement |
|-------|---------|---------|---------|---------------|---------------|-------------|
| Brussels→Antwerp | 927ms | XXms | XXx | 574,075 | XXX | XXXx fewer |
| Brussels→Ghent | 674ms | XXms | XXx | 429,463 | XXX | XXXx fewer |
| Brussels→Namur | 736ms | XXms | XXx | 532,630 | XXX | XXXx fewer |

---

## Known Limitations (First Iteration)

1. **Turn Restrictions:** NOT integrated into CH yet
   - CH queries ignore turn restrictions
   - Routes may use illegal turns
   - Will be fixed in next iteration

2. **Nearest Node Search:** Uses linear search (slow)
   - Finding nearest nodes takes a few seconds
   - Should add R-tree to CHGraph
   - Query time is still measured separately

3. **No Witness Search:** Naive shortcut creation
   - Creates more shortcuts than necessary
   - Larger graph size than optimal
   - Slower preprocessing
   - Will optimize in next iteration

4. **Simple Node Ordering:** Basic edge difference heuristic
   - Not as optimized as production systems
   - Can be improved with better heuristics

---

## Troubleshooting

### Issue: Compilation errors

**Solution:** Check Rust version and dependencies
```bash
rustc --version  # Should be 1.70+
cargo clean
cargo build --release
```

### Issue: Preprocessing takes too long (>30 minutes)

**Possible causes:**
- Debug build instead of release
- Low memory (swapping)
- Inefficient node ordering

**Solution:**
- Ensure using `cargo build --release`
- Monitor with `htop` to check memory usage
- First iteration may be slow, this is expected

### Issue: Query crashes or returns no route

**Possible causes:**
- Coordinates outside Belgium
- Graph data corruption
- Serialization issue

**Solution:**
- Verify coordinates are in Belgium
- Rebuild CH graph from scratch
- Check input graph is valid

### Issue: CH query slower than A*

**Red flag!** Something is wrong. Should NEVER happen.

**Check:**
- Are you using release build?
- Is the CH graph properly loaded?
- Check iteration count (should be <1000, not 500k!)

---

## Next Steps After Testing

Once CH is working and verified:

### Phase 1: Optimization (1-2 weeks)
1. ✅ Add witness search (avoid unnecessary shortcuts)
2. ✅ Better node ordering heuristics
3. ✅ Add R-tree to CHGraph for fast nearest node
4. ✅ Target: <20ms queries

### Phase 2: Turn Restrictions (1 week)
1. ✅ Apply restrictions during preprocessing
2. ✅ Remove illegal edges before contraction
3. ✅ Test correctness vs A* with restrictions

### Phase 3: Comparison & Documentation (3-5 days)
1. ✅ Run full comparison suite
2. ✅ Update BASELINE.md with CH results
3. ✅ Document in PLAN.md
4. ✅ Celebrate! 🎉

### Phase 4: Multi-level PHAST (4-6 weeks)
1. ✅ Implement L0 tiling
2. ✅ Extract highway-only L1 graph
3. ✅ Run CH on smaller L1 graph
4. ✅ Target: <5ms queries

---

## Questions to Answer After Testing

1. **How long did preprocessing take?**
2. **How many shortcuts were created?**
3. **What's the actual query time?**
4. **How many iterations per query?**
5. **Are routes correct (similar to A*)?**
6. **What's the speedup ratio?**
7. **Any crashes or errors?**

---

**Ready to build and test!** 🚀

Good luck! If you encounter issues, check the troubleshooting section or examine the profiling output for clues.
