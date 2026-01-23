# CCH Query Bug - RESOLVED ✅

## Summary

Car/bike routing was failing with "No route found" due to orphaned nodes in the shared CCH topology. **Fixed with per-mode filtered CCH architecture.**

---

## Root Cause

When the CCH was built on all 5M EBG nodes but ~51% became car-inaccessible (via INF weights), some car-accessible nodes became **orphaned** in the hierarchy:
- No finite UP edges (couldn't climb hierarchy)
- No finite incoming DOWN edges (couldn't be reached from above)

This happened because shortcuts were built assuming all nodes are accessible, but car mode only uses ~49% of the graph.

---

## Solution Implemented

**Per-mode filtered CCH architecture** - each mode has its own CCH built on only mode-accessible nodes.

### Pipeline Changes

| Step | Change | Output |
|------|--------|--------|
| 5 | Generate FilteredEbg per mode | `filtered.{car,bike,foot}.ebg` |
| 6 | ND ordering on filtered EBG | `order.{mode}.ebg` |
| 7 | CCH contraction on filtered EBG | `cch.{mode}.topo` |
| 8 | Customization with filtered node mapping | `cch.w.{mode}.u32` |
| 9 | Query server with per-mode CCH | HTTP API |

### FilteredEbg Format

Dense subgraph containing only mode-accessible nodes with ID mappings:
- `filtered_to_original[filtered_id] → original_id`
- `original_to_filtered[original_id] → filtered_id` (u32::MAX if inaccessible)

### Query Flow

1. Spatial index returns **original** EBG node ID
2. Convert to **filtered** ID via `original_to_filtered`
3. Run CCH query in filtered space
4. Convert results back to **original** for geometry

---

## Results on Belgium

| Mode | Filtered Nodes | % of EBG | Unreachable CCH Edges |
|------|----------------|----------|----------------------|
| Car  | 2,447,122     | 48.8%    | **0%** (was ~70%) |
| Bike | 4,770,739     | 95.1%    | **0%** |
| Foot | 4,932,592     | 98.3%    | **0%** |

---

## Verification

All query types tested successfully:

```bash
# Start server
butterfly-route serve --data-dir data/belgium --port 8080

# Test car routing (previously failed)
curl "http://localhost:8080/route?src_lon=4.3517&src_lat=50.8467&dst_lon=4.4210&dst_lat=51.2177&mode=car"
# ✅ Returns route: Brussels → Antwerp, 55.8 min, 60.6 km

# Test bike routing
curl "http://localhost:8080/route?src_lon=4.3517&src_lat=50.8467&dst_lon=4.4210&dst_lat=51.2177&mode=bike"
# ✅ Returns route: 4.2 hours, 73.4 km

# Test foot routing
curl "http://localhost:8080/route?src_lon=4.3517&src_lat=50.8467&dst_lon=4.4210&dst_lat=51.2177&mode=foot"
# ✅ Returns route: 15.9 hours, 73.7 km

# Test matrix
curl "http://localhost:8080/matrix?src_lon=4.3517&src_lat=50.8467&dst_lons=4.4210,5.5714,3.7253&dst_lats=51.2177,50.6333,51.0544&mode=car"
# ✅ Returns durations for all 3 destinations

# Test isochrone
curl "http://localhost:8080/isochrone?lon=4.3517&lat=50.8467&time_s=600&mode=car"
# ✅ Returns polygon with 76,787 reachable nodes
```

---

## Checklist

- [x] Implement FilteredEbg format with node ID mappings
- [x] Step 5: Generate filtered EBGs per mode
- [x] Step 6: ND ordering on filtered EBG
- [x] Step 7: CCH contraction on filtered EBG
- [x] Step 8: Customization with filtered node mapping
- [x] Step 9: Query server with per-mode CCH
- [x] Verify 0% unreachable edges for all modes
- [x] Test car routing works
- [x] Test bike routing works
- [x] Test foot routing works
- [x] Test matrix endpoint
- [x] Test isochrone endpoint

**Status: COMPLETE** ✅
