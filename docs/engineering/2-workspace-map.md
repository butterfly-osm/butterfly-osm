# Workspace map and data flow

(Part of the [engineering guide](../ENGINEERING.md).)

```
dl/                butterfly-dl — streaming OSM downloader (<1 GB RAM, any size)
route/             butterfly-route — the pipeline, the router, the transit engine
bench/             post-deploy gate + competitor harnesses
models/            per-mode *.model.json cost profiles

--- data flow, run once per region ---
region.pbf
  step1-ingest    nodes.sa nodes.si ways.raw relations.raw
  step2-profile   way_attrs.<mode>.bin  turn_rules.<mode>.bin  (+ density class)
  step3-nbg       node-based graph — BUILD-TIME INTERMEDIATE ONLY
  step4-ebg       ebg.nodes ebg.csr ebg.turn_table              <- the routing graph
  step5-weights   w.<mode>.u32  t.<mode>.u32  mask.<mode>.bitset
  step6-lifted    CCH order (nested dissection on NBG, lifted to EBG)
  step7-contract  cch.<mode>.topo
  step8-customize cch.w.<mode>.u32  cch.d.<mode>.u32
  pack            <region>.butterfly  (one mmap'd container, CRC per section)
  serve           REST + Flight
```

Every step writes a `stepN.lock.json` with SHA-256 checksums; `pack` verifies
sections and by default prunes the `step*/` trees.

**One graph.** The edge-based graph is the single source of truth: routing state
is a directed edge id, turn cost is a transition `cost(e_in -> e_out)`. Routes,
matrices and isochrones therefore cannot disagree. The NBG exists only so nested
dissection sees the physical topology; it is never queried. Query engines:

- **PHAST** for isochrones — an upward priority-queue search, then one linear
  rank-order scan of the DOWN edges, block-gated so unreachable rank blocks are
  skipped entirely. **K-lane batching** (K=8) amortises that scan across 8
  sources for bulk matrices and bulk isochrones.
- **Bucket many-to-many** for matrices — explores only towards requested
  targets. `d(s->t) = min_m d(s->m) + d(m->t)`; the target phase is a **reverse**
  search, because `d(t->m) != d(m->t)` on a directed graph.
- **Lopsided PHAST fields** when one side is much smaller — a forward field for
  many-to-one, a reverse field for one-to-many. The choice between bucket and
  PHAST is made by a **router with measured cost constants** (an EWMA of real
  sweep and scan wall times on this host), not a hardcoded size threshold.

Every endpoint snaps through **phantom endpoints**: both directed twins of up to
3 near-equidistant physical edges, with exact partial-edge costs. Never
reintroduce a single directed-edge commit, and never expand seeds at the API
layer — both have been measured, both are much worse.

**Transit** is a RAPTOR engine sharing the process and the foot CCH: `origin ->
access CCH 1-to-N -> RAPTOR rounds over a merged timetable -> egress CCH 1-to-N`.
GTFS and NeTEx-EPIP feeds merge into one `Timetable` with namespaced stop ids;
transfers are ULTRA-preprocessed at startup and cached behind a provenance hash.
