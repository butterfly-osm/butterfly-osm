# Key file paths

(Part of the [engineering guide](../ENGINEERING.md).)

```
route/src/cli.rs                    every subcommand and its flags
route/src/ebg/                      edge-based graph construction (the routing graph)
route/src/ordering_lifted.rs        step 6 nested dissection, lifted NBG -> EBG
route/src/{contraction,customization}.rs   steps 7 and 8
route/src/calibrate.rs              speed-table reading, time_scale, calibration math
route/src/pack.rs                   .butterfly container + export-edges
route/src/formats/                  binary formats, CRC, mmap (sole unsafe carveout)
route/src/matrix/bucket_ch.rs       bucket M2M + the measured-cost matrix router
route/src/range/phast.rs            single-source PHAST (batched_phast.rs = K-lane)
route/src/range/sparse_contour.rs   isochrone topology (one simple polygon)
route/src/server/api.rs             REST router; handlers sit beside it
route/src/server/{state,flight}.rs  server state + boot recustomization; Flight actions
route/src/transit/                  timetable, loaders, RAPTOR, ULTRA transfers
route/src/gate/ (butterfly-gate)    the invariant gate (#646; was bench/postdeploy_gate.py)
```
