# Repo boundaries

(Part of the [engineering guide](../ENGINEERING.md).)

This repository is **public and holds the engine only**: the pipeline, the CCH,
the query server, the calibration mathematics, the benchmark harnesses. Three
things live in private repositories and must never appear here: deploy tooling
(build, precompute, artifact publishing, image tags); infrastructure manifests
(orchestration, volumes, environments, hostnames); the licensed observed-speed
pipeline (feeds, provider names, segment matching, publication).

**The only things that cross the boundary are data artefacts with documented
contracts**: `edge_speeds.parquet` (4-calibration-seam.md) and the reference-trip CSVs
(`route_id,long_1,lat_1,long_2,lat_2,ref_min,ref_km`). Both are defined by their
columns, not by where they come from. No private repository name, data provider,
storage path, host, IP address or client name belongs in engine code, comments,
docs or commit messages; for provenance and cache invalidation use
container-internal identity (section CRCs, input hashes), never a deploy-side
identifier. `scripts/check-upstream-clean.sh` enforces this over **tracked files
and commit messages** in the pre-push range, and runs first in CI. If it fires,
reword generically or move the content — do not add an exemption.
