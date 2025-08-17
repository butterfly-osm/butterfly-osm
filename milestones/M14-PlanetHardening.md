# M14 — Planet Hardening (4 micro-milestones)

## M14.1 — Resume Manifests
**Why**: Resumable builds after failures
**Artifacts**: Phase checkpoint manifests, file digests, restart logic
**Recovery**: Skip completed phases unless `--force`; saves hours on transient failures
**Commit**: `"M14.1: build resume"`

## M14.2 — Dataset Validator
**Why**: Comprehensive dataset verification
**Artifacts**: Standalone validator binary, header checks, digest verification, cross-core consistency
**Usage**: Pre-serve validation tool, keeps serve startup tight
**Commit**: `"M14.2: dataset validator"`

## M14.3 — Hot-Swap Support
**Why**: Zero-downtime dataset updates
**Artifacts**: Atomic dataset switching, validation hooks, rollback safety
**Commit**: `"M14.3: hot-swap datasets"`

## M14.4 — Observability
**Why**: Production monitoring and debugging
**Artifacts**: Always-on tracing spans, `/status.json` endpoint, per-phase metrics (MiB/s, chunks/s, merges, spill bytes)
**Taxonomy**: `pbf.read`, `coarsen`, `collapse`, `geom.passA/B/C`, `toc.write`, `cch.order`, `cch.customize`
**Commit**: `"M14.4: production observability"`

**🔄 PRS v11**: Dataset integrity + hot-swap safety + observability validation