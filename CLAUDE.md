# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

```bash
# Build entire workspace
cargo build --workspace

# Build release
cargo build --workspace --release

# Run all tests
cargo test --workspace

# Run tests for specific package
cargo test -p butterfly-dl
cargo test -p butterfly-route
cargo test -p butterfly-common

# Run single test
cargo test -p butterfly-route test_name

# Lint and format
cargo clippy --workspace --all-targets --all-features -- -D warnings -A clippy::redundant_closure
cargo fmt --all -- --check
cargo fmt --all  # auto-fix

# Build specific tool
cargo build --release -p butterfly-dl
cargo build --release -p butterfly-route
```

## Architecture Overview

**Butterfly-OSM** is a high-performance OSM toolkit built in Rust, organized as a Cargo workspace.

### Workspace Structure

```
butterfly-osm/
├── butterfly-common/        # Shared error handling and utilities
├── tools/
│   ├── butterfly-dl/        # OSM data downloader (production-ready)
│   └── butterfly-route/     # Routing engine (in development)
```

### butterfly-dl

Memory-efficient OSM downloader (<1GB RAM for any file size). Provides both CLI and library APIs.

- **Core modules**: `src/core/` - Downloader, source routing, streaming
- **CLI**: `src/cli/` - Progress display, argument parsing
- **FFI**: `src/ffi.rs` - C-compatible bindings (optional feature `c-bindings`)

Key API: `butterfly_dl::get()`, `butterfly_dl::get_stream()`, `butterfly_dl::get_with_progress()`

### butterfly-route

High-performance routing engine using **edge-based CCH** (Customizable Contraction Hierarchies).

#### Core Principle

**Edge-based graph is the single source of truth.** All queries (P2P routing, distance matrices, isochrones) use the same EBG-based CCH hierarchy. This ensures:
- Turn restrictions are exact
- Penalties applied identically for all query types
- Routes, matrices, and isochrones are internally consistent

#### Pipeline

| Step | Command | Status | Purpose |
|------|---------|--------|---------|
| 1 | `step1-ingest` | ✅ | Parse PBF → `nodes.sa`, `nodes.si`, `ways.raw`, `relations.raw` |
| 2 | `step2-profile` | ✅ | Per-mode attributes → `way_attrs.*.bin`, `turn_rules.*.bin` |
| 3 | `step3-nbg` | ✅ | Node-Based Graph (build-time intermediate only) |
| 4 | `step4-ebg` | ✅ | Edge-Based Graph → `ebg.nodes`, `ebg.csr`, `ebg.turn_table` |
| 5 | `step5-weights` | ✅ | Per-mode weights → `w.*.u32`, `t.*.u32`, `mask.*.bitset` |
| 6 | `step6-order` | ✅ | ND ordering on EBG |
| 7 | `step7-contract` | ✅ | CCH contraction on EBG |
| 8 | `step8-customize` | ✅ | Apply weights to shortcuts |

**Important:** NBG is a build-time intermediate. The ND ordering and CCH contraction must operate on the EBG because:
- Routing state = directed edge ID
- Turn costs are transitions between edges: `cost(e_in → e_out)`
- NBG ordering cannot be directly mapped to EBG

#### Key Modules

- `src/formats/` - Binary file format readers/writers (CRC-verified)
- `src/profiles/` - Routing profiles (car, bike, foot)
- `src/ebg/` - Edge-Based Graph construction (THE routing graph)
- `src/nbg/` - Node-Based Graph (intermediate only)
- `src/validate/` - Lock condition verification per step

### Binary File Formats

All formats use:
- Magic number headers for type identification
- CRC64 checksums (body + file)
- Fixed-size records for memory-mapped access
- Little-endian encoding

Each step produces a `stepN.lock.json` with SHA-256 checksums for reproducibility.

## Implementation Plan

See **[todo_overall.md](todo_overall.md)** for the overall implementation plan, including:
- Step-by-step pipeline specification
- Algorithm details and lock conditions
- Performance targets
- "What NOT to do" constraints

See **[todo_immediate.md](todo_immediate.md)** for immediate bugs and fixes that must be addressed before continuing.

## Testing

**Belgium is the ONLY test dataset. No other countries.**

- File: `belgium.pbf` from Geofabrik
- ~1.9M NBG nodes, ~4M edges → ~5M EBG nodes
- All tests, benchmarks, and validation MUST run on Belgium
- Do NOT create test data for Monaco, Luxembourg, or any other region
- Run the full pipeline on Belgium before considering any step complete

## Code Quality Requirements

**ABSOLUTE REQUIREMENTS — NO EXCEPTIONS:**

1. **No placeholders** — Every function must be fully implemented
2. **No code shortcuts** — No "TODO: implement later", no stub functions
3. **No sloppiness** — Code must be correct, not "good enough"
4. **Prove it works** — Run on belgium.pbf and verify lock conditions pass
5. **No assumptions** — If uncertain, investigate; don't guess

**If code cannot be completed correctly, stop and ask rather than writing incomplete code.**

## Development Principles

**XP Pair Programming Rules**:
- Test-first: Write failing test, then implement
- KISS: Always choose minimal abstraction
- Atomic commits: One logical change per commit
- Conventional Commits: `feat(module): ...`, `fix(module): ...`

**Key constraints**:
- Memory-efficient streaming (fixed-size buffers)
- Deterministic outputs (byte-for-byte reproducible)
- Lock conditions must pass before proceeding to next step
- **One graph, one hierarchy, one query engine** — no separate backends for different query types

## AI Code Review

When consulting AI reviewers (Gemini and Codex), **ALWAYS run them in parallel**:

```bash
# CORRECT - parallel execution
timeout 300 gemini -m gemini-2.5-pro -p "prompt" &
timeout 300 codex -q "prompt" &
wait

# WRONG - sequential execution (wastes time)
timeout 300 gemini -m gemini-2.5-pro -p "prompt"
timeout 300 codex -q "prompt"
```

Tips:
- Use 5 minute timeout for complex reviews
- If Gemini rate limited: `gemini -m gemini-flash-2.5 -p "shorter prompt"`
- Codex uses `-q` flag for queries, `-p` for prompts with context
