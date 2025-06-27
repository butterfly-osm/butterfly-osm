# GitHub Issue Plan for **butterfly‑shrink**

> **Scope**: this board covers only the `butterfly‑shrink` crate/executable described in the final design doc. Other tools (`butterfly‑dl`, validation harness, etc.) are out‑of‑scope.
>
> **Workflow**: Every issue contains a *Definition of Done* (DoD) and **tests** that must pass in CI (`cargo test` + `cargo clippy -- -D warnings`). *Do NOT move an issue to Done until its tests are green in CI.*

| Legend   | Emoji/Label                                   |
| -------- | --------------------------------------------- |
| 🚩       | **blocked‑by** dependency                     |
| 🔗       | **blocks** downstream issue                   |
| 🏷       | **label** (e.g. `core`, `perf`, `qa`, `docs`) |

---

## Milestones

1. **M0 – Bootstrap**  (issues #1‑#4) – compile empty CLI that prints help.
2. **M1 – Steel Thread**  (#5‑#10) – end‑to‑end planet run finishes with node snapping + way rewrite (relations skipped).
3. **M2 – Routing Complete**  (#11‑#15) – turn‑restriction remap, stats, presets.
4. **M3 – QA & CI**  (#16‑#18) – validator, JSON stats, GitHub Actions.
5. **M4 – Docs & Release v0.1**  (#19‑#20).

---

## Issues

### #1 Initial Cargo Scaffold 🏷 core

* Create Cargo workspace, `butterfly-shrink` binary crate, Apache‑2.0 + MIT license.
* CI: build + fmt + clippy.

**DoD**

* `cargo run -- --help` prints CLI skeleton.
* CI badge shows green.

**Tests**

```bash
cargo test --lib   # no tests yet, but must compile
```

---

### #2 PBF Reader & Writer Skeleton 🏷 core  🚩 blocked‑by #1

* Add `pbf-craft` dependency.
* Stream input file & echo nodes to output.

**DoD**

* `butterfly-shrink tests/3nodes.pbf echo.pbf` produces identical file (bitwise).

**Tests**

```rust
#[test]
fn echo_roundtrip() {
    let out = tempdir::TempDir::new("echo").unwrap();
    Command::cargo_bin("butterfly-shrink")
        .unwrap()
        .args(["tests/3nodes.pbf", out.path().join("e.pbf")])
        .assert()
        .success();
    assert_eq!(md5_file!("tests/3nodes.pbf"), md5_file!(out.path().join("e.pbf")));
}
```

---

### #3 Fixed‑Grid Snap Utility 🏷 core  🚩 blocked‑by #1

* Implement `snap_coordinate()` as per design doc.
* Unit tests for lat/lon edge cases.

**DoD**

* `snap_coordinate(52.0, 13.0, 5.0)` returns correct, centred nanodegree pair.
* High-latitude coordinates (e.g., 82.5°N) are snapped correctly without being dropped.

**Tests**
* `tests/grid_snap.rs` with asserts for equator, 60°, 85°N, and 89.9°N to verify correct E-W scaling and latitude clamping as per `PLAN.md`.

---

### #4 RocksDB Integration 🏷 core  🚩 blocked‑by #1

* Add `rocksdb` crate, open temp dir.
* Basic put/get round‑trip.

**DoD**

* Unit test writes 100k random keys and reads them back successfully.
* RocksDB temp directory is created inside `$TMPDIR/butterfly-shrink-{uuid}/`.

---

### #5 Node Streaming Pipeline 🏷 core  🚩 blocked‑by #2 #3 #4 🔗 blocks #6

* Read nodes, snap, dedup via RocksDB, write dense nodes.

**DoD**

* `cargo run tests/berlin-mini.pbf out.pbf` finishes successfully.
* Output node count is less than input node count.
* Temp directory is automatically cleaned up on success or failure.

**Tests**
* Integration test counts nodes with `osmium cat -f osm` and asserts reduction.

---

### #6 Way Rewrite 🏷 core  🚩 blocked‑by #5 🔗 blocks #7

* Map node refs → rep IDs, drop skipped nodes, strip tags.
* Implement fail-fast for forward references.

**DoD**

* Berlin‑mini extract is routable in OSRM (`osrm-extract` succeeds).
* A PBF with a forward reference causes a fatal error with the exact message from `PLAN.md`.

**Tests**

* Assert no way in the output contains a node ID that doesn't exist.
* Integration test with a crafted corrupt PBF triggers the forward-reference error.

---

### #7 Turn‑Restriction Remap 🏷 core  🚩 blocked‑by #6 🔗 blocks #8

* Buffer `type=restriction`, remap `via` to representative node ID.
* Skip and warn on multi-via restrictions.

**DoD**

* Sample restriction test passes (relation `via` ID is updated to the representative ID).
* A warning is logged for multi-via restrictions, matching the format in `PLAN.md`.

---

### #8 Stats & Warnings 🏷 qa  🚩 blocked‑by #7 🔗 blocks #9

* Human and JSON stats output.
* Implement `--dropped-ways` and `--skipped-restrictions` CSV reports.

**DoD**

* `... --stats-format json` produces output matching the structure in `PLAN.md`.
* `... --dropped-ways report.csv` generates a CSV with `way_id,reason` columns.
* `... --skipped-restrictions r.csv` generates a CSV with `relation_id,reason` columns.

---

### #9 Preset Profiles & YAML Config 🏷 enhancement  🚩 blocked‑by #8 🔗 blocks #10

* Implement built‑in `car`, `bike`, `foot` presets.
* Add `--config` flag to merge external YAML file.

**DoD**

* CLI `--preset bike` includes cycleways in the output for the Berlin test.
* A setting from `--config my.yaml` correctly overrides a built-in preset.

---

### #10 Disk‑Space & TMP Checks 🏷 enhancement  🚩 blocked‑by #9 🔗 blocks #11

* Fail early if `$TMPDIR` has insufficient free space.
* Warn if `$TMPDIR` appears to be `tmpfs`.

**DoD**

* Unit test simulating low-space fails with the exact error message from `PLAN.md`.
* Unit test simulating a `tmpfs` mount prints the correct warning from `PLAN.md`.

---

### #11 Validator CLI 🏷 qa  🚩 blocked‑by #10

* Implement `--validate orig.pbf shrunk.pbf`.
* Checks for connectivity, edge preservation, and turn restrictions.

**DoD**

* Germany extract passes validation in under 120 seconds, with output matching `PLAN.md`.

---

### #12 Direct‑I/O Flag 🏷 perf  🚩 blocked‑by #5

* Add `--direct-io` flag.
* Attempt `O_DIRECT`; fall back gracefully with a warning if it fails.

**DoD**

* On a supported Linux system, `strace` confirms the `O_DIRECT` flag is used for file I/O.

---

### #13 GitHub Actions CI 🏷 ci  🚩 blocked‑by #8

* Configure CI to run build, clippy, fmt, unit tests, and integration tests.

**DoD**

* README badge is green.
* CI runs the test matrix against `berlin-mini.pbf` and `germany.pbf`.

---

### #14 Release Docs 🏷 docs  🚩 blocked‑by #13

* Update `README.md` with final CLI usage, examples, and the grid size/disk space table.

**DoD**

* `cargo doc --open` builds without warnings.
* All CLI flags from `PLAN.md` are documented with examples.

---

### #15 v0.1 Tag & Binary Release 🏷 release  🚩 blocked‑by #14

* `cargo install butterfly-shrink` works against a tagged release.

**DoD**

* A GitHub Release is created with checksums for the compiled binary.

---

## Labels

* **core** – essential pipeline code.
* **perf** – performance tweaks.
* **qa** – validation & stats.
* **docs** – documentation.
* **ci** – automation.
* **release** – versioning.

---

### Test matrix summary

| Stage       | Extract      | CI target time            |
| ----------- | ------------ | ------------------------- |
| Echo / snap | `3nodes.pbf` | < 1 s                     |
| Berlin mini | 50 k nodes   | < 15 s                    |
| Germany     | 35 M nodes   | < 8 min (CI large‑runner) |

A PR may not close its issue until **all dependent issues are merged and CI is fully green**.
