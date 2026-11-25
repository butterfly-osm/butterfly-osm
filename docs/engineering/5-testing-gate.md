# Testing and the post-deploy gate

(Part of the [engineering guide](../ENGINEERING.md).)

```bash
cargo test --workspace                     # ~860 tests (unit + integration + doc), no data
cargo test --workspace --all-features      # + the `feature = "bench"` tests (#556)
bash scripts/check-upstream-clean.sh       # public-repo leak guard
BUTTERFLY_REFS_DIR=/data/reference-trips butterfly-gate --base http://localhost:3001 [--quick] [--no-flight]   # cargo build --release --features gate --bin butterfly-gate [--list-gates]
```

Local pre-push and CI run ONE step list, `scripts/ci-steps.sh` — print it with
`bash scripts/ci-steps.sh --list`. In order: the upstream-clean leak guard,
`cargo fmt --check`, clippy with warnings as errors (all targets, all features),
`cargo test --workspace`, `cargo test --workspace --all-features` (#556), a count
guard proving the all-features test list is a strict superset of the default one,
the gate's `py_compile` + `--list-gates` smoke, and the gate's own offline unit
tests (`cargo test --features gate --bin butterfly-gate`, `route/src/gate/tests.rs`). There is no separate
`cargo build` step: `cargo test --workspace` already links `butterfly-route` and
`butterfly-dl`, and `butterfly-bench` is a `--all-features` bin (#591).
`scripts/hooks/install.sh` installs the hook. Skip it with
`BUTTERFLY_NO_VERIFY=1` in emergencies only. Belgium is the only test dataset —
do not add fixtures for other regions.

The post-deploy gate runs against a live server and blocks promotion. `--quick`
runs the invariants only (~30 s) and skips the reference-trip ground truth;
`--trips` and `--refs-prefix` point at CSVs under `$BUTTERFLY_REFS_DIR`. It is
**invariant-based** and holds no measured-then-pasted constants: a pasted
constant only asserts "the server returns what it returned yesterday", which a
regression walks straight through. It checks properties instead — detour bounded
against crow-fly distance, plausible mean speed, distance ≡ polyline ≡ Σ
annotations, `/route` and `/table` agreeing within 3 s, A→B vs B→A symmetry,
isochrone topology and snapped-origin containment, `edges_batch` sums vs live
`/route`, matrix completeness and sparse-output consistency.

The suite asserts **no wall-clock number**. Where it once inferred the matrix
plan from a scaling ratio — flaky on a loaded runner, so it had been downgraded
to a warning — it now reads the plan the server reports for the very request it
made, and fails on the wrong one (#594). `BUTTERFLY_MATRIX_ALGO=bucket|phast`
cannot help here: it is read once at the first matrix call and frozen, so a
live server cannot be flipped; the gate exercises both branches by SHAPE
instead (1×N and N×1 must be `phast_*`, a balanced N×N must be `bucket`).

**One invariant per user ticket.** `gate_ticket_invariants` names every
user-reported failure class and either checks it directly or points at the gate
that already is that invariant. When you fix a user-visible bug, add its
invariant there — that is how a failure class stops being able to ship twice.
