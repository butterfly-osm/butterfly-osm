#!/usr/bin/env bash
# ============================================================================
# THE step list (issue #555). ONE definition, two callers:
#   * .github/workflows/ci.yml  — the merge gate
#   * scripts/hooks/pre-push    — the same checks, before the push
# so the local hook and the remote runner can never drift apart. Add a step
# here and both get it.
#
# Usage: bash scripts/ci-steps.sh          # run every step, stop at the first failure
#        bash scripts/ci-steps.sh --list   # print the step labels, exit 0
#
# Toolchain (rustc/cargo, protoc, python3) is the caller's job: CI installs it
# in earlier workflow steps, the hook uses whatever is on your PATH.
# ============================================================================
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1

# label <TAB> command
STEPS=(
  "upstream-clean (public repo)|bash scripts/check-upstream-clean.sh"
  # #592: the guard's OWN tests. The commit-message half was a silent no-op on
  # every branch without a tracking ref — and still printed its success line —
  # for as long as nobody tested the guard itself. A guard that reports OK
  # without scanning is worse than no guard, so it gets a test like any other.
  "upstream-clean guard tests|bash scripts/test-check-upstream-clean.sh"
  "rustfmt --check|cargo fmt --all -- --check"
  "clippy (deny warnings)|cargo clippy --workspace --all-targets --all-features"
  # No separate `cargo build --workspace` step (#591). Verified: delete
  # target/debug/butterfly-{route,dl}, run `cargo test --workspace --no-run`,
  # and both reappear — cargo builds and uplifts every bin target of a package
  # it runs integration tests for. `cargo build --workspace` would not have
  # built `butterfly-bench` either (`required-features = ["bench"]`); the
  # `test (all features)` step below is what links it. The extra step only
  # re-linked what the test step already produced.
  #
  # --workspace, NOT --lib: route/tests/*.rs never ran before #555. It holds
  # 56 integration tests; 21 run without any data, the other 35 are #[ignore]
  # (they need the Belgium/Luxembourg containers, live feeds or a running
  # server). Those that do probe for a container look for data/ under the repo
  # and package roots plus the opt-in $BUTTERFLY_TEST_DATA_DIR, and self-skip
  # when none of them is there — so this step needs no 24 GB artifact.
  "test (workspace, lib + integration)|cargo test --workspace"
  # #556: tests behind `feature = "bench"` (matrix/range internals, the bench
  # weight profiles) compile under clippy --all-features but NEVER executed:
  # the default-feature run above skips them. Run them, then PROVE the
  # all-features run is a strict superset of the default one — if a refactor
  # re-gates tests behind the feature or the feature stops pulling any in,
  # the count collapses and this step fails instead of silently shrinking CI.
  "test (all features)|cargo test --workspace --all-features"
  "test count (all-features > default)|a=\$(cargo test --workspace --all-features -- --list 2>/dev/null | grep -c ': test\$'); d=\$(cargo test --workspace -- --list 2>/dev/null | grep -c ': test\$'); echo \"tests: all-features=\$a default=\$d\"; [ \"\$a\" -gt \"\$d\" ]"
  # The post-deploy gate is Python: at minimum it must compile and be able to
  # enumerate its gates (catches an import-time or registry break in CI, where
  # no server is running).
  "post-deploy gate compiles|python3 -m py_compile bench/postdeploy_gate.py"
  "post-deploy gate registry|python3 bench/postdeploy_gate.py --list-gates"
  # #594: the gate's own offline unit tests — threshold derivation, refs
  # resolution, geometry, the memoised fetchers, the registry/probe parity
  # tables and the matrix-plan parsing (a missing or wrong reported plan must
  # FAIL). They existed but no runner executed them, so a break in the gate's
  # own logic reached a deploy before anyone looked.
  "post-deploy gate unit tests|python3 bench/test_postdeploy_gate.py"
)

if [[ "${1:-}" == "--list" ]]; then
  for s in "${STEPS[@]}"; do echo "${s%%|*}"; done
  exit 0
fi

PREFIX="${CI_STEP_PREFIX:-[ci]}"
for s in "${STEPS[@]}"; do
  label="${s%%|*}"
  cmd="${s#*|}"
  echo "$PREFIX → $label"
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then echo "::group::$label"; fi
  if ! bash -c "$cmd"; then
    if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then echo "::endgroup::"; fi
    echo "$PREFIX ✗ FAILED: $label ($cmd)"
    exit 1
  fi
  if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then echo "::endgroup::"; fi
done
echo "$PREFIX ✓ all checks passed"
