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
  "rustfmt --check|cargo fmt --all -- --check"
  "clippy (deny warnings)|cargo clippy --workspace --all-targets --all-features"
  "build (workspace)|cargo build --workspace"
  # --workspace, NOT --lib: route/tests/*.rs (24 data-free integration tests)
  # never ran before #555. The Belgium-container tests self-skip without
  # BT_*_CONTAINER, so this still needs no 24 GB artifact.
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
