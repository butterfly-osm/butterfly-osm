#!/bin/bash
# Line/region coverage for the in-process unit tests (cargo-llvm-cov).
# The Belgium-container integration tests are env-gated and excluded here,
# so this is the coverage of the logic that runs WITHOUT the 24 GB artifact.
# Usage: scripts/coverage.sh [--html] [--fail-under N]
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
FAIL_UNDER=""; HTML=""
while [ $# -gt 0 ]; do case "$1" in
  --html) HTML="--html";; --fail-under) FAIL_UNDER="--fail-under-lines $2"; shift;;
esac; shift; done
# shellcheck disable=SC2086
cargo llvm-cov -p butterfly-route -p butterfly-dl -p butterfly-common --lib \
  --summary-only $HTML $FAIL_UNDER
