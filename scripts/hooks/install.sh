#!/bin/bash
# Install the local-CI git hooks for this clone. Idempotent.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
ln -sf ../../scripts/hooks/pre-push "$ROOT/.git/hooks/pre-push"
echo "installed: .git/hooks/pre-push -> scripts/hooks/pre-push"
echo "(skip once with BUTTERFLY_NO_VERIFY=1 git push)"
