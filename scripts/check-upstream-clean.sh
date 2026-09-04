#!/usr/bin/env bash
# ============================================================================
# Upstream-clean guard (MANDATORY pre-push + CI gate).
#
# butterfly-osm is a PUBLIC repository. Private infrastructure, deploy tooling,
# client project names and licensed data-provider names must NEVER appear here —
# they live in the private repos (butterfly-deploy, infra, butterfly-speeds) and
# the deploy-side contract. See CLAUDE.md "Repo Boundaries".
#
# This script fails (non-zero) if any forbidden term leaks into a tracked file,
# so a leak can never be pushed. It is wired into scripts/hooks/pre-push and CI.
# ============================================================================
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Tracked files only, minus:
#  - the historical benchmark archive (a dated record, left as-is)
#  - this script (it necessarily spells the forbidden terms)
mapfile -t FILES < <(
  git ls-files |
    grep -vE '^bench/route/results/|^scripts/check-upstream-clean\.sh$'
)

# competitive_landscape.md legitimately names market COMPETITORS (Google, HERE,
# TomTom, …) as public market analysis — exempt it from the provider check only.
mapfile -t FILES_NO_MKT < <(printf '%s\n' "${FILES[@]}" | grep -vE '^competitive_landscape\.md$')

fail=0
scan() {
  local label="$1" pat="$2"
  shift 2
  local hits
  hits=$(grep -niE "$pat" -- "$@" 2>/dev/null || true)
  if [[ -n "$hits" ]]; then
    echo "❌ upstream leak — $label:"
    echo "$hits" | sed 's/^/    /'
    fail=1
  fi
}

# Infra / deploy tooling / k8s / private-repo & client names — never legitimate
# in the open engine, scanned across ALL tracked files.
scan "infra / deploy / private repo / client" \
  'kubectl|argocd|registry\.lan|registry-prod|\bminio\b|\bmc pipe\b|staging\.lan|10\.0\.[0-9]+\.[0-9]+|artifact-info|hetzner|\bcanopus\b|\bmagellan\b|butterfly-deploy|butterfly-speeds|drivetimes-survey|pharmayou|traffic-flow|traffic_flow|webapp_client|sirius_map|\bnodeport\b' \
  "${FILES[@]}"

# Licensed data-provider names — competitive_landscape.md exempt (see above).
scan "data provider" \
  'tomtom|telraam|\bwaze\b' \
  "${FILES_NO_MKT[@]}"

# Commit MESSAGES are published too. Scan every commit that is not yet
# upstream; files alone let a message leak through on 2026-09-03.
#
# #592: the range must ALWAYS resolve. Keying it on @{upstream} alone made the
# scan a silent no-op on every branch without a tracking ref — a fresh feature
# branch, a worktree branch, anything not yet pushed — which is exactly where
# commit messages are written. The base is now the first of:
#   @{upstream}  →  origin/HEAD  →  origin/main  →  origin/master  →  main  →  master
# and when NONE of them resolves the script FAILS instead of printing OK.
msg_base=""
msg_range=""
if upstream=$(git rev-parse --verify --quiet '@{upstream}'); then
  msg_base="$upstream"
  msg_range="$(git rev-parse --abbrev-ref '@{upstream}')..HEAD"
else
  origin_head=$(git symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null || true)
  for ref in $origin_head origin/main origin/master main master; do
    if base=$(git merge-base HEAD "$ref" 2>/dev/null); then
      msg_base="$base"
      msg_range="$ref(merge-base)..HEAD"
      break
    fi
  done
fi
if [[ -z "$msg_base" ]]; then
  echo "❌ upstream guard cannot resolve a commit range to scan:"
  echo "    no @{upstream}, and no merge-base with origin/HEAD, origin/main,"
  echo "    origin/master, main or master. Refusing to report success on an"
  echo "    unscanned history (#592)."
  exit 1
fi
msg_hits=$(git log "$msg_base..HEAD" --format='%h %s%n%b' 2>/dev/null |
  grep -niE 'kubectl|argocd|registry\.lan|\bminio\b|staging\.lan|10\.0\.[0-9]+\.[0-9]+|butterfly-deploy|butterfly-speeds|drivetimes-survey|sirius_map|tomtom|telraam|\bwaze\b|s3://' || true)
if [[ -n "$msg_hits" ]]; then
  echo "❌ upstream leak — commit message(s) in $msg_range:"
  echo "$msg_hits" | sed 's/^/    /'
  fail=1
fi
msg_count=$(git rev-list --count "$msg_base..HEAD" 2>/dev/null || echo 0)
if [[ $fail -ne 0 ]]; then
  echo
  echo "This repo is PUBLIC. The above belongs in the private repos"
  echo "(butterfly-deploy / infra / butterfly-speeds), NOT in butterfly-osm."
  echo "Reword the leak (generic terms) or move it, then re-run."
  exit 1
fi
echo "✓ upstream clean — no infra / provider / client leaks" \
     "(files: ${#FILES[@]}; messages: $msg_range, $msg_count commit(s))"
