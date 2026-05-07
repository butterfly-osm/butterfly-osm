#!/usr/bin/env bash
# repro_check.sh
#
# Reproducibility verifier. Re-runs the SQL phases (extract → unify → pairs →
# stratify → sample) in a SCRATCH working dir using the existing PBF inputs,
# then diffs every output SHA-256 against the committed MANIFEST.json.
#
# Why "SQL only": redownloading 33 GB of PBFs is wasteful when reproducibility
# is about the build outputs being deterministic given fixed inputs, not about
# the network being deterministic. Input integrity is enforced separately by
# the lock file's SHA-256 entries.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WC_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$WC_DIR/../.." && pwd)"

ORIG_WORK="${REPO_ROOT}/data/world-corpus"
SCRATCH_WORK="${1:-${REPO_ROOT}/data/world-corpus-repro-check}"
MANIFEST="${ORIG_WORK}/MANIFEST.json"

[[ -f "$MANIFEST" ]] || { echo "FATAL: $MANIFEST missing — run build.sh first" >&2; exit 1; }
[[ -d "$ORIG_WORK/inputs" ]] || { echo "FATAL: $ORIG_WORK/inputs missing — no PBFs to reuse" >&2; exit 1; }

echo "[repro] scratch work dir: $SCRATCH_WORK"
rm -rf "$SCRATCH_WORK"
mkdir -p "$SCRATCH_WORK"
ln -s "$ORIG_WORK/inputs" "$SCRATCH_WORK/inputs"

echo "[repro] running build.sh phases extract..manifest in scratch"
WORK_DIR="$SCRATCH_WORK" "$WC_DIR/build.sh" extract
WORK_DIR="$SCRATCH_WORK" "$WC_DIR/build.sh" unify
WORK_DIR="$SCRATCH_WORK" "$WC_DIR/build.sh" pairs
WORK_DIR="$SCRATCH_WORK" "$WC_DIR/build.sh" stratify
WORK_DIR="$SCRATCH_WORK" "$WC_DIR/build.sh" sample
# don't run manifest phase in scratch — we'd overwrite the canonical TSVs in
# bench/geocode/queries/. Instead, hash directly here.

echo "[repro] diffing SHA-256s"
fail=0
total=0
while read -r path expected_sha; do
  total=$((total+1))
  abs="$REPO_ROOT/$path"
  # Outputs in data/world-corpus/* are now in scratch
  if [[ "$path" == data/world-corpus/* ]]; then
    abs="$SCRATCH_WORK/${path#data/world-corpus/}"
  fi
  if [[ ! -f "$abs" ]]; then
    echo "  MISSING: $path"
    fail=$((fail+1))
    continue
  fi
  actual="$(sha256sum "$abs" | awk '{print $1}')"
  if [[ "$actual" != "$expected_sha" ]]; then
    echo "  MISMATCH: $path"
    echo "    expected: $expected_sha"
    echo "    actual:   $actual"
    fail=$((fail+1))
  else
    echo "  OK: $path"
  fi
done < <(jq -r '.outputs[] | "\(.path)\t\(.sha256)"' "$MANIFEST" | tr '\t' ' ')

echo "[repro] $((total-fail))/$total artefacts match"
if [[ $fail -ne 0 ]]; then
  echo "[repro] FAIL — $fail mismatches" >&2
  exit 1
fi
echo "[repro] PASS"
