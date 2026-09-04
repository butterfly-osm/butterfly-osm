#!/usr/bin/env bash
# ============================================================================
# Tests for the upstream-clean guard itself (#592).
#
# The guard is the only thing standing between a private name and this PUBLIC
# repo's permanent history, so it needs its own tests: on 2026-09-04 the
# commit-message half was found to be a silent no-op on every branch without a
# tracking ref — and it still printed its success line.
#
# Each case builds a throwaway git repo, drops the REAL guard script into it,
# and asserts on the exit status and on the output.
#
# The forbidden token is ASSEMBLED AT RUN TIME (never spelled literally in this
# file) so that this test file is itself clean under the guard it tests.
# ============================================================================
set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
GUARD="$REPO_ROOT/scripts/check-upstream-clean.sh"
[[ -f "$GUARD" ]] || {
  echo "guard script not found at $GUARD"
  exit 1
}

TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT

# The k8s CLI name — one of the terms the guard forbids. Assembled from two
# fragments so the literal never appears in this file.
FORBIDDEN="kube$(printf 'ctl')"

pass=0
fail=0
check() { # check <name> <0=ok|1=ko> <detail>
  if [[ "$2" == "0" ]]; then
    echo "  ✓ $1"
    pass=$((pass + 1))
  else
    echo "  ✗ $1"
    echo "$3" | sed 's/^/      /'
    fail=$((fail + 1))
  fi
}

# A repo with NO remote and NO tracking ref: main, then a feature branch
# checked out. Echoes the repo path.
make_repo() {
  local dir="$TMPROOT/$1"
  mkdir -p "$dir/scripts"
  git -C "$dir" init -q -b main
  git -C "$dir" config user.email t@example.invalid
  git -C "$dir" config user.name Test
  cp "$GUARD" "$dir/scripts/check-upstream-clean.sh"
  echo "clean content" >"$dir/README.md"
  git -C "$dir" add -A
  git -C "$dir" commit -q -m "chore: seed"
  git -C "$dir" checkout -q -b feature
  echo "$dir"
}

echo "[guard-test] 1/4 seeded token in a commit message, branch with no upstream → must FAIL"
d=$(make_repo case1)
echo "more clean content" >"$d/other.txt"
git -C "$d" add -A
git -C "$d" commit -q -m "chore: ran $FORBIDDEN on the cluster"
out=$(cd "$d" && bash scripts/check-upstream-clean.sh 2>&1)
rc=$?
[[ $rc -ne 0 ]] && r=0 || r=1
check "exits non-zero on a leaked commit message" "$r" "$out"
grep -qi 'commit message' <<<"$out" && r=0 || r=1
check "names the commit-message scan in the failure" "$r" "$out"

echo "[guard-test] 2/4 clean branch with no upstream → must PASS and NAME the range"
d=$(make_repo case2)
echo "more clean content" >"$d/other.txt"
git -C "$d" add -A
git -C "$d" commit -q -m "feat: something harmless"
out=$(cd "$d" && bash scripts/check-upstream-clean.sh 2>&1)
rc=$?
[[ $rc -eq 0 ]] && r=0 || r=1
check "exits zero when nothing leaks" "$r" "$out"
# The success line must name a non-empty range AND the count actually scanned,
# so "scanned nothing" can never masquerade as a green check.
range=$(sed -n 's/.*messages: \([^,]*\), \([0-9]*\) commit.*/\1|\2/p' <<<"$out")
[[ -n "${range%%|*}" ]] && r=0 || r=1
check "success line names a non-empty range" "$r" "$out"
[[ "${range##*|}" == "1" ]] && r=0 || r=1
check "reports the 1 commit actually scanned (not 0)" "$r" "$out"

echo "[guard-test] 3/4 leak in a tracked FILE still fails (no regression)"
d=$(make_repo case3)
echo "we run $FORBIDDEN here" >"$d/other.txt"
git -C "$d" add -A
git -C "$d" commit -q -m "chore: harmless subject"
out=$(cd "$d" && bash scripts/check-upstream-clean.sh 2>&1)
rc=$?
[[ $rc -ne 0 ]] && r=0 || r=1
check "exits non-zero on a leaked file" "$r" "$out"

echo "[guard-test] 4/4 no resolvable base at all → must FAIL, never print OK"
d=$(make_repo case4)
echo "more clean content" >"$d/other.txt"
git -C "$d" add -A
git -C "$d" commit -q -m "feat: something harmless"
# Orphan history with every default-branch ref deleted: nothing to diff against.
git -C "$d" checkout -q --orphan lonely
git -C "$d" commit -q -m "feat: orphan root"
git -C "$d" branch -q -D main feature
out=$(cd "$d" && bash scripts/check-upstream-clean.sh 2>&1)
rc=$?
[[ $rc -ne 0 ]] && r=0 || r=1
check "exits non-zero when no commit range can be resolved" "$r" "$out"
grep -q '✓ upstream clean' <<<"$out" && r=1 || r=0
check "never prints the success line on an unscanned history" "$r" "$out"

echo "[guard-test] $pass passed, $fail failed"
[[ $fail -eq 0 ]]
