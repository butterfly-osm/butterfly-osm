#!/usr/bin/env bash
# ============================================================================
# Regenerate (or verify) `Dockerfile.tools` — the deprecated shim kept for one
# release so deploy tooling outside this repo that still passes
# `-f Dockerfile.tools` keeps building the same tools image (#573).
#
# Dockerfile syntax has no `include`, so a second file that must build
# standalone cannot literally reference the first one's stages. Rather than
# maintain two hand-written copies — the exact drift #573 is about — the shim
# is DERIVED: it is `Dockerfile` truncated just before the `runtime` stage, so
# `tools` becomes the last (default) stage. One source of truth; the copy is
# mechanical and CI proves it is current.
#
# Usage: bash scripts/gen-dockerfile-tools.sh          # write the shim
#        bash scripts/gen-dockerfile-tools.sh --check  # fail if out of date
# ============================================================================
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

MARKER='# >>> runtime stage'
SRC=Dockerfile
DST=Dockerfile.tools

grep -q "^$MARKER" "$SRC" || {
    echo "$SRC has no '$MARKER' line — cannot derive $DST" >&2
    exit 1
}

# The `# syntax=` parser directive must be the very first line of a Dockerfile
# — before any comment — so it is re-emitted ahead of the generated header and
# dropped from the copied body. It is pinned once, in `Dockerfile`.
SYNTAX_LINE="$(head -n 1 "$SRC")"
case "$SYNTAX_LINE" in
    '# syntax='*) ;;
    *) echo "$SRC must start with a '# syntax=' parser directive" >&2; exit 1 ;;
esac

render() {
    echo "$SYNTAX_LINE"
    cat <<'HEADER'
# GENERATED FILE — DO NOT EDIT BY HAND.
#
# DEPRECATED (#573). The tools image is a target of the single `Dockerfile`:
#
#     docker build --target tools -t <tag> .
#
# This shim is kept for one release so deploy tooling that still passes
# `-f Dockerfile.tools` keeps working. It is `Dockerfile` with the `runtime`
# stage removed, produced by `scripts/gen-dockerfile-tools.sh` and checked by
# CI, so it cannot drift from the real manifest. Edit `Dockerfile`, then run
# that script.

HEADER
    sed -e '1d' -e "/^$MARKER/,\$d" "$SRC"
}

if [[ "${1:-}" == "--check" ]]; then
    if ! diff -u <(render) "$DST"; then
        echo "$DST is out of date — run: bash scripts/gen-dockerfile-tools.sh" >&2
        exit 1
    fi
    echo "$DST is in sync with $SRC"
else
    render > "$DST"
    echo "wrote $DST"
fi
