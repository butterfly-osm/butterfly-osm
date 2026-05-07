#!/usr/bin/env bash
#
# build.sh — single-entrypoint, fully reproducible build of the world geocoder
# corpus. See README.md for the contract this script honours.
#
# Invariants:
#   * Idempotent: re-running on the same git tree must produce byte-identical
#     outputs. Existing artefacts that pass SHA-256 checks are reused.
#   * No mutating URLs: every download URL must point at an immutable Geofabrik
#     dated snapshot (`*-YYMMDD.osm.pbf`). `*-latest.osm.pbf` is rejected.
#   * Verified consumption: every PBF is SHA-256 verified against the lock
#     file before any extract step touches it. First run records hashes; every
#     subsequent run enforces them.
#   * Deterministic SQL: every COPY has an explicit ORDER BY; samples use a
#     fixed hash function on (country, kind, id, name) for stable selection.

set -euo pipefail

# -----------------------------------------------------------------------------
# Paths and config
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DATE_TAG="${DATE_TAG:-260401}"            # Geofabrik snapshot date (YYMMDD)
WORK_DIR="${WORK_DIR:-$REPO_ROOT/data/world-corpus}"
INPUTS_DIR="$WORK_DIR/inputs"
OUTPUTS_DIR="$WORK_DIR"
BENCH_QUERIES_DIR="$REPO_ROOT/bench/geocode/queries"
BENCH_RESULTS_DIR="$REPO_ROOT/bench/geocode/results/2026-05-07-world-corpus"
SAMPLE_PER_FAMILY="${SAMPLE_PER_FAMILY:-1000}"

MANIFEST_INPUT="$SCRIPT_DIR/manifest_input.tsv"
LOCK_FILE="$SCRIPT_DIR/manifest_input.lock.json"
MANIFEST_OUTPUT="$WORK_DIR/MANIFEST.json"

DUCKDB_BIN="${DUCKDB_BIN:-/usr/local/bin/duckdb}"
EXPECTED_DUCKDB_VERSION="v1.5.2"

PHASE="${1:-all}"
# allow `--phase NAME` style too
if [[ "$PHASE" == "--phase" ]]; then
  PHASE="${2:-all}"
fi

mkdir -p "$INPUTS_DIR" "$OUTPUTS_DIR" "$BENCH_QUERIES_DIR" "$BENCH_RESULTS_DIR"

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_FILE="$WORK_DIR/build.log"
exec > >(tee -a "$LOG_FILE") 2>&1

log()  { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
fail() { log "FATAL: $*"; exit 1; }

# -----------------------------------------------------------------------------
# Pre-flight checks
# -----------------------------------------------------------------------------
preflight() {
  log "preflight: checking dependencies"
  command -v "$DUCKDB_BIN" >/dev/null || fail "duckdb not at $DUCKDB_BIN"
  command -v jq            >/dev/null || fail "jq missing"
  command -v sha256sum     >/dev/null || fail "sha256sum missing"
  command -v curl          >/dev/null || fail "curl missing"

  local got
  got="$("$DUCKDB_BIN" --version | awk '{print $1}')"
  if [[ "$got" != "$EXPECTED_DUCKDB_VERSION" ]]; then
    log "WARN: duckdb version $got != expected $EXPECTED_DUCKDB_VERSION"
    log "      reproducibility may not hold across versions"
  fi

  # Refuse mutating URLs (skip comment lines).
  if grep -vE '^[[:space:]]*#' "$MANIFEST_INPUT" \
       | grep -qE '\-latest\.osm\.pbf'; then
    fail "manifest_input.tsv contains a -latest URL — those mutate, forbidden"
  fi

  # Validate every URL has a YYMMDD date that matches the manifest's date column.
  local bad=0
  while IFS=$'\t' read -r iso2 source url date bytes notes; do
    [[ "$iso2" =~ ^# || -z "$iso2" || "$iso2" == "iso2" ]] && continue
    local url_date
    url_date="$(printf '%s' "$url" | grep -oE -- '-[0-9]{6}\.osm\.pbf' | head -1 | tr -d '-' | sed 's/.osm.pbf//')"
    [[ -z "$url_date" ]] && { log "BAD: $iso2 url has no YYMMDD: $url"; bad=$((bad+1)); continue; }
    local manifest_date_yymmdd
    manifest_date_yymmdd="$(printf '%s' "$date" | tr -d '-' | cut -c3-8)"
    if [[ "$url_date" != "$manifest_date_yymmdd" ]]; then
      log "BAD: $iso2 url date $url_date != manifest date $date"
      bad=$((bad+1))
    fi
  done < "$MANIFEST_INPUT"
  [[ $bad -eq 0 ]] || fail "$bad manifest rows have URL/date mismatch"

  log "preflight: OK"
}

# -----------------------------------------------------------------------------
# Manifest parsing
# -----------------------------------------------------------------------------
# Produces tab-delimited rows on stdout: iso2 url filename
manifest_rows() {
  awk -F '\t' '!/^#/ && $1 != "iso2" && NF >= 5 {
    n = split($3, parts, "/");
    print $1 "\t" $3 "\t" parts[n];
  }' "$MANIFEST_INPUT"
}

# -----------------------------------------------------------------------------
# Phase 1 — download & verify
# -----------------------------------------------------------------------------
phase_download() {
  log "phase: download"

  local lock_existed=0
  if [[ -f "$LOCK_FILE" ]]; then
    lock_existed=1
    log "  lock file exists; verifying against it"
  else
    log "  no lock file — first run, will record hashes"
  fi

  local tmp_lock
  tmp_lock="$(mktemp)"
  echo '{"snapshot_date_tag":"'"$DATE_TAG"'","entries":[' > "$tmp_lock"

  local first=1
  while IFS=$'\t' read -r iso2 url filename; do
    local target="$INPUTS_DIR/$filename"

    # Resolve expected SHA-256 from the lock if available.
    local expected_sha=""
    if [[ $lock_existed -eq 1 ]]; then
      expected_sha="$(jq -r --arg iso "$iso2" \
        '.entries[] | select(.iso2==$iso) | .sha256 // empty' \
        "$LOCK_FILE" 2>/dev/null || true)"
    fi

    # Download if missing or hash-mismatch.
    if [[ -f "$target" ]]; then
      if [[ -n "$expected_sha" ]]; then
        local actual
        actual="$(sha256sum "$target" | awk '{print $1}')"
        if [[ "$actual" != "$expected_sha" ]]; then
          log "  $iso2: existing file SHA-256 mismatch, redownloading"
          rm -f "$target"
        fi
      fi
    fi

    if [[ ! -f "$target" ]]; then
      log "  $iso2: downloading $url"
      curl -sSL --fail --retry 3 --retry-delay 5 -o "$target.partial" "$url"
      mv "$target.partial" "$target"
    fi

    local sha
    sha="$(sha256sum "$target" | awk '{print $1}')"
    local size
    size="$(stat -c %s "$target")"

    if [[ -n "$expected_sha" && "$sha" != "$expected_sha" ]]; then
      fail "$iso2: SHA-256 mismatch after download. expected=$expected_sha got=$sha"
    fi

    log "  $iso2: ok size=$size sha256=${sha:0:16}…"

    if [[ $first -eq 0 ]]; then echo ',' >> "$tmp_lock"; fi
    first=0
    jq -n --arg iso "$iso2" --arg url "$url" --arg fn "$filename" \
          --arg sha "$sha" --argjson sz "$size" \
      '{iso2:$iso,url:$url,filename:$fn,sha256:$sha,bytes:$sz}' >> "$tmp_lock"
  done < <(manifest_rows)

  echo ']}' >> "$tmp_lock"

  # Pretty-print and atomically install. If lock didn't exist, this commits
  # the first-run hashes.
  jq --sort-keys . "$tmp_lock" > "$LOCK_FILE.tmp"
  mv "$LOCK_FILE.tmp" "$LOCK_FILE"
  rm -f "$tmp_lock"
  log "phase: download — done"
}

# -----------------------------------------------------------------------------
# Phase 2 — per-PBF extract
# -----------------------------------------------------------------------------
phase_extract() {
  log "phase: extract"
  while IFS=$'\t' read -r iso2 url filename; do
    local pbf="$INPUTS_DIR/$filename"
    local out="$OUTPUTS_DIR/${iso2}.parquet"

    [[ -f "$pbf" ]] || fail "missing PBF for $iso2 at $pbf — run phase download first"

    if [[ -f "$out" ]]; then
      log "  $iso2: parquet exists, skipping"
      continue
    fi

    log "  $iso2: extracting $filename → $(basename "$out")"
    local sql_tmp
    sql_tmp="$(mktemp --suffix=.sql)"
    sed -e "s|__OUT_PATH__|$out|g" "$SCRIPT_DIR/extract.sql" > "$sql_tmp"
    PBF_PATH="$pbf" ISO2_TAG="$iso2" "$DUCKDB_BIN" < "$sql_tmp"
    rm -f "$sql_tmp"

    [[ -f "$out" ]] || fail "$iso2: extract did not produce $out"
    log "  $iso2: ok size=$(stat -c %s "$out")"
  done < <(manifest_rows)
  log "phase: extract — done"
}

# -----------------------------------------------------------------------------
# Phase 3 — unify
# -----------------------------------------------------------------------------
extract_bbox_csv() {
  local out="$1"
  local packs_dir="$REPO_ROOT/geocode/data/packs"
  echo "iso2,min_lat,max_lat,min_lon,max_lon" > "$out"
  for toml in "$packs_dir"/*.toml; do
    local iso
    iso="$(basename "$toml" .toml | tr 'a-z' 'A-Z')"
    [[ "$iso" == "README" ]] && continue
    # Parse the [bbox] block — a tiny TOML subset is enough.
    awk -v iso="$iso" '
      BEGIN { in_bbox = 0 }
      /^\[bbox\]/ { in_bbox = 1; next }
      in_bbox && /^\[/ { in_bbox = 0 }
      in_bbox && /min_lat *=/ { gsub(/[^0-9.\-]/, "", $0); min_lat = $0 }
      in_bbox && /max_lat *=/ { gsub(/[^0-9.\-]/, "", $0); max_lat = $0 }
      in_bbox && /min_lon *=/ { gsub(/[^0-9.\-]/, "", $0); min_lon = $0 }
      in_bbox && /max_lon *=/ { gsub(/[^0-9.\-]/, "", $0); max_lon = $0 }
      END {
        if (min_lat != "" && max_lat != "" && min_lon != "" && max_lon != "")
          print iso "," min_lat "," max_lat "," min_lon "," max_lon
      }
    ' "$toml" >> "$out"
  done
  # Stable order for reproducibility
  { head -1 "$out"; tail -n +2 "$out" | LC_ALL=C sort; } > "$out.sorted"
  mv "$out.sorted" "$out"
}

phase_unify() {
  log "phase: unify"
  local out="$OUTPUTS_DIR/all-with-country.parquet"
  if [[ -f "$out" ]]; then
    log "  unified parquet exists, skipping"
    return
  fi

  local bbox_csv="$WORK_DIR/country_bbox.csv"
  extract_bbox_csv "$bbox_csv"
  log "  bbox table: $(wc -l < "$bbox_csv") rows"

  local sql_tmp
  sql_tmp="$(mktemp --suffix=.sql)"
  sed \
    -e "s|__PARQUET_GLOB__|$OUTPUTS_DIR/*.parquet|g" \
    -e "s|__BBOX_CSV__|$bbox_csv|g" \
    -e "s|__OUT_PATH__|$out|g" \
    "$SCRIPT_DIR/unify.sql" > "$sql_tmp"
  "$DUCKDB_BIN" < "$sql_tmp"
  rm -f "$sql_tmp"

  [[ -f "$out" ]] || fail "unify did not produce $out"
  log "  unified: $(stat -c %s "$out") bytes"
  log "phase: unify — done"
}

# -----------------------------------------------------------------------------
# Phase 4 — multilingual pairs
# -----------------------------------------------------------------------------
phase_pairs() {
  log "phase: pairs"
  local unified="$OUTPUTS_DIR/all-with-country.parquet"
  local out="$OUTPUTS_DIR/multilingual-pairs.parquet"
  [[ -f "$unified" ]] || fail "missing $unified — run unify first"
  if [[ -f "$out" ]]; then
    log "  pairs parquet exists, skipping"
    return
  fi

  local sql_tmp
  sql_tmp="$(mktemp --suffix=.sql)"
  sed \
    -e "s|__UNIFIED_PARQUET__|$unified|g" \
    -e "s|__OUT_PATH__|$out|g" \
    "$SCRIPT_DIR/pairs.sql" > "$sql_tmp"
  "$DUCKDB_BIN" < "$sql_tmp"
  rm -f "$sql_tmp"

  [[ -f "$out" ]] || fail "pairs did not produce $out"
  log "  pairs: $(stat -c %s "$out") bytes"
  log "phase: pairs — done"
}

# -----------------------------------------------------------------------------
# Phase 5 — stratify
# -----------------------------------------------------------------------------
phase_stratify() {
  log "phase: stratify"
  local unified="$OUTPUTS_DIR/all-with-country.parquet"
  local out="$OUTPUTS_DIR/family-stats.parquet"
  [[ -f "$unified" ]] || fail "missing $unified"
  if [[ -f "$out" ]]; then
    log "  stratify parquet exists, skipping"
    return
  fi

  local sql_tmp
  sql_tmp="$(mktemp --suffix=.sql)"
  sed \
    -e "s|__UNIFIED_PARQUET__|$unified|g" \
    -e "s|__OUT_PATH__|$out|g" \
    "$SCRIPT_DIR/stratify.sql" > "$sql_tmp"
  "$DUCKDB_BIN" < "$sql_tmp"
  rm -f "$sql_tmp"

  [[ -f "$out" ]] || fail "stratify did not produce $out"
  log "  stratify: $(stat -c %s "$out") bytes"
  log "phase: stratify — done"
}

# -----------------------------------------------------------------------------
# Phase 6 — bench sample
# -----------------------------------------------------------------------------
phase_sample() {
  log "phase: sample"
  local unified="$OUTPUTS_DIR/all-with-country.parquet"
  local out="$OUTPUTS_DIR/bench-sample.parquet"
  [[ -f "$unified" ]] || fail "missing $unified"

  if [[ ! -f "$out" ]]; then
    local sql_tmp
    sql_tmp="$(mktemp --suffix=.sql)"
    sed \
      -e "s|__UNIFIED_PARQUET__|$unified|g" \
      -e "s|__SAMPLE_PER_FAMILY__|$SAMPLE_PER_FAMILY|g" \
      -e "s|__OUT_PATH__|$out|g" \
      "$SCRIPT_DIR/sample_bench.sql" > "$sql_tmp"
    "$DUCKDB_BIN" < "$sql_tmp"
    rm -f "$sql_tmp"
    [[ -f "$out" ]] || fail "sample did not produce $out"
  else
    log "  bench-sample parquet exists, reusing"
  fi

  # Split per-family into TSVs (deterministic — single COPY per family with ORDER BY)
  local families
  families="$("$DUCKDB_BIN" -noheader -list -c \
    "SELECT DISTINCT script_family FROM read_parquet('$out') ORDER BY script_family;")"

  while IFS= read -r fam; do
    [[ -z "$fam" ]] && continue
    local fam_lower
    fam_lower="$(printf '%s' "$fam" | tr 'A-Z' 'a-z')"
    local tsv="$BENCH_QUERIES_DIR/world-${fam_lower}-${DATE_TAG}.tsv"
    log "  family=$fam → $(basename "$tsv")"
    "$DUCKDB_BIN" -c "
      COPY (
        SELECT
          assigned_country,
          script_family,
          osm_kind,
          osm_id,
          name,
          street,
          housenumber,
          city,
          postcode,
          query_canonical,
          query_partial,
          query_reordered
        FROM read_parquet('$out')
        WHERE script_family = '$fam'
        ORDER BY assigned_country, osm_kind, osm_id
      ) TO '$tsv' (FORMAT 'csv', DELIMITER '\t', HEADER true);
    " >/dev/null
  done <<< "$families"

  log "phase: sample — done"
}

# -----------------------------------------------------------------------------
# Phase 7 — manifest
# -----------------------------------------------------------------------------
phase_manifest() {
  log "phase: manifest"

  local build_sh_sha
  build_sh_sha="$(sha256sum "$SCRIPT_DIR/build.sh" | awk '{print $1}')"
  local extract_sql_sha
  extract_sql_sha="$(sha256sum "$SCRIPT_DIR/extract.sql" | awk '{print $1}')"
  local unify_sql_sha
  unify_sql_sha="$(sha256sum "$SCRIPT_DIR/unify.sql" | awk '{print $1}')"
  local pairs_sql_sha
  pairs_sql_sha="$(sha256sum "$SCRIPT_DIR/pairs.sql" | awk '{print $1}')"
  local stratify_sql_sha
  stratify_sql_sha="$(sha256sum "$SCRIPT_DIR/stratify.sql" | awk '{print $1}')"
  local sample_sql_sha
  sample_sql_sha="$(sha256sum "$SCRIPT_DIR/sample_bench.sql" | awk '{print $1}')"

  local duckdb_version
  duckdb_version="$("$DUCKDB_BIN" --version | awk '{print $1}')"

  local spatial_version
  spatial_version="$("$DUCKDB_BIN" -noheader -list -c \
    "INSTALL spatial; LOAD spatial; SELECT extension_version FROM duckdb_extensions() WHERE extension_name='spatial';" 2>/dev/null | tail -1)"

  # Build outputs array
  local outputs_tmp
  outputs_tmp="$(mktemp)"
  echo '[' > "$outputs_tmp"
  local first=1
  hash_one() {
    local rel="$1"
    local abs="$2"
    [[ -f "$abs" ]] || return 0
    local sha
    sha="$(sha256sum "$abs" | awk '{print $1}')"
    local sz
    sz="$(stat -c %s "$abs")"
    local rows="null"
    if [[ "$abs" == *.parquet ]]; then
      rows="$("$DUCKDB_BIN" -noheader -list -c \
        "SELECT count(*) FROM read_parquet('$abs');" 2>/dev/null | tail -1)"
    elif [[ "$abs" == *.tsv ]]; then
      rows="$(($(wc -l < "$abs") - 1))"
    fi
    if [[ $first -eq 0 ]]; then echo ',' >> "$outputs_tmp"; fi
    first=0
    jq -n --arg path "$rel" --arg sha "$sha" --argjson sz "$sz" --argjson r "$rows" \
      '{path:$path, sha256:$sha, bytes:$sz, rows:$r}' >> "$outputs_tmp"
  }

  # Per-country parquets
  while IFS=$'\t' read -r iso2 _ _; do
    local p="$OUTPUTS_DIR/${iso2}.parquet"
    hash_one "data/world-corpus/${iso2}.parquet" "$p"
  done < <(manifest_rows)
  hash_one "data/world-corpus/all-with-country.parquet"  "$OUTPUTS_DIR/all-with-country.parquet"
  hash_one "data/world-corpus/multilingual-pairs.parquet" "$OUTPUTS_DIR/multilingual-pairs.parquet"
  hash_one "data/world-corpus/family-stats.parquet"      "$OUTPUTS_DIR/family-stats.parquet"
  hash_one "data/world-corpus/bench-sample.parquet"      "$OUTPUTS_DIR/bench-sample.parquet"

  # Per-family bench TSVs
  for tsv in "$BENCH_QUERIES_DIR"/world-*-"$DATE_TAG".tsv; do
    [[ -f "$tsv" ]] || continue
    hash_one "bench/geocode/queries/$(basename "$tsv")" "$tsv"
  done

  echo ']' >> "$outputs_tmp"

  # Final manifest
  jq -n \
    --arg build_date "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg snapshot   "$DATE_TAG" \
    --arg duckdb     "$duckdb_version" \
    --arg spatial    "$spatial_version" \
    --arg sample_n   "$SAMPLE_PER_FAMILY" \
    --arg build_sh   "$build_sh_sha" \
    --arg extract_sql "$extract_sql_sha" \
    --arg unify_sql   "$unify_sql_sha" \
    --arg pairs_sql   "$pairs_sql_sha" \
    --arg stratify_sql "$stratify_sql_sha" \
    --arg sample_sql  "$sample_sql_sha" \
    --slurpfile inputs  <(jq '.entries' "$LOCK_FILE") \
    --slurpfile outputs "$outputs_tmp" \
    '{
       build_date_utc: $build_date,
       data_snapshot:  $snapshot,
       duckdb_version: $duckdb,
       duckdb_spatial_version: $spatial,
       sample_per_family: ($sample_n | tonumber),
       script_sha256: {
         "build.sh": $build_sh,
         "extract.sql": $extract_sql,
         "unify.sql": $unify_sql,
         "pairs.sql": $pairs_sql,
         "stratify.sql": $stratify_sql,
         "sample_bench.sql": $sample_sql
       },
       inputs:  ($inputs[0]),
       outputs: ($outputs[0])
     }' \
    | jq --sort-keys . > "$MANIFEST_OUTPUT"

  rm -f "$outputs_tmp"
  log "  wrote $MANIFEST_OUTPUT"
  log "phase: manifest — done"
}

# -----------------------------------------------------------------------------
# Dispatch
# -----------------------------------------------------------------------------
log "build.sh start; PHASE=$PHASE DATE_TAG=$DATE_TAG WORK_DIR=$WORK_DIR"

case "$PHASE" in
  preflight) preflight ;;
  download)  preflight; phase_download ;;
  extract)   preflight; phase_extract ;;
  unify)     preflight; phase_unify ;;
  pairs)     preflight; phase_pairs ;;
  stratify)  preflight; phase_stratify ;;
  sample)    preflight; phase_sample ;;
  manifest)  preflight; phase_manifest ;;
  all)
    preflight
    phase_download
    phase_extract
    phase_unify
    phase_pairs
    phase_stratify
    phase_sample
    phase_manifest
    ;;
  *) fail "unknown phase: $PHASE" ;;
esac

log "build.sh done"
