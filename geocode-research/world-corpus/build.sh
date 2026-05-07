#!/usr/bin/env bash
#
# build.sh — single-entrypoint, locally-reproducible build of the world
# geocoder corpus. See README.md for the full contract.
#
# Invariants (per #225 brief):
#   * butterfly-dl is the ONLY downloader. No curl, no wget.
#   * DuckDB is the ONLY OSM extractor. No custom Rust parsers in this dir.
#   * Local SHA-256 sidecars (written by butterfly-dl's verified.rs) are the
#     reproducibility primitive. Latest-snapshot URLs are accepted because the
#     sidecar locks the bytes you actually consumed.
#   * Every COPY ... TO has an explicit ORDER BY for deterministic parquet.
#   * Idempotent: re-running on the same git tree with the same input PBFs
#     must produce byte-identical parquet outputs and bench TSVs.

set -euo pipefail

# -----------------------------------------------------------------------------
# Paths and config
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DATE_TAG="${DATE_TAG:-260507}"            # human-readable build tag (YYMMDD)
WORK_DIR="${WORK_DIR:-$REPO_ROOT/data/world-corpus}"
INPUTS_DIR="$WORK_DIR/inputs"
OUTPUTS_DIR="$WORK_DIR"
BENCH_QUERIES_DIR="$REPO_ROOT/bench/geocode/queries"
BENCH_RESULTS_DIR="$REPO_ROOT/bench/geocode/results/2026-05-07-world-corpus"
SAMPLE_PER_FAMILY="${SAMPLE_PER_FAMILY:-1000}"

MANIFEST_INPUT="$SCRIPT_DIR/manifest_input.tsv"
MANIFEST_OUTPUT="$WORK_DIR/MANIFEST.json"

DUCKDB_BIN="${DUCKDB_BIN:-/usr/local/bin/duckdb}"
EXPECTED_DUCKDB_VERSION="v1.5.2"

BUTTERFLY_DL_BIN="${BUTTERFLY_DL_BIN:-$REPO_ROOT/target/release/butterfly-dl}"

# Max parallel downloads (Geofabrik politeness ceiling).
DL_PARALLEL="${DL_PARALLEL:-3}"

PHASE="${1:-all}"
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
# Pre-flight
# -----------------------------------------------------------------------------
preflight() {
  log "preflight: checking dependencies"
  command -v "$DUCKDB_BIN"      >/dev/null || fail "duckdb not at $DUCKDB_BIN"
  command -v jq                 >/dev/null || fail "jq missing"
  command -v sha256sum          >/dev/null || fail "sha256sum missing"
  [[ -x "$BUTTERFLY_DL_BIN" ]] || fail "butterfly-dl not at $BUTTERFLY_DL_BIN — run 'cargo build --release -p butterfly-dl'"

  local got
  got="$("$DUCKDB_BIN" --version | awk '{print $1}')"
  if [[ "$got" != "$EXPECTED_DUCKDB_VERSION" ]]; then
    log "WARN: duckdb version $got != expected $EXPECTED_DUCKDB_VERSION"
    log "      reproducibility may not hold across versions"
  fi

  log "preflight: OK"
}

# -----------------------------------------------------------------------------
# Manifest parsing — emits TSV rows: iso2 \t geofabrik_path \t pbf_filename
# -----------------------------------------------------------------------------
manifest_rows() {
  awk -F '\t' '!/^#/ && $1 != "iso2" && NF >= 3 { print $1 "\t" $2 "\t" $3 }' "$MANIFEST_INPUT"
}

# -----------------------------------------------------------------------------
# Phase: download (butterfly-dl, parallel, sidecar-verified)
# -----------------------------------------------------------------------------
phase_download() {
  log "phase: download (butterfly-dl, parallel=$DL_PARALLEL)"

  local pids=()
  local todo=()

  while IFS=$'\t' read -r iso2 path filename; do
    local target="$INPUTS_DIR/$filename"
    local sidecar="$target.sha256"
    if [[ -f "$target" && -f "$sidecar" ]]; then
      log "  $iso2: already present (sidecar present), skipping"
      continue
    fi
    todo+=("$iso2|$path|$target")
  done < <(manifest_rows)

  if [[ ${#todo[@]} -eq 0 ]]; then
    log "  all PBFs already on disk with sidecars"
    return
  fi

  log "  downloading ${#todo[@]} PBF(s)"

  for entry in "${todo[@]}"; do
    local iso2="${entry%%|*}"
    local rest="${entry#*|}"
    local path="${rest%%|*}"
    local target="${rest##*|}"

    # Throttle to DL_PARALLEL
    while (( ${#pids[@]} >= DL_PARALLEL )); do
      local new_pids=()
      for p in "${pids[@]}"; do
        if kill -0 "$p" 2>/dev/null; then
          new_pids+=("$p")
        else
          wait "$p" || fail "background download $p failed"
        fi
      done
      pids=("${new_pids[@]}")
      [[ ${#pids[@]} -ge $DL_PARALLEL ]] && sleep 1
    done

    log "  $iso2: butterfly-dl $path → $(basename "$target")"
    (
      # Retry up to 5 times with exponential backoff for 429/5xx (Geofabrik 529 etc.)
      attempt=0
      max_attempts=5
      delay=10
      until "$BUTTERFLY_DL_BIN" "$path" "$target" --force >>"$LOG_FILE.dl-$iso2" 2>&1; do
        attempt=$((attempt+1))
        if [[ $attempt -ge $max_attempts ]]; then
          echo "FATAL: butterfly-dl $iso2 failed after $attempt attempts"
          tail -50 "$LOG_FILE.dl-$iso2"
          exit 1
        fi
        echo "[retry] $iso2: attempt $attempt failed, sleeping ${delay}s" >> "$LOG_FILE.dl-$iso2"
        sleep "$delay"
        delay=$((delay*2))
      done
      # butterfly-dl path-shaped fetches (e.g. africa/kenya) do not write
      # the .sha256 sidecar (only region-name-shaped or known-extension
      # callers go through verified::download_verified). Compute the
      # sidecar here so the rest of the pipeline has its reproducibility
      # primitive.
      if [[ ! -f "$target.sha256" ]]; then
        sha256sum "$target" | awk -v fn="$(basename "$target")" '{print $1"  "fn}' > "$target.sha256"
      fi
      [[ -f "$target.sha256" ]] || { echo "FATAL: $iso2 missing sidecar after download"; exit 1; }
    ) &
    pids+=("$!")
  done

  for p in "${pids[@]}"; do
    wait "$p" || fail "background download $p failed"
  done

  # Final sidecar audit.
  local missing=0
  while IFS=$'\t' read -r iso2 path filename; do
    [[ -f "$INPUTS_DIR/$filename.sha256" ]] || { log "  MISSING sidecar: $iso2 ($filename)"; missing=$((missing+1)); }
  done < <(manifest_rows)
  [[ $missing -eq 0 ]] || fail "$missing PBF(s) missing SHA-256 sidecars"

  log "phase: download — done"
}

# -----------------------------------------------------------------------------
# Phase: extract (DuckDB, per-PBF, parallel-safe via per-row temp SQL)
# -----------------------------------------------------------------------------
phase_extract() {
  log "phase: extract"
  while IFS=$'\t' read -r iso2 path filename; do
    local pbf="$INPUTS_DIR/$filename"
    local sidecar="$pbf.sha256"
    local out="$OUTPUTS_DIR/${iso2}.parquet"

    [[ -f "$pbf" ]]     || fail "missing PBF for $iso2 at $pbf — run phase download first"
    [[ -f "$sidecar" ]] || fail "missing sidecar for $iso2 at $sidecar"

    # Verify sidecar matches the file on disk before consumption.
    local expected actual
    expected="$(awk '{print $1}' "$sidecar")"
    actual="$(sha256sum "$pbf" | awk '{print $1}')"
    if [[ "$expected" != "$actual" ]]; then
      fail "$iso2: SHA-256 mismatch — sidecar=$expected actual=$actual"
    fi

    if [[ -f "$out" ]]; then
      log "  $iso2: parquet exists, skipping (sha256 verified ${expected:0:12}…)"
      continue
    fi

    log "  $iso2: extracting $filename → $(basename "$out") (sha256 ${expected:0:12}…)"
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
# Phase: unify (cross-country country assignment via bbox)
# -----------------------------------------------------------------------------
extract_bbox_csv() {
  local out="$1"
  local packs_dir="$REPO_ROOT/geocode/data/packs"
  echo "iso2,min_lat,max_lat,min_lon,max_lon" > "$out"
  for toml in "$packs_dir"/*.toml; do
    local iso
    iso="$(basename "$toml" .toml | tr 'a-z' 'A-Z')"
    [[ "$iso" == "README" ]] && continue
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
  if [[ -f "$SCRIPT_DIR/extra_bboxes.csv" ]]; then
    tail -n +2 "$SCRIPT_DIR/extra_bboxes.csv" >> "$out"
  fi
  { head -1 "$out"; tail -n +2 "$out" | LC_ALL=C sort -u; } > "$out.sorted"
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
# Phase: pairs (multilingual)
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
# Phase: stratify (per script family)
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
# Phase: bench sample
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
          query_id,
          query_text,
          gold_lat,
          gold_lon,
          quality_class,
          assigned_country,
          script_family,
          query_form,
          osm_kind,
          osm_id,
          name
        FROM read_parquet('$out')
        WHERE script_family = '$fam'
        ORDER BY query_form, osm_kind, osm_id
      ) TO '$tsv' (FORMAT 'csv', DELIMITER '\t', HEADER true);
    " >/dev/null
  done <<< "$families"

  log "phase: sample — done"
}

# -----------------------------------------------------------------------------
# Phase: manifest
# -----------------------------------------------------------------------------
phase_manifest() {
  log "phase: manifest"

  local build_sh_sha extract_sql_sha unify_sql_sha pairs_sql_sha stratify_sql_sha sample_sql_sha
  build_sh_sha="$(sha256sum "$SCRIPT_DIR/build.sh" | awk '{print $1}')"
  extract_sql_sha="$(sha256sum "$SCRIPT_DIR/extract.sql" | awk '{print $1}')"
  unify_sql_sha="$(sha256sum "$SCRIPT_DIR/unify.sql" | awk '{print $1}')"
  pairs_sql_sha="$(sha256sum "$SCRIPT_DIR/pairs.sql" | awk '{print $1}')"
  stratify_sql_sha="$(sha256sum "$SCRIPT_DIR/stratify.sql" | awk '{print $1}')"
  sample_sql_sha="$(sha256sum "$SCRIPT_DIR/sample_bench.sql" | awk '{print $1}')"

  local duckdb_version spatial_version
  duckdb_version="$("$DUCKDB_BIN" --version | awk '{print $1}')"
  spatial_version="$("$DUCKDB_BIN" -noheader -list -c \
    "INSTALL spatial; LOAD spatial; SELECT extension_version FROM duckdb_extensions() WHERE extension_name='spatial';" 2>/dev/null | tail -1)"

  # Inputs: read sidecars from disk (the lock IS the sidecar).
  local inputs_tmp
  inputs_tmp="$(mktemp)"
  echo '[' > "$inputs_tmp"
  local first=1
  while IFS=$'\t' read -r iso2 path filename; do
    local pbf="$INPUTS_DIR/$filename"
    local sidecar="$pbf.sha256"
    [[ -f "$pbf" && -f "$sidecar" ]] || continue
    local sha sz
    sha="$(awk '{print $1}' "$sidecar")"
    sz="$(stat -c %s "$pbf")"
    if [[ $first -eq 0 ]]; then echo ',' >> "$inputs_tmp"; fi
    first=0
    jq -n --arg iso "$iso2" --arg path "$path" --arg fn "$filename" \
          --arg sha "$sha" --argjson sz "$sz" \
      '{iso2:$iso, geofabrik_path:$path, filename:$fn, sha256:$sha, bytes:$sz}' >> "$inputs_tmp"
  done < <(manifest_rows)
  echo ']' >> "$inputs_tmp"

  # Outputs.
  local outputs_tmp
  outputs_tmp="$(mktemp)"
  echo '[' > "$outputs_tmp"
  first=1
  hash_one() {
    local rel="$1"
    local abs="$2"
    [[ -f "$abs" ]] || return 0
    local sha sz rows="null"
    sha="$(sha256sum "$abs" | awk '{print $1}')"
    sz="$(stat -c %s "$abs")"
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

  while IFS=$'\t' read -r iso2 _ _; do
    hash_one "data/world-corpus/${iso2}.parquet" "$OUTPUTS_DIR/${iso2}.parquet"
  done < <(manifest_rows)
  hash_one "data/world-corpus/all-with-country.parquet"   "$OUTPUTS_DIR/all-with-country.parquet"
  hash_one "data/world-corpus/multilingual-pairs.parquet" "$OUTPUTS_DIR/multilingual-pairs.parquet"
  hash_one "data/world-corpus/family-stats.parquet"       "$OUTPUTS_DIR/family-stats.parquet"
  hash_one "data/world-corpus/bench-sample.parquet"       "$OUTPUTS_DIR/bench-sample.parquet"

  for tsv in "$BENCH_QUERIES_DIR"/world-*-"$DATE_TAG".tsv; do
    [[ -f "$tsv" ]] || continue
    hash_one "bench/geocode/queries/$(basename "$tsv")" "$tsv"
  done

  echo ']' >> "$outputs_tmp"

  jq -n \
    --arg build_date  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg snapshot    "$DATE_TAG" \
    --arg duckdb      "$duckdb_version" \
    --arg spatial     "$spatial_version" \
    --arg sample_n    "$SAMPLE_PER_FAMILY" \
    --arg build_sh    "$build_sh_sha" \
    --arg extract_sql "$extract_sql_sha" \
    --arg unify_sql   "$unify_sql_sha" \
    --arg pairs_sql   "$pairs_sql_sha" \
    --arg stratify_sql "$stratify_sql_sha" \
    --arg sample_sql  "$sample_sql_sha" \
    --slurpfile inputs  "$inputs_tmp" \
    --slurpfile outputs "$outputs_tmp" \
    '{
       build_date_utc: $build_date,
       data_snapshot:  $snapshot,
       downloader: "butterfly-dl",
       reproducibility_primitive: "local SHA-256 sidecars (butterfly-dl verified.rs)",
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

  rm -f "$inputs_tmp" "$outputs_tmp"
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
