-- unify.sql
--
-- Reads every per-country parquet under $PARQUET_DIR (one row per OSM feature),
-- assigns each feature to a destination country (via addr:country if present,
-- else via bbox lookup), and writes a single deduplicated unified parquet.
--
-- Inputs:
--   $PARQUET_GLOB — sed-replaced glob pattern matching every per-country parquet,
--                   e.g. /abs/path/world-corpus/*.parquet
--   $BBOX_CSV     — sed-replaced absolute path to a 5-column CSV
--                   (iso2, min_lat, max_lat, min_lon, max_lon) extracted by
--                   build.sh from geocode/data/packs/*.toml.
--   __OUT_PATH__  — sed-replaced output parquet path
--
-- Determinism:
--   * No randomness, no threading-sensitive aggregation that breaks ordering.
--   * Final ORDER BY (assigned_country, osm_kind, osm_id) is stable.

LOAD spatial;
-- 224M-row union with bbox classification. Single-threaded with no
-- top-level ORDER BY to avoid the global mergesort that triggers
-- multi-hundred-GB spills on RAM-constrained boxes. Determinism
-- comes from the input contract:
--   * read_parquet(glob) iterates in lexicographic glob order
--   * each per-country input parquet is already ORDER BY (osm_kind, osm_id)
--     (extract.sql guarantees this)
--   * single-threaded execution preserves that order through the
--     classified CTE and the LEFT JOIN
-- Result: byte-identical output across runs given byte-identical inputs,
-- without paying for a 224M-row global sort.
SET memory_limit = '8GB';
SET threads = 1;
-- temp_directory defaults to <working_dir>/.tmp/duckdb_*; that is fine.
-- (We override it on RAM-constrained boxes via env / build.sh, not here.)

-- 1. country bbox table (tiny — ~40 rows)
CREATE OR REPLACE TABLE country_bbox AS
SELECT
  iso2,
  CAST(min_lat AS DOUBLE) AS min_lat,
  CAST(max_lat AS DOUBLE) AS max_lat,
  CAST(min_lon AS DOUBLE) AS min_lon,
  CAST(max_lon AS DOUBLE) AS max_lon
FROM read_csv('__BBOX_CSV__', header=true);

-- 2. Pre-compute bbox lookup for ONLY the multi-country PBFs (GCC, IL).
--    These are the only PBFs whose source_iso2 doesn't already encode
--    the destination country. ~1.3M rows total — small enough to
--    process eagerly without spilling. Goes parallel for this small
--    pre-pass; the big pass below uses threads=1.
SET threads = 4;
CREATE OR REPLACE TEMP TABLE bbox_lookup AS
SELECT
  f.osm_kind,
  f.osm_id,
  MIN(b.iso2) AS bbox_country_iso2
FROM read_parquet('__PARQUET_GLOB__', union_by_name=true) f
JOIN country_bbox b
  ON f.lat BETWEEN b.min_lat AND b.max_lat
 AND f.lon BETWEEN b.min_lon AND b.max_lon
WHERE f.source_iso2 IN ('GCC', 'IL')
  AND f.lat IS NOT NULL
  AND f.lon IS NOT NULL
GROUP BY f.osm_kind, f.osm_id;

-- 3. Big streaming union pass.
--    threads=1 + no ORDER BY → output row order is exactly:
--      "concat input parquets in lexicographic glob order, each parquet
--       in its own ORDER BY (osm_kind, osm_id) order".
--    Same inputs ⇒ same output bytes.
--
--    Uses LEFT JOIN to bbox_lookup which fits in memory (1.3M rows).
SET threads = 1;

COPY (
  SELECT
    coalesce(
      CASE
        WHEN c.addr_country IS NOT NULL
             AND length(c.addr_country) = 2
             AND regexp_matches(c.addr_country, '^[A-Z]{2}$')
          THEN c.addr_country
        ELSE NULL
      END,
      bl.bbox_country_iso2,
      c.source_iso2
    ) AS assigned_country,
    c.source_iso2,
    c.osm_kind,
    c.osm_id,
    c.lat,
    c.lon,
    c.name,
    c.name_en, c.name_fr, c.name_de, c.name_nl, c.name_es, c.name_it, c.name_pt,
    c.name_ru, c.name_uk, c.name_el,
    c.name_ja, c.name_ko, c.name_zh, c.name_zh_hans, c.name_zh_hant,
    c.name_ar, c.name_fa, c.name_he,
    c.name_hi, c.name_bn, c.name_ta, c.name_te,
    c.name_th, c.name_tr, c.name_pl, c.name_cs, c.name_hu, c.name_sv, c.name_da, c.name_no, c.name_fi,
    c.alt_name, c.short_name, c.official_name, c.old_name, c.loc_name, c.int_name,
    c.name_left, c.name_right,
    c.addr_housenumber, c.addr_housename, c.addr_street, c.addr_place, c.addr_unit, c.addr_floor,
    c.addr_suburb, c.addr_district, c.addr_subdistrict, c.addr_hamlet, c.addr_village,
    c.addr_town, c.addr_city, c.addr_county, c.addr_province, c.addr_state,
    c.addr_postcode, c.addr_country, c.addr_full,
    c.place, c.amenity, c.tourism, c.historic, c.natural_kind, c.shop, c.building, c.landuse,
    c.leisure, c.office, c.highway, c.railway, c.aeroway, c.waterway, c.boundary, c.admin_level,
    c.wikipedia, c.wikidata, c.population
  FROM read_parquet('__PARQUET_GLOB__', union_by_name=true) c
  LEFT JOIN bbox_lookup bl
    ON bl.osm_kind = c.osm_kind
   AND bl.osm_id   = c.osm_id
) TO '__OUT_PATH__' (
  FORMAT 'parquet',
  COMPRESSION 'zstd',
  COMPRESSION_LEVEL 3,
  ROW_GROUP_SIZE 200000
);
