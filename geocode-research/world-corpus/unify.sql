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
SET memory_limit = '16GB';
SET threads = 8;

-- 1. country bbox table
CREATE OR REPLACE TABLE country_bbox AS
SELECT
  iso2,
  CAST(min_lat AS DOUBLE) AS min_lat,
  CAST(max_lat AS DOUBLE) AS max_lat,
  CAST(min_lon AS DOUBLE) AS min_lon,
  CAST(max_lon AS DOUBLE) AS max_lon
FROM read_csv('__BBOX_CSV__', header=true);

-- 2. raw union of every per-country parquet (auto-schema-merged)
CREATE OR REPLACE VIEW raw_features AS
SELECT *
FROM read_parquet('__PARQUET_GLOB__', union_by_name=true);

-- 3. classify each feature into an iso2:
--    a) trust addr:country if it's a 2-letter uppercase code already
--    b) else bbox lookup (find the country whose bbox contains the point)
--    c) else fallback to source_iso2 (the PBF tag the row came from)
COPY (
  WITH
  parsed AS (
    SELECT
      *,
      CASE
        WHEN addr_country IS NOT NULL AND length(addr_country) = 2
             AND regexp_matches(addr_country, '^[A-Z]{2}$')
          THEN addr_country
        ELSE NULL
      END AS addr_country_iso2
    FROM raw_features
  ),
  with_bbox AS (
    SELECT
      p.*,
      -- bbox lookup; pick the alphabetically-first matching country for stability
      (
        SELECT MIN(b.iso2)
        FROM country_bbox b
        WHERE p.lat IS NOT NULL AND p.lon IS NOT NULL
          AND p.lat BETWEEN b.min_lat AND b.max_lat
          AND p.lon BETWEEN b.min_lon AND b.max_lon
      ) AS bbox_country_iso2
    FROM parsed p
  )
  SELECT
    coalesce(addr_country_iso2, bbox_country_iso2, source_iso2) AS assigned_country,
    -- preserve everything else
    source_iso2,
    osm_kind,
    osm_id,
    lat,
    lon,
    name,
    name_en, name_fr, name_de, name_nl, name_es, name_it, name_pt,
    name_ru, name_uk, name_el,
    name_ja, name_ko, name_zh, name_zh_hans, name_zh_hant,
    name_ar, name_fa, name_he,
    name_hi, name_bn, name_ta, name_te,
    name_th, name_tr, name_pl, name_cs, name_hu, name_sv, name_da, name_no, name_fi,
    alt_name, short_name, official_name, old_name, loc_name, int_name,
    name_left, name_right,
    addr_housenumber, addr_housename, addr_street, addr_place, addr_unit, addr_floor,
    addr_suburb, addr_district, addr_subdistrict, addr_hamlet, addr_village,
    addr_town, addr_city, addr_county, addr_province, addr_state,
    addr_postcode, addr_country, addr_full,
    place, amenity, tourism, historic, natural_kind, shop, building, landuse,
    leisure, office, highway, railway, aeroway, waterway, boundary, admin_level,
    wikipedia, wikidata, population
  FROM with_bbox
  ORDER BY
    coalesce(addr_country_iso2, bbox_country_iso2, source_iso2),
    osm_kind,
    osm_id
) TO '__OUT_PATH__' (
  FORMAT 'parquet',
  COMPRESSION 'zstd',
  COMPRESSION_LEVEL 3,
  ROW_GROUP_SIZE 200000
);
