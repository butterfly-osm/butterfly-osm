-- sample_bench.sql
--
-- Deterministically samples up to $SAMPLE_PER_FAMILY records per script family
-- and emits 3 query forms per record: canonical, partial (drop one field),
-- reordered (swap city/street). Output is a single parquet keyed by
-- script_family; build.sh splits it into per-family TSVs deterministically.
--
-- Inputs:
--   $UNIFIED_PARQUET   — sed-replaced absolute path
--   $SAMPLE_PER_FAMILY — sed-replaced integer (default 1000)
--   __OUT_PATH__       — sed-replaced output parquet path
--
-- Determinism:
--   * SETSEED(0.42) before every random call.
--   * row_number() ORDER BY (script_family, osm_kind, osm_id, hash) gives a
--     totally-ordered candidate list per family that does not depend on
--     scheduling.
--   * NO use of TABLESAMPLE (which is non-deterministic in DuckDB).

SET memory_limit = '16GB';
SET threads = 8;

-- Same script classifier as stratify.sql (kept inline so this file is self-contained)
CREATE OR REPLACE MACRO classify_script(s) AS (
  CASE
    WHEN s IS NULL OR length(s) = 0 THEN 'Unknown'
    WHEN regexp_matches(s, '[一-鿿㐀-䶿]') THEN 'Han'
    WHEN regexp_matches(s, '[぀-ゟ゠-ヿ]') THEN 'Japanese'
    WHEN regexp_matches(s, '[가-힯ᄀ-ᇿ]') THEN 'Hangul'
    WHEN regexp_matches(s, '[؀-ۿݐ-ݿﭐ-﷿ﹰ-﻿]') THEN 'Arabic'
    WHEN regexp_matches(s, '[֐-׿]') THEN 'Hebrew'
    WHEN regexp_matches(s, '[ऀ-ॿ]') THEN 'Devanagari'
    WHEN regexp_matches(s, '[ঀ-৿]') THEN 'Bengali'
    WHEN regexp_matches(s, '[஀-௿]') THEN 'Tamil'
    WHEN regexp_matches(s, '[ఀ-౿]') THEN 'Telugu'
    WHEN regexp_matches(s, '[฀-๿]') THEN 'Thai'
    WHEN regexp_matches(s, '[Ͱ-Ͽ]') THEN 'Greek'
    WHEN regexp_matches(s, '[Ѐ-ӿ]') THEN 'Cyrillic'
    WHEN regexp_matches(s, '[A-Za-z-ɏ]') THEN 'Latin'
    ELSE 'Other'
  END
);

-- 1. Filter to addressable records (need a name + at least city or street info)
CREATE OR REPLACE TEMP TABLE candidates AS
SELECT
  assigned_country,
  osm_kind,
  osm_id,
  name,
  coalesce(addr_street, '')  AS street,
  coalesce(addr_housenumber, '') AS housenumber,
  coalesce(addr_city, addr_town, addr_village, addr_hamlet, '') AS city,
  coalesce(addr_postcode, '') AS postcode,
  classify_script(name) AS script_family,
  -- Stable per-row hash, the ONLY source of randomness in the sample
  hash(printf('%s|%s|%d|%s', assigned_country, osm_kind, osm_id, name)) AS row_hash
FROM read_parquet('__UNIFIED_PARQUET__')
WHERE name IS NOT NULL
  AND length(name) > 1
  AND (addr_street IS NOT NULL OR addr_city IS NOT NULL OR place IS NOT NULL);

-- 2. Per-family deterministic sample: rank by hash, take first N
CREATE OR REPLACE TEMP TABLE sampled AS
WITH ranked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY script_family
      ORDER BY row_hash, assigned_country, osm_kind, osm_id
    ) AS rn
  FROM candidates
)
SELECT *
FROM ranked
WHERE rn <= __SAMPLE_PER_FAMILY__;

-- 3. Generate 3 query forms per sampled record, one row per query.
--    query_form ∈ {canonical, partial, reordered}
--      canonical: "name, street housenumber, city postcode"
--      partial  : "name, city"
--      reordered: "city, street housenumber, name"
--
-- Each row carries its gold lat/lon so the bench harness can score recall
-- without further joins. quality_class follows the existing convention used
-- by bench/geocode/queries/<country>.tsv ("clean" if all four address fields
-- present, else "noisy").
CREATE OR REPLACE TEMP TABLE expanded AS
WITH base AS (
  SELECT
    s.assigned_country,
    s.script_family,
    s.osm_kind,
    s.osm_id,
    s.name,
    s.street,
    s.housenumber,
    s.city,
    s.postcode,
    -- recover the lat/lon from the unified parquet
    u.lat AS gold_lat,
    u.lon AS gold_lon
  FROM sampled s
  JOIN read_parquet('__UNIFIED_PARQUET__') u
    ON u.osm_kind = s.osm_kind
   AND u.osm_id   = s.osm_id
)
SELECT
  printf('%s-%s-%s-%010d',
         lower(assigned_country),
         lower(script_family),
         form,
         row_number() OVER (PARTITION BY script_family, form ORDER BY osm_kind, osm_id)) AS query_id,
  query_text,
  gold_lat,
  gold_lon,
  CASE WHEN street <> '' AND housenumber <> '' AND city <> '' AND postcode <> ''
       THEN 'clean' ELSE 'noisy' END AS quality_class,
  assigned_country,
  script_family,
  form AS query_form,
  osm_kind,
  osm_id,
  name
FROM base,
  LATERAL (VALUES
    ('canonical',
       trim(both ', ' FROM regexp_replace(
         printf('%s, %s %s, %s %s', name, street, housenumber, city, postcode),
         ',\s*,', ',', 'g'))),
    ('partial',
       trim(both ', ' FROM regexp_replace(
         printf('%s, %s', name, city),
         ',\s*,', ',', 'g'))),
    ('reordered',
       trim(both ', ' FROM regexp_replace(
         printf('%s, %s %s, %s', city, street, housenumber, name),
         ',\s*,', ',', 'g')))
  ) AS f(form, query_text);

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
  FROM expanded
  ORDER BY script_family, query_form, osm_kind, osm_id
) TO '__OUT_PATH__' (
  FORMAT 'parquet',
  COMPRESSION 'zstd',
  COMPRESSION_LEVEL 3,
  ROW_GROUP_SIZE 50000
);
