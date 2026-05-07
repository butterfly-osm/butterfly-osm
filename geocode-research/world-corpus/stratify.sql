-- stratify.sql
--
-- Detects the dominant Unicode script of every record's `name` and aggregates
-- per-(country, script_family) counts. Used to drive the bench-query sample
-- so each script family gets balanced representation.
--
-- Inputs:
--   $UNIFIED_PARQUET — sed-replaced absolute path to the unified parquet
--   __OUT_PATH__     — sed-replaced output parquet path
--
-- Determinism: deterministic regex-based classifier; ORDER BY on output.

SET memory_limit = '16GB';
SET threads = 8;

CREATE OR REPLACE MACRO classify_script(s) AS (
  -- Order matters: check the rare/specific scripts before the catch-all Latin.
  -- We classify by the FIRST character class that matches anywhere in the
  -- string. Mixed-script strings get tagged by the first non-Latin script
  -- detected; pure ASCII falls through to Latin.
  CASE
    WHEN s IS NULL OR length(s) = 0 THEN 'Unknown'
    -- CJK Unified Ideographs (U+4E00..U+9FFF), Extension A (U+3400..U+4DBF)
    WHEN regexp_matches(s, '[一-鿿㐀-䶿]') THEN 'Han'
    -- Hiragana (U+3040..U+309F) + Katakana (U+30A0..U+30FF) → Japanese
    WHEN regexp_matches(s, '[぀-ゟ゠-ヿ]') THEN 'Japanese'
    -- Hangul Syllables (U+AC00..U+D7AF) + Jamo (U+1100..U+11FF)
    WHEN regexp_matches(s, '[가-힯ᄀ-ᇿ]') THEN 'Hangul'
    -- Arabic (U+0600..U+06FF) + Supplement (U+0750..U+077F) + Arabic Presentation (U+FB50..U+FDFF, U+FE70..U+FEFF)
    WHEN regexp_matches(s, '[؀-ۿݐ-ݿﭐ-﷿ﹰ-﻿]') THEN 'Arabic'
    -- Hebrew (U+0590..U+05FF)
    WHEN regexp_matches(s, '[֐-׿]') THEN 'Hebrew'
    -- Devanagari (U+0900..U+097F)
    WHEN regexp_matches(s, '[ऀ-ॿ]') THEN 'Devanagari'
    -- Bengali (U+0980..U+09FF)
    WHEN regexp_matches(s, '[ঀ-৿]') THEN 'Bengali'
    -- Tamil (U+0B80..U+0BFF)
    WHEN regexp_matches(s, '[஀-௿]') THEN 'Tamil'
    -- Telugu (U+0C00..U+0C7F)
    WHEN regexp_matches(s, '[ఀ-౿]') THEN 'Telugu'
    -- Thai (U+0E00..U+0E7F)
    WHEN regexp_matches(s, '[฀-๿]') THEN 'Thai'
    -- Greek (U+0370..U+03FF)
    WHEN regexp_matches(s, '[Ͱ-Ͽ]') THEN 'Greek'
    -- Cyrillic (U+0400..U+04FF)
    WHEN regexp_matches(s, '[Ѐ-ӿ]') THEN 'Cyrillic'
    -- Latin Extended-A/B and basic Latin
    WHEN regexp_matches(s, '[A-Za-z-ɏ]') THEN 'Latin'
    ELSE 'Other'
  END
);

COPY (
  WITH classified AS (
    SELECT
      assigned_country,
      classify_script(name) AS script_family,
      osm_kind,
      osm_id
    FROM read_parquet('__UNIFIED_PARQUET__')
    WHERE name IS NOT NULL
  )
  SELECT
    assigned_country,
    script_family,
    count(*) AS n_records,
    count(DISTINCT osm_id) AS n_unique_osm_ids
  FROM classified
  GROUP BY assigned_country, script_family
  ORDER BY assigned_country, script_family
) TO '__OUT_PATH__' (
  FORMAT 'parquet',
  COMPRESSION 'zstd',
  COMPRESSION_LEVEL 3,
  ROW_GROUP_SIZE 100000
);
