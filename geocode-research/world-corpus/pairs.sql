-- pairs.sql
--
-- Extracts cross-language name pairs from the unified corpus, one row per
-- (record, language pair). Used as multilingual training/eval data.
--
-- Inputs:
--   $UNIFIED_PARQUET — sed-replaced absolute path to the unified parquet
--   __OUT_PATH__     — sed-replaced output parquet path
--
-- Determinism: ORDER BY on every output. No sampling here.

SET memory_limit = '16GB';
SET threads = 8;

-- Wide table → "tall" form: one row per (record, language, text)
CREATE OR REPLACE TEMP TABLE name_long AS
WITH wide AS (
  SELECT
    assigned_country, osm_kind, osm_id,
    name, name_en, name_fr, name_de, name_nl, name_es, name_it, name_pt,
    name_ru, name_uk, name_el,
    name_ja, name_ko, name_zh, name_zh_hans, name_zh_hant,
    name_ar, name_fa, name_he,
    name_hi, name_bn, name_ta, name_te,
    name_th, name_tr, name_pl, name_cs, name_hu, name_sv, name_da, name_no, name_fi,
    alt_name, official_name, int_name
  FROM read_parquet('__UNIFIED_PARQUET__')
),
piv AS (
  -- emit a (lang, text) row per non-null name column
  SELECT assigned_country, osm_kind, osm_id, 'default'   AS lang, name           AS text FROM wide WHERE name           IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'en'        AS lang, name_en        AS text FROM wide WHERE name_en        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'fr'        AS lang, name_fr        AS text FROM wide WHERE name_fr        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'de'        AS lang, name_de        AS text FROM wide WHERE name_de        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'nl'        AS lang, name_nl        AS text FROM wide WHERE name_nl        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'es'        AS lang, name_es        AS text FROM wide WHERE name_es        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'it'        AS lang, name_it        AS text FROM wide WHERE name_it        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'pt'        AS lang, name_pt        AS text FROM wide WHERE name_pt        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'ru'        AS lang, name_ru        AS text FROM wide WHERE name_ru        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'uk'        AS lang, name_uk        AS text FROM wide WHERE name_uk        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'el'        AS lang, name_el        AS text FROM wide WHERE name_el        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'ja'        AS lang, name_ja        AS text FROM wide WHERE name_ja        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'ko'        AS lang, name_ko        AS text FROM wide WHERE name_ko        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'zh'        AS lang, name_zh        AS text FROM wide WHERE name_zh        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'zh-Hans'   AS lang, name_zh_hans   AS text FROM wide WHERE name_zh_hans   IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'zh-Hant'   AS lang, name_zh_hant   AS text FROM wide WHERE name_zh_hant   IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'ar'        AS lang, name_ar        AS text FROM wide WHERE name_ar        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'fa'        AS lang, name_fa        AS text FROM wide WHERE name_fa        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'he'        AS lang, name_he        AS text FROM wide WHERE name_he        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'hi'        AS lang, name_hi        AS text FROM wide WHERE name_hi        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'bn'        AS lang, name_bn        AS text FROM wide WHERE name_bn        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'ta'        AS lang, name_ta        AS text FROM wide WHERE name_ta        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'te'        AS lang, name_te        AS text FROM wide WHERE name_te        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'th'        AS lang, name_th        AS text FROM wide WHERE name_th        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'tr'        AS lang, name_tr        AS text FROM wide WHERE name_tr        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'pl'        AS lang, name_pl        AS text FROM wide WHERE name_pl        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'cs'        AS lang, name_cs        AS text FROM wide WHERE name_cs        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'hu'        AS lang, name_hu        AS text FROM wide WHERE name_hu        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'sv'        AS lang, name_sv        AS text FROM wide WHERE name_sv        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'da'        AS lang, name_da        AS text FROM wide WHERE name_da        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'no'        AS lang, name_no        AS text FROM wide WHERE name_no        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'fi'        AS lang, name_fi        AS text FROM wide WHERE name_fi        IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'alt'       AS lang, alt_name       AS text FROM wide WHERE alt_name       IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'official'  AS lang, official_name  AS text FROM wide WHERE official_name  IS NOT NULL
  UNION ALL SELECT assigned_country, osm_kind, osm_id, 'int'       AS lang, int_name       AS text FROM wide WHERE int_name       IS NOT NULL
)
SELECT * FROM piv;

-- Records with at least 2 distinct languages
CREATE OR REPLACE TEMP TABLE multilingual_records AS
SELECT osm_kind, osm_id
FROM name_long
GROUP BY osm_kind, osm_id
HAVING count(DISTINCT lang) >= 2;

-- Self-join to emit ordered language pairs (lang_a < lang_b for stability)
COPY (
  SELECT
    a.assigned_country,
    a.osm_kind,
    a.osm_id,
    a.lang AS lang_a,
    a.text AS text_a,
    b.lang AS lang_b,
    b.text AS text_b
  FROM name_long a
  JOIN name_long b
    ON a.osm_kind = b.osm_kind
   AND a.osm_id   = b.osm_id
   AND a.lang     < b.lang        -- ordered, no duplicates
   AND a.text     <> b.text       -- skip identical strings
  JOIN multilingual_records m
    ON m.osm_kind = a.osm_kind
   AND m.osm_id   = a.osm_id
  ORDER BY a.assigned_country, a.osm_kind, a.osm_id, a.lang, b.lang
) TO '__OUT_PATH__' (
  FORMAT 'parquet',
  COMPRESSION 'zstd',
  COMPRESSION_LEVEL 3,
  ROW_GROUP_SIZE 200000
);
