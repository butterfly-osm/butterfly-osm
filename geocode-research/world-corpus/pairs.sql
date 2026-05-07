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
--
-- Implementation: UNPIVOT to rotate the wide name_* columns into a tall
-- (lang, text) form in a single scan.

SET memory_limit = '16GB';
SET threads = 8;

CREATE OR REPLACE TEMP TABLE name_long AS
SELECT * FROM (
  UNPIVOT (
    SELECT
      assigned_country, osm_kind, osm_id,
      name           AS "default",
      name_en        AS en,
      name_fr        AS fr,
      name_de        AS de,
      name_nl        AS nl,
      name_es        AS es,
      name_it        AS it,
      name_pt        AS pt,
      name_ru        AS ru,
      name_uk        AS uk,
      name_el        AS el,
      name_ja        AS ja,
      name_ko        AS ko,
      name_zh        AS zh,
      name_zh_hans   AS "zh-Hans",
      name_zh_hant   AS "zh-Hant",
      name_ar        AS ar,
      name_fa        AS fa,
      name_he        AS he,
      name_hi        AS hi,
      name_bn        AS bn,
      name_ta        AS ta,
      name_te        AS te,
      name_th        AS th,
      name_tr        AS tr,
      name_pl        AS pl,
      name_cs        AS cs,
      name_hu        AS hu,
      name_sv        AS sv,
      name_da        AS da,
      name_no        AS no,
      name_fi        AS fi,
      alt_name       AS alt,
      official_name  AS official,
      int_name       AS int
    FROM read_parquet('__UNIFIED_PARQUET__')
  )
  ON "default", en, fr, de, nl, es, it, pt, ru, uk, el,
     ja, ko, zh, "zh-Hans", "zh-Hant",
     ar, fa, he,
     hi, bn, ta, te,
     th, tr, pl, cs, hu, sv, da, no, fi,
     alt, official, int
  INTO NAME lang VALUE text
)
WHERE text IS NOT NULL AND length(text) > 0;

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
