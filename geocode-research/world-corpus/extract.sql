-- extract.sql
--
-- Per-PBF extraction. Reads one Geofabrik snapshot via ST_ReadOsm and writes a
-- per-country parquet file with all geocoder-relevant tags.
--
-- Inputs:
--   $PBF_PATH  — absolute path to the .osm.pbf file (read via getenv)
--   $ISO2_TAG  — ISO-3166-1 alpha-2 written into every row's source_iso2 (getenv)
--   __OUT_PATH__ — sed-replaced to the output parquet path before execution
--                  (DuckDB's COPY ... TO target must be a string literal, not
--                  a function call, so build.sh substitutes the token at run
--                  time. Fully deterministic — same input PBF + same date tag
--                  always produces byte-identical parquet.)
--
-- Determinism:
--   * ORDER BY id at the COPY level so parquet row groups are stable across runs.
--   * No random sampling here; this is a straight projection.
--   * ZSTD compression with explicit ROW_GROUP_SIZE so file framing is stable.

LOAD spatial;

SET memory_limit = '8GB';
SET threads = 8;

COPY (
  SELECT
    getenv('ISO2_TAG')                                  AS source_iso2,
    kind                                                AS osm_kind,
    id                                                  AS osm_id,
    lat,
    lon,

    -- Names (multilingual)
    map_extract(tags, 'name')[1]            AS name,
    map_extract(tags, 'name:en')[1]         AS name_en,
    map_extract(tags, 'name:fr')[1]         AS name_fr,
    map_extract(tags, 'name:de')[1]         AS name_de,
    map_extract(tags, 'name:nl')[1]         AS name_nl,
    map_extract(tags, 'name:es')[1]         AS name_es,
    map_extract(tags, 'name:it')[1]         AS name_it,
    map_extract(tags, 'name:pt')[1]         AS name_pt,
    map_extract(tags, 'name:ru')[1]         AS name_ru,
    map_extract(tags, 'name:uk')[1]         AS name_uk,
    map_extract(tags, 'name:el')[1]         AS name_el,
    map_extract(tags, 'name:ja')[1]         AS name_ja,
    map_extract(tags, 'name:ko')[1]         AS name_ko,
    map_extract(tags, 'name:zh')[1]         AS name_zh,
    map_extract(tags, 'name:zh-Hans')[1]    AS name_zh_hans,
    map_extract(tags, 'name:zh-Hant')[1]    AS name_zh_hant,
    map_extract(tags, 'name:ar')[1]         AS name_ar,
    map_extract(tags, 'name:fa')[1]         AS name_fa,
    map_extract(tags, 'name:he')[1]         AS name_he,
    map_extract(tags, 'name:hi')[1]         AS name_hi,
    map_extract(tags, 'name:bn')[1]         AS name_bn,
    map_extract(tags, 'name:ta')[1]         AS name_ta,
    map_extract(tags, 'name:te')[1]         AS name_te,
    map_extract(tags, 'name:th')[1]         AS name_th,
    map_extract(tags, 'name:tr')[1]         AS name_tr,
    map_extract(tags, 'name:pl')[1]         AS name_pl,
    map_extract(tags, 'name:cs')[1]         AS name_cs,
    map_extract(tags, 'name:hu')[1]         AS name_hu,
    map_extract(tags, 'name:sv')[1]         AS name_sv,
    map_extract(tags, 'name:da')[1]         AS name_da,
    map_extract(tags, 'name:no')[1]         AS name_no,
    map_extract(tags, 'name:fi')[1]         AS name_fi,
    map_extract(tags, 'alt_name')[1]        AS alt_name,
    map_extract(tags, 'short_name')[1]      AS short_name,
    map_extract(tags, 'official_name')[1]   AS official_name,
    map_extract(tags, 'old_name')[1]        AS old_name,
    map_extract(tags, 'loc_name')[1]        AS loc_name,
    map_extract(tags, 'int_name')[1]        AS int_name,
    map_extract(tags, 'name:left')[1]       AS name_left,
    map_extract(tags, 'name:right')[1]      AS name_right,

    -- Address fields
    map_extract(tags, 'addr:housenumber')[1] AS addr_housenumber,
    map_extract(tags, 'addr:housename')[1]   AS addr_housename,
    map_extract(tags, 'addr:street')[1]      AS addr_street,
    map_extract(tags, 'addr:place')[1]       AS addr_place,
    map_extract(tags, 'addr:unit')[1]        AS addr_unit,
    map_extract(tags, 'addr:floor')[1]       AS addr_floor,
    map_extract(tags, 'addr:suburb')[1]      AS addr_suburb,
    map_extract(tags, 'addr:district')[1]    AS addr_district,
    map_extract(tags, 'addr:subdistrict')[1] AS addr_subdistrict,
    map_extract(tags, 'addr:hamlet')[1]      AS addr_hamlet,
    map_extract(tags, 'addr:village')[1]     AS addr_village,
    map_extract(tags, 'addr:town')[1]        AS addr_town,
    map_extract(tags, 'addr:city')[1]        AS addr_city,
    map_extract(tags, 'addr:county')[1]      AS addr_county,
    map_extract(tags, 'addr:province')[1]    AS addr_province,
    map_extract(tags, 'addr:state')[1]       AS addr_state,
    map_extract(tags, 'addr:postcode')[1]    AS addr_postcode,
    map_extract(tags, 'addr:country')[1]     AS addr_country,
    map_extract(tags, 'addr:full')[1]        AS addr_full,

    -- Feature classification
    map_extract(tags, 'place')[1]      AS place,
    map_extract(tags, 'amenity')[1]    AS amenity,
    map_extract(tags, 'tourism')[1]    AS tourism,
    map_extract(tags, 'historic')[1]   AS historic,
    map_extract(tags, 'natural')[1]    AS natural_kind,
    map_extract(tags, 'shop')[1]       AS shop,
    map_extract(tags, 'building')[1]   AS building,
    map_extract(tags, 'landuse')[1]    AS landuse,
    map_extract(tags, 'leisure')[1]    AS leisure,
    map_extract(tags, 'office')[1]     AS office,
    map_extract(tags, 'highway')[1]    AS highway,
    map_extract(tags, 'railway')[1]    AS railway,
    map_extract(tags, 'aeroway')[1]    AS aeroway,
    map_extract(tags, 'waterway')[1]   AS waterway,
    map_extract(tags, 'boundary')[1]   AS boundary,
    map_extract(tags, 'admin_level')[1] AS admin_level,

    -- External identity
    map_extract(tags, 'wikipedia')[1]  AS wikipedia,
    map_extract(tags, 'wikidata')[1]   AS wikidata,
    map_extract(tags, 'population')[1] AS population
  FROM ST_ReadOsm(getenv('PBF_PATH'))
  WHERE list_contains(map_keys(tags), 'name')
     OR list_contains(map_keys(tags), 'addr:street')
     OR list_contains(map_keys(tags), 'addr:housenumber')
     OR list_contains(map_keys(tags), 'place')
  ORDER BY osm_kind, osm_id
) TO '__OUT_PATH__' (
  FORMAT 'parquet',
  COMPRESSION 'zstd',
  COMPRESSION_LEVEL 3,
  ROW_GROUP_SIZE 100000
);
