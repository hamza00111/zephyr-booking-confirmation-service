-- =====================================================================
--  Oracle - Space breakdown (READ-ONLY: queries metadata only)
--
--  Answers "where does the space go?" before deciding on a reorg:
--    1. Space by segment type for the schema (tables vs indexes vs LOBs)
--    2. Datafile size vs space actually allocated to segments,
--       per tablespace (disk Oracle holds but objects don't use)
--
--  Works in SQL*Plus, SQLcl, SQL Developer and any JDBC client.
--  Replace 'ZEPHYR' below with the schema owner if it differs.
--  Needs SELECT_CATALOG_ROLE (or DBA).
-- =====================================================================

-- 1. Space by segment type
SELECT segment_type,
       COUNT(*)                            AS segments,
       ROUND(SUM(bytes) / 1073741824, 1)   AS gb
  FROM dba_segments
 WHERE owner = 'ZEPHYR'
 GROUP BY segment_type
 ORDER BY gb DESC;

-- 2. Datafile size vs segment usage, per tablespace
SELECT df.tablespace_name,
       ROUND(df.bytes / 1073741824, 1)                   AS file_gb,
       ROUND(NVL(sg.bytes, 0) / 1073741824, 1)           AS segments_gb,
       ROUND((df.bytes - NVL(sg.bytes, 0)) / 1073741824, 1) AS unused_gb
  FROM (SELECT tablespace_name, SUM(bytes) AS bytes
          FROM dba_data_files
         GROUP BY tablespace_name) df
  LEFT JOIN (SELECT tablespace_name, SUM(bytes) AS bytes
               FROM dba_segments
              GROUP BY tablespace_name) sg
    ON sg.tablespace_name = df.tablespace_name
 ORDER BY file_gb DESC;
