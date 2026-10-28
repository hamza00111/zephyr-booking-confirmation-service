-- =====================================================================
--  Oracle 23ai - Reorg diagnostic (READ-ONLY: queries metadata only,
--  changes nothing, reads no business data)
--
--  Run in SQL*Plus or SQLcl as a user with SELECT_CATALOG_ROLE
--  (or DBA). Output is spooled to reorg_diag.txt.
--
--  1. Set the schema name below (UPPERCASE)
--  2. Run:  @reorg_diagnostic.sql
-- =====================================================================

DEFINE schema = 'MY_SCHEMA'

SET PAGESIZE 500 LINESIZE 300 TRIMSPOOL ON VERIFY OFF FEEDBACK ON
COLUMN table_name      FORMAT A30
COLUMN index_name      FORMAT A30
COLUMN segment_name    FORMAT A30
COLUMN partition_name  FORMAT A25
COLUMN tablespace_name FORMAT A20
COLUMN column_name     FORMAT A25
COLUMN recommendations FORMAT A90
COLUMN banner_full     FORMAT A100

SPOOL reorg_diag.txt

PROMPT ===== 1. VERSION / EDITION =====
SELECT version_full, edition FROM v$instance;
SELECT banner_full FROM v$version;
SELECT name, value FROM v$parameter
 WHERE name IN ('db_block_size', 'compatible', 'enable_goldengate_replication');

PROMPT ===== 2. DATABASE ROLE / REDO (Data Guard, replication) =====
SELECT database_role, log_mode, force_logging, supplemental_log_data_min
  FROM v$database;

PROMPT ===== 3. TABLESPACES USED BY SCHEMA (ASSM, free space) =====
SELECT t.tablespace_name, t.block_size, t.bigfile, t.segment_space_management,
       ROUND(df.total_mb)          AS total_mb,
       ROUND(NVL(fs.free_mb, 0))   AS free_mb,
       ROUND(df.max_mb)            AS max_autoextend_mb
  FROM dba_tablespaces t
  JOIN (SELECT tablespace_name,
               SUM(bytes) / 1048576                     AS total_mb,
               SUM(GREATEST(maxbytes, bytes)) / 1048576 AS max_mb
          FROM dba_data_files
         GROUP BY tablespace_name) df
    ON df.tablespace_name = t.tablespace_name
  LEFT JOIN (SELECT tablespace_name, SUM(bytes) / 1048576 AS free_mb
               FROM dba_free_space
              GROUP BY tablespace_name) fs
    ON fs.tablespace_name = t.tablespace_name
 WHERE t.tablespace_name IN (SELECT DISTINCT tablespace_name
                               FROM dba_segments
                              WHERE owner = UPPER('&schema'))
 ORDER BY 1;

PROMPT ===== 4. TOP 30 SEGMENTS BY SIZE =====
SELECT segment_name, partition_name, segment_type, tablespace_name,
       ROUND(bytes / 1048576, 1) AS size_mb, blocks, extents
  FROM dba_segments
 WHERE owner = UPPER('&schema')
 ORDER BY bytes DESC
 FETCH FIRST 30 ROWS ONLY;

PROMPT ===== 5. TABLES: ESTIMATED WASTED SPACE (top 50 by size) =====
PROMPT (est_waste_pct of ~10-15% is normal block overhead; >30% is a reorg candidate)
WITH seg AS (
  SELECT segment_name, SUM(bytes) AS bytes
    FROM dba_segments
   WHERE owner = UPPER('&schema')
     AND segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
   GROUP BY segment_name)
SELECT t.table_name, t.partitioned, t.row_movement,
       t.num_rows, t.blocks, t.avg_row_len,
       NVL(t.pct_free, 10)                                   AS pct_free,
       t.chain_cnt,
       ROUND(seg.bytes / 1048576, 1)                         AS alloc_mb,
       ROUND(t.num_rows * t.avg_row_len / 1048576, 1)        AS data_mb,
       ROUND(100 * (1 - (t.num_rows * t.avg_row_len)
             / NULLIF(seg.bytes * (1 - NVL(t.pct_free, 10) / 100), 0)), 1)
                                                             AS est_waste_pct,
       t.last_analyzed, s.stale_stats
  FROM dba_tables t
  JOIN seg ON seg.segment_name = t.table_name
  LEFT JOIN dba_tab_statistics s
    ON s.owner = t.owner AND s.table_name = t.table_name AND s.object_type = 'TABLE'
 WHERE t.owner = UPPER('&schema')
 ORDER BY seg.bytes DESC
 FETCH FIRST 50 ROWS ONLY;

PROMPT ===== 6. INDEXES (top 50 by size) =====
WITH seg AS (
  SELECT segment_name, SUM(bytes) AS bytes
    FROM dba_segments
   WHERE owner = UPPER('&schema')
     AND segment_type IN ('INDEX', 'INDEX PARTITION', 'INDEX SUBPARTITION')
   GROUP BY segment_name)
SELECT i.index_name, i.table_name, i.index_type, i.uniqueness, i.partitioned,
       i.blevel, i.leaf_blocks, i.distinct_keys, i.num_rows,
       i.clustering_factor,
       t.blocks   AS tab_blocks,
       t.num_rows AS tab_rows,
       ROUND(seg.bytes / 1048576, 1) AS size_mb,
       i.last_analyzed
  FROM dba_indexes i
  JOIN seg ON seg.segment_name = i.index_name
  LEFT JOIN dba_tables t
    ON t.owner = i.table_owner AND t.table_name = i.table_name
 WHERE i.owner = UPPER('&schema')
 ORDER BY seg.bytes DESC
 FETCH FIRST 50 ROWS ONLY;

PROMPT ===== 7. UNUSABLE INDEXES / PARTITIONS =====
SELECT index_name, NULL AS partition_name, status
  FROM dba_indexes
 WHERE owner = UPPER('&schema') AND status = 'UNUSABLE'
UNION ALL
SELECT index_name, partition_name, status
  FROM dba_ind_partitions
 WHERE index_owner = UPPER('&schema') AND status = 'UNUSABLE';

PROMPT ===== 8. SPECIAL STRUCTURES (partitioned / IOT / compressed) =====
SELECT table_name, partitioned, iot_type, compression, compress_for, temporary
  FROM dba_tables
 WHERE owner = UPPER('&schema')
   AND (partitioned = 'YES' OR iot_type IS NOT NULL OR compression = 'ENABLED')
 ORDER BY table_name;

PROMPT ===== 9. LOB COLUMNS =====
SELECT l.table_name, l.column_name, l.securefile, l.segment_name,
       ROUND(s.bytes / 1048576, 1) AS size_mb
  FROM dba_lobs l
  LEFT JOIN (SELECT segment_name, SUM(bytes) AS bytes
               FROM dba_segments
              WHERE owner = UPPER('&schema')
              GROUP BY segment_name) s
    ON s.segment_name = l.segment_name
 WHERE l.owner = UPPER('&schema')
 ORDER BY s.bytes DESC NULLS LAST;

PROMPT ===== 10. AUTO SEGMENT ADVISOR RECOMMENDATIONS =====
PROMPT (may fail without EXECUTE on DBMS_SPACE - that is fine, skip it)
SELECT segment_name, partition_name, segment_type,
       ROUND(allocated_space   / 1048576, 1) AS alloc_mb,
       ROUND(used_space        / 1048576, 1) AS used_mb,
       ROUND(reclaimable_space / 1048576, 1) AS reclaim_mb,
       recommendations
  FROM TABLE(dbms_space.asa_recommendations())
 WHERE segment_owner = UPPER('&schema')
 ORDER BY reclaimable_space DESC;

SPOOL OFF
PROMPT Done - send back reorg_diag.txt
