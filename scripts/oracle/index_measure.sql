-- =====================================================================
--  Oracle - Index measurement (before / after a reorg)
--
--  1. Size on disk of every index on a table that contains a column
--  2. How full one index is inside (deleted entries, leaf usage)
--
--  Run in SQL*Plus / SQLcl. Set the names below, then run the file
--  before the update/delete, after it, and after compact + finalize.
--
--  LOCAL / PREPROD ONLY: ANALYZE ... VALIDATE STRUCTURE blocks writes to
--  the table while it runs. INDEX_STATS only shows the last index
--  analyzed, in the current session.
--
--  Expected:
--    before            : baseline mb, low pct_deleted, high pct_used
--    after the update  : mb up, pct_deleted ~50%, pct_used down
--    after the reorg   : mb back near baseline, pct_deleted ~0
-- =====================================================================

DEFINE owner      = 'ZEPHYR'
DEFINE table_name = 'BOOKING_CONFIRMATION_OUTGOING_EVENT'
DEFINE column     = 'PROCESSING_STATUS'
-- Index for step 2: pick one name from the step 1 results
DEFINE index_name = 'IDX_BOOKING_CONFIRMATION_OUTGOING_EVENT_DESTINATION_SYSTEM_PROCESSING_STATUS_ID'

SET PAGESIZE 200 LINESIZE 200 VERIFY OFF
COLUMN index_name FORMAT A80
COLUMN name       FORMAT A80

PROMPT ===== 1. Size on disk of the indexes containing &column =====
SELECT s.segment_name AS index_name,
       ROUND(s.bytes / 1048576, 1) AS mb,
       s.blocks
  FROM dba_segments s
 WHERE s.owner = UPPER('&owner')
   AND s.segment_name IN (SELECT index_name FROM dba_ind_columns
                           WHERE table_owner = UPPER('&owner')
                             AND table_name  = UPPER('&table_name')
                             AND column_name = UPPER('&column'))
 ORDER BY mb DESC;

PROMPT ===== 2. Inside &index_name =====
ANALYZE INDEX &owner..&index_name VALIDATE STRUCTURE;

SELECT name,
       height,
       lf_blks                                          AS leaf_blocks,
       lf_rows                                          AS entries,
       del_lf_rows                                      AS deleted_entries,
       ROUND(del_lf_rows * 100 / NULLIF(lf_rows, 0), 1) AS pct_deleted,
       pct_used
  FROM index_stats;
