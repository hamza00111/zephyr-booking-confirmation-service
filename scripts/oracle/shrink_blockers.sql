-- =====================================================================
--  Oracle - Why can't a table be shrunk? (READ-ONLY)
--
--  ORA-10631 "SHRINK clause should not be specified for this object"
--  is raised when a table has:
--    - a function-based index        -> use ALTER TABLE ... MOVE ONLINE
--    - compression                   -> use ALTER TABLE ... MOVE ONLINE
--    - a LONG / LONG RAW column      -> neither shrink nor move: migrate to LOB
--    - an index-organized structure  -> depends on options
--  Every query below should return no rows for a shrinkable table
--  (query 3 returns one row per table: check its columns).
--
--  Query 5 shows how each function-based index is defined. A column
--  declared DESC is stored as a function-based index: it appears as a
--  hidden SYS_NC...$ column with DESCEND = DESC, and query 2 shows the
--  real column name. An ascending index serves "ORDER BY x DESC" just as
--  well (backward range scan) and does not block a shrink.
--
--  Covers the four PKG_REORG event tables. Uses ALL_ views, so it runs as
--  ZEPHYR_ADMIN or any account that can see the tables.
--  Run in SQL*Plus / SQLcl. Set the owner below:
--    local: ZEPHYR      other envs: BOOKING_CONFIRMATION
-- =====================================================================

DEFINE owner = 'BOOKING_CONFIRMATION'

SET PAGESIZE 200 LINESIZE 200 VERIFY OFF
COLUMN table_name        FORMAT A40
COLUMN index_name        FORMAT A60
COLUMN index_type        FORMAT A25
COLUMN column_name       FORMAT A30
COLUMN column_expression FORMAT A80
COLUMN compress_for      FORMAT A20
COLUMN descend           FORMAT A7

PROMPT ===== 1. Function-based indexes =====
SELECT table_name, index_name, index_type
  FROM all_indexes
 WHERE table_owner = UPPER('&owner')
   AND table_name IN ('BOOKING_CONFIRMATION_STAGED_EVENT',
                      'BOOKING_CONFIRMATION_OUTGOING_EVENT',
                      'BOOKING_CONFIRMATION_PROCESSED_EVENT',
                      'BOOKING_CONFIRMATION_CAPTURED_EVENT')
   AND (index_type LIKE 'FUNCTION-BASED%' OR index_type IN ('DOMAIN', 'BITMAP'))
 ORDER BY table_name, index_name;

PROMPT ===== 2. Expressions behind the function-based indexes =====
SELECT table_name, index_name, column_expression
  FROM all_ind_expressions
 WHERE table_owner = UPPER('&owner')
   AND table_name IN ('BOOKING_CONFIRMATION_STAGED_EVENT',
                      'BOOKING_CONFIRMATION_OUTGOING_EVENT',
                      'BOOKING_CONFIRMATION_PROCESSED_EVENT',
                      'BOOKING_CONFIRMATION_CAPTURED_EVENT')
 ORDER BY table_name, index_name;

PROMPT ===== 3. Compression / index-organized (one row per table) =====
SELECT table_name, compression, compress_for, iot_type
  FROM all_tables
 WHERE owner = UPPER('&owner')
   AND table_name IN ('BOOKING_CONFIRMATION_STAGED_EVENT',
                      'BOOKING_CONFIRMATION_OUTGOING_EVENT',
                      'BOOKING_CONFIRMATION_PROCESSED_EVENT',
                      'BOOKING_CONFIRMATION_CAPTURED_EVENT')
 ORDER BY table_name;

PROMPT ===== 4. LONG / LONG RAW columns =====
SELECT table_name, column_name, data_type
  FROM all_tab_columns
 WHERE owner = UPPER('&owner')
   AND table_name IN ('BOOKING_CONFIRMATION_STAGED_EVENT',
                      'BOOKING_CONFIRMATION_OUTGOING_EVENT',
                      'BOOKING_CONFIRMATION_PROCESSED_EVENT',
                      'BOOKING_CONFIRMATION_CAPTURED_EVENT')
   AND data_type IN ('LONG', 'LONG RAW')
 ORDER BY table_name, column_name;

PROMPT ===== 5. Columns of the function-based indexes (DESC columns) =====
SELECT table_name, index_name, column_position, column_name, descend
  FROM all_ind_columns
 WHERE table_owner = UPPER('&owner')
   AND index_name IN (SELECT index_name
                        FROM all_indexes
                       WHERE table_owner = UPPER('&owner')
                         AND table_name IN ('BOOKING_CONFIRMATION_STAGED_EVENT',
                                            'BOOKING_CONFIRMATION_OUTGOING_EVENT',
                                            'BOOKING_CONFIRMATION_PROCESSED_EVENT',
                                            'BOOKING_CONFIRMATION_CAPTURED_EVENT')
                         AND index_type LIKE 'FUNCTION-BASED%')
 ORDER BY table_name, index_name, column_position;
