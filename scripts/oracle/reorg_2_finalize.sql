-- =====================================================================
--  Oracle reorg - PHASE 2: finalize (run in the maintenance window)
--
--  SHRINK SPACE lowers the high-water mark and releases the space
--  freed by phase 1. Rows are already compacted, so this is quick,
--  but it takes a BRIEF EXCLUSIVE LOCK per table: DML on that table
--  waits for a few seconds. Then restores row movement, refreshes
--  statistics and verifies indexes and objects.
--
--  Run in SQL*Plus / SQLcl after reorg_1_compact.sql.
--  Stops at the first error. Log: reorg_2_finalize.log
-- =====================================================================

DEFINE schema = 'ZEPHYR'

WHENEVER SQLERROR EXIT FAILURE
SET ECHO ON TIMING ON SERVEROUTPUT ON VERIFY OFF LINESIZE 200 PAGESIZE 200
COLUMN table_name   FORMAT A40
COLUMN segment_name FORMAT A60
COLUMN object_name  FORMAT A60
SPOOL reorg_2_finalize.log

ALTER SESSION SET ddl_lock_timeout = 60;

-- ---------------------------------------------------------------------
-- 1. Lower the high-water mark and release space (tables + indexes + LOBs)
-- ---------------------------------------------------------------------
ALTER TABLE &schema..BOOKING_CONFIRMATION_STAGED_EVENT    SHRINK SPACE CASCADE;
ALTER TABLE &schema..BOOKING_CONFIRMATION_OUTGOING_EVENT  SHRINK SPACE CASCADE;
ALTER TABLE &schema..BOOKING_CONFIRMATION_PROCESSED_EVENT SHRINK SPACE CASCADE;
ALTER TABLE &schema..BOOKING_CONFIRMATION_CAPTURED_EVENT  SHRINK SPACE CASCADE;

-- ---------------------------------------------------------------------
-- 2. Put row movement back the way it was
-- ---------------------------------------------------------------------
ALTER TABLE &schema..BOOKING_CONFIRMATION_STAGED_EVENT    DISABLE ROW MOVEMENT;
ALTER TABLE &schema..BOOKING_CONFIRMATION_OUTGOING_EVENT  DISABLE ROW MOVEMENT;
ALTER TABLE &schema..BOOKING_CONFIRMATION_PROCESSED_EVENT DISABLE ROW MOVEMENT;
ALTER TABLE &schema..BOOKING_CONFIRMATION_CAPTURED_EVENT  DISABLE ROW MOVEMENT;

-- ---------------------------------------------------------------------
-- 3. Fresh optimizer statistics (tables + indexes)
-- ---------------------------------------------------------------------
EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname => UPPER('&schema'), tabname => 'BOOKING_CONFIRMATION_STAGED_EVENT',    cascade => TRUE);
EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname => UPPER('&schema'), tabname => 'BOOKING_CONFIRMATION_OUTGOING_EVENT',  cascade => TRUE);
EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname => UPPER('&schema'), tabname => 'BOOKING_CONFIRMATION_PROCESSED_EVENT', cascade => TRUE);
EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname => UPPER('&schema'), tabname => 'BOOKING_CONFIRMATION_CAPTURED_EVENT',  cascade => TRUE);

-- ---------------------------------------------------------------------
-- 4. Verification: sizes AFTER (compare with reorg_1_compact.log),
--    no unusable indexes, no invalid objects
-- ---------------------------------------------------------------------
WHENEVER SQLERROR CONTINUE

WITH t AS (
  SELECT column_value AS table_name
    FROM TABLE(sys.odcivarchar2list(
           'BOOKING_CONFIRMATION_STAGED_EVENT',
           'BOOKING_CONFIRMATION_OUTGOING_EVENT',
           'BOOKING_CONFIRMATION_PROCESSED_EVENT',
           'BOOKING_CONFIRMATION_CAPTURED_EVENT'))),
objs AS (
  SELECT t.table_name, t.table_name AS seg FROM t
  UNION ALL
  SELECT i.table_name, i.index_name FROM dba_indexes i JOIN t ON t.table_name = i.table_name
   WHERE i.table_owner = UPPER('&schema')
  UNION ALL
  SELECT l.table_name, l.segment_name FROM dba_lobs l JOIN t ON t.table_name = l.table_name
   WHERE l.owner = UPPER('&schema'))
SELECT o.table_name, s.segment_type, s.segment_name,
       ROUND(SUM(s.bytes) / 1048576, 1) AS mb
  FROM objs o
  JOIN dba_segments s ON s.owner = UPPER('&schema') AND s.segment_name = o.seg
 GROUP BY o.table_name, s.segment_type, s.segment_name
 ORDER BY o.table_name, mb DESC;

PROMPT Expect no rows below (unusable indexes):
SELECT index_name, status
  FROM dba_indexes
 WHERE owner = UPPER('&schema')
   AND status NOT IN ('VALID', 'N/A');

PROMPT Expect no rows below (invalid objects):
SELECT object_name, object_type
  FROM dba_objects
 WHERE owner = UPPER('&schema')
   AND status = 'INVALID';

-- ---------------------------------------------------------------------
-- 5. OPTIONAL - give the disk space back (Oracle 23ai, bigfile
--    tablespaces only). Steps 1-4 free space INSIDE the tablespace;
--    the datafile stays the same size until this runs.
--    Heavy operation: rehearse on preprod and run separately.
-- ---------------------------------------------------------------------
-- EXEC DBMS_SPACE.SHRINK_TABLESPACE('USERS', shrink_mode => DBMS_SPACE.TS_MODE_ANALYZE);
-- EXEC DBMS_SPACE.SHRINK_TABLESPACE('USERS');

SPOOL OFF
PROMPT Phase 2 done - send back reorg_1_compact.log and reorg_2_finalize.log
