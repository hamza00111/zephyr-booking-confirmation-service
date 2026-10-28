-- =====================================================================
--  Oracle reorg - PHASE 1: prechecks + online compaction
--
--  Moves rows to the start of each segment (SHRINK SPACE COMPACT).
--  ONLINE: row-level locks only, the high-water mark does not move,
--  so it can run during business hours. Rows locked by the relay are
--  simply waited on; the relay's SKIP LOCKED skips rows being moved.
--
--  Run in SQL*Plus / SQLcl as a user that can ALTER the tables
--  (schema owner or DBA). Stops at the first error.
--  Log: reorg_1_compact.log
-- =====================================================================

DEFINE schema = 'ZEPHYR'

WHENEVER SQLERROR EXIT FAILURE
SET ECHO ON TIMING ON SERVEROUTPUT ON VERIFY OFF LINESIZE 200 PAGESIZE 200
COLUMN table_name   FORMAT A40
COLUMN segment_name FORMAT A60
SPOOL reorg_1_compact.log

-- ---------------------------------------------------------------------
-- 0. Sizes BEFORE (tables + their indexes + their LOBs)
-- ---------------------------------------------------------------------
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

-- ---------------------------------------------------------------------
-- 1. Blockers: SHRINK is not allowed with function-based or domain
--    indexes, compressed tables, or outside ASSM tablespaces.
--    If this fails, use ALTER TABLE ... MOVE ONLINE instead.
-- ---------------------------------------------------------------------
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n
    FROM dba_indexes
   WHERE table_owner = UPPER('&schema')
     AND table_name IN ('BOOKING_CONFIRMATION_STAGED_EVENT',
                        'BOOKING_CONFIRMATION_OUTGOING_EVENT',
                        'BOOKING_CONFIRMATION_PROCESSED_EVENT',
                        'BOOKING_CONFIRMATION_CAPTURED_EVENT')
     AND (index_type LIKE 'FUNCTION-BASED%' OR index_type = 'DOMAIN');
  IF n > 0 THEN
    RAISE_APPLICATION_ERROR(-20001, n || ' function-based/domain index(es) found: use MOVE ONLINE, not SHRINK');
  END IF;

  SELECT COUNT(*) INTO n
    FROM dba_tables t
    JOIN dba_tablespaces ts ON ts.tablespace_name = t.tablespace_name
   WHERE t.owner = UPPER('&schema')
     AND t.table_name IN ('BOOKING_CONFIRMATION_STAGED_EVENT',
                          'BOOKING_CONFIRMATION_OUTGOING_EVENT',
                          'BOOKING_CONFIRMATION_PROCESSED_EVENT',
                          'BOOKING_CONFIRMATION_CAPTURED_EVENT')
     AND (t.compression = 'ENABLED' OR ts.segment_space_management <> 'AUTO');
  IF n > 0 THEN
    RAISE_APPLICATION_ERROR(-20002, n || ' table(s) compressed or not in an ASSM tablespace: use MOVE ONLINE, not SHRINK');
  END IF;

  DBMS_OUTPUT.PUT_LINE('Prechecks OK - SHRINK is possible');
END;
/

-- ---------------------------------------------------------------------
-- 2. Online compaction. DDL waits up to 60s for locks instead of
--    failing immediately with ORA-00054.
--    ENABLE ROW MOVEMENT invalidates cursors on the table once
--    (brief re-parse in the application).
-- ---------------------------------------------------------------------
ALTER SESSION SET ddl_lock_timeout = 60;

ALTER TABLE &schema..BOOKING_CONFIRMATION_STAGED_EVENT    ENABLE ROW MOVEMENT;
ALTER TABLE &schema..BOOKING_CONFIRMATION_STAGED_EVENT    SHRINK SPACE COMPACT CASCADE;

ALTER TABLE &schema..BOOKING_CONFIRMATION_OUTGOING_EVENT  ENABLE ROW MOVEMENT;
ALTER TABLE &schema..BOOKING_CONFIRMATION_OUTGOING_EVENT  SHRINK SPACE COMPACT CASCADE;

ALTER TABLE &schema..BOOKING_CONFIRMATION_PROCESSED_EVENT ENABLE ROW MOVEMENT;
ALTER TABLE &schema..BOOKING_CONFIRMATION_PROCESSED_EVENT SHRINK SPACE COMPACT CASCADE;

ALTER TABLE &schema..BOOKING_CONFIRMATION_CAPTURED_EVENT  ENABLE ROW MOVEMENT;
ALTER TABLE &schema..BOOKING_CONFIRMATION_CAPTURED_EVENT  SHRINK SPACE COMPACT CASCADE;

SPOOL OFF
PROMPT Phase 1 done - run reorg_2_finalize.sql in the maintenance window
