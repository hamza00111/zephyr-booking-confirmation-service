-- =====================================================================
--  Oracle reorg automation - least-privilege setup
--
--  Model: a DEFINER'S-RIGHTS package owned by the table owner
--  (BOOKING_CONFIRMATION on prod) does the shrink/stats work on a
--  hardcoded table list. The automation account (ZEPHYR_ADMIN) only
--  needs CREATE SESSION + EXECUTE on that package:
--    - no DBA, no ALTER ANY TABLE, no ANALYZE ANY, no ALTER ANY INDEX
--    - no access to the schema owner's credentials (it can even be a
--      schema-only account: CREATE USER ... NO AUTHENTICATION)
--    - it cannot touch any table outside the list
--
--  The owner needs no extra privilege: altering and gathering stats on
--  its own objects is always allowed (roles are not needed, which matters
--  because roles are disabled inside definer's-rights code).
--
--  Section 1 can ship as a Liquibase changeset (runs as the table owner).
--  Section 2 is run once by a DBA.
--
--  Run in SQL*Plus / SQLcl. Set the two names below per environment:
--    prod : owner = BOOKING_CONFIRMATION, runner = ZEPHYR_ADMIN
--    local: owner = ZEPHYR,               runner = ZEPHYR_ADMIN
-- =====================================================================

DEFINE owner  = 'BOOKING_CONFIRMATION'
DEFINE runner = 'ZEPHYR_ADMIN'

-- ---------------------------------------------------------------------
-- 1. Package, created in the table owner's schema
-- ---------------------------------------------------------------------
CREATE OR REPLACE PACKAGE &owner..reorg_maint AUTHID DEFINER AS
  -- Online: row locks only, high-water mark unchanged. Safe in business hours.
  PROCEDURE compact;
  -- Maintenance window: brief exclusive lock per table while the HWM drops,
  -- then stats + verification. Raises an error if anything is left broken,
  -- so the scheduler marks the run as failed.
  PROCEDURE finalize;
END reorg_maint;
/

CREATE OR REPLACE PACKAGE BODY &owner..reorg_maint AS

  c_tables CONSTANT sys.odcivarchar2list := sys.odcivarchar2list(
    'BOOKING_CONFIRMATION_STAGED_EVENT',
    'BOOKING_CONFIRMATION_OUTGOING_EVENT',
    'BOOKING_CONFIRMATION_PROCESSED_EVENT',
    'BOOKING_CONFIRMATION_CAPTURED_EVENT');

  PROCEDURE run(p_sql IN VARCHAR2) IS
  BEGIN
    DBMS_OUTPUT.PUT_LINE(TO_CHAR(SYSTIMESTAMP, 'HH24:MI:SS') || '  ' || p_sql);
    EXECUTE IMMEDIATE p_sql;
  END run;

  FUNCTION q(p_table IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    RETURN DBMS_ASSERT.ENQUOTE_NAME(p_table);
  END q;

  PROCEDURE compact IS
  BEGIN
    run('ALTER SESSION SET ddl_lock_timeout = 60');
    FOR i IN 1 .. c_tables.COUNT LOOP
      run('ALTER TABLE ' || q(c_tables(i)) || ' ENABLE ROW MOVEMENT');
      run('ALTER TABLE ' || q(c_tables(i)) || ' SHRINK SPACE COMPACT');
    END LOOP;
  END compact;

  PROCEDURE finalize IS
    n PLS_INTEGER;
  BEGIN
    run('ALTER SESSION SET ddl_lock_timeout = 60');
    FOR i IN 1 .. c_tables.COUNT LOOP
      run('ALTER TABLE ' || q(c_tables(i)) || ' SHRINK SPACE CASCADE');
      run('ALTER TABLE ' || q(c_tables(i)) || ' DISABLE ROW MOVEMENT');
      DBMS_STATS.GATHER_TABLE_STATS(
        ownname => $$PLSQL_UNIT_OWNER,
        tabname => c_tables(i),
        cascade => TRUE);
    END LOOP;

    SELECT COUNT(*) INTO n
      FROM user_indexes
     WHERE table_name IN (SELECT column_value FROM TABLE(c_tables))
       AND status NOT IN ('VALID', 'N/A');
    IF n > 0 THEN
      RAISE_APPLICATION_ERROR(-20010, n || ' unusable index(es) after reorg');
    END IF;

    SELECT COUNT(*) INTO n FROM user_objects WHERE status = 'INVALID';
    IF n > 0 THEN
      RAISE_APPLICATION_ERROR(-20011, n || ' invalid object(s) after reorg');
    END IF;
  END finalize;

END reorg_maint;
/

-- ---------------------------------------------------------------------
-- 2. Grants to the existing automation account (run once as DBA)
-- ---------------------------------------------------------------------
GRANT CREATE SESSION TO &runner;
GRANT EXECUTE ON &owner..reorg_maint TO &runner;

-- Only if the job is scheduled INSIDE Oracle with DBMS_SCHEDULER
-- (not needed when Control-M / Autosys / cron calls sqlplus):
-- GRANT CREATE JOB TO &runner;

-- ---------------------------------------------------------------------
-- 3. How the job calls it
-- ---------------------------------------------------------------------
--   Daily, business hours:      EXEC booking_confirmation.reorg_maint.compact
--   Weekly, maintenance window: EXEC booking_confirmation.reorg_maint.finalize
--
-- Optional DBMS_SCHEDULER version (as ZEPHYR_ADMIN, needs CREATE JOB):
-- BEGIN
--   DBMS_SCHEDULER.CREATE_JOB(
--     job_name        => 'REORG_COMPACT',
--     job_type        => 'PLSQL_BLOCK',
--     job_action      => 'BEGIN booking_confirmation.reorg_maint.compact; END;',
--     repeat_interval => 'FREQ=DAILY;BYHOUR=14;BYMINUTE=0',
--     enabled         => TRUE);
--   DBMS_SCHEDULER.CREATE_JOB(
--     job_name        => 'REORG_FINALIZE',
--     job_type        => 'PLSQL_BLOCK',
--     job_action      => 'BEGIN booking_confirmation.reorg_maint.finalize; END;',
--     repeat_interval => 'FREQ=WEEKLY;BYDAY=SUN;BYHOUR=3;BYMINUTE=0',
--     enabled         => TRUE);
-- END;
-- /
