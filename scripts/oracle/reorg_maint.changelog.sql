--liquibase formatted sql

-- =====================================================================
--  REORG_MAINT - online shrink of the booking confirmation event tables
--
--  Created in the schema Liquibase connects as (the table owner):
--    prod : BOOKING_CONFIRMATION      local: ZEPHYR
--  Object names are unqualified, so the same changelog works everywhere.
--
--  Definer's rights: the package runs with the owner's rights, so the
--  automation account (ZEPHYR_ADMIN) only needs CREATE SESSION + EXECUTE
--  on it. No DBA, ALTER ANY TABLE or ANALYZE ANY. The owner grants
--  EXECUTE itself, so no DBA step is needed.
--
--  Called by the scheduler as ZEPHYR_ADMIN:
--    compact  - online, row locks only:        EXEC booking_confirmation.reorg_maint.compact
--    finalize - brief exclusive lock per table: EXEC booking_confirmation.reorg_maint.finalize
--               (quiet window; raises an error if indexes/objects are left broken)
-- =====================================================================

--changeset hamza:reorg-maint-spec runOnChange:true splitStatements:false
CREATE OR REPLACE PACKAGE reorg_maint AUTHID DEFINER AS
  -- Online: row locks only, high-water mark unchanged. Safe with traffic.
  PROCEDURE compact;
  -- Quiet window: brief exclusive lock per table while the HWM drops,
  -- then stats + verification. Raises an error if anything is left broken,
  -- so the scheduler marks the run as failed.
  PROCEDURE finalize;
END reorg_maint;
--rollback DROP PACKAGE reorg_maint;

--changeset hamza:reorg-maint-body runOnChange:true splitStatements:false
CREATE OR REPLACE PACKAGE BODY reorg_maint AS

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
      run('ALTER TABLE ' || q(c_tables(i)) || ' SHRINK SPACE COMPACT CASCADE');
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
--rollback DROP PACKAGE BODY reorg_maint;

--changeset hamza:reorg-maint-grant
GRANT EXECUTE ON reorg_maint TO ZEPHYR_ADMIN;
--rollback REVOKE EXECUTE ON reorg_maint FROM ZEPHYR_ADMIN;
