--liquibase formatted sql

-- =====================================================================
--  PKG_REORG - online shrink of the booking confirmation event tables
--
--  Goes in the ZEPHYR_ADMIN changelog, next to PKG_ARCHIVE and
--  PKG_PROCESS_LOG. ZEPHYR_ADMIN owns and runs the package; the tables
--  live in another schema, passed as a parameter:
--    local      : EXEC pkg_reorg.compact('ZEPHYR')
--    other envs : EXEC pkg_reorg.compact('BOOKING_CONFIRMATION')
--
--  Privileges: ZEPHYR_ADMIN needs ALTER on each table, granted DIRECTLY
--  by the table owner (see pkg_reorg_grants.changelog.sql). Grants through
--  a role do not work: roles are disabled inside stored PL/SQL.
--
--  compact  - online, row locks only. Safe with traffic.
--  finalize - brief exclusive lock per table while the high-water mark
--             drops. Quiet window. Raises an error if an index is left
--             unusable, so the scheduler marks the run as failed.
--
--  Statistics are NOT gathered here: DBMS_STATS on another schema's
--  tables needs ANALYZE ANY (database-wide). After a shrink only the
--  block count is outdated, which makes full scans look costlier and
--  does not hurt index-driven access. If ANALYZE ANY is granted to
--  ZEPHYR_ADMIN, add DBMS_STATS.GATHER_TABLE_STATS in finalize.
-- =====================================================================

--changeset hamza:pkg-reorg-spec runOnChange:true splitStatements:false
CREATE OR REPLACE PACKAGE pkg_reorg AS
  -- Online: moves rows to the start of each segment, row locks only.
  PROCEDURE compact(p_owner IN VARCHAR2);
  -- Quiet window: lowers the high-water mark and releases the space.
  PROCEDURE finalize(p_owner IN VARCHAR2);
END pkg_reorg;
--rollback DROP PACKAGE pkg_reorg;

--changeset hamza:pkg-reorg-body runOnChange:true splitStatements:false
CREATE OR REPLACE PACKAGE BODY pkg_reorg AS

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

  -- "OWNER"."TABLE"; SCHEMA_NAME fails if the schema does not exist
  FUNCTION tab(p_owner IN VARCHAR2, p_table IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    RETURN DBMS_ASSERT.ENQUOTE_NAME(DBMS_ASSERT.SCHEMA_NAME(UPPER(p_owner)))
        || '.' || DBMS_ASSERT.ENQUOTE_NAME(p_table);
  END tab;

  PROCEDURE compact(p_owner IN VARCHAR2) IS
  BEGIN
    run('ALTER SESSION SET ddl_lock_timeout = 60');
    FOR i IN 1 .. c_tables.COUNT LOOP
      run('ALTER TABLE ' || tab(p_owner, c_tables(i)) || ' ENABLE ROW MOVEMENT');
      run('ALTER TABLE ' || tab(p_owner, c_tables(i)) || ' SHRINK SPACE COMPACT CASCADE');
    END LOOP;
  END compact;

  PROCEDURE finalize(p_owner IN VARCHAR2) IS
    n PLS_INTEGER;
  BEGIN
    run('ALTER SESSION SET ddl_lock_timeout = 60');
    FOR i IN 1 .. c_tables.COUNT LOOP
      run('ALTER TABLE ' || tab(p_owner, c_tables(i)) || ' SHRINK SPACE CASCADE');
      run('ALTER TABLE ' || tab(p_owner, c_tables(i)) || ' DISABLE ROW MOVEMENT');
    END LOOP;

    SELECT COUNT(*) INTO n
      FROM all_indexes
     WHERE table_owner = UPPER(p_owner)
       AND table_name IN (SELECT column_value FROM TABLE(c_tables))
       AND status NOT IN ('VALID', 'N/A');
    IF n > 0 THEN
      RAISE_APPLICATION_ERROR(-20010, n || ' unusable index(es) after reorg');
    END IF;
  END finalize;

END pkg_reorg;
--rollback DROP PACKAGE BODY pkg_reorg;
