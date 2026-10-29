--liquibase formatted sql

-- =====================================================================
--  PKG_REORG - online shrink of the booking confirmation event tables
--
--  Goes in the ZEPHYR_ADMIN (admin) changelog, next to PKG_ARCHIVE and
--  PKG_PROCESS_LOG, and is run by zephyr-framework-db-job-cli connected
--  as ZEPHYR_ADMIN (see jobs/reorg-compact-jobs.yml and
--  jobs/reorg-finalize-jobs.yml). The tables live in the owner schema
--  passed as p_owner: ZEPHYR locally, BOOKING_CONFIRMATION elsewhere.
--
--  Privileges: ZEPHYR_ADMIN needs ALTER on each table, granted DIRECTLY
--  by the table owner (pkg_reorg_grants.changelog.sql). ALTER is an
--  object privilege, so the DDL works cross-schema; grants through a
--  role do not, because roles are disabled inside stored PL/SQL.
--
--  p_compact  - online, row locks only. Safe with traffic.
--  p_finalize - brief exclusive lock per table while the high-water mark
--               drops. Quiet window.
--
--  Each table is processed independently: a failure (e.g. ORA-00054 lock
--  timeout) is reported and the next table still runs; the call raises at
--  the end so the job is marked failed. One DBMS_OUTPUT line per table;
--  the job CLI prefixes it with "DB_JOB_OUTPUT job=<job>":
--    table=<table> status=<OK|FAILED> durationMs=<n> [error="..."]
--
--  p_compact shrinks the tables only: COMPACT is not supported for
--  SecureFile LOBs (ORA-10695), so indexes and LOBs are shrunk by the
--  SHRINK SPACE CASCADE of p_finalize.
--
--  Statistics are NOT gathered: DBMS_STATS on another schema's tables
--  needs ANALYZE ANY (database-wide). After a shrink only the block count
--  is outdated, which does not hurt index-driven access.
-- =====================================================================

--changeset hamza:pkg-reorg-spec runOnChange:true splitStatements:false
CREATE OR REPLACE PACKAGE pkg_reorg AS
  -- Online: moves rows to the start of each segment, row locks only.
  PROCEDURE p_compact(p_owner IN VARCHAR2);
  -- Quiet window: lowers the high-water mark and releases the space.
  PROCEDURE p_finalize(p_owner IN VARCHAR2);
END pkg_reorg;
--rollback DROP PACKAGE pkg_reorg;

--changeset hamza:pkg-reorg-body runOnChange:true splitStatements:false
CREATE OR REPLACE PACKAGE BODY pkg_reorg AS

  c_tables CONSTANT sys.odcivarchar2list := sys.odcivarchar2list(
    'BOOKING_CONFIRMATION_STAGED_EVENT',
    'BOOKING_CONFIRMATION_OUTGOING_EVENT',
    'BOOKING_CONFIRMATION_PROCESSED_EVENT',
    'BOOKING_CONFIRMATION_CAPTURED_EVENT');

  -- "OWNER"."TABLE"; SCHEMA_NAME fails if the schema does not exist
  FUNCTION tab(p_owner IN VARCHAR2, p_table IN VARCHAR2) RETURN VARCHAR2 IS
  BEGIN
    RETURN DBMS_ASSERT.ENQUOTE_NAME(DBMS_ASSERT.SCHEMA_NAME(UPPER(p_owner)))
        || '.' || DBMS_ASSERT.ENQUOTE_NAME(p_table);
  END tab;

  FUNCTION ms_since(p_start IN TIMESTAMP WITH TIME ZONE) RETURN NUMBER IS
    l INTERVAL DAY(9) TO SECOND(3) := SYSTIMESTAMP - p_start;
  BEGIN
    RETURN ROUND((EXTRACT(DAY FROM l) * 86400 + EXTRACT(HOUR FROM l) * 3600
                + EXTRACT(MINUTE FROM l) * 60 + EXTRACT(SECOND FROM l)) * 1000);
  END ms_since;

  -- The job CLI prefixes each line with "DB_JOB_OUTPUT job=<job>"
  PROCEDURE emit(p_table  IN VARCHAR2,
                 p_status IN VARCHAR2,
                 p_start  IN TIMESTAMP WITH TIME ZONE,
                 p_error  IN VARCHAR2 DEFAULT NULL) IS
  BEGIN
    DBMS_OUTPUT.PUT_LINE('table=' || p_table
      || ' status=' || p_status
      || ' durationMs=' || ms_since(p_start)
      || CASE WHEN p_error IS NOT NULL
              THEN ' error="' || REPLACE(p_error, CHR(10), ' ') || '"' END);
  END emit;

  PROCEDURE process(p_job IN VARCHAR2, p_owner IN VARCHAR2, p_finalize IN BOOLEAN) IS
    l_table  VARCHAR2(300);
    l_start  TIMESTAMP WITH TIME ZONE;
    l_failed PLS_INTEGER := 0;
    l_unusable PLS_INTEGER;
  BEGIN
    -- Wait up to 60s for locks instead of failing at once with ORA-00054
    EXECUTE IMMEDIATE 'ALTER SESSION SET ddl_lock_timeout = 60';

    FOR i IN 1 .. c_tables.COUNT LOOP
      l_start := SYSTIMESTAMP;
      BEGIN
        l_table := tab(p_owner, c_tables(i));
        IF p_finalize THEN
          EXECUTE IMMEDIATE 'ALTER TABLE ' || l_table || ' SHRINK SPACE CASCADE';
          EXECUTE IMMEDIATE 'ALTER TABLE ' || l_table || ' DISABLE ROW MOVEMENT';
        ELSE
          EXECUTE IMMEDIATE 'ALTER TABLE ' || l_table || ' ENABLE ROW MOVEMENT';
          EXECUTE IMMEDIATE 'ALTER TABLE ' || l_table || ' SHRINK SPACE COMPACT';
        END IF;
        emit(c_tables(i), 'OK', l_start);
      EXCEPTION
        WHEN OTHERS THEN
          l_failed := l_failed + 1;
          emit(c_tables(i), 'FAILED', l_start, SQLERRM);
      END;
    END LOOP;

    IF p_finalize THEN
      -- ALL_INDEXES.STATUS is VALID or UNUSABLE (N/A for partitioned indexes)
      SELECT COUNT(*) INTO l_unusable
        FROM all_indexes
       WHERE table_owner = UPPER(p_owner)
         AND table_name IN (SELECT column_value FROM TABLE(c_tables))
         AND status = 'UNUSABLE';
      IF l_unusable > 0 THEN
        RAISE_APPLICATION_ERROR(-20010, l_unusable || ' unusable index(es) after ' || p_job);
      END IF;
    END IF;

    IF l_failed > 0 THEN
      RAISE_APPLICATION_ERROR(-20011, l_failed || ' table(s) failed in ' || p_job);
    END IF;
  END process;

  PROCEDURE p_compact(p_owner IN VARCHAR2) IS
  BEGIN
    process('reorg-compact', p_owner, p_finalize => FALSE);
  END p_compact;

  PROCEDURE p_finalize(p_owner IN VARCHAR2) IS
  BEGIN
    process('reorg-finalize', p_owner, p_finalize => TRUE);
  END p_finalize;

END pkg_reorg;
--rollback DROP PACKAGE BODY pkg_reorg;
