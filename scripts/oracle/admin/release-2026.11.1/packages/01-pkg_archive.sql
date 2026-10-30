--liquibase formatted sql

--changeset j24720:pkg_archive splitStatements:false runOnChange:true
CREATE OR REPLACE PACKAGE pkg_archive AS
    -- Archives then purges the tables of p_owner configured in archive_table / archive_retention (every enabled table,
    -- or only p_table_name). For each table and enabled retention row (scope value):
    -- Retentions are calendar days, cut at midnight of the database clock: N = dated before TRUNC(SYSDATE) - N + 1
    -- (1 = yesterday or earlier), whatever the time of the run.
    --   1. rows matching archive_condition and whose date_column is at least archive_after_days days old are moved
    --      (all columns shared with the archive table) to archive_table_name, by batches of batch_size;
    --   2. rows of the archive table archived at least purge_after_days days ago are deleted, by batches.
    -- A purge-only table (archive_table_name NULL) skips step 1; step 2 deletes, from the table itself, the rows
    -- matching archive_condition (every row when it is empty) whose date_column is at least purge_after_days days old.
    -- Every step is logged in process_log. A failing table/scope is rolled back, logged and skipped; the call then
    -- ends with ORA-20010 so that the caller (db job runner) reports the failure.
    -- Requires direct grants to the package owner: SELECT, DELETE on the tables, SELECT, INSERT, DELETE on the archives.
    PROCEDURE p_run(p_owner IN VARCHAR2, p_table_name IN VARCHAR2 DEFAULT NULL);
END pkg_archive;
--rollback DROP PACKAGE pkg_archive;

--changeset j24720:pkg_archive_body splitStatements:false runOnChange:true
CREATE OR REPLACE PACKAGE BODY pkg_archive AS

    c_process CONSTANT VARCHAR2(128) := 'pkg_archive';
    c_all_scopes CONSTANT VARCHAR2(1) := '*';

    -- how the scope of a retention row is applied
    c_scope_none CONSTANT PLS_INTEGER := 0; -- table without scope_column
    c_scope_value CONSTANT PLS_INTEGER := 1; -- scope_column = scope_value
    c_scope_others CONSTANT PLS_INTEGER := 2; -- '*': values without a retention row of their own, and NULL

    TYPE t_table_list IS TABLE OF archive_table%ROWTYPE;
    TYPE t_retention_list IS TABLE OF archive_retention%ROWTYPE;

    -- SQL of the step being executed and its bind values: logged with the step result, or with its error
    g_sql CLOB;
    g_binds VARCHAR2(1000);

    PROCEDURE set_sql(p_sql IN CLOB, p_binds IN VARCHAR2) IS
    BEGIN
        g_sql := p_sql;
        g_binds := p_binds;
    END set_sql;

    FUNCTION quote(p_name IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN DBMS_ASSERT.ENQUOTE_NAME(p_name, FALSE);
    END quote;

    FUNCTION qualified(p_owner IN VARCHAR2, p_name IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        RETURN quote(p_owner) || '.' || quote(p_name);
    END qualified;

    PROCEDURE log_write(p_level IN VARCHAR2, p_run_id IN VARCHAR2, p_owner IN VARCHAR2, p_step IN VARCHAR2,
                        p_table_name IN VARCHAR2, p_scope IN VARCHAR2, p_message IN VARCHAR2,
                        p_row_count IN NUMBER DEFAULT NULL, p_sql_text IN CLOB DEFAULT NULL) IS
    BEGIN
        pkg_process_log.log_write(p_level, p_run_id, p_owner, c_process, p_step, p_table_name, p_scope, p_message,
                                  p_row_count, p_sql_text);
    END log_write;

    -- ' - binds: ...' suffix for the log message of a step
    FUNCTION binds_suffix RETURN VARCHAR2 IS
    BEGIN
        RETURN CASE WHEN g_binds IS NOT NULL THEN ' - binds: ' || g_binds END;
    END binds_suffix;

    PROCEDURE require_table(p_owner IN VARCHAR2, p_table IN VARCHAR2) IS
        l_count PLS_INTEGER;
    BEGIN
        SELECT COUNT(*) INTO l_count FROM all_tables WHERE owner = p_owner AND table_name = p_table;
        IF l_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20011,
                                    'Table ' || p_owner || '.' || p_table || ' does not exist or is not visible to '
                                        || USER || ' (missing grant?)');
        END IF;
    END require_table;

    PROCEDURE require_column(p_owner IN VARCHAR2, p_table IN VARCHAR2, p_column IN VARCHAR2) IS
        l_count PLS_INTEGER;
    BEGIN
        SELECT COUNT(*)
        INTO l_count
        FROM all_tab_columns
        WHERE owner = p_owner
          AND table_name = p_table
          AND column_name = p_column;
        IF l_count = 0 THEN
            RAISE_APPLICATION_ERROR(-20012, 'Column ' || p_column || ' not found in ' || p_owner || '.' || p_table);
        END IF;
    END require_column;

    -- archive_condition is concatenated into the archive query. Reject what could end or neutralise the rest of the
    -- query (statement separator, comments), then check that it parses as a predicate on the table: parse only,
    -- nothing is executed. Unknown columns / syntax errors are reported with the Oracle message.
    -- It may be empty only for a purge-only table.
    PROCEDURE check_condition(p_cfg IN archive_table%ROWTYPE) IS
        l_cursor INTEGER;
    BEGIN
        IF TRIM(p_cfg.archive_condition) IS NULL THEN
            IF p_cfg.archive_table_name IS NULL THEN
                RETURN;
            END IF;
            RAISE_APPLICATION_ERROR(-20013, 'archive_condition is empty, it is required when archive_table_name is set');
        END IF;
        IF INSTR(p_cfg.archive_condition, ';') > 0 OR INSTR(p_cfg.archive_condition, '--') > 0
            OR INSTR(p_cfg.archive_condition, '/*') > 0 THEN
            RAISE_APPLICATION_ERROR(-20017, 'archive_condition must not contain '';'', ''--'' or ''/*''');
        END IF;
        set_sql('SELECT 1 FROM ' || qualified(p_cfg.owner, p_cfg.table_name) || ' x WHERE ('
                    || p_cfg.archive_condition || ')', 'none (parse only)');
        l_cursor := DBMS_SQL.OPEN_CURSOR;
        BEGIN
            DBMS_SQL.PARSE(l_cursor, g_sql, DBMS_SQL.NATIVE);
            DBMS_SQL.CLOSE_CURSOR(l_cursor);
            set_sql(NULL, NULL);
        EXCEPTION
            WHEN OTHERS THEN
                IF DBMS_SQL.IS_OPEN(l_cursor) THEN
                    DBMS_SQL.CLOSE_CURSOR(l_cursor);
                END IF;
                RAISE_APPLICATION_ERROR(-20018, 'Invalid archive_condition: ' || SQLERRM);
        END;
    END check_condition;

    -- Columns present in both the table and its archive (dictionary order), except the archive's own columns.
    -- Table columns missing from the archive are reported as a warning: their values are not archived.
    FUNCTION common_columns(p_cfg IN archive_table%ROWTYPE, p_run_id IN VARCHAR2) RETURN VARCHAR2 IS
        l_columns VARCHAR2(32767);
        l_missing VARCHAR2(4000);
    BEGIN
        FOR c IN (SELECT l.column_name, CASE WHEN a.column_name IS NULL THEN 'N' ELSE 'Y' END AS in_archive
                  FROM all_tab_cols l
                           LEFT JOIN all_tab_cols a
                                     ON a.owner = l.owner
                                         AND a.table_name = p_cfg.archive_table_name
                                         AND a.column_name = l.column_name
                                         AND a.virtual_column = 'NO'
                                         AND a.hidden_column = 'NO'
                  WHERE l.owner = p_cfg.owner
                    AND l.table_name = p_cfg.table_name
                    AND l.virtual_column = 'NO'
                    AND l.hidden_column = 'NO'
                    AND l.column_name NOT IN ('ARCHIVE_ID', 'ARCHIVED_AT')
                  ORDER BY l.column_id)
            LOOP
                IF c.in_archive = 'Y' THEN
                    l_columns := l_columns || CASE WHEN l_columns IS NOT NULL THEN ', ' END || quote(c.column_name);
                ELSE
                    l_missing := SUBSTR(l_missing || CASE WHEN l_missing IS NOT NULL THEN ', ' END || c.column_name, 1,
                                        3000);
                END IF;
            END LOOP;
        IF l_columns IS NULL THEN
            RAISE_APPLICATION_ERROR(-20014, 'No column in common between ' || p_cfg.owner || '.' || p_cfg.table_name
                || ' and ' || p_cfg.archive_table_name);
        END IF;
        IF l_missing IS NOT NULL THEN
            log_write(pkg_process_log.c_warning, p_run_id, p_cfg.owner, 'CONFIG', p_cfg.table_name, NULL,
                      'Columns missing from ' || p_cfg.archive_table_name || ', their values are not archived: ' ||
                      l_missing);
        END IF;
        RETURN l_columns;
    END common_columns;

    -- Extra predicate on alias x for the scope of a retention row. Bind order: :scope_value, or :owner, :table_name.
    FUNCTION scope_predicate(p_cfg IN archive_table%ROWTYPE, p_mode IN PLS_INTEGER) RETURN VARCHAR2 IS
    BEGIN
        RETURN CASE p_mode
                   WHEN c_scope_none THEN NULL
                   WHEN c_scope_value THEN ' AND x.' || quote(p_cfg.scope_column) || ' = :scope_value'
                   ELSE ' AND NOT EXISTS (SELECT 1 FROM archive_retention r WHERE r.owner = :owner'
                       || ' AND r.table_name = :table_name AND r.scope_value = x.' || quote(p_cfg.scope_column) || ')'
            END;
    END scope_predicate;

    -- bind values of scope_predicate, for the log
    FUNCTION scope_binds(p_cfg IN archive_table%ROWTYPE, p_ret IN archive_retention%ROWTYPE, p_mode IN PLS_INTEGER)
        RETURN VARCHAR2 IS
    BEGIN
        RETURN CASE p_mode
                   WHEN c_scope_value THEN ', :scope_value=' || p_ret.scope_value
                   WHEN c_scope_others THEN ', :owner=' || p_cfg.owner || ', :table_name=' || p_cfg.table_name
            END;
    END scope_binds;

    PROCEDURE archive_rows(p_cfg IN archive_table%ROWTYPE,
                           p_ret IN archive_retention%ROWTYPE,
                           p_mode IN PLS_INTEGER,
                           p_columns IN VARCHAR2,
                           o_archived OUT NUMBER) IS
        l_live     VARCHAR2(300) := qualified(p_cfg.owner, p_cfg.table_name);
        l_archive  VARCHAR2(300) := qualified(p_cfg.owner, p_cfg.archive_table_name);
        l_select   VARCHAR2(32767);
        l_insert   VARCHAR2(32767);
        l_delete   VARCHAR2(32767);
        l_cursor   SYS_REFCURSOR;
        l_ids      SYS.ODCIVARCHAR2LIST;
        l_inserted NUMBER;
        l_deleted  NUMBER;
    BEGIN
        o_archived := 0;
        l_select := 'SELECT ROWIDTOCHAR(x.ROWID) FROM ' || l_live || ' x'
            || ' WHERE (' || p_cfg.archive_condition || ')'
            || ' AND x.' || quote(p_cfg.date_column)
            || ' < TRUNC(SYSDATE) - :archive_after_days + 1'
            || scope_predicate(p_cfg, p_mode)
            || ' FOR UPDATE SKIP LOCKED';
        l_insert := 'INSERT INTO ' || l_archive || ' (' || p_columns || ') SELECT ' || p_columns || ' FROM ' || l_live
            || ' WHERE ROWID IN (SELECT CHARTOROWID(column_value) FROM TABLE(:ids))';
        l_delete := 'DELETE FROM ' || l_live || ' WHERE ROWID IN (SELECT CHARTOROWID(column_value) FROM TABLE(:ids))';
        set_sql(TO_CLOB(l_select) || ';' || CHR(10) || TO_CLOB(l_insert) || ';' || CHR(10) || TO_CLOB(l_delete) || ';',
                ':archive_after_days=' || p_ret.archive_after_days || scope_binds(p_cfg, p_ret, p_mode)
                    || ', fetch limit=' || p_ret.batch_size || ', :ids=rowids fetched by each batch');
        LOOP
            -- rows are locked as they are fetched; locks are kept until the COMMIT of the batch
            CASE p_mode
                WHEN c_scope_none THEN OPEN l_cursor FOR l_select USING p_ret.archive_after_days;
                WHEN c_scope_value THEN OPEN l_cursor FOR l_select USING p_ret.archive_after_days, p_ret.scope_value;
                ELSE OPEN l_cursor FOR l_select USING p_ret.archive_after_days, p_cfg.owner, p_cfg.table_name;
                END CASE;
            FETCH l_cursor BULK COLLECT INTO l_ids LIMIT p_ret.batch_size;
            CLOSE l_cursor;
            EXIT WHEN l_ids.COUNT = 0;

            EXECUTE IMMEDIATE l_insert USING l_ids;
            l_inserted := SQL%ROWCOUNT;
            EXECUTE IMMEDIATE l_delete USING l_ids;
            l_deleted := SQL%ROWCOUNT;
            IF l_inserted <> l_deleted THEN
                RAISE_APPLICATION_ERROR(-20015, 'Inserted ' || l_inserted || ' rows into ' || l_archive
                    || ' but deleted ' || l_deleted || ' from ' || l_live);
            END IF;
            COMMIT;
            o_archived := o_archived + l_deleted;
            EXIT WHEN l_ids.COUNT < p_ret.batch_size;
        END LOOP;
    END archive_rows;

    PROCEDURE purge_rows(p_cfg IN archive_table%ROWTYPE,
                         p_ret IN archive_retention%ROWTYPE,
                         p_mode IN PLS_INTEGER,
                         o_purged OUT NUMBER) IS
        l_delete  VARCHAR2(32767);
        l_deleted NUMBER;
    BEGIN
        o_purged := 0;
        IF p_cfg.archive_table_name IS NULL THEN
            -- purge-only table: its own rows matching archive_condition (all rows when it is empty), by date_column
            l_delete := 'DELETE FROM ' || qualified(p_cfg.owner, p_cfg.table_name) || ' x'
                || ' WHERE x.' || quote(p_cfg.date_column) || ' < TRUNC(SYSDATE) - :purge_after_days + 1'
                || CASE WHEN TRIM(p_cfg.archive_condition) IS NOT NULL
                        THEN ' AND (' || p_cfg.archive_condition || ')' END;
        ELSE
            l_delete := 'DELETE FROM ' || qualified(p_cfg.owner, p_cfg.archive_table_name) || ' x'
                || ' WHERE x.ARCHIVED_AT < TRUNC(SYSDATE) - :purge_after_days + 1';
        END IF;
        l_delete := l_delete
            || scope_predicate(p_cfg, p_mode)
            || ' AND ROWNUM <= :batch_size';
        set_sql(l_delete || ';', ':purge_after_days=' || p_ret.purge_after_days || scope_binds(p_cfg, p_ret, p_mode)
            || ', :batch_size=' || p_ret.batch_size);
        LOOP
            CASE p_mode
                WHEN c_scope_none THEN EXECUTE IMMEDIATE l_delete USING p_ret.purge_after_days, p_ret.batch_size;
                WHEN c_scope_value
                    THEN EXECUTE IMMEDIATE l_delete USING p_ret.purge_after_days, p_ret.scope_value, p_ret.batch_size;
                ELSE EXECUTE IMMEDIATE l_delete USING p_ret.purge_after_days, p_cfg.owner, p_cfg.table_name,
                    p_ret.batch_size;
                END CASE;
            l_deleted := SQL%ROWCOUNT;
            COMMIT;
            o_purged := o_purged + l_deleted;
            EXIT WHEN l_deleted < p_ret.batch_size;
        END LOOP;
    END purge_rows;

    -- Validates the configuration of one table against the dictionary; returns the column list to archive (NULL for a
    -- purge-only table).
    FUNCTION prepare_table(p_cfg IN archive_table%ROWTYPE, p_run_id IN VARCHAR2) RETURN VARCHAR2 IS
    BEGIN
        require_table(p_cfg.owner, p_cfg.table_name);
        require_column(p_cfg.owner, p_cfg.table_name, p_cfg.date_column);
        IF p_cfg.scope_column IS NOT NULL THEN
            require_column(p_cfg.owner, p_cfg.table_name, p_cfg.scope_column);
        END IF;
        check_condition(p_cfg);
        IF p_cfg.archive_table_name IS NULL THEN
            RETURN NULL;
        END IF;
        require_table(p_cfg.owner, p_cfg.archive_table_name);
        require_column(p_cfg.owner, p_cfg.archive_table_name, 'ARCHIVED_AT');
        IF p_cfg.scope_column IS NOT NULL THEN
            require_column(p_cfg.owner, p_cfg.archive_table_name, p_cfg.scope_column);
        END IF;
        RETURN common_columns(p_cfg, p_run_id);
    END prepare_table;

    PROCEDURE p_run(p_owner IN VARCHAR2, p_table_name IN VARCHAR2 DEFAULT NULL) IS
        l_owner      VARCHAR2(128) := UPPER(TRIM(p_owner));
        l_table      VARCHAR2(128) := UPPER(TRIM(p_table_name));
        l_run_id     VARCHAR2(32)  := pkg_process_log.new_run_id;
        l_tables     t_table_list;
        l_retentions t_retention_list;
        l_columns    VARCHAR2(32767);
        l_mode       PLS_INTEGER;
        l_archived   NUMBER;
        l_purged     NUMBER;
        l_failures   PLS_INTEGER   := 0;
        l_first_err  VARCHAR2(1000);
        l_valid      BOOLEAN;

        -- counts a failure and keeps the first message, repeated in the final ORA-20010 so the caller sees the cause
        PROCEDURE fail(p_message IN VARCHAR2) IS
        BEGIN
            l_failures := l_failures + 1;
            l_first_err := NVL(l_first_err, SUBSTRB(p_message, 1, 1000)); -- bytes: VARCHAR2(1000) is a byte limit
        END fail;
    BEGIN
        IF l_owner IS NULL THEN
            RAISE_APPLICATION_ERROR(-20001, 'p_owner is required');
        END IF;
        log_write(pkg_process_log.c_info, l_run_id, l_owner, 'START', l_table, NULL, 'Archive run started');
        DBMS_OUTPUT.PUT_LINE('run_id=' || l_run_id || ' owner=' || l_owner || ' table=' || NVL(l_table, '*'));

        SELECT * BULK COLLECT
        INTO l_tables
        FROM archive_table
        WHERE owner = l_owner
          AND enabled = 'Y'
          AND (l_table IS NULL OR table_name = l_table)
        ORDER BY table_name;

        IF l_tables.COUNT = 0 THEN
            fail('No enabled archive_table row for owner ' || l_owner || ' and table ' || NVL(l_table, '*'));
            log_write(pkg_process_log.c_error, l_run_id, l_owner, 'CONFIG', l_table, NULL, l_first_err);
            DBMS_OUTPUT.PUT_LINE('status=FAILED error=' || l_first_err);
        END IF;

        FOR t IN 1 .. l_tables.COUNT
            LOOP
                l_valid := TRUE;
                BEGIN
                    set_sql(NULL, NULL);
                    l_columns := prepare_table(l_tables(t), l_run_id);
                EXCEPTION
                    WHEN OTHERS THEN
                        l_valid := FALSE;
                        fail(l_tables(t).table_name || ': ' || SQLERRM);
                        log_write(pkg_process_log.c_error, l_run_id, l_owner, 'CONFIG', l_tables(t).table_name, NULL,
                                  SQLERRM, NULL, g_sql);
                        DBMS_OUTPUT.PUT_LINE('table=' || l_tables(t).table_name || ' status=FAILED error=' || SQLERRM);
                END;

                IF l_valid THEN
                    SELECT * BULK COLLECT
                    INTO l_retentions
                    FROM archive_retention
                    WHERE owner = l_owner
                      AND table_name = l_tables(t).table_name
                      AND enabled = 'Y'
                    ORDER BY CASE WHEN scope_value = c_all_scopes THEN 1 ELSE 0 END, scope_value;

                    IF l_retentions.COUNT = 0 THEN
                        log_write(pkg_process_log.c_warning, l_run_id, l_owner, 'CONFIG', l_tables(t).table_name, NULL,
                                  'No enabled archive_retention row, nothing archived');
                        DBMS_OUTPUT.PUT_LINE('table=' || l_tables(t).table_name ||
                                             ' status=SKIPPED reason=no enabled archive_retention row');
                    END IF;

                    FOR r IN 1 .. l_retentions.COUNT
                        LOOP
                            BEGIN
                                IF l_tables(t).archive_table_name IS NULL
                                    AND l_retentions(r).archive_after_days IS NOT NULL THEN
                                    RAISE_APPLICATION_ERROR(-20019,
                                                            'Table has no archive_table_name (purge only), archive_after_days must be NULL');
                                ELSIF l_tables(t).archive_table_name IS NOT NULL
                                    AND l_retentions(r).archive_after_days IS NULL THEN
                                    RAISE_APPLICATION_ERROR(-20019,
                                                            'Table has an archive_table_name, archive_after_days is required');
                                END IF;
                                IF l_tables(t).scope_column IS NULL THEN
                                    IF l_retentions(r).scope_value <> c_all_scopes THEN
                                        RAISE_APPLICATION_ERROR(-20016,
                                                                'Table has no scope_column, only scope_value ''*'' is allowed');
                                    END IF;
                                    l_mode := c_scope_none;
                                ELSIF l_retentions(r).scope_value = c_all_scopes THEN
                                    l_mode := c_scope_others;
                                ELSE
                                    l_mode := c_scope_value;
                                END IF;

                                IF l_tables(t).archive_table_name IS NULL THEN
                                    l_archived := 0; -- purge-only table
                                ELSE
                                    set_sql(NULL, NULL);
                                    archive_rows(l_tables(t), l_retentions(r), l_mode, l_columns, l_archived);
                                    log_write(pkg_process_log.c_info, l_run_id, l_owner, 'ARCHIVE', l_tables(t).table_name,
                                              l_retentions(r).scope_value,
                                              'Rows moved to ' || l_tables(t).archive_table_name || ' (dated before '
                                                  || TO_CHAR(TRUNC(SYSDATE) - l_retentions(r).archive_after_days + 1,
                                                             'YYYY-MM-DD') || ')' || binds_suffix,
                                              l_archived, g_sql);
                                END IF;

                                set_sql(NULL, NULL);
                                purge_rows(l_tables(t), l_retentions(r), l_mode, l_purged);
                                log_write(pkg_process_log.c_info, l_run_id, l_owner, 'PURGE', l_tables(t).table_name,
                                          l_retentions(r).scope_value,
                                          'Rows deleted from ' || NVL(l_tables(t).archive_table_name, l_tables(t).table_name)
                                              || CASE WHEN l_tables(t).archive_table_name IS NULL
                                                      THEN ' (dated before ' ELSE ' (archived before ' END
                                              || TO_CHAR(TRUNC(SYSDATE) - l_retentions(r).purge_after_days + 1,
                                                         'YYYY-MM-DD') || ')' || binds_suffix,
                                          l_purged, g_sql);

                                DBMS_OUTPUT.PUT_LINE('table=' || l_tables(t).table_name || ' scope=' ||
                                                     l_retentions(r).scope_value
                                    || ' archived=' || l_archived || ' purged=' || l_purged);
                            EXCEPTION
                                WHEN OTHERS THEN
                                    ROLLBACK;
                                    fail(l_tables(t).table_name || ' scope=' || l_retentions(r).scope_value || ': ' ||
                                         SQLERRM);
                                    log_write(pkg_process_log.c_error, l_run_id, l_owner, 'ERROR',
                                              l_tables(t).table_name,
                                              l_retentions(r).scope_value,
                                              SQLERRM || ' ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || binds_suffix,
                                              NULL, g_sql);
                                    DBMS_OUTPUT.PUT_LINE('table=' || l_tables(t).table_name || ' scope='
                                        || l_retentions(r).scope_value || ' status=FAILED error=' || SQLERRM);
                            END;
                        END LOOP;
                END IF;
            END LOOP;

        log_write(CASE WHEN l_failures = 0 THEN pkg_process_log.c_info ELSE pkg_process_log.c_error END,
                  l_run_id, l_owner, 'END', l_table, NULL,
                  'Archive run finished: ' || l_tables.COUNT || ' table(s), ' || l_failures || ' failure(s)');
        IF l_failures > 0 THEN
            RAISE_APPLICATION_ERROR(-20010, l_failures || ' archive step(s) failed for ' || l_owner || ', first: '
                || l_first_err || ' (see process_log run_id=' || l_run_id || ')');
        END IF;
    END p_run;
END pkg_archive;
--rollback DROP PACKAGE BODY pkg_archive;
