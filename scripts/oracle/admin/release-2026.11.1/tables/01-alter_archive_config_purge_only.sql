--liquibase formatted sql

--changeset j24720:archive_table_purge_only
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:N SELECT nullable FROM all_tab_columns WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND table_name = 'ARCHIVE_TABLE' AND column_name = 'ARCHIVE_TABLE_NAME'
-- Purge-only tables: archive_table_name NULL means the rows are deleted in place by pkg_archive instead of being moved
-- to an archive. ck_archive_table_upper already accepts NULL (a CHECK only fails when FALSE).
-- Marked ran without executing when the column is already nullable (ALTER ... NULL would fail with ORA-01451).
ALTER TABLE archive_table MODIFY archive_table_name NULL;
--rollback ALTER TABLE archive_table MODIFY archive_table_name NOT NULL;

--changeset j24720:archive_retention_purge_only
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:N SELECT nullable FROM all_tab_columns WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND table_name = 'ARCHIVE_RETENTION' AND column_name = 'ARCHIVE_AFTER_DAYS'
-- archive_after_days is NULL for a purge-only table (and required otherwise, checked by pkg_archive). For such a table,
-- purge_after_days is the number of days since date_column before a row is deleted.
-- ck_archive_retention_days already accepts NULL (a CHECK only fails when FALSE).
-- Marked ran without executing when the column is already nullable (ALTER ... NULL would fail with ORA-01451).
ALTER TABLE archive_retention MODIFY archive_after_days NULL;
--rollback ALTER TABLE archive_retention MODIFY archive_after_days NOT NULL;

--changeset j24720:archive_table_condition_nullable
--preconditions onFail:MARK_RAN
--precondition-sql-check expectedResult:N SELECT nullable FROM all_tab_columns WHERE owner = SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AND table_name = 'ARCHIVE_TABLE' AND column_name = 'ARCHIVE_CONDITION'
-- archive_condition is optional: it is an extra filter (e.g. on the status) on top of the date_column one, which always
-- applies. Without it, every row whose date_column is old enough is archived (or deleted for a purge-only table).
-- Marked ran without executing when the column is already nullable (ALTER ... NULL would fail with ORA-01451).
ALTER TABLE archive_table MODIFY archive_condition NULL;
--rollback ALTER TABLE archive_table MODIFY archive_condition NOT NULL;
