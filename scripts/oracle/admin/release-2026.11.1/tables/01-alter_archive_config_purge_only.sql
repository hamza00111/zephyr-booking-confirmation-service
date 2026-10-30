--liquibase formatted sql

--changeset j24720:archive_table_purge_only
-- Purge-only tables: archive_table_name NULL means the rows are deleted in place by pkg_archive instead of being moved
-- to an archive. ck_archive_table_upper already accepts NULL (a CHECK only fails when FALSE).
ALTER TABLE archive_table MODIFY archive_table_name NULL;
--rollback ALTER TABLE archive_table MODIFY archive_table_name NOT NULL;

--changeset j24720:archive_retention_purge_only
-- archive_after_days is NULL for a purge-only table (and required otherwise, checked by pkg_archive). For such a table,
-- purge_after_days is the number of days since date_column before a row in a final status is deleted.
-- ck_archive_retention_days already accepts NULL (a CHECK only fails when FALSE).
ALTER TABLE archive_retention MODIFY archive_after_days NULL;
--rollback ALTER TABLE archive_retention MODIFY archive_after_days NOT NULL;

--changeset j24720:ck_archive_table_condition
-- archive_condition stays required for an archived table: it selects the rows in a final status.
ALTER TABLE archive_table ADD CONSTRAINT ck_archive_table_condition
    CHECK (archive_table_name IS NULL OR archive_condition IS NOT NULL);
--rollback ALTER TABLE archive_table DROP CONSTRAINT ck_archive_table_condition;

--changeset j24720:archive_table_condition_nullable
-- archive_condition is optional for a purge-only table: without it, every row whose date_column is at least
-- purge_after_days days old is deleted.
ALTER TABLE archive_table MODIFY archive_condition NULL;
--rollback ALTER TABLE archive_table MODIFY archive_condition NOT NULL;
