--liquibase formatted sql

-- =====================================================================
--  Grants for ZEPHYR_ADMIN.PKG_REORG
--
--  Goes in the TABLE OWNER's changelog (ZEPHYR locally,
--  BOOKING_CONFIRMATION on other envs). The owner grants on its own
--  tables, so no DBA is needed.
--
--  ALTER is the only privilege PKG_REORG needs: it covers
--  ENABLE/DISABLE ROW MOVEMENT and SHRINK SPACE [COMPACT] CASCADE.
--  These must stay DIRECT grants (not through a role) because roles are
--  disabled inside stored PL/SQL.
-- =====================================================================

--changeset hamza:grant-alter-event-tables-to-zephyr-admin
GRANT ALTER ON BOOKING_CONFIRMATION_STAGED_EVENT    TO ZEPHYR_ADMIN;
GRANT ALTER ON BOOKING_CONFIRMATION_OUTGOING_EVENT  TO ZEPHYR_ADMIN;
GRANT ALTER ON BOOKING_CONFIRMATION_PROCESSED_EVENT TO ZEPHYR_ADMIN;
GRANT ALTER ON BOOKING_CONFIRMATION_CAPTURED_EVENT  TO ZEPHYR_ADMIN;
--rollback REVOKE ALTER ON BOOKING_CONFIRMATION_STAGED_EVENT    FROM ZEPHYR_ADMIN;
--rollback REVOKE ALTER ON BOOKING_CONFIRMATION_OUTGOING_EVENT  FROM ZEPHYR_ADMIN;
--rollback REVOKE ALTER ON BOOKING_CONFIRMATION_PROCESSED_EVENT FROM ZEPHYR_ADMIN;
--rollback REVOKE ALTER ON BOOKING_CONFIRMATION_CAPTURED_EVENT  FROM ZEPHYR_ADMIN;
