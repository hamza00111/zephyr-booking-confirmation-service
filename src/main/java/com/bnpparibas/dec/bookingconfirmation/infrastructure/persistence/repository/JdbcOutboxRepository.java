package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/**
 * Oracle JDBC implementation of the outbox (mirrors the publisher's relay drain semantics).
 *
 * <p>{@link #findNew} claims rows with {@code FOR UPDATE SKIP LOCKED} (runs inside the RELAY
 * transaction). No data is ever lost on send failure: transient failures go to {@code SEND_FAILURE}
 * with an exponential-backoff {@code NEXT_ATTEMPT_AT} and are retried indefinitely by
 * {@link #requeueReady}; only poison goes to {@code PARKED} (retained, replayable).
 */
@Repository
public class JdbcOutboxRepository implements OutboxRepository {

    private static final String INSERT_SQL =
            """
            INSERT INTO BOOKING_CONFIRMATION_OUTBOX
                (REGION, IDEMPOTENCY_KEY, DESTINATION, MESSAGE_KEY, EVENT_TYPE, EVENT_PAYLOAD, INBOX_ID, TRACE_ID, HEADERS, PROCESSING_STATUS)
            VALUES
                (:region, :idempotencyKey, :destination, :messageKey, :eventType, :payload, :inboxId, :traceId, :headers, 'NEW')
            """;

    private static final String SELECT_NEW =
            """
            SELECT ID, REGION, IDEMPOTENCY_KEY, DESTINATION, MESSAGE_KEY, EVENT_TYPE, EVENT_PAYLOAD, INBOX_ID, TRACE_ID, HEADERS
            FROM BOOKING_CONFIRMATION_OUTBOX
            WHERE ID IN (
                SELECT ID
                FROM BOOKING_CONFIRMATION_OUTBOX
                WHERE PROCESSING_STATUS = 'NEW'
                  AND REGION = :region
                ORDER BY ID
                FETCH FIRST :limit ROWS ONLY
            )
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_SENT =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'SENT', UPDATED_ON = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    // Transient failure: schedule the next retry with exponential backoff capped at :capSeconds.
    // POWER(2, RETRY_COUNT) uses the pre-increment count so the first retry waits :baseSeconds.
    private static final String MARK_SEND_FAILURE =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'SEND_FAILURE',
                RETRY_COUNT       = RETRY_COUNT + 1,
                NEXT_ATTEMPT_AT   = SYSTIMESTAMP
                                  + NUMTODSINTERVAL(LEAST(:capSeconds, :baseSeconds * POWER(2, RETRY_COUNT)), 'SECOND'),
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    private static final String MARK_PARKED =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'PARKED',
                RETRY_COUNT       = RETRY_COUNT + 1,
                NEXT_ATTEMPT_AT   = NULL,
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    private static final String REQUEUE_READY =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'NEW',
                NEXT_ATTEMPT_AT   = NULL,
                ERROR_MESSAGE     = NULL,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'SEND_FAILURE'
              AND REGION = :region
              AND (NEXT_ATTEMPT_AT IS NULL OR NEXT_ATTEMPT_AT <= SYSTIMESTAMP)
            """;

    // Cancel a not-yet-delivered CREATE for a deleted trade: park it so it is never published (retained).
    private static final String PARK_UNSENT_FOR_KEY =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'PARKED',
                NEXT_ATTEMPT_AT   = NULL,
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS IN ('NEW', 'SEND_FAILURE')
              AND REGION = :region
              AND MESSAGE_KEY = :messageKey
            """;

    // Retire a parked CREATE that a later AMEND was promoted in place of: PARKED -> SUPERSEDED.
    private static final String SUPERSEDE_PARKED_FOR_KEY =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'SUPERSEDED',
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'PARKED'
              AND EVENT_TYPE = 'CREATED'
              AND REGION = :region
              AND MESSAGE_KEY = :messageKey
            """;

    // Retention: only resolved rows are deletable. PARKED/SEND_FAILURE/NEW are never purged.
    private static final String PURGE_SENT =
            """
            DELETE FROM BOOKING_CONFIRMATION_OUTBOX
            WHERE REGION = :region
              AND PROCESSING_STATUS IN ('SENT', 'SUPERSEDED')
              AND UPDATED_ON < SYSTIMESTAMP - NUMTODSINTERVAL(:retentionDays, 'DAY')
            """;

    private static final RowMapper<OutboxEvent> ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getLong("ID"),
            Region.valueOf(rs.getString("REGION")),
            rs.getString("IDEMPOTENCY_KEY"),
            rs.getString("DESTINATION"),
            rs.getString("MESSAGE_KEY"),
            readEventType(rs.getString("EVENT_TYPE")),
            rs.getString("EVENT_PAYLOAD"),
            rs.getObject("INBOX_ID", Long.class),
            rs.getString("TRACE_ID"),
            rs.getString("HEADERS"));

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final double backoffBaseSeconds;
    private final double backoffCapSeconds;

    public JdbcOutboxRepository(
            @Qualifier(ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE) final NamedParameterJdbcTemplate jdbcTemplate,
            @Value("${app.booking-confirmation.relay.retry-backoff-base-ms:5000}") final long backoffBaseMs,
            @Value("${app.booking-confirmation.relay.retry-backoff-cap-ms:300000}") final long backoffCapMs) {
        this.jdbcTemplate = jdbcTemplate;
        this.backoffBaseSeconds = backoffBaseMs / 1000.0;
        this.backoffCapSeconds = backoffCapMs / 1000.0;
    }

    private static TradeEventType readEventType(final String value) {
        return value == null ? null : TradeEventType.valueOf(value);
    }

    @Override
    public void insertAll(final List<OutboxEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        final SqlParameterSource[] batch = events.stream()
                .map(event -> new MapSqlParameterSource()
                        .addValue("region", event.region().name())
                        .addValue("idempotencyKey", event.idempotencyKey())
                        .addValue("destination", event.destination())
                        .addValue("messageKey", event.messageKey())
                        .addValue("eventType", event.eventType() == null ? null : event.eventType().name())
                        .addValue("payload", event.payload())
                        .addValue("inboxId", event.inboxId())
                        .addValue("traceId", event.traceId())
                        .addValue("headers", event.headers()))
                .toArray(SqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(INSERT_SQL, batch);
    }

    @Override
    public List<OutboxEvent> findNew(final Region region, final int limit) {
        final MapSqlParameterSource params =
                new MapSqlParameterSource().addValue("region", region.name()).addValue("limit", limit);
        return jdbcTemplate.query(SELECT_NEW, params, ROW_MAPPER);
    }

    @Override
    public void markSent(final Region region, final List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_SENT, new MapSqlParameterSource().addValue("ids", ids).addValue("region", region.name()));
    }

    @Override
    public void markSendFailure(final Region region, final List<Long> ids, final String errorMessage) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_SEND_FAILURE,
                new MapSqlParameterSource()
                        .addValue("ids", ids)
                        .addValue("region", region.name())
                        .addValue("baseSeconds", backoffBaseSeconds)
                        .addValue("capSeconds", backoffCapSeconds)
                        .addValue("errorMessage", JdbcInboxRepository.truncate(errorMessage)));
    }

    @Override
    public void markParked(final Region region, final List<Long> ids, final String errorMessage) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_PARKED,
                new MapSqlParameterSource()
                        .addValue("ids", ids)
                        .addValue("region", region.name())
                        .addValue("errorMessage", JdbcInboxRepository.truncate(errorMessage)));
    }

    @Override
    public int requeueReady(final Region region) {
        return jdbcTemplate.update(
                REQUEUE_READY, new MapSqlParameterSource().addValue("region", region.name()));
    }

    @Override
    public int parkUnsentForKey(final Region region, final String messageKey, final String reason) {
        return jdbcTemplate.update(
                PARK_UNSENT_FOR_KEY,
                new MapSqlParameterSource()
                        .addValue("region", region.name())
                        .addValue("messageKey", messageKey)
                        .addValue("errorMessage", JdbcInboxRepository.truncate(reason)));
    }

    @Override
    public int supersedeParkedForKey(final Region region, final String messageKey, final String reason) {
        return jdbcTemplate.update(
                SUPERSEDE_PARKED_FOR_KEY,
                new MapSqlParameterSource()
                        .addValue("region", region.name())
                        .addValue("messageKey", messageKey)
                        .addValue("errorMessage", JdbcInboxRepository.truncate(reason)));
    }

    @Override
    public int purgeSent(final Region region, final int retentionDays) {
        return jdbcTemplate.update(
                PURGE_SENT,
                new MapSqlParameterSource().addValue("region", region.name()).addValue("retentionDays", retentionDays));
    }
}
