package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import java.util.Collection;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Oracle JDBC implementation of the inbox.
 *
 * <p>{@link #insertIfAbsent} dedupes via the {@code UQ_INBOX_DEDUPE (REGION, IDEMPOTENCY_KEY)}
 * constraint. {@link #findNew} claims rows with {@code FOR UPDATE SKIP LOCKED}, so it must run inside
 * the PROCESS transaction.
 */
@Repository
public class JdbcInboxRepository implements InboxRepository {

    private static final String INSERT_SQL =
            """
            INSERT INTO BOOKING_CONFIRMATION_INBOX
                (REGION, IDEMPOTENCY_KEY, SOURCE_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, MESSAGE_KEY,
                 RAW_PAYLOAD, TRACE_ID, HEADERS, PROCESSING_STATUS)
            VALUES
                (:region, :idempotencyKey, :sourceTopic, :partition, :offset, :messageKey,
                 :rawPayload, :traceId, :headers, 'NEW')
            """;

    private static final String SELECT_NEW =
            """
            SELECT ID, REGION, IDEMPOTENCY_KEY, SOURCE_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, MESSAGE_KEY, RAW_PAYLOAD, TRACE_ID, HEADERS
            FROM BOOKING_CONFIRMATION_INBOX
            WHERE ID IN (
                SELECT ID
                FROM BOOKING_CONFIRMATION_INBOX
                WHERE PROCESSING_STATUS = 'NEW'
                  AND REGION = :region
                ORDER BY ID
                FETCH FIRST :limit ROWS ONLY
            )
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_PROCESSED =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'PROCESSED', UPDATED_ON = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    private static final String MARK_AGGREGATED =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'AGGREGATED', UPDATED_ON = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    private static final String MARK_BLOCKED =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'BLOCKED', UPDATED_ON = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    // Release the barrier for a delivered trade: BLOCKED -> NEW, re-drained in ID order next tick.
    private static final String RELEASE_BLOCKED =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'NEW', UPDATED_ON = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'BLOCKED'
              AND REGION = :region
              AND MESSAGE_KEY IN (:keys)
            """;

    // Transform failure: schedule the next retry with exponential backoff capped at :capSeconds.
    private static final String MARK_PROCESS_FAILURE =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'PROCESS_FAILURE',
                RETRY_COUNT       = RETRY_COUNT + 1,
                NEXT_ATTEMPT_AT   = SYSTIMESTAMP
                                  + NUMTODSINTERVAL(LEAST(:capSeconds, :baseSeconds * POWER(2, RETRY_COUNT)), 'SECOND'),
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    private static final String MARK_INVALID =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'INVALID',
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    private static final String MARK_PARKED =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'PARKED',
                NEXT_ATTEMPT_AT   = NULL,
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    // No-loss requeue: promote ready failures back to NEW within budget; park the rest (never drop).
    private static final String REQUEUE_READY =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'NEW',
                NEXT_ATTEMPT_AT   = NULL,
                ERROR_MESSAGE     = NULL,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'PROCESS_FAILURE'
              AND REGION = :region
              AND RETRY_COUNT < :maxRetries
              AND (NEXT_ATTEMPT_AT IS NULL OR NEXT_ATTEMPT_AT <= SYSTIMESTAMP)
            """;

    private static final String PARK_EXHAUSTED =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'PARKED',
                NEXT_ATTEMPT_AT   = NULL,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'PROCESS_FAILURE'
              AND REGION = :region
              AND RETRY_COUNT >= :maxRetries
            """;

    // Retention: only fully-processed rows are deletable. PARKED/INVALID/in-flight are never purged.
    private static final String PURGE_PROCESSED =
            """
            DELETE FROM BOOKING_CONFIRMATION_INBOX
            WHERE REGION = :region
              AND PROCESSING_STATUS IN ('PROCESSED', 'AGGREGATED')
              AND UPDATED_ON < SYSTIMESTAMP - NUMTODSINTERVAL(:retentionDays, 'DAY')
            """;

    private static final RowMapper<InboxMessage> ROW_MAPPER = (rs, rowNum) -> new InboxMessage(
            rs.getLong("ID"),
            Region.valueOf(rs.getString("REGION")),
            rs.getString("IDEMPOTENCY_KEY"),
            rs.getString("SOURCE_TOPIC"),
            rs.getObject("KAFKA_PARTITION", Integer.class),
            rs.getObject("KAFKA_OFFSET", Long.class),
            rs.getString("MESSAGE_KEY"),
            rs.getString("RAW_PAYLOAD"),
            rs.getString("TRACE_ID"),
            rs.getString("HEADERS"));

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final double backoffBaseSeconds;
    private final double backoffCapSeconds;

    public JdbcInboxRepository(
            @Qualifier(ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE) final NamedParameterJdbcTemplate jdbcTemplate,
            @Value("${app.booking-confirmation.process.retry-backoff-base-ms:5000}") final long backoffBaseMs,
            @Value("${app.booking-confirmation.process.retry-backoff-cap-ms:300000}") final long backoffCapMs) {
        this.jdbcTemplate = jdbcTemplate;
        this.backoffBaseSeconds = backoffBaseMs / 1000.0;
        this.backoffCapSeconds = backoffCapMs / 1000.0;
    }

    @Override
    public boolean insertIfAbsent(final InboxMessage message) {
        final MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("region", message.region().name())
                .addValue("idempotencyKey", message.idempotencyKey())
                .addValue("sourceTopic", message.sourceTopic())
                .addValue("partition", message.partition())
                .addValue("offset", message.offset())
                .addValue("messageKey", message.messageKey())
                .addValue("rawPayload", message.rawPayload())
                .addValue("traceId", message.traceId())
                .addValue("headers", message.headers());
        try {
            return jdbcTemplate.update(INSERT_SQL, params) > 0;
        } catch (final DuplicateKeyException duplicate) {
            // Already ingested (at-least-once redelivery) — idempotent no-op.
            return false;
        }
    }

    @Override
    public List<InboxMessage> findNew(final Region region, final int limit) {
        final MapSqlParameterSource params =
                new MapSqlParameterSource().addValue("region", region.name()).addValue("limit", limit);
        return jdbcTemplate.query(SELECT_NEW, params, ROW_MAPPER);
    }

    @Override
    public void markProcessed(final Region region, final List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_PROCESSED,
                new MapSqlParameterSource().addValue("ids", ids).addValue("region", region.name()));
    }

    @Override
    public void markAggregated(final Region region, final List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_AGGREGATED,
                new MapSqlParameterSource().addValue("ids", ids).addValue("region", region.name()));
    }

    @Override
    public void markBlocked(final Region region, final List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_BLOCKED,
                new MapSqlParameterSource().addValue("ids", ids).addValue("region", region.name()));
    }

    @Override
    public int releaseBlocked(final Region region, final Collection<String> messageKeys) {
        if (messageKeys.isEmpty()) {
            return 0;
        }
        return jdbcTemplate.update(
                RELEASE_BLOCKED,
                new MapSqlParameterSource().addValue("region", region.name()).addValue("keys", messageKeys));
    }

    @Override
    public void markProcessFailure(final Region region, final List<Long> ids, final String errorMessage) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_PROCESS_FAILURE,
                new MapSqlParameterSource()
                        .addValue("ids", ids)
                        .addValue("region", region.name())
                        .addValue("baseSeconds", backoffBaseSeconds)
                        .addValue("capSeconds", backoffCapSeconds)
                        .addValue("errorMessage", truncate(errorMessage)));
    }

    @Override
    public void markInvalid(final Region region, final List<Long> ids, final String errorMessage) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_INVALID,
                new MapSqlParameterSource()
                        .addValue("ids", ids)
                        .addValue("region", region.name())
                        .addValue("errorMessage", truncate(errorMessage)));
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
                        .addValue("errorMessage", truncate(errorMessage)));
    }

    @Override
    public int requeueFailed(final Region region, final int maxRetries) {
        final MapSqlParameterSource params =
                new MapSqlParameterSource().addValue("region", region.name()).addValue("maxRetries", maxRetries);
        final int promoted = jdbcTemplate.update(REQUEUE_READY, params);
        jdbcTemplate.update(PARK_EXHAUSTED, params); // exhausted rows are parked, never dropped
        return promoted;
    }

    @Override
    public int purgeProcessed(final Region region, final int retentionDays) {
        return jdbcTemplate.update(
                PURGE_PROCESSED,
                new MapSqlParameterSource().addValue("region", region.name()).addValue("retentionDays", retentionDays));
    }

    static String truncate(final String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 3900 ? message : message.substring(0, 3900);
    }
}
