package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.RequeueOutcome;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import java.util.Collection;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/**
 * Oracle JDBC implementation of the outbox (mirrors the publisher's relay drain semantics).
 *
 * <p>{@link #findNew} claims rows with {@code FOR UPDATE SKIP LOCKED} (runs inside the RELAY
 * transaction), restricted to owned partitions and gated head-of-line per key: a {@code NEW} row is
 * only claimed once every earlier same-key row is {@code SENT}, so a failed event never lets its
 * key's later events overtake it (ADR 0001). {@link #requeueFailed} promotes {@code SEND_FAILURE}
 * rows back to {@code NEW} while the retry budget remains, otherwise to {@code RETRY_EXHAUSTED}.
 */
@Repository
public class JdbcOutboxRepository implements OutboxRepository {

    private static final String INSERT_SQL =
            """
            INSERT INTO BOOKING_CONFIRMATION_OUTBOX
                (REGION, IDEMPOTENCY_KEY, DESTINATION, MESSAGE_KEY, KAFKA_PARTITION, EVENT_PAYLOAD, INBOX_ID, TRACE_ID, HEADERS, PROCESSING_STATUS)
            VALUES
                (:region, :idempotencyKey, :destination, :messageKey, :kafkaPartition, :payload, :inboxId, :traceId, :headers, 'NEW')
            """;

    private static final String SELECT_NEW =
            """
            SELECT ID, REGION, IDEMPOTENCY_KEY, DESTINATION, MESSAGE_KEY, KAFKA_PARTITION, EVENT_PAYLOAD, INBOX_ID, TRACE_ID, HEADERS
            FROM BOOKING_CONFIRMATION_OUTBOX
            WHERE ID IN (
                SELECT o.ID
                FROM BOOKING_CONFIRMATION_OUTBOX o
                WHERE o.PROCESSING_STATUS = 'NEW'
                  AND o.REGION = :region
                  AND o.KAFKA_PARTITION IN (:partitions)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM BOOKING_CONFIRMATION_OUTBOX b
                      WHERE b.REGION = o.REGION
                        AND b.MESSAGE_KEY = o.MESSAGE_KEY
                        AND b.ID < o.ID
                        AND b.PROCESSING_STATUS <> 'SENT'
                  )
                ORDER BY o.ID
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

    private static final String MARK_SEND_FAILURE =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'SEND_FAILURE',
                RETRY_COUNT       = RETRY_COUNT + 1,
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    // Same terminal-status transition as MARK_SEND_FAILURE but without RETRY_COUNT increment:
    // a rejected send was never attempted, so it must not consume the poison-message budget.
    private static final String MARK_SEND_REJECTED =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'SEND_FAILURE',
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    // Promote and exhaust as separate statements so each reports its own row count — exhaustion
    // (budget spent, row parked terminally) is the alerting signal and must not hide in a sum.
    private static final String REQUEUE_PROMOTE =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'NEW',
                ERROR_MESSAGE     = NULL,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'SEND_FAILURE'
              AND REGION = :region
              AND KAFKA_PARTITION IN (:partitions)
              AND RETRY_COUNT < :maxRetries
            """;

    private static final String REQUEUE_EXHAUST =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'RETRY_EXHAUSTED',
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'SEND_FAILURE'
              AND REGION = :region
              AND KAFKA_PARTITION IN (:partitions)
              AND RETRY_COUNT >= :maxRetries
            """;

    private static final RowMapper<OutboxEvent> ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getLong("ID"),
            Region.valueOf(rs.getString("REGION")),
            rs.getString("IDEMPOTENCY_KEY"),
            rs.getString("DESTINATION"),
            rs.getString("MESSAGE_KEY"),
            rs.getObject("KAFKA_PARTITION", Integer.class),
            rs.getString("EVENT_PAYLOAD"),
            rs.getObject("INBOX_ID", Long.class),
            rs.getString("TRACE_ID"),
            rs.getString("HEADERS"));

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcOutboxRepository(
            @Qualifier(ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE) final NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
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
                        .addValue("kafkaPartition", event.kafkaPartition())
                        .addValue("payload", event.payload())
                        .addValue("inboxId", event.inboxId())
                        .addValue("traceId", event.traceId())
                        .addValue("headers", event.headers()))
                .toArray(SqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(INSERT_SQL, batch);
    }

    @Override
    public List<OutboxEvent> findNew(
            final Region region, final Collection<Integer> partitions, final int limit) {
        if (partitions.isEmpty()) {
            return List.of();
        }
        final MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("region", region.name())
                .addValue("partitions", partitions)
                .addValue("limit", limit);
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
                        .addValue("errorMessage", JdbcInboxRepository.truncate(errorMessage)));
    }

    @Override
    public void markSendRejected(final Region region, final List<Long> ids, final String errorMessage) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_SEND_REJECTED,
                new MapSqlParameterSource()
                        .addValue("ids", ids)
                        .addValue("region", region.name())
                        .addValue("errorMessage", JdbcInboxRepository.truncate(errorMessage)));
    }

    /**
     * {@inheritDoc}
     *
     * <p>Partition-scoped like every drain: a row whose {@code KAFKA_PARTITION} is {@code NULL}
     * can never match {@code IN (:partitions)} and is invisible to both promotion and the RELAY
     * drain — it needs a one-off manual backfill of the partition before the pipeline will touch it
     * again.
     */
    @Override
    public RequeueOutcome requeueFailed(final Region region, final Collection<Integer> partitions, final int maxRetries) {
        if (partitions.isEmpty()) {
            return RequeueOutcome.NONE;
        }
        final MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("region", region.name())
                .addValue("partitions", partitions)
                .addValue("maxRetries", maxRetries);
        final int promoted = jdbcTemplate.update(REQUEUE_PROMOTE, params);
        final int exhausted = jdbcTemplate.update(REQUEUE_EXHAUST, params);
        return new RequeueOutcome(promoted, exhausted);
    }
}
