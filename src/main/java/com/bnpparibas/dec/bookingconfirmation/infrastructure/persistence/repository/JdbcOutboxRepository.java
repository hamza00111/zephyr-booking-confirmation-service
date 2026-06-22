package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
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
 * transaction). {@link #requeueFailed} promotes {@code SEND_FAILURE} rows back to {@code NEW} while
 * the retry budget remains, otherwise to {@code RETRY_EXHAUSTED}.
 */
@Repository
public class JdbcOutboxRepository implements OutboxRepository {

    private static final String INSERT_SQL =
            """
            INSERT INTO BOOKING_CONFIRMATION_OUTBOX
                (REGION, IDEMPOTENCY_KEY, DESTINATION, MESSAGE_KEY, EVENT_PAYLOAD, INBOX_ID, TRACE_ID, HEADERS, PROCESSING_STATUS)
            VALUES
                (:region, :idempotencyKey, :destination, :messageKey, :payload, :inboxId, :traceId, :headers, 'NEW')
            """;

    private static final String SELECT_NEW =
            """
            SELECT ID, REGION, IDEMPOTENCY_KEY, DESTINATION, MESSAGE_KEY, EVENT_PAYLOAD, INBOX_ID, TRACE_ID, HEADERS
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

    private static final String MARK_SEND_FAILURE =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = 'SEND_FAILURE',
                RETRY_COUNT       = RETRY_COUNT + 1,
                ERROR_MESSAGE     = :errorMessage,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE ID IN (:ids) AND REGION = :region
            """;

    private static final String REQUEUE_FAILED =
            """
            UPDATE BOOKING_CONFIRMATION_OUTBOX
            SET PROCESSING_STATUS = CASE WHEN RETRY_COUNT < :maxRetries THEN 'NEW' ELSE 'RETRY_EXHAUSTED' END,
                ERROR_MESSAGE     = CASE WHEN RETRY_COUNT < :maxRetries THEN NULL ELSE ERROR_MESSAGE END,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS = 'SEND_FAILURE'
              AND REGION = :region
            """;

    private static final RowMapper<OutboxEvent> ROW_MAPPER = (rs, rowNum) -> new OutboxEvent(
            rs.getLong("ID"),
            Region.valueOf(rs.getString("REGION")),
            rs.getString("IDEMPOTENCY_KEY"),
            rs.getString("DESTINATION"),
            rs.getString("MESSAGE_KEY"),
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
                        .addValue("errorMessage", JdbcInboxRepository.truncate(errorMessage)));
    }

    @Override
    public int requeueFailed(final Region region, final int maxRetries) {
        return jdbcTemplate.update(
                REQUEUE_FAILED,
                new MapSqlParameterSource().addValue("region", region.name()).addValue("maxRetries", maxRetries));
    }
}
