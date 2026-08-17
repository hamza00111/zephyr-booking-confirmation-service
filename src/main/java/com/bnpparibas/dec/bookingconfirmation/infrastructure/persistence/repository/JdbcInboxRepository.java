package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.AggregatedLink;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.RequeueOutcome;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import java.util.Collection;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
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

    /**
     * Batch insert-if-absent. MERGE (rather than the {@code IGNORE_ROW_ON_DUPKEY_INDEX} hint) so
     * the dedupe rule is spelled out logically in the ON clause instead of naming a physical
     * index, and so the driver reports real per-row counts (1 inserted / 0 duplicate) instead of
     * {@code SUCCESS_NO_INFO}.
     */
    private static final String INSERT_ALL_SQL =
            """
            MERGE INTO BOOKING_CONFIRMATION_INBOX t
            USING (SELECT :region AS REGION, :idempotencyKey AS IDEMPOTENCY_KEY FROM dual) s
               ON (t.REGION = s.REGION AND t.IDEMPOTENCY_KEY = s.IDEMPOTENCY_KEY)
             WHEN NOT MATCHED THEN INSERT
                (REGION, IDEMPOTENCY_KEY, SOURCE_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, MESSAGE_KEY,
                 RAW_PAYLOAD, TRACE_ID, HEADERS, PROCESSING_STATUS)
             VALUES
                (s.REGION, s.IDEMPOTENCY_KEY, :sourceTopic, :partition, :offset, :messageKey,
                 :rawPayload, :traceId, :headers, 'NEW')
            """;

    private static final String INSERT_PARKED_SQL =
            """
            INSERT INTO BOOKING_CONFIRMATION_INBOX
                (REGION, IDEMPOTENCY_KEY, SOURCE_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, MESSAGE_KEY,
                 RAW_PAYLOAD, TRACE_ID, HEADERS, PROCESSING_STATUS, ERROR_MESSAGE)
            VALUES
                (:region, :idempotencyKey, :sourceTopic, :partition, :offset, :messageKey,
                 :rawPayload, :traceId, :headers, 'INGEST_FAILURE', :errorMessage)
            """;

    // Promote and exhaust as separate statements so each reports its own row count — exhaustion
    // (budget spent, row parked terminally) is the alerting signal and must not hide in a sum.
    private static final String REQUEUE_PROMOTE =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'NEW',
                ERROR_MESSAGE     = NULL,
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS IN ('PROCESS_FAILURE', 'INGEST_FAILURE')
              AND REGION = :region
              AND KAFKA_PARTITION IN (:partitions)
              AND RETRY_COUNT < :maxRetries
            """;

    private static final String REQUEUE_EXHAUST =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'INVALID',
                UPDATED_ON        = SYSTIMESTAMP
            WHERE PROCESSING_STATUS IN ('PROCESS_FAILURE', 'INGEST_FAILURE')
              AND REGION = :region
              AND KAFKA_PARTITION IN (:partitions)
              AND RETRY_COUNT >= :maxRetries
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
                  AND KAFKA_PARTITION IN (:partitions)
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
            SET PROCESSING_STATUS = 'AGGREGATED', AGGREGATED_INTO_ID = :intoId, UPDATED_ON = SYSTIMESTAMP
            WHERE ID = :id AND REGION = :region
            """;

    private static final String MARK_PROCESS_FAILURE =
            """
            UPDATE BOOKING_CONFIRMATION_INBOX
            SET PROCESSING_STATUS = 'PROCESS_FAILURE',
                RETRY_COUNT       = RETRY_COUNT + 1,
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

    public JdbcInboxRepository(
            @Qualifier(ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE) final NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean insertIfAbsent(final InboxMessage message) {
        try {
            return jdbcTemplate.update(INSERT_SQL, insertParams(message)) > 0;
        } catch (final DuplicateKeyException duplicate) {
            // Already ingested (at-least-once redelivery) — idempotent no-op.
            return false;
        }
    }

    @Override
    public int[] insertAllIfAbsent(final List<InboxMessage> messages) {
        if (messages.isEmpty()) {
            return new int[0];
        }
        return jdbcTemplate.batchUpdate(
                INSERT_ALL_SQL,
                messages.stream().map(JdbcInboxRepository::insertParams).toArray(MapSqlParameterSource[]::new));
    }

    private static MapSqlParameterSource insertParams(final InboxMessage message) {
        return new MapSqlParameterSource()
                .addValue("region", message.region().name())
                .addValue("idempotencyKey", message.idempotencyKey())
                .addValue("sourceTopic", message.sourceTopic())
                .addValue("partition", message.partition())
                .addValue("offset", message.offset())
                .addValue("messageKey", message.messageKey())
                .addValue("rawPayload", message.rawPayload())
                .addValue("traceId", message.traceId())
                .addValue("headers", message.headers());
    }

    @Override
    public List<InboxMessage> findNew(
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
    public void markProcessed(final Region region, final List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        jdbcTemplate.update(
                MARK_PROCESSED,
                new MapSqlParameterSource().addValue("ids", ids).addValue("region", region.name()));
    }

    @Override
    public void markAggregated(final Region region, final List<AggregatedLink> links) {
        if (links.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(
                MARK_AGGREGATED,
                links.stream()
                        .map(link -> new MapSqlParameterSource()
                                .addValue("id", link.id())
                                .addValue("intoId", link.aggregatedIntoId())
                                .addValue("region", region.name()))
                        .toArray(MapSqlParameterSource[]::new));
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
    public boolean insertParked(final InboxMessage message, final String errorMessage) {
        final MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("region", message.region().name())
                .addValue("idempotencyKey", message.idempotencyKey())
                .addValue("sourceTopic", message.sourceTopic())
                .addValue("partition", message.partition())
                .addValue("offset", message.offset())
                .addValue("messageKey", message.messageKey())
                .addValue("rawPayload", message.rawPayload())
                .addValue("traceId", message.traceId())
                .addValue("headers", message.headers())
                .addValue("errorMessage", truncate(errorMessage));
        try {
            return jdbcTemplate.update(INSERT_PARKED_SQL, params) > 0;
        } catch (final DuplicateKeyException duplicate) {
            // Already in the inbox (parked earlier or ingested normally) — idempotent no-op.
            return false;
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Partition-scoped like every drain: a row whose {@code KAFKA_PARTITION} is {@code NULL}
     * (written by a pre-ADR-0001 code path) can never match {@code IN (:partitions)} and is
     * invisible to both promotion and the PROCESS drain — it needs a one-off manual backfill of the
     * partition before the pipeline will touch it again.
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

    static String truncate(final String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 3900 ? message : message.substring(0, 3900);
    }
}
