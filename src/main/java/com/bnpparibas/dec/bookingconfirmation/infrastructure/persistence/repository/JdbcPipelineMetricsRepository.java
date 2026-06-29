package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxStatus;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxStatus;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.PipelineMetricsRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/** Oracle JDBC implementation of the observability counts. */
@Repository
public class JdbcPipelineMetricsRepository implements PipelineMetricsRepository {

    private static final String COUNT_INBOX =
            "SELECT COUNT(*) FROM BOOKING_CONFIRMATION_INBOX WHERE REGION = :region AND PROCESSING_STATUS = :status";

    private static final String COUNT_OUTBOX =
            "SELECT COUNT(*) FROM BOOKING_CONFIRMATION_OUTBOX WHERE REGION = :region AND PROCESSING_STATUS = :status";

    // Age (seconds) of the oldest row still awaiting delivery — the headline no-data-loss alert.
    private static final String OLDEST_UNSENT_SECONDS =
            """
            SELECT NVL(ROUND((CAST(SYSTIMESTAMP AS DATE) - CAST(MIN(CREATED_ON) AS DATE)) * 86400), 0)
            FROM BOOKING_CONFIRMATION_OUTBOX
            WHERE REGION = :region AND PROCESSING_STATUS IN ('NEW', 'SEND_FAILURE')
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcPipelineMetricsRepository(
            @Qualifier(ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE) final NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public long inboxCount(final Region region, final InboxStatus status) {
        return count(COUNT_INBOX, region, status.name());
    }

    @Override
    public long outboxCount(final Region region, final OutboxStatus status) {
        return count(COUNT_OUTBOX, region, status.name());
    }

    @Override
    public long oldestUnsentOutboxAgeSeconds(final Region region) {
        final Long seconds = jdbcTemplate.queryForObject(
                OLDEST_UNSENT_SECONDS, new MapSqlParameterSource().addValue("region", region.name()), Long.class);
        return seconds == null ? 0L : seconds;
    }

    private long count(final String sql, final Region region, final String status) {
        final Long count = jdbcTemplate.queryForObject(
                sql,
                new MapSqlParameterSource().addValue("region", region.name()).addValue("status", status),
                Long.class);
        return count == null ? 0L : count;
    }
}
