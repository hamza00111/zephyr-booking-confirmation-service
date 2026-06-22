package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Oracle JDBC distributed lock keyed by {@code region.processType}, using an atomic {@code MERGE} to
 * acquire/refresh/steal in one round-trip (no TOCTOU race). Mirrors the publisher's lock semantics.
 */
@Repository
@Slf4j
public class JdbcDistributedLockRepository implements DistributedLockRepository {

    private static final String MERGE_SQL =
            """
            MERGE INTO BOOKING_CONFIRMATION_REGION_LOCK tgt
            USING (SELECT :lockKey AS lock_key FROM dual) src
            ON (tgt.lock_key = src.lock_key)
            WHEN MATCHED THEN
                UPDATE SET
                    locked_by       = :instanceId,
                    locked_at       = SYSDATE,
                    lock_expires_at = SYSDATE + NUMTODSINTERVAL(:ttlSeconds, 'SECOND')
                WHERE tgt.lock_expires_at < SYSDATE
                   OR tgt.locked_by = :instanceId
            WHEN NOT MATCHED THEN
                INSERT (lock_key, locked_by, locked_at, lock_expires_at)
                VALUES (
                    :lockKey,
                    :instanceId,
                    SYSDATE,
                    SYSDATE + NUMTODSINTERVAL(:ttlSeconds, 'SECOND'))
            """;

    private static final String RELEASE_ALL_SQL =
            """
            DELETE FROM BOOKING_CONFIRMATION_REGION_LOCK
            WHERE locked_by = :instanceId
            """;

    private static final String RELEASE_KEY_SQL =
            """
            DELETE FROM BOOKING_CONFIRMATION_REGION_LOCK
            WHERE lock_key = :lockKey AND locked_by = :instanceId
            """;

    private static final String IS_HELD_BY_SQL =
            """
            SELECT COUNT(*) FROM BOOKING_CONFIRMATION_REGION_LOCK
            WHERE lock_key = :lockKey
              AND locked_by = :instanceId
              AND lock_expires_at >= SYSDATE
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcDistributedLockRepository(
            @Qualifier(ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE) final NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean acquireOrRefresh(
            final Region region, final ProcessType processType, final InstanceId instanceId, final Duration ttl) {
        final String lockKey = processType.keyFor(region);
        final MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("lockKey", lockKey)
                .addValue("instanceId", instanceId.value())
                .addValue("ttlSeconds", ttl.getSeconds());

        final boolean acquired = jdbcTemplate.update(MERGE_SQL, params) > 0;
        if (!acquired) {
            log.debug("[{}] Lock held by another instance — skipping tick", lockKey);
        }
        return acquired;
    }

    @Override
    public void releaseAll(final InstanceId instanceId) {
        try {
            final int released =
                    jdbcTemplate.update(RELEASE_ALL_SQL, new MapSqlParameterSource("instanceId", instanceId.value()));
            log.info("Released {} region lock(s) for instanceId={}", released, instanceId);
        } catch (final RuntimeException exception) {
            log.warn("Failed to release locks for instanceId={} — peers recover via TTL expiry", instanceId, exception);
        }
    }

    @Override
    public void releaseForRegion(
            final Region region, final ProcessType processType, final InstanceId instanceId) {
        final String lockKey = processType.keyFor(region);
        try {
            final int released = jdbcTemplate.update(
                    RELEASE_KEY_SQL,
                    new MapSqlParameterSource("lockKey", lockKey).addValue("instanceId", instanceId.value()));
            if (released > 0) {
                log.info("[{}] Region lock released for instanceId={}", lockKey, instanceId);
            }
        } catch (final RuntimeException exception) {
            log.warn("[{}] Failed to release region lock — peer recovers via TTL expiry", lockKey, exception);
        }
    }

    @Override
    public boolean isHeldBy(final Region region, final ProcessType processType, final InstanceId instanceId) {
        final String lockKey = processType.keyFor(region);
        final Integer count = jdbcTemplate.queryForObject(
                IS_HELD_BY_SQL,
                new MapSqlParameterSource("lockKey", lockKey).addValue("instanceId", instanceId.value()),
                Integer.class);
        return count != null && count > 0;
    }
}
