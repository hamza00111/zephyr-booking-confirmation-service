package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static com.bnpparibas.dec.bookingconfirmation.infrastructure.config.datasource.ZephyrDataSourceConfiguration.ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE;

import com.bnpparibas.dec.bookingconfirmation.domain.model.CreateState;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.TradeGateRepository;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Repository;

/**
 * Oracle JDBC implementation of the create-barrier ledger. Upserts use {@code MERGE} so the gate is
 * idempotent under the at-least-once pipeline.
 */
@Repository
public class JdbcTradeGateRepository implements TradeGateRepository {

    private static final String SELECT_STATES =
            """
            SELECT MESSAGE_KEY, CREATE_STATE
            FROM BOOKING_CONFIRMATION_TRADE_GATE
            WHERE REGION = :region AND MESSAGE_KEY IN (:keys)
            """;

    private static final String MERGE_STATE =
            """
            MERGE INTO BOOKING_CONFIRMATION_TRADE_GATE g
            USING (SELECT :region AS REGION, :messageKey AS MESSAGE_KEY FROM dual) src
            ON (g.REGION = src.REGION AND g.MESSAGE_KEY = src.MESSAGE_KEY)
            WHEN MATCHED THEN UPDATE SET g.CREATE_STATE = :state, g.UPDATED_ON = SYSTIMESTAMP
            WHEN NOT MATCHED THEN INSERT (REGION, MESSAGE_KEY, CREATE_STATE, UPDATED_ON)
                VALUES (:region, :messageKey, :state, SYSTIMESTAMP)
            """;

    private final NamedParameterJdbcTemplate jdbcTemplate;

    public JdbcTradeGateRepository(
            @Qualifier(ZEPHYR_NAMED_PARAMETER_JDBC_TEMPLATE) final NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public Map<String, CreateState> statesFor(final Region region, final Collection<String> messageKeys) {
        final Map<String, CreateState> states = new HashMap<>();
        if (messageKeys.isEmpty()) {
            return states;
        }
        final MapSqlParameterSource params =
                new MapSqlParameterSource().addValue("region", region.name()).addValue("keys", messageKeys);
        jdbcTemplate.query(SELECT_STATES, params, rs ->
                states.put(rs.getString("MESSAGE_KEY"), CreateState.valueOf(rs.getString("CREATE_STATE"))));
        return states;
    }

    @Override
    public void upsert(final Region region, final String messageKey, final CreateState state) {
        jdbcTemplate.update(MERGE_STATE, params(region, messageKey, state));
    }

    @Override
    public void markSent(final Region region, final Collection<String> messageKeys) {
        if (messageKeys.isEmpty()) {
            return;
        }
        final SqlParameterSource[] batch = messageKeys.stream()
                .map(key -> params(region, key, CreateState.SENT))
                .toArray(SqlParameterSource[]::new);
        jdbcTemplate.batchUpdate(MERGE_STATE, batch);
    }

    private static MapSqlParameterSource params(final Region region, final String messageKey, final CreateState state) {
        return new MapSqlParameterSource()
                .addValue("region", region.name())
                .addValue("messageKey", messageKey)
                .addValue("state", state.name());
    }
}
