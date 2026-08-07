package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.RequeueOutcome;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Behavioural unit tests for the JDBC outbox repository (mocked template). SQL text and row mapping
 * are covered by the integration tests against real Oracle.
 */
@ExtendWith(MockitoExtension.class)
class JdbcOutboxRepositoryTest {

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    private JdbcOutboxRepository repository;

    @BeforeEach
    void setUp() {
        repository = new JdbcOutboxRepository(jdbcTemplate);
    }

    @Test
    void insertAll_shouldNotTouchDatabase_whenEmpty() {
        repository.insertAll(List.of());

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void insertAll_shouldBatchInsert_oneParamSourcePerEvent() {
        repository.insertAll(List.of(event(1L), event(2L)));

        verify(jdbcTemplate).batchUpdate(anyString(), argThat((SqlParameterSource[] batch) -> batch.length == 2));
    }

    @Test
    void markSent_shouldNotTouchDatabase_whenIdsEmpty() {
        repository.markSent(Region.AMER, List.of());

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void markSendFailure_shouldNotTouchDatabase_whenIdsEmpty() {
        repository.markSendFailure(Region.AMER, List.of(), "irrelevant");

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void requeueFailed_shouldReportPromotedAndExhaustedSeparately() {
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class))).willReturn(7, 2);

        assertThat(repository.requeueFailed(Region.AMER, Set.of(0), 5)).isEqualTo(new RequeueOutcome(7, 2));
    }

    @Test
    void requeueFailed_shouldNotTouchDatabase_whenNoOwnedPartitions() {
        assertThat(repository.requeueFailed(Region.AMER, Set.of(), 5)).isEqualTo(RequeueOutcome.NONE);

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void markSendRejected_shouldNotConsumeRetryBudget() {
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class))).willReturn(1);

        repository.markSendRejected(Region.AMER, List.of(1L), "breaker open");

        verify(jdbcTemplate).update(
                argThat((String sql) -> !sql.contains("RETRY_COUNT") && sql.contains("SEND_FAILURE")),
                any(SqlParameterSource.class));
    }

    private static OutboxEvent event(long inboxId) {
        return new OutboxEvent(null, Region.AMER, "idem-" + inboxId, "published", "K1", 0, "{}", inboxId, "trace", null);
    }
}
