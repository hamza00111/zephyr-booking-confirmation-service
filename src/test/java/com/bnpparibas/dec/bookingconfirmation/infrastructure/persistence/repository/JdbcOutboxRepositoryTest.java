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
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import java.util.List;
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
        repository = new JdbcOutboxRepository(jdbcTemplate, 5000, 300000);
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
    void markParked_shouldNotTouchDatabase_whenIdsEmpty() {
        repository.markParked(Region.AMER, List.of(), "irrelevant");

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void requeueReady_shouldReturnPromotedRowCount() {
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class))).willReturn(7);

        assertThat(repository.requeueReady(Region.AMER)).isEqualTo(7);
    }

    private static OutboxEvent event(long inboxId) {
        return new OutboxEvent(
                null, Region.AMER, "idem-" + inboxId, "published", "K1",
                TradeEventType.CREATED, "{}", inboxId, "trace", null);
    }
}
