package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.domain.model.AggregatedLink;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

/**
 * Behavioural unit tests for the JDBC inbox repository: the empty-batch short-circuit and the
 * dedupe-on-conflict contract. SQL correctness and row mapping are covered by the integration tests
 * (against a real Oracle), not here — here the {@code NamedParameterJdbcTemplate} is mocked.
 */
@ExtendWith(MockitoExtension.class)
class JdbcInboxRepositoryTest {

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Test
    void markProcessed_shouldNotTouchDatabase_whenIdsEmpty() {
        var repository = new JdbcInboxRepository(jdbcTemplate);

        repository.markProcessed(Region.AMER, List.of());

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void insertIfAbsent_shouldReturnTrue_whenRowInserted() {
        var repository = new JdbcInboxRepository(jdbcTemplate);
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class))).willReturn(1);

        assertThat(repository.insertIfAbsent(message())).isTrue();
    }

    @Test
    void insertIfAbsent_shouldReturnFalse_whenDuplicateKeyViolation() {
        var repository = new JdbcInboxRepository(jdbcTemplate);
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class)))
                .willThrow(new DuplicateKeyException("UQ_INBOX_DEDUPE"));

        assertThat(repository.insertIfAbsent(message())).isFalse();
    }

    @Test
    void markAggregated_shouldNotTouchDatabase_whenLinksEmpty() {
        var repository = new JdbcInboxRepository(jdbcTemplate);

        repository.markAggregated(Region.AMER, List.of());

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void markAggregated_shouldBatchUpdateWithLineageLink() {
        var repository = new JdbcInboxRepository(jdbcTemplate);
        given(jdbcTemplate.batchUpdate(anyString(), any(SqlParameterSource[].class)))
                .willReturn(new int[] {1, 1});

        repository.markAggregated(
                Region.AMER,
                List.of(new AggregatedLink(1L, 3L), new AggregatedLink(2L, null)));

        verify(jdbcTemplate).batchUpdate(anyString(), argThat((SqlParameterSource[] batch) -> batch.length == 2
                && Long.valueOf(3L).equals(batch[0].getValue("intoId"))
                && batch[1].getValue("intoId") == null));
    }

    @Test
    void insertAllIfAbsent_shouldNotTouchDatabase_whenMessagesEmpty() {
        var repository = new JdbcInboxRepository(jdbcTemplate);

        assertThat(repository.insertAllIfAbsent(List.of())).isEmpty();

        verifyNoInteractions(jdbcTemplate);
    }

    @Test
    void insertAllIfAbsent_shouldBatchUpdateOncePerMessage() {
        var repository = new JdbcInboxRepository(jdbcTemplate);
        given(jdbcTemplate.batchUpdate(anyString(), any(SqlParameterSource[].class)))
                .willReturn(new int[] {1, 0});

        int[] counts = repository.insertAllIfAbsent(List.of(message(), message()));

        assertThat(counts).containsExactly(1, 0);
        verify(jdbcTemplate)
                .batchUpdate(anyString(), argThat((SqlParameterSource[] batch) -> batch.length == 2));
    }

    private static InboxMessage message() {
        return new InboxMessage(null, Region.AMER, "idem-1", "topic", 0, 0L, "K1", "{}", "trace-1", null);
    }
}
