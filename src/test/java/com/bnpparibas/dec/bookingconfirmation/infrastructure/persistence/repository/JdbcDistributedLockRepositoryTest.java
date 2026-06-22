package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

@ExtendWith(MockitoExtension.class)
class JdbcDistributedLockRepositoryTest {

    private static final InstanceId INSTANCE = new InstanceId("host-42");

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    private JdbcDistributedLockRepository repository;

    @BeforeEach
    void setUp() {
        repository = new JdbcDistributedLockRepository(jdbcTemplate);
    }

    @Test
    void acquireOrRefresh_shouldReturnTrue_whenMergeAffectsRow() {
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class))).willReturn(1);

        assertThat(repository.acquireOrRefresh(Region.AMER, ProcessType.RELAY, INSTANCE, Duration.ofSeconds(9))).isTrue();
    }

    @Test
    void acquireOrRefresh_shouldReturnFalse_whenLockHeldByPeer() {
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class))).willReturn(0);

        assertThat(repository.acquireOrRefresh(Region.AMER, ProcessType.RELAY, INSTANCE, Duration.ofSeconds(9))).isFalse();
    }

    @Test
    void acquireOrRefresh_shouldBindLockKeyAndScalarValues_notDomainObjects() {
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class))).willReturn(1);

        repository.acquireOrRefresh(Region.AMER, ProcessType.RELAY, INSTANCE, Duration.ofSeconds(9));

        verify(jdbcTemplate).update(anyString(), argThat((SqlParameterSource params) ->
                "AMER.RELAY".equals(params.getValue("lockKey"))
                        && "host-42".equals(params.getValue("instanceId"))   // the String, never the InstanceId record
                        && Long.valueOf(9L).equals(params.getValue("ttlSeconds")))); // long seconds, never a Duration
    }

    @Test
    void isHeldBy_shouldReturnTrue_whenLiveLockExists() {
        given(jdbcTemplate.queryForObject(anyString(), any(SqlParameterSource.class), eq(Integer.class))).willReturn(1);

        assertThat(repository.isHeldBy(Region.AMER, ProcessType.RELAY, INSTANCE)).isTrue();
    }

    @Test
    void isHeldBy_shouldReturnFalse_whenNoLiveLock() {
        given(jdbcTemplate.queryForObject(anyString(), any(SqlParameterSource.class), eq(Integer.class))).willReturn(0);

        assertThat(repository.isHeldBy(Region.AMER, ProcessType.RELAY, INSTANCE)).isFalse();
    }

    @Test
    void releaseAll_shouldSwallowExceptions_soShutdownIsNotBlocked() {
        given(jdbcTemplate.update(anyString(), any(SqlParameterSource.class)))
                .willThrow(new RuntimeException("db down"));

        assertThatNoException().isThrownBy(() -> repository.releaseAll(INSTANCE));
    }
}
