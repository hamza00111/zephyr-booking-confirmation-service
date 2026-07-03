package com.bnpparibas.dec.bookingconfirmation.infrastructure.persistence.repository;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.oracle.OracleContainer;

/**
 * Integration test for the outbox drain against a real Oracle. Covers the two ordering-critical
 * behaviours of ADR 0001 that the mocked-template unit tests cannot exercise — partition scoping and
 * per-key head-of-line gating — because both live in SQL ({@code KAFKA_PARTITION IN (...)} and the
 * {@code NOT EXISTS} gate) with {@code FOR UPDATE SKIP LOCKED}, which H2 cannot reproduce faithfully.
 *
 * <p>Drains run inside a transaction (as they do in the RELAY stage). Runs under {@code mvn verify}
 * (failsafe) and skips automatically where Docker is unavailable.
 */
@Testcontainers(disabledWithoutDocker = true)
class OutboxOrderingIT {

    private static final Region REGION = Region.EMEA;
    private static final int PARTITION = 2;

    @Container
    private static final OracleContainer ORACLE = new OracleContainer("gvenzl/oracle-free:23-slim-faststart");

    private static NamedParameterJdbcTemplate jdbc;
    private static TransactionTemplate tx;
    private static JdbcOutboxRepository outbox;

    @BeforeAll
    static void initSchema() {
        final DriverManagerDataSource dataSource =
                new DriverManagerDataSource(ORACLE.getJdbcUrl(), ORACLE.getUsername(), ORACLE.getPassword());
        dataSource.setDriverClassName(ORACLE.getDriverClassName());
        new ResourceDatabasePopulator(new ClassPathResource("db/schema-oracle.sql")).execute(dataSource);
        jdbc = new NamedParameterJdbcTemplate(dataSource);
        tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
        outbox = new JdbcOutboxRepository(jdbc);
    }

    @BeforeEach
    void clean() {
        jdbc.getJdbcTemplate().update("DELETE FROM BOOKING_CONFIRMATION_OUTBOX");
    }

    @Test
    void drain_returnsOnlyOwnedPartitions() {
        outbox.insertAll(List.of(row("K1", 2, "P2"), row("K2", 5, "P5")));

        assertThat(payloads(drain(Set.of(2)))).containsExactly("P2");
        assertThat(payloads(drain(Set.of(5)))).containsExactly("P5");
        assertThat(payloads(drain(Set.of(2, 5)))).containsExactlyInAnyOrder("P2", "P5");
        assertThat(drain(Set.of())).isEmpty();
    }

    @Test
    void headOfLine_withholdsLaterSameKeyEvent_untilEarlierIsSent() {
        outbox.insertAll(List.of(row("K", PARTITION, "CREATED"), row("K", PARTITION, "AMENDED")));

        // Only CREATED is eligible; AMENDED is gated behind its earlier same-key row.
        List<OutboxEvent> first = drain(Set.of(PARTITION));
        assertThat(payloads(first)).containsExactly("CREATED");

        // Mark CREATED sent → AMENDED unblocks.
        outbox.markSent(REGION, List.of(first.get(0).id()));
        assertThat(payloads(drain(Set.of(PARTITION)))).containsExactly("AMENDED");
    }

    @Test
    void headOfLine_failedEarlierEvent_blocksKey_untilRequeuedAndSent() {
        outbox.insertAll(List.of(row("K", PARTITION, "CREATED"), row("K", PARTITION, "AMENDED")));

        List<OutboxEvent> created = drain(Set.of(PARTITION));
        outbox.markSendFailure(REGION, List.of(created.get(0).id()), "boom");

        // CREATED is SEND_FAILURE and AMENDED is gated behind it → nothing to relay.
        assertThat(drain(Set.of(PARTITION))).isEmpty();

        // Requeue promotes CREATED back to NEW; it is eligible again, AMENDED still gated.
        outbox.requeueFailed(REGION, Set.of(PARTITION), 5);
        List<OutboxEvent> retry = drain(Set.of(PARTITION));
        assertThat(payloads(retry)).containsExactly("CREATED");

        // Once CREATED is finally sent, AMENDED flows — never before it.
        outbox.markSent(REGION, List.of(retry.get(0).id()));
        assertThat(payloads(drain(Set.of(PARTITION)))).containsExactly("AMENDED");
    }

    @Test
    void headOfLine_doesNotBlockOtherKeys() {
        outbox.insertAll(List.of(row("K1", PARTITION, "K1-CREATED"), row("K2", PARTITION, "K2-CREATED")));

        List<OutboxEvent> all = drain(Set.of(PARTITION));
        final long k1Id = all.stream()
                .filter(event -> "K1-CREATED".equals(event.payload()))
                .findFirst()
                .orElseThrow()
                .id();
        outbox.markSendFailure(REGION, List.of(k1Id), "boom");

        // K1 is blocked, but K2 (a different key) stays relayable.
        assertThat(payloads(drain(Set.of(PARTITION)))).containsExactly("K2-CREATED");
    }

    /** Claims eligible rows inside a transaction, as the RELAY stage does. */
    private static List<OutboxEvent> drain(final Set<Integer> partitions) {
        return tx.execute(status -> outbox.findNew(REGION, partitions, 100));
    }

    private static OutboxEvent row(final String key, final int partition, final String payload) {
        return new OutboxEvent(
                null, REGION, "idem-" + payload, "published.emea", key, partition, payload, null, "trace", null);
    }

    private static List<String> payloads(final List<OutboxEvent> events) {
        return events.stream().map(OutboxEvent::payload).toList();
    }
}
