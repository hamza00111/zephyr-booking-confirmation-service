package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.event.DomainEventPublisher;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

@ExtendWith(MockitoExtension.class)
class DefaultBookingRelayServiceTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private DomainEventPublisher publisher;

    private final OwnedPartitions ownedPartitions = new OwnedPartitions();

    @Test
    void tick_shouldMarkSentForSuccessesAndSendFailureForFailures() {
        var service = relayService();
        var ok = outboxEvent(1L);
        var bad = outboxEvent(2L);
        given(outboxRepository.findNew(eq(Region.AMER), any(), eq(100))).willReturn(List.of(ok, bad));
        Map<Long, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        futures.put(1L, CompletableFuture.completedFuture(null));
        futures.put(2L, CompletableFuture.failedFuture(new RuntimeException("boom")));
        given(publisher.sendAll(Region.AMER, List.of(ok, bad))).willReturn(futures);

        service.tick();

        verify(outboxRepository).markSent(Region.AMER, List.of(1L));
        verify(outboxRepository).markSendFailure(Region.AMER, List.of(2L), "Kafka send failed in RELAY stage");
    }

    @Test
    void tick_shouldNotPublish_whenOutboxEmpty() {
        var service = relayService();
        given(outboxRepository.findNew(eq(Region.AMER), any(), eq(100))).willReturn(List.of());

        service.tick();

        verifyNoInteractions(publisher);
        verify(outboxRepository, never()).markSent(any(), any());
    }

    @Test
    void tick_shouldMarkRejectedWithoutBurningRetryBudget_whenBreakerRejectsSends() {
        var service = relayService();
        var sentEvent = outboxEvent(1L);
        var rejectedEvent = outboxEvent(2L);
        given(outboxRepository.findNew(eq(Region.AMER), any(), eq(100)))
                .willReturn(List.of(sentEvent, rejectedEvent));
        Map<Long, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        futures.put(1L, CompletableFuture.completedFuture(null));
        futures.put(2L, CompletableFuture.failedFuture(notPermitted()));
        given(publisher.sendAll(Region.AMER, List.of(sentEvent, rejectedEvent))).willReturn(futures);

        service.tick();

        verify(outboxRepository).markSent(Region.AMER, List.of(1L));
        verify(outboxRepository).markSendFailure(Region.AMER, List.of(), "Kafka send failed in RELAY stage");
        verify(outboxRepository)
                .markSendRejected(Region.AMER, List.of(2L), "Relay circuit breaker open — send not attempted");
    }

    @Test
    void tick_shouldMarkFailed_whenSendNeverConfirmsWithinBatchDeadline() {
        var service = relayService();
        var event = outboxEvent(1L);
        given(outboxRepository.findNew(eq(Region.AMER), any(), eq(100))).willReturn(List.of(event));
        Map<Long, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        futures.put(1L, new CompletableFuture<>()); // never completes — await must time out, not hang
        given(publisher.sendAll(Region.AMER, List.of(event))).willReturn(futures);

        service.tick();

        verify(outboxRepository).markSent(Region.AMER, List.of());
        verify(outboxRepository).markSendFailure(Region.AMER, List.of(1L), "Kafka send failed in RELAY stage");
    }

    @Test
    void tick_shouldMarkRejectedAndRestoreInterruptFlag_whenAwaitInterrupted() {
        var service = relayService();
        var event = outboxEvent(1L);
        given(outboxRepository.findNew(eq(Region.AMER), any(), eq(100))).willReturn(List.of(event));
        Map<Long, CompletableFuture<Void>> futures = new LinkedHashMap<>();
        futures.put(1L, new CompletableFuture<>()); // incomplete → get() throws InterruptedException
        given(publisher.sendAll(Region.AMER, List.of(event))).willReturn(futures);

        Thread.currentThread().interrupt();
        try {
            service.tick();
            // Interrupt is infrastructure (shutdown), not a message fault — no budget consumed.
            assertThat(Thread.currentThread().isInterrupted()).as("interrupt flag restored").isTrue();
        } finally {
            Thread.interrupted(); // clear so later tests are unaffected
        }

        verify(outboxRepository)
                .markSendRejected(Region.AMER, List.of(1L), "Relay circuit breaker open — send not attempted");
    }

    @Test
    void tick_shouldSkip_whenNoOwnedPartitions() {
        var service = new DefaultBookingRelayService(
                Region.AMER, new OwnedPartitions(), 100, 200, outboxRepository, publisher, transactionTemplate(),
                BookingConfirmationMetrics.noop());

        service.tick();

        verifyNoInteractions(outboxRepository, publisher);
    }

    private DefaultBookingRelayService relayService() {
        ownedPartitions.add(Region.AMER, 0);
        // 200ms send-await deadline keeps the timeout test fast.
        return new DefaultBookingRelayService(
                Region.AMER, ownedPartitions, 100, 200, outboxRepository, publisher, transactionTemplate(),
                BookingConfirmationMetrics.noop());
    }

    private static CallNotPermittedException notPermitted() {
        var breaker = CircuitBreaker.ofDefaults("AMER.RELAY");
        breaker.transitionToOpenState();
        return CallNotPermittedException.createCallNotPermittedException(breaker);
    }

    private static OutboxEvent outboxEvent(long id) {
        return new OutboxEvent(id, Region.AMER, "idem-" + id, "published", "K1", 0, "{}", id, "trace", null);
    }

    private static TransactionTemplate transactionTemplate() {
        return new TransactionTemplate(new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) {
                return new SimpleTransactionStatus();
            }

            @Override
            public void commit(TransactionStatus status) {}

            @Override
            public void rollback(TransactionStatus status) {}
        });
    }
}
