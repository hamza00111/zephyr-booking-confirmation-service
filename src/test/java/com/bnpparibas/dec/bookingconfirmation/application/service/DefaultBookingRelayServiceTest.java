package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.domain.event.DomainEventPublisher;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import java.time.Duration;
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

    private static final InstanceId INSTANCE = new InstanceId("test-instance");

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private DomainEventPublisher publisher;

    @Mock
    private DistributedLockRepository lockRepository;

    @Test
    void tick_shouldMarkSentForSuccessesAndSendFailureForFailures() {
        var service = relayService();
        lockAcquired();
        var ok = outboxEvent(1L);
        var bad = outboxEvent(2L);
        given(outboxRepository.findNew(Region.AMER, 100)).willReturn(List.of(ok, bad));
        Map<Long, CompletableFuture<?>> futures = new LinkedHashMap<>();
        futures.put(1L, CompletableFuture.completedFuture("ok"));
        futures.put(2L, CompletableFuture.failedFuture(new RuntimeException("boom")));
        given(publisher.sendAll(Region.AMER, List.of(ok, bad))).willReturn(futures);

        service.tick();

        verify(outboxRepository).markSent(Region.AMER, List.of(1L));
        verify(outboxRepository).markSendFailure(Region.AMER, List.of(2L), "Kafka send failed in RELAY stage");
    }

    @Test
    void tick_shouldNotPublish_whenOutboxEmpty() {
        var service = relayService();
        lockAcquired();
        given(outboxRepository.findNew(Region.AMER, 100)).willReturn(List.of());

        service.tick();

        verifyNoInteractions(publisher);
        verify(outboxRepository, never()).markSent(any(), any());
    }

    private DefaultBookingRelayService relayService() {
        return new DefaultBookingRelayService(
                Region.AMER, 1000, 100, 1, outboxRepository, publisher, transactionTemplate(), lockRepository, INSTANCE);
    }

    private void lockAcquired() {
        given(lockRepository.acquireOrRefresh(
                        eq(Region.AMER), eq(ProcessType.RELAY), eq(INSTANCE), eq(Duration.ofMillis(1000))))
                .willReturn(true);
    }

    private static OutboxEvent outboxEvent(long id) {
        return new OutboxEvent(id, Region.AMER, "idem-" + id, "published", "K1", "{}", id, "trace", null);
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
