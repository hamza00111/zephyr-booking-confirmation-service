package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.event.DomainEventPublisher;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRelayService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * RELAY stage: drains NEW outbox rows and publishes them to {@code published.<region>}.
 *
 * <p>The transaction spans drain ({@code FOR UPDATE SKIP LOCKED}) -> publish -> mark. Each send is
 * awaited (bounded by the producer's delivery timeout); successes are marked SENT, failures
 * SEND_FAILURE for the requeue stage. Delivery is at-least-once.
 */
public class DefaultBookingRelayService extends AbstractRegionScopedService implements BookingRelayService {

    private final int batchSize;
    private final OutboxRepository outboxRepository;
    private final DomainEventPublisher publisher;
    private final TransactionTemplate transactionTemplate;

    public DefaultBookingRelayService(
            final Region region,
            final OwnedPartitions ownedPartitions,
            final int batchSize,
            final OutboxRepository outboxRepository,
            final DomainEventPublisher publisher,
            final TransactionTemplate transactionTemplate) {
        super(region, ownedPartitions);
        this.batchSize = batchSize;
        this.outboxRepository = outboxRepository;
        this.publisher = publisher;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    protected void doTick(final Set<Integer> ownedPartitions) {
        transactionTemplate.executeWithoutResult(status -> {
            final List<OutboxEvent> batch = outboxRepository.findNew(region(), ownedPartitions, batchSize);
            if (batch.isEmpty()) {
                return;
            }

            final Map<Long, CompletableFuture<?>> futures = publisher.sendAll(region(), batch);
            final List<Long> sent = new ArrayList<>();
            final List<Long> failed = new ArrayList<>();
            futures.forEach((id, future) -> {
                if (awaitSuccess(id, future)) {
                    sent.add(id);
                } else {
                    failed.add(id);
                }
            });

            outboxRepository.markSent(region(), sent);
            outboxRepository.markSendFailure(region(), failed, "Kafka send failed in RELAY stage");
            log.debug("[{}] Relayed {} ({} failed) of {} drained",
                    processIdentifier(), sent.size(), failed.size(), batch.size());
        });
    }

    private boolean awaitSuccess(final long id, final CompletableFuture<?> future) {
        try {
            future.get();
            return true;
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("[{}] Interrupted awaiting send for outbox id={}", processIdentifier(), id);
            return false;
        } catch (final java.util.concurrent.ExecutionException sendFailure) {
            log.warn("[{}] Send failed for outbox id={}: {}", processIdentifier(), id, sendFailure.getMessage());
            return false;
        }
    }
}
