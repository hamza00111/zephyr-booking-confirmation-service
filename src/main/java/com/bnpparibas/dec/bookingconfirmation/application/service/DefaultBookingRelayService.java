package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.domain.event.DomainEventPublisher;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.TradeGateRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRelayService;
import com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.SendFailureClassifier;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * RELAY stage: drains NEW outbox rows and publishes them to {@code published.<region>}.
 *
 * <p>The transaction spans drain ({@code FOR UPDATE SKIP LOCKED}) -> publish -> mark. Each send is
 * awaited (bounded by the producer's delivery timeout). Successes are marked SENT. Failures are
 * classified for the no-data-loss guarantee: <em>infrastructure</em> failures (broker down, timeout,
 * circuit open) go to SEND_FAILURE and are retried indefinitely with backoff; only <em>poison</em>
 * (too-large, non-serializable) is PARKED — retained and replayable, never dropped. Delivery is
 * at-least-once.
 */
public class DefaultBookingRelayService extends AbstractRegionScopedService implements BookingRelayService {

    private final int batchSize;
    private final InboxRepository inboxRepository;
    private final OutboxRepository outboxRepository;
    private final TradeGateRepository tradeGateRepository;
    private final DomainEventPublisher publisher;
    private final TransactionTemplate transactionTemplate;

    public DefaultBookingRelayService(
            final Region region,
            final long tickIntervalMs,
            final int batchSize,
            final int lockTtlMultiplier,
            final InboxRepository inboxRepository,
            final OutboxRepository outboxRepository,
            final TradeGateRepository tradeGateRepository,
            final DomainEventPublisher publisher,
            final TransactionTemplate transactionTemplate,
            final DistributedLockRepository lockRepository,
            final InstanceId instanceId) {
        super(region, tickIntervalMs, lockTtlMultiplier, lockRepository, instanceId);
        this.batchSize = batchSize;
        this.inboxRepository = inboxRepository;
        this.outboxRepository = outboxRepository;
        this.tradeGateRepository = tradeGateRepository;
        this.publisher = publisher;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    protected void doTick() {
        transactionTemplate.executeWithoutResult(status -> {
            final List<OutboxEvent> batch = outboxRepository.findNew(region(), batchSize);
            if (batch.isEmpty()) {
                return;
            }

            final Map<Long, CompletableFuture<?>> futures = publisher.sendAll(region(), batch);
            final List<Long> sent = new ArrayList<>();
            final List<Long> retry = new ArrayList<>();
            final List<Long> parked = new ArrayList<>();
            futures.forEach((id, future) -> {
                switch (await(id, future)) {
                    case SENT -> sent.add(id);
                    case PARK -> parked.add(id);
                    case RETRY -> retry.add(id);
                }
            });

            outboxRepository.markSent(region(), sent);
            outboxRepository.markSendFailure(region(), retry, "Kafka send failed in RELAY stage (will retry)");
            outboxRepository.markParked(region(), parked, "Poison message — not retryable; parked for review");
            releaseDeliveredCreates(batch, sent);
            if (!parked.isEmpty()) {
                log.error("[{}] Parked {} poison outbox row(s) — needs review: {}",
                        processIdentifier(), parked.size(), parked);
            }
            log.debug("[{}] Relayed {} ({} retry, {} parked) of {} drained",
                    processIdentifier(), sent.size(), retry.size(), parked.size(), batch.size());
        });
    }

    /**
     * Create-barrier release: for every CREATE just delivered, mark its trade gate SENT and release
     * the amendments held behind that barrier ({@code BLOCKED -> NEW}) so they flow on the next PROCESS
     * tick — downstream now sees CREATE before AMEND, in order.
     */
    private void releaseDeliveredCreates(final List<OutboxEvent> batch, final List<Long> sent) {
        final Set<Long> sentIds = Set.copyOf(sent);
        final Set<String> deliveredCreateKeys = new LinkedHashSet<>();
        for (final OutboxEvent event : batch) {
            if (event.eventType() == TradeEventType.CREATED
                    && event.messageKey() != null
                    && sentIds.contains(event.id())) {
                deliveredCreateKeys.add(event.messageKey());
            }
        }
        if (deliveredCreateKeys.isEmpty()) {
            return;
        }
        tradeGateRepository.markSent(region(), deliveredCreateKeys);
        final int released = inboxRepository.releaseBlocked(region(), deliveredCreateKeys);
        if (released > 0) {
            log.info("[{}] Released {} blocked row(s) for {} delivered trade(s)",
                    processIdentifier(), released, deliveredCreateKeys.size());
        }
    }

    private enum SendOutcome {
        SENT,
        RETRY,
        PARK
    }

    private SendOutcome await(final long id, final CompletableFuture<?> future) {
        try {
            future.get();
            return SendOutcome.SENT;
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("[{}] Interrupted awaiting send for outbox id={}", processIdentifier(), id);
            return SendOutcome.RETRY;
        } catch (final ExecutionException sendFailure) {
            if (SendFailureClassifier.isPoison(sendFailure)) {
                log.error("[{}] Poison send for outbox id={}: {}",
                        processIdentifier(), id, sendFailure.getMessage());
                return SendOutcome.PARK;
            }
            log.warn("[{}] Transient send failure for outbox id={} (will retry): {}",
                    processIdentifier(), id, sendFailure.getMessage());
            return SendOutcome.RETRY;
        }
    }
}
