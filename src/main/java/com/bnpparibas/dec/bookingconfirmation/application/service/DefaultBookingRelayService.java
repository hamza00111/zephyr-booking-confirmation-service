package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.event.DomainEventPublisher;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRelayService;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * RELAY stage: drains NEW outbox rows and publishes them to {@code published.<region>}.
 *
 * <p>The transaction spans drain ({@code FOR UPDATE SKIP LOCKED}, owned partitions only, gated
 * head-of-line per key) -> publish -> mark. All sends of the batch are awaited under one shared
 * deadline ({@code sendAwaitTimeoutMs}, above the producer's delivery timeout) so a hung producer
 * future cannot freeze the tick and its transaction indefinitely. Successes are marked SENT;
 * genuine failures SEND_FAILURE (consuming retry budget); breaker rejections and interrupts are
 * marked without consuming budget. Delivery is at-least-once, and per-key order is preserved end to
 * end (ADR 0001).
 */
public class DefaultBookingRelayService extends AbstractRegionScopedService implements BookingRelayService {

    private final int batchSize;
    private final long sendAwaitTimeoutMs;
    private final OutboxRepository outboxRepository;
    private final DomainEventPublisher publisher;
    private final TransactionTemplate transactionTemplate;

    public DefaultBookingRelayService(
            final Region region,
            final OwnedPartitions ownedPartitions,
            final int batchSize,
            final long sendAwaitTimeoutMs,
            final OutboxRepository outboxRepository,
            final DomainEventPublisher publisher,
            final TransactionTemplate transactionTemplate,
            final BookingConfirmationMetrics metrics) {
        super(region, ownedPartitions, metrics);
        this.batchSize = batchSize;
        this.sendAwaitTimeoutMs = sendAwaitTimeoutMs;
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

            final Map<Long, CompletableFuture<Void>> futures = publisher.sendAll(region(), batch);
            // One deadline for the whole batch (sends are in flight concurrently): the tick's total
            // await is bounded by sendAwaitTimeoutMs regardless of batch size, even when the
            // producer never completes a future.
            final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(sendAwaitTimeoutMs);
            final List<Long> sent = new ArrayList<>();
            final List<Long> failed = new ArrayList<>();
            final List<Long> rejected = new ArrayList<>();
            futures.forEach((id, future) -> {
                switch (await(id, future, deadlineNanos)) {
                    case SENT -> sent.add(id);
                    case FAILED -> failed.add(id);
                    case REJECTED -> rejected.add(id);
                }
            });

            outboxRepository.markSent(region(), sent);
            outboxRepository.markSendFailure(region(), failed, "Kafka send failed in RELAY stage");
            outboxRepository.markSendRejected(region(), rejected, "Relay circuit breaker open — send not attempted");
            metrics.relaySent(region(), sent.size());
            metrics.relayFailed(region(), failed.size());
            metrics.relayRejected(region(), rejected.size());
            log.debug("[{}] Relayed {} ({} failed, {} rejected) of {} drained",
                    processIdentifier(), sent.size(), failed.size(), rejected.size(), batch.size());
        });
    }

    /**
     * Outcome of awaiting one send: {@code FAILED} is a genuine per-message failure and consumes
     * retry budget; {@code REJECTED} is an infrastructure condition (breaker open, interrupt) where
     * no send was attempted and the budget must not be consumed.
     */
    private enum SendOutcome {
        SENT,
        FAILED,
        REJECTED
    }

    private SendOutcome await(final long id, final CompletableFuture<Void> future, final long deadlineNanos) {
        try {
            future.get(Math.max(1L, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS);
            return SendOutcome.SENT;
        } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("[{}] Interrupted awaiting send for outbox id={}", processIdentifier(), id);
            return SendOutcome.REJECTED;
        } catch (final TimeoutException timeout) {
            // The send did not confirm in time; it may still land later — at-least-once delivery
            // tolerates the duplicate, and the unconfirmed attempt consumes retry budget.
            log.warn(
                    "[{}] Send unconfirmed within the batch deadline for outbox id={}", processIdentifier(), id);
            return SendOutcome.FAILED;
        } catch (final java.util.concurrent.ExecutionException sendFailure) {
            if (sendFailure.getCause() instanceof CallNotPermittedException) {
                return SendOutcome.REJECTED;
            }
            log.warn("[{}] Send failed for outbox id={}: {}", processIdentifier(), id, sendFailure.getMessage());
            return SendOutcome.FAILED;
        }
    }
}
