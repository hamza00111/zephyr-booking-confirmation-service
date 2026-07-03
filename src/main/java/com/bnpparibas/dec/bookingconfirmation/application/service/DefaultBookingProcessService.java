package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeEventTransformer;
import com.bnpparibas.dec.bookingconfirmation.domain.model.BookingConfirmationAggregation;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ParsedTradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingConfirmationTradeEventAggregator;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingProcessService;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * PROCESS stage: drains NEW inbox rows, reads each event's type, aggregates per trade (Kafka
 * message key), transforms each surviving payload into its outbox event (bind under the group's
 * emitted type, enrich, filter, serialize — see {@link TradeEventTransformer}), and within a single
 * transaction inserts the resulting outbox events and marks every drained row.
 *
 * <p>The transaction spans drain ({@code FOR UPDATE SKIP LOCKED}) -> aggregate -> insert -> mark so
 * the claimed rows stay locked until commit. Unparseable payloads are marked INVALID and excluded
 * from aggregation; rows collapsed away by aggregation are AGGREGATED; filtered-out survivors are
 * PROCESSED with no outbox row. A transform or typed-binding failure marks the survivor <em>and
 * its collapsed siblings</em> PROCESS_FAILURE without aborting the rest of the batch — the REQUEUE
 * stage later promotes the whole group back to NEW so a retry re-aggregates it intact (the emitted
 * type is derived from the full group, so retrying the survivor alone could emit the wrong type).
 */
public class DefaultBookingProcessService extends AbstractRegionScopedService implements BookingProcessService {

    private final int batchSize;
    private final InboxRepository inboxRepository;
    private final OutboxRepository outboxRepository;
    private final BookingConfirmationTradeEventAggregator tradeEventAggregator;
    private final TradeEventTransformer tradeEventTransformer;
    private final TransactionTemplate transactionTemplate;

    public DefaultBookingProcessService(
            final RegionScope scope,
            final int batchSize,
            final InboxRepository inboxRepository,
            final OutboxRepository outboxRepository,
            final BookingConfirmationTradeEventAggregator tradeEventAggregator,
            final TradeEventTransformer tradeEventTransformer,
            final TransactionTemplate transactionTemplate) {
        super(scope);
        this.batchSize = batchSize;
        this.inboxRepository = inboxRepository;
        this.outboxRepository = outboxRepository;
        this.tradeEventAggregator = tradeEventAggregator;
        this.tradeEventTransformer = tradeEventTransformer;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    protected void doTick(final Set<Integer> ownedPartitions) {
        transactionTemplate.executeWithoutResult(status -> {
            final List<InboxMessage> batch = inboxRepository.findNew(region(), ownedPartitions, batchSize);
            if (batch.isEmpty()) {
                return;
            }
            final TickOutcome outcome = new TickOutcome();
            final List<ParsedTradeEvent> events = classify(batch, outcome);
            for (final BookingConfirmationAggregation aggregation : tradeEventAggregator.aggregate(events)) {
                apply(aggregation, outcome);
            }
            persist(batch.size(), outcome);
        });
    }

    /** Reads each message's event type; unreadable ones go straight to INVALID. */
    private List<ParsedTradeEvent> classify(final List<InboxMessage> batch, final TickOutcome outcome) {
        final List<ParsedTradeEvent> events = new ArrayList<>();
        for (final InboxMessage message : batch) {
            final Optional<TradeEventType> type = tradeEventTransformer.eventType(message.rawPayload());
            if (type.isPresent()) {
                events.add(new ParsedTradeEvent(message, type.get()));
            } else {
                log.warn("[{}] No readable event type for inbox id={}", processIdentifier(), message.id());
                outcome.invalid.add(message.id());
            }
        }
        return events;
    }

    /** Transforms one aggregation group's survivor; a failure fails the whole group. */
    private void apply(final BookingConfirmationAggregation aggregation, final TickOutcome outcome) {
        final ParsedTradeEvent survivor = aggregation.survivor();
        if (survivor == null) {
            // Whole group netted out: created and busted within this drain.
            outcome.aggregatedAway.addAll(aggregation.collapsedIds());
            return;
        }
        final InboxMessage message = survivor.message();
        try (var ignored = TraceMdc.scope(message.traceId())) {
            tradeEventTransformer.toOutboxEvent(message, aggregation.emitAs()).ifPresent(outcome.toPublish::add);
            outcome.processed.add(message.id());
            outcome.aggregatedAway.addAll(aggregation.collapsedIds());
        } catch (final RuntimeException transformFailure) {
            log.error("[{}] Transform failed for inbox id={}", processIdentifier(), message.id(), transformFailure);
            // Fail the whole group, not just the survivor: emitAs is derived from the group.
            // A retry usually re-drains the members together (adjacent ids); if a batch
            // boundary splits them, id order still emits them oldest-first, which is the
            // same correct-downstream outcome as a group split across two drains.
            outcome.failed.add(message.id());
            outcome.failed.addAll(aggregation.collapsedIds());
        }
    }

    /** One batched insert + batched status updates, all in the single tick transaction. */
    private void persist(final int drained, final TickOutcome outcome) {
        outboxRepository.insertAll(outcome.toPublish);
        inboxRepository.markProcessed(region(), outcome.processed);
        inboxRepository.markAggregated(region(), outcome.aggregatedAway);
        inboxRepository.markInvalid(region(), outcome.invalid, "No readable event type in payload");
        inboxRepository.markProcessFailure(region(), outcome.failed, "Transform failed in PROCESS stage");
        metrics.processInvalid(region(), outcome.invalid.size());
        metrics.processFailed(region(), outcome.failed.size());
        log.debug(
                "[{}] Drained {}: {} published, {} processed, {} aggregated away, {} invalid, {} failed",
                processIdentifier(),
                drained,
                outcome.toPublish.size(),
                outcome.processed.size(),
                outcome.aggregatedAway.size(),
                outcome.invalid.size(),
                outcome.failed.size());
    }

    /** Per-tick accumulator for the fate of every drained row. */
    private static final class TickOutcome {
        private final List<OutboxEvent> toPublish = new ArrayList<>();
        private final List<Long> processed = new ArrayList<>();
        private final List<Long> aggregatedAway = new ArrayList<>();
        private final List<Long> invalid = new ArrayList<>();
        private final List<Long> failed = new ArrayList<>();
    }
}
