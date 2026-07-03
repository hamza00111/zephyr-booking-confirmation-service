package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeEnricher;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeFilter;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ParsedTradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeAggregation;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingProcessService;
import com.bnpparibas.dec.bookingconfirmation.domain.service.TradeEventAggregator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * PROCESS stage: drains NEW inbox rows, reads each event's type, aggregates per trade (Kafka
 * message key), binds each surviving payload to the typed envelope, transforms it (enrich + filter
 * — no-op for now), serializes it back, and within a single transaction inserts the resulting
 * outbox events and marks every drained row.
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
    private final String publishedTopic;
    private final InboxRepository inboxRepository;
    private final OutboxRepository outboxRepository;
    private final TradeEventCodec tradeEventCodec;
    private final TradeEventAggregator tradeEventAggregator;
    private final TradeEnricher tradeEnricher;
    private final TradeFilter tradeFilter;
    private final TransactionTemplate transactionTemplate;

    public DefaultBookingProcessService(
            final Region region,
            final OwnedPartitions ownedPartitions,
            final int batchSize,
            final String publishedTopic,
            final InboxRepository inboxRepository,
            final OutboxRepository outboxRepository,
            final TradeEventCodec tradeEventCodec,
            final TradeEventAggregator tradeEventAggregator,
            final TradeEnricher tradeEnricher,
            final TradeFilter tradeFilter,
            final TransactionTemplate transactionTemplate,
            final BookingConfirmationMetrics metrics) {
        super(region, ownedPartitions, metrics);
        this.batchSize = batchSize;
        this.publishedTopic = publishedTopic;
        this.inboxRepository = inboxRepository;
        this.outboxRepository = outboxRepository;
        this.tradeEventCodec = tradeEventCodec;
        this.tradeEventAggregator = tradeEventAggregator;
        this.tradeEnricher = tradeEnricher;
        this.tradeFilter = tradeFilter;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    protected void doTick(final Set<Integer> ownedPartitions) {
        transactionTemplate.executeWithoutResult(status -> {
            final List<InboxMessage> batch = inboxRepository.findNew(region(), ownedPartitions, batchSize);
            if (batch.isEmpty()) {
                return;
            }

            final List<ParsedTradeEvent> events = new ArrayList<>();
            final List<Long> invalid = new ArrayList<>();
            for (final InboxMessage message : batch) {
                final Optional<TradeEventType> type = tradeEventCodec.eventType(message.rawPayload());
                if (type.isPresent()) {
                    events.add(new ParsedTradeEvent(message, type.get()));
                } else {
                    log.warn("[{}] No readable event type for inbox id={}", processIdentifier(), message.id());
                    invalid.add(message.id());
                }
            }

            final List<OutboxEvent> toPublish = new ArrayList<>();
            final List<Long> processed = new ArrayList<>();
            final List<Long> aggregatedAway = new ArrayList<>();
            final List<Long> failed = new ArrayList<>();
            for (final TradeAggregation aggregation : tradeEventAggregator.aggregate(events)) {
                final ParsedTradeEvent survivor = aggregation.survivor();
                if (survivor == null) {
                    // Whole group netted out: created and busted within this drain.
                    aggregatedAway.addAll(aggregation.collapsedIds());
                    continue;
                }
                final InboxMessage message = survivor.message();
                try (var ignored = TraceMdc.scope(message.traceId())) {
                    // Re-typing stays JSON-level, before binding: the rewritten discriminator makes
                    // Jackson instantiate the target subtype (records cannot change class).
                    final String payload = survivor.type() == aggregation.emitAs()
                            ? message.rawPayload()
                            : tradeEventCodec.rewriteType(message.rawPayload(), aggregation.emitAs());
                    final TradeEvent event = tradeEventCodec.deserialize(payload);
                    final TradeEvent enriched = tradeEnricher.enrich(region(), event);
                    if (tradeFilter.keep(region(), enriched)) {
                        toPublish.add(new OutboxEvent(
                                null,
                                region(),
                                message.idempotencyKey(),
                                publishedTopic,
                                message.messageKey(),
                                message.partition(),
                                tradeEventCodec.serialize(enriched),
                                message.id(),
                                message.traceId(),
                                message.headers()));
                    }
                    processed.add(message.id());
                    aggregatedAway.addAll(aggregation.collapsedIds());
                } catch (final RuntimeException transformFailure) {
                    log.error(
                            "[{}] Transform failed for inbox id={}",
                            processIdentifier(),
                            message.id(),
                            transformFailure);
                    // Fail the whole group, not just the survivor: emitAs is derived from the group.
                    // A retry usually re-drains the members together (adjacent ids); if a batch
                    // boundary splits them, id order still emits them oldest-first, which is the
                    // same correct-downstream outcome as a group split across two drains.
                    failed.add(message.id());
                    failed.addAll(aggregation.collapsedIds());
                }
            }

            // One batched insert + batched status updates, all in this single tick transaction.
            outboxRepository.insertAll(toPublish);
            inboxRepository.markProcessed(region(), processed);
            inboxRepository.markAggregated(region(), aggregatedAway);
            inboxRepository.markInvalid(region(), invalid, "No readable event type in payload");
            inboxRepository.markProcessFailure(region(), failed, "Transform failed in PROCESS stage");
            metrics.processInvalid(region(), invalid.size());
            metrics.processFailed(region(), failed.size());
            log.debug(
                    "[{}] Drained {}: {} published, {} processed, {} aggregated away, {} invalid, {} failed",
                    processIdentifier(),
                    batch.size(),
                    toPublish.size(),
                    processed.size(),
                    aggregatedAway.size(),
                    invalid.size(),
                    failed.size());
        });
    }
}
