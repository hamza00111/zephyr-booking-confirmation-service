package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeEnricher;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeFilter;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ParsedTradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeAggregation;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingProcessService;
import com.bnpparibas.dec.bookingconfirmation.domain.service.TradeEventAggregator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * PROCESS stage: drains NEW inbox rows, reads each event's type, aggregates per trade (Kafka
 * message key), transforms the surviving events (enrich + filter — no-op for now), and within a
 * single transaction inserts the resulting outbox events and marks every drained row.
 *
 * <p>The transaction spans drain ({@code FOR UPDATE SKIP LOCKED}) -> aggregate -> insert -> mark so
 * the claimed rows stay locked until commit. Unparseable payloads are marked INVALID and excluded
 * from aggregation; rows collapsed away by aggregation are AGGREGATED; filtered-out survivors are
 * PROCESSED with no outbox row. A transform failure marks the survivor PROCESS_FAILURE without
 * aborting the rest of the batch — its collapsed siblings stay AGGREGATED, so nothing of that trade
 * is published and the failure remains visible to ops.
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
            final int batchSize,
            final String publishedTopic,
            final InboxRepository inboxRepository,
            final OutboxRepository outboxRepository,
            final TradeEventCodec tradeEventCodec,
            final TradeEventAggregator tradeEventAggregator,
            final TradeEnricher tradeEnricher,
            final TradeFilter tradeFilter,
            final TransactionTemplate transactionTemplate) {
        super(region);
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
    protected void doTick() {
        transactionTemplate.executeWithoutResult(status -> {
            final List<InboxMessage> batch = inboxRepository.findNew(region(), batchSize);
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
                aggregatedAway.addAll(aggregation.collapsedIds());
                final ParsedTradeEvent survivor = aggregation.survivor();
                if (survivor == null) {
                    continue; // whole group netted out: created and busted within this drain
                }
                final InboxMessage message = survivor.message();
                try (var ignored = TraceMdc.scope(message.traceId())) {
                    final String payload = survivor.type() == aggregation.emitAs()
                            ? message.rawPayload()
                            : tradeEventCodec.rewriteType(message.rawPayload(), aggregation.emitAs());
                    final String enriched = tradeEnricher.enrich(region(), payload);
                    if (tradeFilter.keep(region(), enriched)) {
                        toPublish.add(new OutboxEvent(
                                null,
                                region(),
                                message.idempotencyKey(),
                                publishedTopic,
                                message.messageKey(),
                                message.partition(),
                                enriched,
                                message.id(),
                                message.traceId(),
                                message.headers()));
                    }
                    processed.add(message.id());
                } catch (final RuntimeException transformFailure) {
                    log.error(
                            "[{}] Transform failed for inbox id={}",
                            processIdentifier(),
                            message.id(),
                            transformFailure);
                    failed.add(message.id());
                }
            }

            // One batched insert + batched status updates, all in this single tick transaction.
            outboxRepository.insertAll(toPublish);
            inboxRepository.markProcessed(region(), processed);
            inboxRepository.markAggregated(region(), aggregatedAway);
            inboxRepository.markInvalid(region(), invalid, "No readable event type in payload");
            inboxRepository.markProcessFailure(region(), failed, "Transform failed in PROCESS stage");
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
