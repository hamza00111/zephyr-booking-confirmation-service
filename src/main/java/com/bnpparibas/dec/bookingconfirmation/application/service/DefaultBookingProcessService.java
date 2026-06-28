package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeEnricher;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeFilter;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.CreateState;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ParsedTradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeAggregation;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.TradeGateRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingProcessService;
import com.bnpparibas.dec.bookingconfirmation.domain.service.TradeEventAggregator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * PROCESS stage: drains NEW inbox rows, reads each event's type, aggregates per trade (Kafka message
 * key), applies the <b>create-barrier</b>, transforms the surviving events (enrich + filter), and
 * within a single transaction inserts the resulting outbox events and marks every drained row.
 *
 * <p><b>Create-barrier.</b> The downstream third party rejects an AMEND/BUST whose CREATE it never
 * received, so an AMEND must never be relayed before its trade's CREATE reaches SENT. A per-trade gate
 * ({@link TradeGateRepository}) records whether a CREATE has been emitted/delivered for each key:
 *
 * <ul>
 *   <li><b>CREATE</b> → emit and set the gate IN_FLIGHT (or re-type to AMEND if the trade is already
 *       SENT downstream).
 *   <li><b>AMEND, gate SENT</b> → emit normally.
 *   <li><b>AMEND, gate IN_FLIGHT</b> → BLOCK (held until the CREATE is delivered, then released in
 *       order by RELAY).
 *   <li><b>AMEND, gate NONE/FAILED</b> → AUTO-PROMOTE into a CREATE (the payload is a full snapshot),
 *       so a terminally-failed CREATE self-heals with no human in the loop.
 *   <li><b>BUST before delivery</b> → emit nothing, cancel any not-yet-delivered CREATE, set the gate
 *       VOID (the trade was born and killed before downstream saw it).
 * </ul>
 *
 * <p>Because PROCESS runs single-writer per region (region lock + single-threaded tick), gate upserts
 * for a key never race within a region. Unparseable payloads are INVALID; rows collapsed by
 * aggregation are AGGREGATED; a transform failure marks the survivor PROCESS_FAILURE (retried with
 * backoff) without aborting the batch.
 */
public class DefaultBookingProcessService extends AbstractRegionScopedService implements BookingProcessService {

    private final int batchSize;
    private final String publishedTopic;
    private final InboxRepository inboxRepository;
    private final OutboxRepository outboxRepository;
    private final TradeGateRepository tradeGateRepository;
    private final TradeEventCodec tradeEventCodec;
    private final TradeEventAggregator tradeEventAggregator;
    private final TradeEnricher tradeEnricher;
    private final TradeFilter tradeFilter;
    private final TransactionTemplate transactionTemplate;

    public DefaultBookingProcessService(
            final Region region,
            final long tickIntervalMs,
            final int batchSize,
            final int lockTtlMultiplier,
            final String publishedTopic,
            final InboxRepository inboxRepository,
            final OutboxRepository outboxRepository,
            final TradeGateRepository tradeGateRepository,
            final TradeEventCodec tradeEventCodec,
            final TradeEventAggregator tradeEventAggregator,
            final TradeEnricher tradeEnricher,
            final TradeFilter tradeFilter,
            final TransactionTemplate transactionTemplate,
            final DistributedLockRepository lockRepository,
            final InstanceId instanceId) {
        super(region, tickIntervalMs, lockTtlMultiplier, lockRepository, instanceId);
        this.batchSize = batchSize;
        this.publishedTopic = publishedTopic;
        this.inboxRepository = inboxRepository;
        this.outboxRepository = outboxRepository;
        this.tradeGateRepository = tradeGateRepository;
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

            final List<TradeAggregation> aggregations = tradeEventAggregator.aggregate(events);
            final Map<String, CreateState> gateStates = readGateStates(aggregations);

            final Outcome outcome = new Outcome();
            for (final TradeAggregation aggregation : aggregations) {
                outcome.aggregatedAway.addAll(aggregation.collapsedIds());
                final ParsedTradeEvent survivor = aggregation.survivor();
                if (survivor == null) {
                    continue; // whole group netted out: created and busted within this drain
                }
                final InboxMessage message = survivor.message();
                try (var ignored = TraceMdc.scope(message.traceId())) {
                    applyBarrier(aggregation, gateStates, outcome);
                } catch (final RuntimeException transformFailure) {
                    log.error("[{}] Transform failed for inbox id={}", processIdentifier(), message.id(), transformFailure);
                    outcome.failed.add(message.id());
                }
            }

            // One batched insert + batched status updates, all in this single tick transaction.
            outboxRepository.insertAll(outcome.toPublish);
            inboxRepository.markProcessed(region(), outcome.processed);
            inboxRepository.markAggregated(region(), outcome.aggregatedAway);
            inboxRepository.markBlocked(region(), outcome.blocked);
            inboxRepository.markSuperseded(region(), outcome.superseded);
            inboxRepository.markInvalid(region(), invalid, "No readable event type in payload");
            inboxRepository.markProcessFailure(region(), outcome.failed, "Transform failed in PROCESS stage");
            log.debug(
                    "[{}] Drained {}: {} published, {} processed, {} blocked, {} aggregated, {} invalid, {} failed, {} gate updates",
                    processIdentifier(),
                    batch.size(),
                    outcome.toPublish.size(),
                    outcome.processed.size(),
                    outcome.blocked.size(),
                    outcome.aggregatedAway.size(),
                    invalid.size(),
                    outcome.failed.size(),
                    outcome.gateKeys.size());
        });
    }

    /** Bulk-reads the gate state for every survivor's trade key (keyless survivors are skipped). */
    private Map<String, CreateState> readGateStates(final List<TradeAggregation> aggregations) {
        final List<String> keys = aggregations.stream()
                .map(TradeAggregation::survivor)
                .filter(survivor -> survivor != null && survivor.message().messageKey() != null)
                .map(survivor -> survivor.message().messageKey())
                .distinct()
                .toList();
        return tradeGateRepository.statesFor(region(), keys);
    }

    /** Applies the create-barrier decision matrix for one trade's survivor. */
    private void applyBarrier(
            final TradeAggregation aggregation, final Map<String, CreateState> gateStates, final Outcome outcome) {
        final InboxMessage message = aggregation.survivor().message();
        final String key = message.messageKey();
        final TradeEventType emitAs = aggregation.emitAs();

        // A keyless event cannot be gated — emit it as the aggregator decided (legacy behaviour).
        if (key == null) {
            emit(aggregation, emitAs, outcome);
            return;
        }

        final CreateState state = gateStates.getOrDefault(key, CreateState.NONE);
        switch (emitAs) {
            case CREATED -> onCreated(aggregation, key, state, outcome);
            case AMENDED -> onAmended(aggregation, key, state, outcome);
            case BUSTED -> onBusted(aggregation, key, state, outcome);
        }
    }

    private void onCreated(
            final TradeAggregation aggregation, final String key, final CreateState state, final Outcome outcome) {
        switch (state) {
            case SENT -> {
                // Trade already exists downstream — a second CREATE is really an amendment.
                emit(aggregation, TradeEventType.AMENDED, outcome);
            }
            case IN_FLIGHT -> {
                // A CREATE is already staged but not yet delivered — drop this redundant one.
                outcome.processed.add(aggregation.survivor().id());
            }
            default -> { // NONE | FAILED | VOID
                if (emit(aggregation, TradeEventType.CREATED, outcome)) {
                    setGate(key, CreateState.IN_FLIGHT, outcome);
                }
            }
        }
    }

    private void onAmended(
            final TradeAggregation aggregation, final String key, final CreateState state, final Outcome outcome) {
        switch (state) {
            case SENT -> emit(aggregation, TradeEventType.AMENDED, outcome);
            case IN_FLIGHT -> {
                // Hold until the CREATE is delivered; RELAY releases this row, in order, once it is SENT.
                outcome.blocked.add(aggregation.survivor().id());
            }
            case VOID -> {
                // Trade was busted before any CREATE was emitted — an amendment is meaningless.
                outcome.processed.add(aggregation.survivor().id());
            }
            default -> { // NONE | FAILED — auto-promote the full-snapshot amendment into a CREATE
                if (emit(aggregation, TradeEventType.CREATED, outcome)) {
                    setGate(key, CreateState.IN_FLIGHT, outcome);
                }
            }
        }
    }

    private void onBusted(
            final TradeAggregation aggregation, final String key, final CreateState state, final Outcome outcome) {
        if (state == CreateState.SENT) {
            emit(aggregation, TradeEventType.BUSTED, outcome);
            return;
        }
        // Born and killed before downstream saw it: cancel any not-yet-delivered CREATE and void the trade.
        final int cancelled = outboxRepository.parkUnsentForKey(region(), key, "Trade busted before CREATE delivered");
        if (cancelled > 0) {
            log.info("[{}] Cancelled {} unsent row(s) for busted trade key={}", processIdentifier(), cancelled, key);
        }
        setGate(key, CreateState.VOID, outcome);
        outcome.processed.add(aggregation.survivor().id());
    }

    /**
     * Re-types (if needed), enriches and filters the survivor's payload, staging an outbox event when
     * the filter keeps it. Returns {@code true} if an event was actually staged for publication.
     */
    private boolean emit(final TradeAggregation aggregation, final TradeEventType finalType, final Outcome outcome) {
        final ParsedTradeEvent survivor = aggregation.survivor();
        final InboxMessage message = survivor.message();
        final String payload = survivor.type() == finalType
                ? message.rawPayload()
                : tradeEventCodec.rewriteType(message.rawPayload(), finalType);
        final String enriched = tradeEnricher.enrich(region(), payload);
        final boolean keep = tradeFilter.keep(region(), enriched);
        if (keep) {
            outcome.toPublish.add(new OutboxEvent(
                    null,
                    region(),
                    message.idempotencyKey(),
                    publishedTopic,
                    message.messageKey(),
                    finalType,
                    enriched,
                    message.id(),
                    message.traceId(),
                    message.headers()));
        }
        outcome.processed.add(message.id()); // emitted or intentionally filtered — either way, processed
        return keep;
    }

    private void setGate(final String key, final CreateState state, final Outcome outcome) {
        tradeGateRepository.upsert(region(), key, state);
        outcome.gateKeys.add(key);
    }

    /** Mutable accumulator for one tick's outbox events and per-status inbox id lists. */
    private static final class Outcome {
        private final List<OutboxEvent> toPublish = new ArrayList<>();
        private final List<Long> processed = new ArrayList<>();
        private final List<Long> aggregatedAway = new ArrayList<>();
        private final List<Long> blocked = new ArrayList<>();
        private final List<Long> superseded = new ArrayList<>();
        private final List<Long> failed = new ArrayList<>();
        private final List<String> gateKeys = new ArrayList<>();
    }
}
