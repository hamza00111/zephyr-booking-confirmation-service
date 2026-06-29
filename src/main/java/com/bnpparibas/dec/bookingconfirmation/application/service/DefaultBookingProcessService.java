package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.TraceMdc;
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
import com.bnpparibas.dec.zephyr.domain.trade.events.TradeEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * PROCESS stage: drains NEW inbox rows, reads each event's type, aggregates per trade (Kafka message
 * key), applies the <b>create-barrier</b>, transforms the surviving events (filter), and
 * within a single transaction inserts the resulting outbox events and marks every drained row.
 *
 * <p><b>Create-barrier.</b> The downstream third party rejects an AMEND/DELETE whose CREATE it never
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
 *   <li><b>DELETE before delivery</b> → emit nothing, cancel any not-yet-delivered CREATE, set the gate
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
                    continue; // whole group netted out: created and deleted within this drain
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
            inboxRepository.markInvalid(region(), invalid, "No readable event type in payload");
            inboxRepository.markInvalid(region(), outcome.invalid, "TradeEvent payload failed to deserialize");
            inboxRepository.markProcessFailure(region(), outcome.failed, "Transform failed in PROCESS stage");
            log.debug(
                    "[{}] Drained {}: {} published, {} processed, {} blocked, {} aggregated, {} invalid, {} failed, {} gate updates",
                    processIdentifier(),
                    batch.size(),
                    outcome.toPublish.size(),
                    outcome.processed.size(),
                    outcome.blocked.size(),
                    outcome.aggregatedAway.size(),
                    invalid.size() + outcome.invalid.size(),
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
            case DELETED -> onDeleted(aggregation, key, state, outcome);
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
                // Trade was deleted before any CREATE was emitted — an amendment is meaningless.
                outcome.processed.add(aggregation.survivor().id());
            }
            case FAILED -> {
                // The trade's CREATE failed terminally (parked). The amend is a full snapshot, so promote
                // it into a CREATE in the dead one's place — the trade self-heals with no human in the loop.
                if (emit(aggregation, TradeEventType.CREATED, outcome)) {
                    setGate(key, CreateState.IN_FLIGHT, outcome);
                    final int retired =
                            outboxRepository.supersedeParkedForKey(region(), key, "Superseded by promoted AMEND");
                    log.info("[{}] Auto-promoted AMEND to CREATE for trade key={}; retired {} parked CREATE row(s)",
                            processIdentifier(), key, retired);
                }
            }
            default -> { // NONE — no CREATE ever seen; the full-snapshot amend establishes the trade as a CREATE
                if (emit(aggregation, TradeEventType.CREATED, outcome)) {
                    setGate(key, CreateState.IN_FLIGHT, outcome);
                }
            }
        }
    }

    private void onDeleted(
            final TradeAggregation aggregation, final String key, final CreateState state, final Outcome outcome) {
        if (state == CreateState.SENT) {
            // Downstream already has the CREATE — it must learn of the delete; the trade is then gone.
            // (If the delete payload is itself INVALID, emit() returns false: leave the gate SENT so a
            // replayed delete can still retract the trade.)
            if (emit(aggregation, TradeEventType.DELETED, outcome)) {
                setGate(key, CreateState.VOID, outcome);
            }
            return;
        }
        // Try to cancel a not-yet-delivered CREATE. If a RELAY delivery is in flight this UPDATE blocks
        // on its row lock, so by the time it returns the race is already decided.
        final int cancelled = outboxRepository.parkUnsentForKey(region(), key, "Trade deleted before CREATE delivered");
        if (cancelled > 0) {
            // Parked the staged CREATE before delivery — downstream never saw the trade, so emit nothing.
            log.info("[{}] Cancelled {} unsent row(s) for deleted trade key={}", processIdentifier(), cancelled, key);
            setGate(key, CreateState.VOID, outcome);
            outcome.processed.add(aggregation.survivor().id());
            return;
        }
        // Nothing to cancel: either no CREATE was ever staged, or RELAY delivered it while we raced. The
        // tick-start gate read may be stale, so re-read now — the park above has serialised behind any
        // in-flight RELAY commit, which flips the outbox row and the gate to SENT atomically.
        final CreateState current =
                tradeGateRepository.statesFor(region(), List.of(key)).getOrDefault(key, CreateState.NONE);
        if (current == CreateState.SENT) {
            // Delivery won the race: the CREATE reached downstream, so the delete must follow it.
            log.info("[{}] CREATE delivered while deleting key={}; emitting DELETE to retract it",
                    processIdentifier(), key);
            if (emit(aggregation, TradeEventType.DELETED, outcome)) {
                setGate(key, CreateState.VOID, outcome);
            }
            return;
        }
        // No CREATE ever reached downstream (never staged, or failed terminally) — emit nothing.
        setGate(key, CreateState.VOID, outcome);
        outcome.processed.add(aggregation.survivor().id());
    }

    /**
     * Re-types (if needed) the survivor's payload to the final type, deserializes it to a typed
     * {@link TradeEvent} read view, and asks the filter whether to keep it — staging the <b>faithful</b>
     * payload (never the rebuilt object) when so. If the body cannot be bound to a {@code TradeEvent}
     * the row is marked INVALID (retained, never published): the type already parsed in {@code doTick}
     * and unknown fields are tolerated, so a bind failure is a real structural defect, not something to
     * forward past the business rules. Returns {@code true} iff an event was staged for publication.
     */
    private boolean emit(final TradeAggregation aggregation, final TradeEventType finalType, final Outcome outcome) {
        final ParsedTradeEvent survivor = aggregation.survivor();
        final InboxMessage message = survivor.message();
        final String payload = survivor.type() == finalType
                ? message.rawPayload()
                : tradeEventCodec.rewriteType(message.rawPayload(), finalType);
        final Optional<TradeEvent> view = tradeEventCodec.deserialize(payload);
        if (view.isEmpty()) {
            log.warn("[{}] TradeEvent failed to deserialize for inbox id={}; marking INVALID",
                    processIdentifier(), message.id());
            outcome.invalid.add(message.id());
            return false;
        }
        final boolean keep = tradeFilter.keep(region(), view.get());
        if (keep) {
            outcome.toPublish.add(new OutboxEvent(
                    null,
                    region(),
                    message.idempotencyKey(),
                    publishedTopic,
                    message.messageKey(),
                    finalType,
                    payload,
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
        private final List<Long> failed = new ArrayList<>();
        private final List<Long> invalid = new ArrayList<>();
        private final List<String> gateKeys = new ArrayList<>();
    }
}
