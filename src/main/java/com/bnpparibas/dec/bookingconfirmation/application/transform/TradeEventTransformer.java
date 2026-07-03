package com.bnpparibas.dec.bookingconfirmation.application.transform;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import java.util.Optional;

/**
 * Turns a surviving inbox message into its outbox event for one region: bind the payload under the
 * aggregation's emitted type, enrich, filter, serialize, and wrap with the region's published
 * topic. Failures propagate as runtime exceptions so the PROCESS stage can mark the whole
 * aggregation group PROCESS_FAILURE.
 *
 * <p>Per-region and stateless; built by the registry alongside the per-region process service.
 */
public class TradeEventTransformer {

    private final Region region;
    private final String publishedTopic;
    private final TradeEventCodec codec;
    private final TradeEnricher enricher;
    private final TradeFilter filter;

    public TradeEventTransformer(
            final Region region,
            final String publishedTopic,
            final TradeEventCodec codec,
            final TradeEnricher enricher,
            final TradeFilter filter) {
        this.region = region;
        this.publishedTopic = publishedTopic;
        this.codec = codec;
        this.enricher = enricher;
        this.filter = filter;
    }

    /** Classification gate: empty when the payload is unparseable or carries no known type. */
    public Optional<TradeEventType> eventType(final String payload) {
        return codec.eventType(payload);
    }

    /**
     * @return the outbox event to publish, or empty when the filter drops it (the row is still
     *     marked PROCESSED — dropped, not failed).
     * @throws com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventBindingException on
     *     binding/serialization failure
     */
    public Optional<OutboxEvent> toOutboxEvent(final InboxMessage message, final TradeEventType emitAs) {
        final TradeEvent event = codec.deserialize(message.rawPayload(), emitAs);
        final TradeEvent enriched = enricher.enrich(region, event);
        if (!filter.keep(region, enriched)) {
            return Optional.empty();
        }
        return Optional.of(new OutboxEvent(
                null,
                region,
                message.idempotencyKey(),
                publishedTopic,
                message.messageKey(),
                message.partition(),
                codec.serialize(enriched),
                message.id(),
                message.traceId(),
                message.headers()));
    }
}
