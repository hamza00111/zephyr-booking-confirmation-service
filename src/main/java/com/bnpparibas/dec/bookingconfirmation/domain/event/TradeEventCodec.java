package com.bnpparibas.dec.bookingconfirmation.domain.event;

import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.zephyr.domain.trade.events.TradeEvent;
import java.util.Optional;

/**
 * Port for the TradeEvent payload introspection the PROCESS stage needs.
 *
 * <p>The lightweight reads ({@link #eventType}, {@link #traceId}, {@link #rewriteType}) operate on the
 * raw JSON tree so the payload can be republished faithfully. {@link #deserialize} binds the payload
 * to the typed {@link TradeEvent} domain model for filter/business decisions — those decisions never
 * rebuild the published bytes, so a bind failure is tolerated.
 */
public interface TradeEventCodec {

    /**
     * Reads the event type from the payload.
     *
     * @return empty when the payload is unparseable or carries no recognizable type — such rows are
     *     marked INVALID and excluded from aggregation.
     */
    Optional<TradeEventType> eventType(String payload);

    /**
     * Returns the payload with its type discriminators rewritten to {@code target}. Used when a
     * Created+Amended group collapses into a CREATED event carrying the amended payload.
     */
    String rewriteType(String payload, TradeEventType target);

    /**
     * Reads the {@code traceId} correlation id from the event body (a top-level field the upstream
     * publisher stamps), used to correlate this service's logs across the decoupled pipeline.
     *
     * @return empty when the payload is unparseable or carries no {@code traceId}.
     */
    Optional<String> traceId(String payload);

    /**
     * Deserializes the payload into the typed {@link TradeEvent} domain model (polymorphic on the
     * type discriminator) for filter/business decisions in the PROCESS stage.
     *
     * @return empty when the payload cannot be bound to a known {@code TradeEvent} subtype — the PROCESS
     *     stage then marks the row INVALID (retained for inspection/replay), never publishing an event
     *     the business rules could not vet.
     */
    Optional<TradeEvent> deserialize(String payload);
}
