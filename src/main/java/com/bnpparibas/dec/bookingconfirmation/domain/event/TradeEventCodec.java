package com.bnpparibas.dec.bookingconfirmation.domain.event;

import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import java.util.Optional;

/**
 * Port for the TradeEvent payload handling the PROCESS stage needs: cheap introspection
 * ({@link #eventType}, {@link #traceId}), the JSON-level type rewrite used by aggregation, and the
 * typed {@link #deserialize}/{@link #serialize} round-trip that feeds the enrich/filter seam.
 *
 * <p>The typed view is the consumer-side envelope copy in {@code domain.model.trade}; the trade
 * body itself stays an opaque, losslessly round-tripped node.
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
     * Binds the payload to the typed envelope. Accepts both the current wire vocabulary
     * ({@code TRADE_CREATED}…) and legacy forms; the returned event's concrete type follows the
     * payload's discriminator.
     *
     * @throws TradeEventBindingException when the payload is unparseable, carries an unknown type,
     *     or fails binding.
     */
    TradeEvent deserialize(String payload);

    /**
     * Serializes the typed envelope back to the wire JSON (discriminator in the current
     * {@code TRADE_*} vocabulary; the opaque trade body round-trips unchanged).
     *
     * @throws TradeEventBindingException on serialization failure (a programming error in practice).
     */
    String serialize(TradeEvent event);
}
