package com.bnpparibas.dec.bookingconfirmation.domain.event;

import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import java.util.Optional;

/**
 * Port for the TradeEvent payload handling the PROCESS stage needs: cheap introspection
 * ({@link #eventType}, {@link #traceId}) and the typed {@link #deserialize}/{@link #serialize}
 * round-trip that feeds the enrich/filter seam.
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
     * Reads the {@code traceId} correlation id from the event body (a top-level field the upstream
     * publisher stamps), used to correlate this service's logs across the decoupled pipeline.
     *
     * @return empty when the payload is unparseable or carries no {@code traceId}.
     */
    Optional<String> traceId(String payload);

    /**
     * Binds the payload to the typed envelope <em>as</em> the given type: the payload's own
     * discriminator is overridden by {@code as} (a no-op when they already match), so aggregation
     * can emit a group's survivor under the group's derived type — e.g. a Created+Amended group
     * collapsing into a {@code TradeCreated} carrying the amended payload. The type was already
     * resolved once by {@link #eventType} during classification; it is not re-resolved here.
     *
     * @throws TradeEventBindingException when the payload is unparseable, is not a JSON object, or
     *     fails binding.
     */
    TradeEvent deserialize(String payload, TradeEventType as);

    /**
     * Serializes the typed envelope back to the wire JSON (discriminator in the current
     * {@code TRADE_*} vocabulary; the opaque trade body round-trips unchanged).
     *
     * @throws TradeEventBindingException on serialization failure (a programming error in practice).
     */
    String serialize(TradeEvent event);
}
