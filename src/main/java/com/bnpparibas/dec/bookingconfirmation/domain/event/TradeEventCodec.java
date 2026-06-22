package com.bnpparibas.dec.bookingconfirmation.domain.event;

import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import java.util.Optional;

/**
 * Port for the minimal TradeEvent payload introspection the PROCESS stage needs.
 *
 * <p>The payload is otherwise treated as an opaque string end-to-end — the typed TradeEvent model
 * lives in the producing system's repo and is deliberately not a dependency of this service (the
 * upcoming enrich/filter work may revisit that).
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
}
