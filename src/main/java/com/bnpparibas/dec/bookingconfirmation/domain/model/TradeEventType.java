package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Lifecycle type of an inbound TradeEvent (abstract {@code TradeEvent} with three concrete
 * subtypes, owned by the producing system — not a dependency of this service).
 *
 * <p>Drives per-trade aggregation in the PROCESS stage: events of the same trade within one drain
 * collapse into at most one published event.
 */
public enum TradeEventType {
    CREATED,
    AMENDED,
    DELETED
}
