package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * An inbox message whose event type has been read from the payload — the unit the
 * {@link com.bnpparibas.dec.bookingconfirmation.domain.service.TradeEventAggregator} operates on.
 *
 * <p>The payload itself stays the opaque raw JSON carried by {@code message}; only the type
 * discriminator has been parsed out.
 */
public record ParsedTradeEvent(InboxMessage message, TradeEventType type) {

    /** The inbox row id (always present here — parsed events come from drained rows). */
    public Long id() {
        return message.id();
    }
}
