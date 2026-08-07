package com.bnpparibas.dec.bookingconfirmation.domain.model;

import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.EventChangeType;

/**
 * Lifecycle type of an inbound TradeEvent, mirroring the shared domain repo's enum: the constant
 * names are the {@code TRADE_*} vocabulary, and {@link #type()} is the wire discriminator carried
 * in the {@code eventType} field ({@code TradeCreated} / {@code TradeAmended} /
 * {@code TradeDeleted}) — also the Jackson subtype name, via {@code TradeEventTypeMixin}'s
 * {@code @JsonValue}.
 *
 * <p>Drives per-trade aggregation in the PROCESS stage: events of the same trade within one drain
 * collapse into at most one published event.
 */
public enum TradeEventType implements EventChangeType {
    TRADE_CREATED("TradeCreated"),
    TRADE_AMENDED("TradeAmended"),
    TRADE_DELETED("TradeDeleted");

    private final String type;

    TradeEventType(final String type) {
        this.type = type;
    }

    @Override
    public String type() {
        return this.type;
    }
}
