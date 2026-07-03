package com.bnpparibas.dec.bookingconfirmation.domain.model.trade;

/**
 * Wire-level change type carried in the trade-event envelope's {@code eventType} field (also the
 * Jackson subtype discriminator, via {@code TradeEventTypeMixin}'s {@code @JsonValue} on
 * {@link #type()}).
 *
 * <p>Distinct from the aggregation enum {@code domain/model/TradeEventType}
 * (CREATED/AMENDED/BUSTED): this one speaks the producer's vocabulary; the codec maps between the
 * two ({@code TRADE_DELETED} ↔ {@code BUSTED}).
 */
public enum EventChangeType {
    TRADE_CREATED,
    TRADE_AMENDED,
    TRADE_DELETED;

    public String type() {
        return name();
    }
}
