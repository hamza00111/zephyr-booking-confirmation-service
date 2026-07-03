package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Serializes {@code TradeEventType} as its wire string ({@code type()}) (and, being an enum, Jackson derives the
 * reverse mapping for deserialization). Mixin so the copied enum stays annotation-free, mirroring
 * the publisher service.
 */
public interface TradeEventTypeMixin {

    @JsonValue
    String type();
}
