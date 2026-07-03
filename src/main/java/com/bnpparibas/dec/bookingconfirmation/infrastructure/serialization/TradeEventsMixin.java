package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeAmendedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeCreatedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeDeletedEvent;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Polymorphic binding for the trade-event envelope, keyed on the existing {@code eventType} field
 * (mirrors the publisher service's mixin). Applied as a mixin so the domain model stays free of
 * serialization annotations.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "eventType",
        visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TradeCreatedEvent.class, name = "TRADE_CREATED"),
    @JsonSubTypes.Type(value = TradeAmendedEvent.class, name = "TRADE_AMENDED"),
    @JsonSubTypes.Type(value = TradeDeletedEvent.class, name = "TRADE_DELETED")
})
public interface TradeEventsMixin {}
