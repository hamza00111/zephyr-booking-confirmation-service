package com.bnpparibas.dec.bookingconfirmation.infrastructure.config.serialization;

import com.bnpparibas.dec.zephyr.domain.trade.events.TradeAmendedEvent;
import com.bnpparibas.dec.zephyr.domain.trade.events.TradeCreatedEvent;
import com.bnpparibas.dec.zephyr.domain.trade.events.TradeDeletedEvent;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Polymorphic mix-in applied (in {@link SerializationConfig}) to the annotation-free
 * {@code TradeEvent} domain model so Jackson can resolve the concrete subtype from the wire
 * discriminator. (Jackson 3 still sources these annotations from {@code com.fasterxml.jackson.annotation}.)
 *
 * <p><b>VERIFY on the VM</b> against the {@code com.bnpparibas.dec.zephyr.domain.trade.events}
 * artifact and a real internal-topic message:
 *
 * <ul>
 *   <li><b>Amend subtype class.</b> This binds {@code TradeAmendedEvent}, but the publisher's copy
 *       imports {@code TradeUpdatedEvent}. The {@code name} is the wire string; the {@code value}
 *       must be whatever the artifact actually names that class.
 *   <li><b>Discriminator.</b> This uses {@code As.PROPERTY} (default property {@code @type}, values
 *       {@code TRADE_*}). If your messages carry the type only in {@code eventType} (un-prefixed
 *       values), switch to {@code @JsonTypeInfo(use = Id.NAME, include = As.EXISTING_PROPERTY,
 *       property = "eventType", visible = true)} with names {@code CREATED}/{@code AMENDED}/{@code DELETED}.
 * </ul>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = TradeCreatedEvent.class, name = "TRADE_CREATED"),
    @JsonSubTypes.Type(value = TradeAmendedEvent.class, name = "TRADE_AMENDED"),
    @JsonSubTypes.Type(value = TradeDeletedEvent.class, name = "TRADE_DELETED"),
})
public interface TradeEventsMixin {}
