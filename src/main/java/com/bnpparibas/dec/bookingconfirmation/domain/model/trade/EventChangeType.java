package com.bnpparibas.dec.bookingconfirmation.domain.model.trade;

/**
 * Change-type contract of the trade-event envelope, mirroring the shared domain repo's interface.
 * {@link #type()} is the wire string carried in the {@code eventType} field; the concrete
 * implementation is {@code domain.model.TradeEventType}.
 */
public interface EventChangeType {

    String type();
}
