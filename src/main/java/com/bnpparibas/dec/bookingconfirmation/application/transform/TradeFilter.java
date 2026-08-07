package com.bnpparibas.dec.bookingconfirmation.application.transform;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;

/**
 * Filtering seam (IRIS_TRADE_FILTER rules — implemented next week).
 *
 * <p>Operates on the typed trade-event envelope. The default {@code @Bean} keeps every message.
 * When the real rules land, dropped messages are marked PROCESSED in the inbox with no outbox row
 * written.
 */
@FunctionalInterface
public interface TradeFilter {

    /** {@code true} to keep (publish) the event, {@code false} to drop it. */
    boolean keep(Region region, TradeEvent event);
}
