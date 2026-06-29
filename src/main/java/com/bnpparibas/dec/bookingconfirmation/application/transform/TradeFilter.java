package com.bnpparibas.dec.bookingconfirmation.application.transform;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.zephyr.domain.trade.events.TradeEvent;

/**
 * Filtering seam (IRIS_TRADE_FILTER rules — implemented next week).
 *
 * <p>Decides keep/drop on the typed {@link TradeEvent} read view; it never rebuilds the payload (the
 * faithful original is what gets published). The default {@code @Bean} keeps every message. When the
 * real rules land, dropped messages are marked PROCESSED in the inbox with no outbox row written.
 */
@FunctionalInterface
public interface TradeFilter {

    /** {@code true} to keep (publish) the message, {@code false} to drop it. */
    boolean keep(Region region, TradeEvent tradeEvent);
}
