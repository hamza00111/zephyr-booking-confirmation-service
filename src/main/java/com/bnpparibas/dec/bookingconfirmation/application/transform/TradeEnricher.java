package com.bnpparibas.dec.bookingconfirmation.application.transform;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;

/**
 * Enrichment seam (IRIS_TRADE_ENRICH rules — implemented next week).
 *
 * <p>Operates on the typed trade-event envelope; the trade body itself is an opaque node the rules
 * can read or replace. The default {@code @Bean} is a pass-through no-op.
 */
@FunctionalInterface
public interface TradeEnricher {

    /** Returns the enriched event (records are immutable — return a new instance to change it). */
    TradeEvent enrich(Region region, TradeEvent event);
}
