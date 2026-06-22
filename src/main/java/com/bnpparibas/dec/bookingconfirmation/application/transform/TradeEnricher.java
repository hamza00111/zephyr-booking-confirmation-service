package com.bnpparibas.dec.bookingconfirmation.application.transform;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;

/**
 * Enrichment seam (IRIS_TRADE_ENRICH rules — implemented next week).
 *
 * <p>Operates on the raw pivot payload for now (store-and-forward); when the real rules land this can
 * switch to a typed {@code Trade} pivot. The default {@code @Bean} is a pass-through no-op.
 */
@FunctionalInterface
public interface TradeEnricher {

    /** Returns the enriched pivot payload. */
    String enrich(Region region, String pivotPayload);
}
