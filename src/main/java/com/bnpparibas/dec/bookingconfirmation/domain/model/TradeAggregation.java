package com.bnpparibas.dec.bookingconfirmation.domain.model;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Outcome of collapsing one trade's events within a single PROCESS drain.
 *
 * <p>{@code survivor} is the event whose payload gets published, re-typed as {@code emitAs} when
 * the two differ (Created+Amended collapses into a CREATED event carrying the amended payload). A
 * {@code null} survivor means the group nets out to nothing — the trade was created and busted
 * within the same drain, so downstream never needs to hear about it. {@code collapsedIds} are the
 * inbox rows superseded or netted out by the aggregation — marked AGGREGATED, never published.
 */
public record TradeAggregation(
        @Nullable ParsedTradeEvent survivor, @Nullable TradeEventType emitAs, List<Long> collapsedIds) {

    public static TradeAggregation emit(
            final ParsedTradeEvent survivor, final TradeEventType emitAs, final List<Long> collapsedIds) {
        return new TradeAggregation(survivor, emitAs, collapsedIds);
    }

    public static TradeAggregation dropAll(final List<Long> collapsedIds) {
        return new TradeAggregation(null, null, collapsedIds);
    }
}
