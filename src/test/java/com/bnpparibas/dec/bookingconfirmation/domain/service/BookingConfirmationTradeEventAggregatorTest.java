package com.bnpparibas.dec.bookingconfirmation.domain.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ParsedTradeEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import java.util.List;
import org.junit.jupiter.api.Test;

class BookingConfirmationBookingConfirmationTradeEventAggregatorTest {

    private final BookingConfirmationTradeEventAggregator aggregator = new BookingConfirmationTradeEventAggregator();

    @Test
    void aggregate_shouldEmitSingleCreatedWithLatestPayload_whenCreatedThenAmended() {
        var result = aggregator.aggregate(List.of(
                event(1, "K1", TradeEventType.TRADE_CREATED),
                event(2, "K1", TradeEventType.TRADE_AMENDED)));

        assertThat(result).singleElement().satisfies(agg -> {
            assertThat(agg.emitAs()).isEqualTo(TradeEventType.TRADE_CREATED);
            assertThat(agg.survivor().id()).isEqualTo(2L);        // amended payload survives, re-typed CREATED
            assertThat(agg.collapsedIds()).containsExactly(1L);   // the original CREATED is collapsed
        });
    }

    @Test
    void aggregate_shouldDropEntireGroup_whenCreatedAndBustedInSameWindow() {
        var result = aggregator.aggregate(List.of(
                event(1, "K1", TradeEventType.TRADE_CREATED),
                event(2, "K1", TradeEventType.TRADE_AMENDED),
                event(3, "K1", TradeEventType.TRADE_DELETED)));

        assertThat(result).singleElement().satisfies(agg -> {
            assertThat(agg.survivor()).isNull();                  // born and killed — nothing published
            assertThat(agg.collapsedIds()).containsExactly(1L, 2L, 3L);
        });
    }

    @Test
    void aggregate_shouldEmitBusted_whenAmendedThenBustedWithoutCreated() {
        var result = aggregator.aggregate(List.of(
                event(1, "K1", TradeEventType.TRADE_AMENDED),
                event(2, "K1", TradeEventType.TRADE_DELETED)));

        assertThat(result).singleElement().satisfies(agg -> {
            assertThat(agg.emitAs()).isEqualTo(TradeEventType.TRADE_DELETED);
            assertThat(agg.survivor().id()).isEqualTo(2L);
        });
    }

    @Test
    void aggregate_shouldEmitLatestAmended_whenMultipleAmendmentsWithoutCreated() {
        var result = aggregator.aggregate(List.of(
                event(1, "K1", TradeEventType.TRADE_AMENDED),
                event(2, "K1", TradeEventType.TRADE_AMENDED)));

        assertThat(result).singleElement().satisfies(agg -> {
            assertThat(agg.emitAs()).isEqualTo(TradeEventType.TRADE_AMENDED);
            assertThat(agg.survivor().id()).isEqualTo(2L);
        });
    }

    @Test
    void aggregate_shouldLetBustedDominate_whenBustedArrivesBeforeAmend() {
        var result = aggregator.aggregate(List.of(
                event(1, "K1", TradeEventType.TRADE_DELETED),
                event(2, "K1", TradeEventType.TRADE_AMENDED)));

        assertThat(result).singleElement().satisfies(agg -> {
            assertThat(agg.emitAs()).isEqualTo(TradeEventType.TRADE_DELETED);
            assertThat(agg.survivor().id()).isEqualTo(1L);        // the bust payload, not the later amend
        });
    }

    @Test
    void aggregate_shouldKeepGroupsIndependent_whenTradesInterleave() {
        var result = aggregator.aggregate(List.of(
                event(1, "K1", TradeEventType.TRADE_CREATED),
                event(2, "K2", TradeEventType.TRADE_AMENDED),
                event(3, "K1", TradeEventType.TRADE_AMENDED),
                event(4, "K2", TradeEventType.TRADE_DELETED)));

        assertThat(result).hasSize(2);
        assertThat(result.get(0).emitAs()).isEqualTo(TradeEventType.TRADE_CREATED);     // K1, first seen
        assertThat(result.get(0).survivor().id()).isEqualTo(3L);
        assertThat(result.get(1).emitAs()).isEqualTo(TradeEventType.TRADE_DELETED);      // K2
        assertThat(result.get(1).survivor().id()).isEqualTo(4L);
    }

    @Test
    void aggregate_shouldNotGroupNullKeyedEventsTogether() {
        var result = aggregator.aggregate(List.of(
                event(1, null, TradeEventType.TRADE_CREATED),
                event(2, null, TradeEventType.TRADE_DELETED)));

        assertThat(result).hasSize(2);   // a missing key cannot identify a trade — each stays a singleton
    }

    @Test
    void aggregate_shouldKeepNullKeyedEventsSeparate_evenWhenTheirIdsAreNull() {
        // No message key AND no row id — the events must still form two singleton groups.
        var first = new ParsedTradeEvent(
                new InboxMessage(null, Region.AMER, "idem-a", "topic", 0, 0L, null, "{}", null, null),
                TradeEventType.TRADE_CREATED);
        var second = new ParsedTradeEvent(
                new InboxMessage(null, Region.AMER, "idem-b", "topic", 0, 1L, null, "{}", null, null),
                TradeEventType.TRADE_DELETED);

        assertThat(aggregator.aggregate(List.of(first, second))).hasSize(2);
    }

    private static ParsedTradeEvent event(long id, String messageKey, TradeEventType type) {
        var message = new InboxMessage(
                id, Region.AMER, "idem-" + id, "topic", 0, id, messageKey,
                "{\"eventType\":\"" + type + "\"}", null, null);
        return new ParsedTradeEvent(message, type);
    }
}
