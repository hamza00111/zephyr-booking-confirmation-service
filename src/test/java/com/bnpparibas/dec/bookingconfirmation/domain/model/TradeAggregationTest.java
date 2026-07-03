package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TradeAggregationTest {

    private final ParsedTradeEvent survivor = new ParsedTradeEvent(
            new InboxMessage(1L, Region.AMER, "idem", "topic", 0, 0L, "key", "raw", "trace", null),
            TradeEventType.CREATED);

    @Test
    void emit_carriesSurvivorEmitTypeAndCollapsedIds() {
        var aggregation = TradeAggregation.emit(survivor, TradeEventType.CREATED, List.of(2L, 3L));

        assertThat(aggregation.survivor()).isSameAs(survivor);
        assertThat(aggregation.emitAs()).isEqualTo(TradeEventType.CREATED);
        assertThat(aggregation.collapsedIds()).containsExactly(2L, 3L);
    }

    @Test
    void dropAll_hasNoSurvivor() {
        var aggregation = TradeAggregation.dropAll(List.of(4L, 5L));

        assertThat(aggregation.survivor()).isNull();
        assertThat(aggregation.emitAs()).isNull();
        assertThat(aggregation.collapsedIds()).containsExactly(4L, 5L);
    }
}
