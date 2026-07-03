package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ParsedTradeEventTest {

    @Test
    void id_delegatesToMessage_andAccessorsHold() {
        var message = new InboxMessage(5L, Region.AMER, "idem", "topic", 0, 0L, "key", "raw", "trace", null);
        var parsed = new ParsedTradeEvent(message, TradeEventType.CREATED);

        assertThat(parsed.id()).isEqualTo(5L);
        assertThat(parsed.type()).isEqualTo(TradeEventType.CREATED);
        assertThat(parsed.message()).isSameAs(message);
    }
}
