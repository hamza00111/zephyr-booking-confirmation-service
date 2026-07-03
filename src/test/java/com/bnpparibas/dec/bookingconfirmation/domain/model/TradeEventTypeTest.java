package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TradeEventTypeTest {

    @Test
    void values_areTheThreeLifecycleTypes() {
        assertThat(TradeEventType.values())
                .containsExactly(TradeEventType.CREATED, TradeEventType.AMENDED, TradeEventType.BUSTED);
    }

    @Test
    void valueOf_resolvesByName() {
        assertThat(TradeEventType.valueOf("BUSTED")).isEqualTo(TradeEventType.BUSTED);
    }
}
