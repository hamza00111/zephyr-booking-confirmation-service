package com.bnpparibas.dec.bookingconfirmation.domain.model;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TradeEventTypeTest {

    @Test
    void values_areTheThreeLifecycleTypes() {
        assertThat(TradeEventType.values())
                .containsExactly(
                        TradeEventType.TRADE_CREATED, TradeEventType.TRADE_AMENDED, TradeEventType.TRADE_DELETED);
    }

    @Test
    void type_isTheWireDiscriminator() {
        assertThat(TradeEventType.TRADE_CREATED.type()).isEqualTo("TradeCreated");
        assertThat(TradeEventType.TRADE_AMENDED.type()).isEqualTo("TradeAmended");
        assertThat(TradeEventType.TRADE_DELETED.type()).isEqualTo("TradeDeleted");
    }
}
