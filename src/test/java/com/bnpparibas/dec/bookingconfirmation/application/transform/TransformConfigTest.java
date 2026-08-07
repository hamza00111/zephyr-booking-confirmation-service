package com.bnpparibas.dec.bookingconfirmation.application.transform;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeCreatedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import org.junit.jupiter.api.Test;

class TransformConfigTest {

    private static final TradeEvent EVENT = new TradeCreatedEvent(
            null, TradeEventType.TRADE_CREATED, null, null, null, null, null, null, null, null, null, null);

    private final TransformConfig config = new TransformConfig();

    @Test
    void noOpTradeEnricher_returnsEventUnchanged() {
        assertThat(config.noOpTradeEnricher().enrich(Region.AMER, EVENT)).isSameAs(EVENT);
    }

    @Test
    void allowAllTradeFilter_keepsEverything() {
        assertThat(config.allowAllTradeFilter().keep(Region.AMER, EVENT)).isTrue();
    }
}
