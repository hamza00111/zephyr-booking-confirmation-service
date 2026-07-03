package com.bnpparibas.dec.bookingconfirmation.application.transform;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import org.junit.jupiter.api.Test;

class TransformConfigTest {

    private final TransformConfig config = new TransformConfig();

    @Test
    void noOpTradeEnricher_returnsPayloadUnchanged() {
        assertThat(config.noOpTradeEnricher().enrich(Region.AMER, "payload")).isEqualTo("payload");
    }

    @Test
    void allowAllTradeFilter_keepsEverything() {
        assertThat(config.allowAllTradeFilter().keep(Region.AMER, "payload")).isTrue();
    }
}
