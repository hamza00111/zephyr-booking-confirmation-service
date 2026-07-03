package com.bnpparibas.dec.bookingconfirmation.domain.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import org.junit.jupiter.api.Test;

class BookingRelayServiceTest {

    @Test
    void processType_defaultsToRelay() {
        BookingRelayService service = new BookingRelayService() {
            @Override
            public Region region() {
                return Region.AMER;
            }

            @Override
            public void tick() {
                // no-op
            }

            @Override
            public String processIdentifier() {
                return "AMER.RELAY";
            }
        };

        assertThat(service.processType()).isEqualTo(ProcessType.RELAY);
    }
}
