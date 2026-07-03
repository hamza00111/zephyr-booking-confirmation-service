package com.bnpparibas.dec.bookingconfirmation.domain.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import org.junit.jupiter.api.Test;

class BookingRequeueServiceTest {

    @Test
    void processType_defaultsToRequeue() {
        BookingRequeueService service = new BookingRequeueService() {
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
                return "AMER.REQUEUE";
            }
        };

        assertThat(service.processType()).isEqualTo(ProcessType.REQUEUE);
    }
}
