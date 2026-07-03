package com.bnpparibas.dec.bookingconfirmation.domain.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import org.junit.jupiter.api.Test;

class BookingProcessServiceTest {

    @Test
    void processType_defaultsToProcess() {
        BookingProcessService service = new BookingProcessService() {
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
                return "AMER.PROCESS";
            }
        };

        assertThat(service.processType()).isEqualTo(ProcessType.PROCESS);
    }
}
