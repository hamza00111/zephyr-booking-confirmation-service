package com.bnpparibas.dec.bookingconfirmation.domain.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;

/** RELAY stage: drain the outbox, publish to the region's published topic. */
public interface BookingRelayService extends BookingConfirmationService {

    @Override
    default ProcessType processType() {
        return ProcessType.RELAY;
    }
}
