package com.bnpparibas.dec.bookingconfirmation.domain.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;

/** PROCESS stage: drain the inbox, transform, write to the outbox. */
public interface BookingProcessService extends BookingConfirmationService {

    @Override
    default ProcessType processType() {
        return ProcessType.PROCESS;
    }
}
