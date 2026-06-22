package com.bnpparibas.dec.bookingconfirmation.domain.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;

/** REQUEUE stage: promote failed outbox rows back to NEW within the retry budget. */
public interface BookingRequeueService extends BookingConfirmationService {

    @Override
    default ProcessType processType() {
        return ProcessType.REQUEUE;
    }
}
