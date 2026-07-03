package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.RequeueOutcome;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultBookingRequeueServiceTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private InboxRepository inboxRepository;

    @Test
    void tick_shouldRequeueFailedOutboxAndInboxRows_forOwnedPartitions() {
        var ownedPartitions = new OwnedPartitions();
        ownedPartitions.add(Region.AMER, 0);
        var service = new DefaultBookingRequeueService(
                new RegionScope(Region.AMER, ownedPartitions, BookingConfirmationMetrics.noop()),
                5, 3, outboxRepository, inboxRepository);
        given(outboxRepository.requeueFailed(eq(Region.AMER), any(), eq(5))).willReturn(RequeueOutcome.NONE);
        given(inboxRepository.requeueFailed(eq(Region.AMER), any(), eq(3))).willReturn(RequeueOutcome.NONE);

        service.tick();

        verify(outboxRepository).requeueFailed(eq(Region.AMER), any(), eq(5));
        verify(inboxRepository).requeueFailed(eq(Region.AMER), any(), eq(3));
    }

    @Test
    void tick_shouldSkip_whenNoOwnedPartitions() {
        var service = new DefaultBookingRequeueService(
                new RegionScope(Region.AMER, new OwnedPartitions(), BookingConfirmationMetrics.noop()),
                5, 3, outboxRepository, inboxRepository);

        service.tick();

        verifyNoInteractions(outboxRepository, inboxRepository);
    }
}
