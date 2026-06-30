package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.Mockito.verify;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultBookingRequeueServiceTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Test
    void tick_shouldRequeueFailedRows() {
        var service = new DefaultBookingRequeueService(Region.AMER, 5, outboxRepository);

        service.tick();

        verify(outboxRepository).requeueFailed(Region.AMER, 5);
    }
}
