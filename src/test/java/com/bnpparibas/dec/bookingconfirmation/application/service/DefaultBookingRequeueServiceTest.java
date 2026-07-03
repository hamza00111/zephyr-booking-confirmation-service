package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
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
    void tick_shouldRequeueFailedRows_forOwnedPartitions() {
        var ownedPartitions = new OwnedPartitions();
        ownedPartitions.add(Region.AMER, 0);
        var service = new DefaultBookingRequeueService(Region.AMER, ownedPartitions, 5, outboxRepository);

        service.tick();

        verify(outboxRepository).requeueFailed(eq(Region.AMER), any(), eq(5));
    }

    @Test
    void tick_shouldSkip_whenNoOwnedPartitions() {
        var service = new DefaultBookingRequeueService(Region.AMER, new OwnedPartitions(), 5, outboxRepository);

        service.tick();

        verifyNoInteractions(outboxRepository);
    }
}
