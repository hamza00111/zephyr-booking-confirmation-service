package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultBookingRequeueServiceTest {

    private static final InstanceId INSTANCE = new InstanceId("test-instance");

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private DistributedLockRepository lockRepository;

    @Test
    void tick_shouldRequeueAndPurge_whenLockAcquired() {
        var service = new DefaultBookingRequeueService(
                Region.AMER, 1000, 5, 7, 1, inboxRepository, outboxRepository, lockRepository, INSTANCE);
        given(lockRepository.acquireOrRefresh(
                        eq(Region.AMER), eq(ProcessType.REQUEUE), eq(INSTANCE), eq(Duration.ofMillis(1000))))
                .willReturn(true);

        service.tick();

        verify(outboxRepository).requeueReady(Region.AMER);
        verify(inboxRepository).requeueFailed(Region.AMER, 5);
        verify(outboxRepository).purgeSent(Region.AMER, 7);
        verify(inboxRepository).purgeProcessed(Region.AMER, 7);
    }

    @Test
    void tick_shouldSkip_whenLockHeldByPeer() {
        var service = new DefaultBookingRequeueService(
                Region.AMER, 1000, 5, 7, 1, inboxRepository, outboxRepository, lockRepository, INSTANCE);
        given(lockRepository.acquireOrRefresh(
                        eq(Region.AMER), eq(ProcessType.REQUEUE), eq(INSTANCE), eq(Duration.ofMillis(1000))))
                .willReturn(false);

        service.tick();

        verifyNoInteractions(outboxRepository);
        verifyNoInteractions(inboxRepository);
    }
}
