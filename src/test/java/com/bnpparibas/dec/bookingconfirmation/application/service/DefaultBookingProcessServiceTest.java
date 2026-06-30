package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.TradeEventAggregator;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.SimpleTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

@ExtendWith(MockitoExtension.class)
class DefaultBookingProcessServiceTest {

    private static final String PAYLOAD = "{\"eventType\":\"CREATED\",\"traceId\":\"trace-1\"}";

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private TradeEventCodec tradeEventCodec;

    @Test
    void tick_shouldStageOutboxEventCarryingTraceId_andMarkProcessed_whenSingleCreated() {
        var service = processService();
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));

        service.tick();

        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && events.get(0).inboxId() == 1L && "trace-1".equals(events.get(0).traceId())));
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void tick_shouldMarkInvalid_whenEventTypeUnreadable() {
        var service = processService();
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.empty());

        service.tick();

        verify(inboxRepository).markInvalid(Region.AMER, List.of(1L), "No readable event type in payload");
        verify(inboxRepository).markProcessed(Region.AMER, List.of());
    }

    @Test
    void tick_shouldDoNothing_whenInboxEmpty() {
        var service = processService();
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of());

        service.tick();

        verify(outboxRepository, never()).insertAll(any());
        verify(inboxRepository, never()).markProcessed(any(), any());
    }

    private DefaultBookingProcessService processService() {
        return new DefaultBookingProcessService(
                Region.AMER, 200, "published",
                inboxRepository, outboxRepository, tradeEventCodec, new TradeEventAggregator(),
                (region, payload) -> payload, (region, payload) -> true,
                transactionTemplate());
    }

    private static InboxMessage inbox(long id, String messageKey) {
        return new InboxMessage(id, Region.AMER, "idem-" + id, "topic", 0, id, messageKey, PAYLOAD, "trace-1", null);
    }

    private static TransactionTemplate transactionTemplate() {
        return new TransactionTemplate(new PlatformTransactionManager() {
            @Override
            public TransactionStatus getTransaction(TransactionDefinition definition) {
                return new SimpleTransactionStatus();
            }

            @Override
            public void commit(TransactionStatus status) {}

            @Override
            public void rollback(TransactionStatus status) {}
        });
    }
}
