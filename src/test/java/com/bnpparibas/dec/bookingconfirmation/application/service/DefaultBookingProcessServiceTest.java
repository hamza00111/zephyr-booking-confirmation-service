package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
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
    private static final String BUSTED_PAYLOAD = "{\"eventType\":\"BUSTED\",\"traceId\":\"trace-1\"}";

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private TradeEventCodec tradeEventCodec;

    private final OwnedPartitions ownedPartitions = new OwnedPartitions();

    @Test
    void tick_shouldStageOutboxEventCarryingTraceId_andMarkProcessed_whenSingleCreated() {
        var service = processService();
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));

        service.tick();

        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && events.get(0).inboxId() == 1L && "trace-1".equals(events.get(0).traceId())));
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void tick_shouldMarkInvalid_whenEventTypeUnreadable() {
        var service = processService();
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.empty());

        service.tick();

        verify(inboxRepository).markInvalid(Region.AMER, List.of(1L), "No readable event type in payload");
        verify(inboxRepository).markProcessed(Region.AMER, List.of());
    }

    @Test
    void tick_shouldDoNothing_whenInboxEmpty() {
        var service = processService();
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of());

        service.tick();

        verify(outboxRepository, never()).insertAll(any());
        verify(inboxRepository, never()).markProcessed(any(), any());
    }

    @Test
    void tick_shouldMarkProcessFailure_whenTransformThrows() {
        var owned = new OwnedPartitions();
        owned.add(Region.AMER, 0);
        var service = new DefaultBookingProcessService(
                Region.AMER, owned, 200, "published",
                inboxRepository, outboxRepository, tradeEventCodec, new TradeEventAggregator(),
                (region, payload) -> {
                    throw new RuntimeException("boom");
                },
                (region, payload) -> true,
                transactionTemplate());
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));

        service.tick();

        verify(inboxRepository).markProcessFailure(Region.AMER, List.of(1L), "Transform failed in PROCESS stage");
        verify(outboxRepository).insertAll(argThat(List::isEmpty));
    }

    @Test
    void tick_shouldProcessButNotPublish_whenFilterDropsTheEvent() {
        var owned = new OwnedPartitions();
        owned.add(Region.AMER, 0);
        var service = new DefaultBookingProcessService(
                Region.AMER, owned, 200, "published",
                inboxRepository, outboxRepository, tradeEventCodec, new TradeEventAggregator(),
                (region, payload) -> payload,
                (region, payload) -> false,
                transactionTemplate());
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));

        service.tick();

        verify(outboxRepository).insertAll(argThat(List::isEmpty));
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void tick_shouldMarkAggregated_whenCreateAndBustCollapse() {
        var service = processService();
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200)))
                .willReturn(List.of(inbox(1L, "K", PAYLOAD), inbox(2L, "K", BUSTED_PAYLOAD)));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeEventCodec.eventType(BUSTED_PAYLOAD)).willReturn(Optional.of(TradeEventType.BUSTED));

        service.tick();

        verify(inboxRepository)
                .markAggregated(eq(Region.AMER), argThat(ids -> ids.size() == 2 && ids.containsAll(List.of(1L, 2L))));
        verify(outboxRepository).insertAll(argThat(List::isEmpty));
    }

    @Test
    void tick_shouldSkip_whenNoOwnedPartitions() {
        var service = new DefaultBookingProcessService(
                Region.AMER, new OwnedPartitions(), 200, "published",
                inboxRepository, outboxRepository, tradeEventCodec, new TradeEventAggregator(),
                (region, payload) -> payload, (region, payload) -> true,
                transactionTemplate());

        service.tick();

        verifyNoInteractions(inboxRepository, outboxRepository);
    }

    private DefaultBookingProcessService processService() {
        ownedPartitions.add(Region.AMER, 0);
        return new DefaultBookingProcessService(
                Region.AMER, ownedPartitions, 200, "published",
                inboxRepository, outboxRepository, tradeEventCodec, new TradeEventAggregator(),
                (region, payload) -> payload, (region, payload) -> true,
                transactionTemplate());
    }

    private static InboxMessage inbox(long id, String messageKey) {
        return inbox(id, messageKey, PAYLOAD);
    }

    private static InboxMessage inbox(long id, String messageKey, String payload) {
        return new InboxMessage(id, Region.AMER, "idem-" + id, "topic", 0, id, messageKey, payload, "trace-1", null);
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
