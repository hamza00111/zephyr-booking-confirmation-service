package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeEnricher;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeFilter;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventBindingException;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.EventChangeType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeCreatedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
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

    private static final String PAYLOAD = "{\"eventType\":\"TRADE_CREATED\",\"traceId\":\"trace-1\"}";
    private static final String BUSTED_PAYLOAD = "{\"eventType\":\"TRADE_DELETED\",\"traceId\":\"trace-1\"}";
    private static final TradeEvent EVENT = typedEvent();
    private static final String SERIALIZED = "{\"eventType\":\"TRADE_CREATED\",\"roundTripped\":true}";

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private TradeEventCodec tradeEventCodec;

    private final OwnedPartitions ownedPartitions = new OwnedPartitions();

    @Test
    void tick_shouldStageRoundTrippedOutboxEventCarryingTraceIdAndPartition_whenSingleCreated() {
        var service = processService();
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeEventCodec.deserialize(PAYLOAD)).willReturn(EVENT);
        given(tradeEventCodec.serialize(EVENT)).willReturn(SERIALIZED);

        service.tick();

        // kafkaPartition must be carried through — a NULL-partition outbox row is invisible to the
        // partition-scoped relay drain and would sit NEW forever (ADR 0001).
        verify(outboxRepository).insertAll(argThat(events -> events.size() == 1
                && events.get(0).inboxId() == 1L
                && "trace-1".equals(events.get(0).traceId())
                && Integer.valueOf(0).equals(events.get(0).kafkaPartition())
                && SERIALIZED.equals(events.get(0).payload())));
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
    void tick_shouldMarkWholeGroupProcessFailure_whenTransformFails() {
        var service = processService(
                (region, event) -> {
                    throw new IllegalStateException("enrich boom");
                },
                (region, event) -> true);
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200)))
                .willReturn(List.of(inbox(1L, "K1"), inbox(2L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeEventCodec.deserialize(PAYLOAD)).willReturn(EVENT);

        service.tick();

        // Survivor AND its collapsed sibling fail together so a retry re-aggregates the whole group.
        verify(inboxRepository)
                .markProcessFailure(Region.AMER, List.of(2L, 1L), "Transform failed in PROCESS stage");
        verify(inboxRepository).markAggregated(Region.AMER, List.of());
        verify(outboxRepository).insertAll(List.of());
    }

    @Test
    void tick_shouldMarkWholeGroupProcessFailure_whenTypedBindingFails() {
        var service = processService();
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200)))
                .willReturn(List.of(inbox(1L, "K1"), inbox(2L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeEventCodec.deserialize(PAYLOAD))
                .willThrow(new TradeEventBindingException("envelope field has wrong shape"));

        service.tick();

        verify(inboxRepository)
                .markProcessFailure(Region.AMER, List.of(2L, 1L), "Transform failed in PROCESS stage");
        verify(outboxRepository).insertAll(List.of());
    }

    @Test
    void tick_shouldMarkProcessedWithoutOutboxRow_whenFilterDropsSurvivor() {
        var service = processService((region, event) -> event, (region, event) -> false);
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeEventCodec.deserialize(PAYLOAD)).willReturn(EVENT);

        service.tick();

        verify(outboxRepository).insertAll(List.of());
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
        verify(outboxRepository).insertAll(List.of());
    }

    @Test
    void tick_shouldRewriteThenBindAndSerialize_whenSurvivorTypeDiffersFromEmittedType() {
        var service = processService();
        var created = inbox(1L, "K1", PAYLOAD);
        var amended = inbox(2L, "K1", "{\"eventType\":\"TRADE_AMENDED\"}");
        given(inboxRepository.findNew(eq(Region.AMER), any(), eq(200))).willReturn(List.of(created, amended));
        given(tradeEventCodec.eventType(created.rawPayload())).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeEventCodec.eventType(amended.rawPayload())).willReturn(Optional.of(TradeEventType.AMENDED));
        given(tradeEventCodec.rewriteType(amended.rawPayload(), TradeEventType.CREATED)).willReturn("{rewritten}");
        given(tradeEventCodec.deserialize("{rewritten}")).willReturn(EVENT);
        given(tradeEventCodec.serialize(EVENT)).willReturn(SERIALIZED);

        service.tick();

        // CREATED present in the group → the surviving AMENDED payload is emitted as CREATED.
        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && SERIALIZED.equals(events.get(0).payload())));
        verify(inboxRepository).markProcessed(Region.AMER, List.of(2L));
        verify(inboxRepository).markAggregated(Region.AMER, List.of(1L));
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
    void tick_shouldSkip_whenNoOwnedPartitions() {
        var service = new DefaultBookingProcessService(
                Region.AMER, new OwnedPartitions(), 200, "published",
                inboxRepository, outboxRepository, tradeEventCodec, new TradeEventAggregator(),
                (region, event) -> event, (region, event) -> true,
                transactionTemplate(), BookingConfirmationMetrics.noop());

        service.tick();

        verifyNoInteractions(inboxRepository, outboxRepository);
    }

    private DefaultBookingProcessService processService() {
        return processService((region, event) -> event, (region, event) -> true);
    }

    private DefaultBookingProcessService processService(final TradeEnricher enricher, final TradeFilter filter) {
        ownedPartitions.add(Region.AMER, 0);
        return new DefaultBookingProcessService(
                Region.AMER, ownedPartitions, 200, "published",
                inboxRepository, outboxRepository, tradeEventCodec, new TradeEventAggregator(),
                enricher, filter,
                transactionTemplate(), BookingConfirmationMetrics.noop());
    }

    private static InboxMessage inbox(long id, String messageKey) {
        return inbox(id, messageKey, PAYLOAD);
    }

    private static InboxMessage inbox(long id, String messageKey, String payload) {
        return new InboxMessage(id, Region.AMER, "idem-" + id, "topic", 0, id, messageKey, payload, "trace-1", null);
    }

    private static TradeEvent typedEvent() {
        return new TradeCreatedEvent(
                null, EventChangeType.TRADE_CREATED, null, null, null, null, null, null, null, null, null, null);
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
