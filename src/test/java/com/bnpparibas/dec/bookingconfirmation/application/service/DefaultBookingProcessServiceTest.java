package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.CreateState;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.TradeGateRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.TradeEventAggregator;
import com.bnpparibas.dec.zephyr.domain.trade.events.TradeEvent;
import java.time.Duration;
import java.util.List;
import java.util.Map;
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

    private static final InstanceId INSTANCE = new InstanceId("test-instance");
    private static final String PAYLOAD = "{\"eventType\":\"CREATED\",\"traceId\":\"trace-1\"}";
    private static final TradeEvent TRADE_EVENT = mock(TradeEvent.class);

    @Mock
    private InboxRepository inboxRepository;

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private TradeGateRepository tradeGateRepository;

    @Mock
    private TradeEventCodec tradeEventCodec;

    @Mock
    private DistributedLockRepository lockRepository;

    @Test
    void tick_shouldStageOutboxEventCarryingTraceId_andMarkProcessed_whenSingleCreated() {
        var service = processService();
        lockAcquired();
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeEventCodec.deserialize(PAYLOAD)).willReturn(Optional.of(TRADE_EVENT));
        given(tradeGateRepository.statesFor(eq(Region.AMER), any())).willReturn(Map.of());

        service.tick();

        verify(outboxRepository).insertAll(argThat(events -> events.size() == 1
                && events.get(0).inboxId() == 1L
                && events.get(0).eventType() == TradeEventType.CREATED
                && "trace-1".equals(events.get(0).traceId())));
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
        verify(tradeGateRepository).upsert(Region.AMER, "K1", CreateState.IN_FLIGHT);
    }

    @Test
    void tick_shouldMarkInvalid_whenEventTypeUnreadable() {
        var service = processService();
        lockAcquired();
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.empty());

        service.tick();

        verify(inboxRepository).markInvalid(Region.AMER, List.of(1L), "No readable event type in payload");
        verify(inboxRepository).markProcessed(Region.AMER, List.of());
    }

    @Test
    void tick_shouldMarkInvalid_whenPayloadFailsToDeserialize() {
        var service = processService();
        lockAcquired();
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.CREATED));
        given(tradeGateRepository.statesFor(eq(Region.AMER), any())).willReturn(Map.of());
        given(tradeEventCodec.deserialize(PAYLOAD)).willReturn(Optional.empty());

        service.tick();

        // The type parsed, but the body did not bind to a TradeEvent — INVALID, never published.
        verify(inboxRepository).markInvalid(Region.AMER, List.of(1L), "TradeEvent payload failed to deserialize");
        verify(outboxRepository).insertAll(List.of());
        verify(tradeGateRepository, never()).upsert(any(), any(), any());
    }

    @Test
    void tick_shouldDoNothing_whenInboxEmpty() {
        var service = processService();
        lockAcquired();
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of());

        service.tick();

        verify(outboxRepository, never()).insertAll(any());
        verify(inboxRepository, never()).markProcessed(any(), any());
    }

    // ---- Create-barrier (Scenario 4) ----------------------------------------------------------

    @Test
    void barrier_shouldBlockAmend_whenCreateInFlight() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.AMENDED);
        gateState(CreateState.IN_FLIGHT);

        service.tick();

        // AMEND held until the CREATE is delivered — nothing published, row marked BLOCKED.
        verify(outboxRepository).insertAll(List.of());
        verify(inboxRepository).markBlocked(Region.AMER, List.of(1L));
        verify(inboxRepository).markProcessed(Region.AMER, List.of());
    }

    @Test
    void barrier_shouldAutoPromoteAmendToCreate_whenNoCreateSeen() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.AMENDED);
        gateNone();
        given(tradeEventCodec.rewriteType(PAYLOAD, TradeEventType.CREATED)).willReturn("RETYPED_CREATE");
        given(tradeEventCodec.deserialize("RETYPED_CREATE")).willReturn(Optional.of(TRADE_EVENT));

        service.tick();

        // Full-snapshot amend becomes a CREATE; gate goes IN_FLIGHT; self-healing, no human.
        verify(outboxRepository).insertAll(argThat(events -> events.size() == 1
                && events.get(0).eventType() == TradeEventType.CREATED
                && "RETYPED_CREATE".equals(events.get(0).payload())));
        verify(tradeGateRepository).upsert(Region.AMER, "K1", CreateState.IN_FLIGHT);
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void barrier_shouldAutoPromoteAmendToCreate_whenPriorCreateFailed() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.AMENDED);
        gateState(CreateState.FAILED);
        given(tradeEventCodec.rewriteType(PAYLOAD, TradeEventType.CREATED)).willReturn("RETYPED_CREATE");
        given(tradeEventCodec.deserialize("RETYPED_CREATE")).willReturn(Optional.of(TRADE_EVENT));

        service.tick();

        // Prior CREATE failed terminally; the full-snapshot amend is promoted into the CREATE in its place.
        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && events.get(0).eventType() == TradeEventType.CREATED));
        verify(tradeGateRepository).upsert(Region.AMER, "K1", CreateState.IN_FLIGHT);
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void barrier_shouldEmitAmend_whenCreateAlreadySent() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.AMENDED);
        gateState(CreateState.SENT);
        given(tradeEventCodec.deserialize(PAYLOAD)).willReturn(Optional.of(TRADE_EVENT));

        service.tick();

        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && events.get(0).eventType() == TradeEventType.AMENDED));
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void barrier_shouldVoidTradeAndCancelUnsentCreate_whenDeletedBeforeDelivery() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.DELETED);
        gateState(CreateState.IN_FLIGHT);
        given(outboxRepository.parkUnsentForKey(Region.AMER, "K1", "Trade deleted before CREATE delivered"))
                .willReturn(1);

        service.tick();

        // Cancelled the staged CREATE before delivery — void the trade, emit nothing.
        verify(tradeGateRepository).upsert(Region.AMER, "K1", CreateState.VOID);
        verify(outboxRepository).insertAll(List.of());
    }

    @Test
    void barrier_shouldEmitDelete_whenCreateWonTheRaceDuringDelete() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.DELETED);
        // Gate looks IN_FLIGHT at tick start, but RELAY delivers the CREATE concurrently: the park finds
        // nothing to cancel (0) and the re-read reveals SENT — the delete must still be published.
        given(tradeGateRepository.statesFor(eq(Region.AMER), any()))
                .willReturn(Map.of("K1", CreateState.IN_FLIGHT), Map.of("K1", CreateState.SENT));
        given(outboxRepository.parkUnsentForKey(Region.AMER, "K1", "Trade deleted before CREATE delivered"))
                .willReturn(0);
        given(tradeEventCodec.deserialize(PAYLOAD)).willReturn(Optional.of(TRADE_EVENT));

        service.tick();

        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && events.get(0).eventType() == TradeEventType.DELETED));
        verify(tradeGateRepository).upsert(Region.AMER, "K1", CreateState.VOID);
    }

    @Test
    void barrier_shouldRetypeCreateToAmend_whenTradeAlreadySent() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.CREATED);
        gateState(CreateState.SENT);
        given(tradeEventCodec.rewriteType(PAYLOAD, TradeEventType.AMENDED)).willReturn("RETYPED_AMEND");
        given(tradeEventCodec.deserialize("RETYPED_AMEND")).willReturn(Optional.of(TRADE_EVENT));

        service.tick();

        // A second CREATE for a trade downstream already knows about is really an amendment.
        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && events.get(0).eventType() == TradeEventType.AMENDED));
    }

    private void drained(TradeEventType type) {
        given(inboxRepository.findNew(Region.AMER, 200)).willReturn(List.of(inbox(1L, "K1")));
        given(tradeEventCodec.eventType(PAYLOAD)).willReturn(Optional.of(type));
    }

    private void gateState(CreateState state) {
        given(tradeGateRepository.statesFor(eq(Region.AMER), any())).willReturn(Map.of("K1", state));
    }

    private void gateNone() {
        given(tradeGateRepository.statesFor(eq(Region.AMER), any())).willReturn(Map.of());
    }

    private DefaultBookingProcessService processService() {
        return new DefaultBookingProcessService(
                Region.AMER, 1000, 200, 1, "published",
                inboxRepository, outboxRepository, tradeGateRepository, tradeEventCodec, new TradeEventAggregator(),
                (region, tradeEvent) -> true,
                transactionTemplate(), lockRepository, INSTANCE);
    }

    private void lockAcquired() {
        given(lockRepository.acquireOrRefresh(
                        eq(Region.AMER), eq(ProcessType.PROCESS), eq(INSTANCE), eq(Duration.ofMillis(1000))))
                .willReturn(true);
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
