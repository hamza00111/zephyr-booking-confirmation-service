package com.bnpparibas.dec.bookingconfirmation.application.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
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

        service.tick();

        // Full-snapshot amend becomes a CREATE; gate goes IN_FLIGHT; self-healing, no human.
        verify(outboxRepository).insertAll(argThat(events -> events.size() == 1
                && events.get(0).eventType() == TradeEventType.CREATED
                && "RETYPED_CREATE".equals(events.get(0).payload())));
        verify(tradeGateRepository).upsert(Region.AMER, "K1", CreateState.IN_FLIGHT);
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void barrier_shouldEmitAmend_whenCreateAlreadySent() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.AMENDED);
        gateState(CreateState.SENT);

        service.tick();

        verify(outboxRepository).insertAll(argThat(events ->
                events.size() == 1 && events.get(0).eventType() == TradeEventType.AMENDED));
        verify(inboxRepository).markProcessed(Region.AMER, List.of(1L));
    }

    @Test
    void barrier_shouldVoidTradeAndCancelUnsentCreate_whenBustedBeforeDelivery() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.BUSTED);
        gateState(CreateState.IN_FLIGHT);

        service.tick();

        // Born and killed before downstream saw it: cancel the staged CREATE, void the trade, emit nothing.
        verify(outboxRepository).parkUnsentForKey(Region.AMER, "K1", "Trade busted before CREATE delivered");
        verify(tradeGateRepository).upsert(Region.AMER, "K1", CreateState.VOID);
        verify(outboxRepository).insertAll(List.of());
    }

    @Test
    void barrier_shouldRetypeCreateToAmend_whenTradeAlreadySent() {
        var service = processService();
        lockAcquired();
        drained(TradeEventType.CREATED);
        gateState(CreateState.SENT);
        given(tradeEventCodec.rewriteType(PAYLOAD, TradeEventType.AMENDED)).willReturn("RETYPED_AMEND");

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
                (region, payload) -> payload, (region, payload) -> true,
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
