package com.bnpparibas.dec.bookingconfirmation.application.transform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeCreatedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class TradeEventTransformerTest {

    private static final String PAYLOAD = "{\"eventType\":\"TradeCreated\"}";
    private static final TradeEvent EVENT = new TradeCreatedEvent(
            null, TradeEventType.TRADE_CREATED, null, null, null, null, null, null, null, null, null, null);
    private static final InboxMessage MESSAGE =
            new InboxMessage(7L, Region.AMER, "idem-7", "internal.amer", 4, 42L, "K1", PAYLOAD, "trace-7", "h=1");

    @Mock
    private TradeEventCodec codec;

    @Test
    void toOutboxEvent_shouldCarryEveryMessageAttribute_intoTheOutboxRow() {
        given(codec.deserialize(PAYLOAD, TradeEventType.TRADE_CREATED)).willReturn(EVENT);
        given(codec.serialize(EVENT)).willReturn("{serialized}");
        var transformer = transformer((region, event) -> event, (region, event) -> true);

        Optional<OutboxEvent> outbox = transformer.toOutboxEvent(MESSAGE, TradeEventType.TRADE_CREATED);

        assertThat(outbox).hasValueSatisfying(event -> {
            assertThat(event.region()).isEqualTo(Region.AMER);
            assertThat(event.idempotencyKey()).isEqualTo("idem-7");
            assertThat(event.destination()).isEqualTo("published.amer");
            assertThat(event.messageKey()).isEqualTo("K1");
            assertThat(event.kafkaPartition()).isEqualTo(4);
            assertThat(event.payload()).isEqualTo("{serialized}");
            assertThat(event.inboxId()).isEqualTo(7L);
            assertThat(event.traceId()).isEqualTo("trace-7");
            assertThat(event.headers()).isEqualTo("h=1");
        });
    }

    @Test
    void toOutboxEvent_shouldSerializeTheEnrichedEvent_notTheOriginal() {
        var enriched = new TradeCreatedEvent(
                "2.0", TradeEventType.TRADE_CREATED, null, null, null, null, null, null, null, null, null, null);
        given(codec.deserialize(PAYLOAD, TradeEventType.TRADE_CREATED)).willReturn(EVENT);
        given(codec.serialize(enriched)).willReturn("{enriched}");
        var transformer = transformer((region, event) -> enriched, (region, event) -> true);

        Optional<OutboxEvent> outbox = transformer.toOutboxEvent(MESSAGE, TradeEventType.TRADE_CREATED);

        assertThat(outbox).hasValueSatisfying(event -> assertThat(event.payload()).isEqualTo("{enriched}"));
    }

    @Test
    void toOutboxEvent_shouldReturnEmpty_whenFilterDropsTheEvent() {
        given(codec.deserialize(PAYLOAD, TradeEventType.TRADE_CREATED)).willReturn(EVENT);
        var transformer = transformer((region, event) -> event, (region, event) -> false);

        assertThat(transformer.toOutboxEvent(MESSAGE, TradeEventType.TRADE_CREATED)).isEmpty();
    }

    @Test
    void eventType_shouldDelegateToTheCodec() {
        given(codec.eventType(PAYLOAD)).willReturn(Optional.of(TradeEventType.TRADE_CREATED));
        var transformer = transformer((region, event) -> event, (region, event) -> true);

        assertThat(transformer.eventType(PAYLOAD)).contains(TradeEventType.TRADE_CREATED);
    }

    private TradeEventTransformer transformer(final TradeEnricher enricher, final TradeFilter filter) {
        return new TradeEventTransformer(Region.AMER, "published.amer", codec, enricher, filter);
    }
}
