package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventBindingException;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.FlowDirection;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeAmendedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeCreatedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeDeletedEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class JacksonTradeEventCodecTest {

    private static final String CREATED_ENVELOPE =
            """
            {
              "version": "1.0",
              "eventType": "TradeCreated",
              "eventId": "0e37ee11-93ad-4f60-9be2-2e9a95e35771",
              "pivotId": {"value": "TR-1"},
              "traceId": "50fc0ac5-1111-2222-3333-6b9c00000000",
              "externalSystem": "IRIS",
              "flowDirection": "INBOUND",
              "hub": "NY",
              "occurredAt": "2026-07-01T10:15:30Z",
              "recordedAt": "2026-07-01T10:15:31Z",
              "auditInfo": {"initiator": "N/A", "reason": "N/A", "comment": ""},
              "payload": {"tradeRef": "TR-1", "notional": 1000000.1234567890123456789, "legs": [{"id": 1}]}
            }
            """;

    private final JacksonTradeEventCodec codec =
            new JacksonTradeEventCodec(TradeEventJacksonConfig.buildTradeEventMapper());

    @ParameterizedTest
    @MethodSource("typedPayloads")
    void eventType_shouldResolveWireVocabulary(String payload, TradeEventType expected) {
        assertThat(codec.eventType(payload)).contains(expected);
    }

    static Stream<Arguments> typedPayloads() {
        return Stream.of(
                Arguments.of("{\"eventType\":\"TradeCreated\"}", TradeEventType.TRADE_CREATED),
                Arguments.of("{\"eventType\":\"TradeAmended\"}", TradeEventType.TRADE_AMENDED),
                Arguments.of("{\"eventType\":\"TradeDeleted\"}", TradeEventType.TRADE_DELETED),
                Arguments.of("{\"eventType\":\" TradeCreated \"}", TradeEventType.TRADE_CREATED)); // trimmed
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "not json at all",
                "{\"eventType\":\"NUKED\"}",
                "{\"eventId\":\"x\"}",
                "{}",
                // Retired vocabularies are no longer accepted.
                "{\"eventType\":\"CREATED\"}",
                "{\"eventType\":\"TRADE_CREATED\"}",
                "{\"@type\":\"TRADE_AMENDED\"}"
            })
    void eventType_shouldBeEmpty_whenTypeMissingOrUnreadable(String payload) {
        assertThat(codec.eventType(payload)).isEmpty();
    }

    @Test
    void traceId_shouldExtractTraceIdField_whenPresent() {
        assertThat(codec.traceId("{\"traceId\":\"50fc0ac5-6b9c\",\"eventType\":\"TradeCreated\"}"))
                .contains("50fc0ac5-6b9c");
    }

    @Test
    void traceId_shouldBeEmpty_whenFieldAbsent() {
        assertThat(codec.traceId("{\"eventType\":\"TradeCreated\"}")).isEmpty();
    }

    @Test
    void traceId_shouldBeEmpty_whenPayloadUnparseable() {
        assertThat(codec.traceId("}{ broken")).isEmpty();
    }

    @Test
    void rewriteType_shouldEmitWireVocabulary_andPreserveOtherFields() {
        String amended = "{\"eventType\":\"TradeAmended\",\"traceId\":\"t-1\",\"seq\":7}";

        String rewritten = codec.rewriteType(amended, TradeEventType.TRADE_CREATED);

        assertThat(codec.eventType(rewritten)).contains(TradeEventType.TRADE_CREATED);
        assertThat(rewritten)
                .contains("\"eventType\":\"TradeCreated\"")
                .contains("\"traceId\":\"t-1\"")
                .contains("\"seq\":7");
    }

    @Test
    void deserialize_shouldBindEnvelope_andKeepPayloadLossless() {
        TradeEvent event = codec.deserialize(CREATED_ENVELOPE);

        assertThat(event).isInstanceOf(TradeCreatedEvent.class);
        assertThat(event.eventType()).isEqualTo(TradeEventType.TRADE_CREATED);
        assertThat(event.eventId()).isEqualTo(UUID.fromString("0e37ee11-93ad-4f60-9be2-2e9a95e35771"));
        assertThat(event.flowDirection()).isEqualTo(FlowDirection.INBOUND);
        assertThat(event.occurredAt()).isEqualTo(Instant.parse("2026-07-01T10:15:30Z"));
        assertThat(event.auditInfo().initiator()).isEqualTo("N/A");
        assertThat(event.pivotId().get("value").asText()).isEqualTo("TR-1");
        // Opaque payload: decimal precision must survive (BigDecimal, not double).
        assertThat(event.payload().get("notional").decimalValue())
                .isEqualByComparingTo(new BigDecimal("1000000.1234567890123456789"));
    }

    @ParameterizedTest
    @MethodSource("subtypeBindings")
    void deserialize_shouldPickConcreteSubtype_fromDiscriminator(String discriminator, Class<?> expected) {
        TradeEvent event = codec.deserialize("{\"eventType\":\"" + discriminator + "\",\"hub\":\"NY\"}");

        assertThat(event).isInstanceOf(expected);
    }

    static Stream<Arguments> subtypeBindings() {
        return Stream.of(
                Arguments.of("TradeCreated", TradeCreatedEvent.class),
                Arguments.of("TradeAmended", TradeAmendedEvent.class),
                Arguments.of("TradeDeleted", TradeDeletedEvent.class));
    }

    @Test
    void serialize_shouldRoundTripLosslessly_withWireDiscriminator() {
        TradeEvent event = codec.deserialize(CREATED_ENVELOPE);

        String json = codec.serialize(event);

        assertThat(json).contains("\"eventType\":\"TradeCreated\"");
        TradeEvent reread = codec.deserialize(json);
        assertThat(reread).isEqualTo(event);
    }

    @Test
    void deserialize_shouldThrowBindingException_whenPayloadNotJson() {
        assertThatThrownBy(() -> codec.deserialize("not json"))
                .isInstanceOf(TradeEventBindingException.class);
    }

    @Test
    void deserialize_shouldThrowBindingException_whenTypeUnknown() {
        assertThatThrownBy(() -> codec.deserialize("{\"eventType\":\"NUKED\"}"))
                .isInstanceOf(TradeEventBindingException.class);
    }

    @Test
    void deserialize_shouldThrowBindingException_whenEnvelopeFieldHasWrongShape() {
        assertThatThrownBy(() -> codec.deserialize("{\"eventType\":\"TradeCreated\",\"eventId\":\"not a uuid\"}"))
                .isInstanceOf(TradeEventBindingException.class);
    }
}
