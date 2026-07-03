package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventBindingException;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.EventChangeType;
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
              "eventType": "TRADE_CREATED",
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
    void eventType_shouldResolveType_acrossAllVocabularies(String payload, TradeEventType expected) {
        assertThat(codec.eventType(payload)).contains(expected);
    }

    static Stream<Arguments> typedPayloads() {
        return Stream.of(
                // Current wire vocabulary (mixin subtype names).
                Arguments.of("{\"eventType\":\"TRADE_CREATED\"}", TradeEventType.CREATED),
                Arguments.of("{\"eventType\":\"TRADE_AMENDED\"}", TradeEventType.AMENDED),
                Arguments.of("{\"eventType\":\"TRADE_DELETED\"}", TradeEventType.BUSTED),
                // Legacy bare names.
                Arguments.of("{\"eventType\":\"CREATED\"}", TradeEventType.CREATED),
                Arguments.of("{\"eventType\":\"AMENDED\"}", TradeEventType.AMENDED),
                Arguments.of("{\"eventType\":\"BUSTED\"}", TradeEventType.BUSTED),
                Arguments.of("{\"eventType\":\"created\"}", TradeEventType.CREATED),       // case-insensitive
                // Camel forms from earlier mixin revisions.
                Arguments.of("{\"eventType\":\"TradeCreated\"}", TradeEventType.CREATED),
                Arguments.of("{\"eventType\":\"TradeDeleted\"}", TradeEventType.BUSTED),
                // Legacy @type fallback.
                Arguments.of("{\"@type\":\"TRADE_AMENDED\"}", TradeEventType.AMENDED),
                Arguments.of("{\"@type\":\"TRADE_BUSTED\",\"eventId\":\"x\"}", TradeEventType.BUSTED));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not json at all", "{\"eventType\":\"NUKED\"}", "{\"eventId\":\"x\"}", "{}"})
    void eventType_shouldBeEmpty_whenTypeMissingOrUnreadable(String payload) {
        assertThat(codec.eventType(payload)).isEmpty();
    }

    @Test
    void traceId_shouldExtractTraceIdField_whenPresent() {
        assertThat(codec.traceId("{\"traceId\":\"50fc0ac5-6b9c\",\"eventType\":\"TRADE_CREATED\"}"))
                .contains("50fc0ac5-6b9c");
    }

    @Test
    void traceId_shouldBeEmpty_whenFieldAbsent() {
        assertThat(codec.traceId("{\"eventType\":\"TRADE_CREATED\"}")).isEmpty();
    }

    @Test
    void traceId_shouldBeEmpty_whenPayloadUnparseable() {
        assertThat(codec.traceId("}{ broken")).isEmpty();
    }

    @Test
    void rewriteType_shouldEmitWireVocabulary_andPreserveOtherFields() {
        String amended = "{\"@type\":\"TRADE_AMENDED\",\"eventType\":\"TRADE_AMENDED\",\"traceId\":\"t-1\",\"seq\":7}";

        String rewritten = codec.rewriteType(amended, TradeEventType.CREATED);

        assertThat(codec.eventType(rewritten)).contains(TradeEventType.CREATED);
        assertThat(rewritten)
                .contains("\"eventType\":\"TRADE_CREATED\"")
                .contains("\"@type\":\"TRADE_CREATED\"")
                .contains("\"traceId\":\"t-1\"")
                .contains("\"seq\":7");
    }

    @Test
    void deserialize_shouldBindEnvelope_andKeepPayloadLossless() {
        TradeEvent event = codec.deserialize(CREATED_ENVELOPE);

        assertThat(event).isInstanceOf(TradeCreatedEvent.class);
        assertThat(event.eventType()).isEqualTo(EventChangeType.TRADE_CREATED);
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
                Arguments.of("TRADE_CREATED", TradeCreatedEvent.class),
                Arguments.of("TRADE_AMENDED", TradeAmendedEvent.class),
                Arguments.of("TRADE_DELETED", TradeDeletedEvent.class),
                // Legacy vocabulary binds too: the codec normalizes the discriminator before binding.
                Arguments.of("CREATED", TradeCreatedEvent.class),
                Arguments.of("BUSTED", TradeDeletedEvent.class));
    }

    @Test
    void serialize_shouldRoundTripLosslessly_withWireDiscriminator() {
        TradeEvent event = codec.deserialize(CREATED_ENVELOPE);

        String json = codec.serialize(event);

        assertThat(json).contains("\"eventType\":\"TRADE_CREATED\"");
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
        assertThatThrownBy(() -> codec.deserialize("{\"eventType\":\"TRADE_CREATED\",\"eventId\":\"not a uuid\"}"))
                .isInstanceOf(TradeEventBindingException.class);
    }
}
