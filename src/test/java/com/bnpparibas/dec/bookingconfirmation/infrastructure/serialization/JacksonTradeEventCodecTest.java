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

    /** Transcribed from a real TradeCreated event on the internal topic (ids shortened). */
    private static final String CREATED_ENVELOPE =
            """
            {
              "version": "1.0",
              "eventType": "TradeCreated",
              "eventId": "d0e41e27-6c8f-495e-aa70-42334ff320ad",
              "pivotId": {
                "id": "444071456_1"
              },
              "traceId": "075944c1-c852-4e57-be76-c1fc9bbc5036",
              "externalSystem": "UBIX",
              "flowDirection": "INBOUND",
              "hub": "US",
              "occurredAt": "2026-07-02T16:09:59.839430556Z",
              "recordedAt": "2026-07-02T16:09:59.839441669Z",
              "auditInfo": {
                "initiator": "Booking confirmation publisher",
                "reason": "Automated booking confirmation publishing workflow",
                "comment": "Zephyr booking confirmation publisher service stream"
              },
              "payload": {
                "externalSystemTradeId": {"id": "444071456_1"},
                "externalSystem": {"name": "UBIX", "description": "Ubix back-office"},
                "references": {
                  "externalSystemTradeId": "444071456_1",
                  "originalTradeId": null,
                  "secondaryTradeId": null,
                  "previousTradeId": null,
                  "universalTradeId": null,
                  "basketId": null,
                  "orderId": "G182559711",
                  "reportTrackingNumber": null
                },
                "tradeUpdateDateTime": "2026-07-01T09:01:39",
                "matchingStatus": "MATCHED",
                "clearingStatus": "CLEARED",
                "notional": 1000000.1234567890123456789
              }
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
        assertThat(event.eventId()).isEqualTo(UUID.fromString("d0e41e27-6c8f-495e-aa70-42334ff320ad"));
        assertThat(event.flowDirection()).isEqualTo(FlowDirection.INBOUND);
        assertThat(event.externalSystem()).isEqualTo("UBIX");
        assertThat(event.hub()).isEqualTo("US");
        // Nulls inside the opaque payload survive untouched.
        assertThat(event.payload().get("references").get("originalTradeId").isNull()).isTrue();
        // Nanosecond precision from the real producer must survive the Instant round-trip.
        assertThat(event.occurredAt()).isEqualTo(Instant.parse("2026-07-02T16:09:59.839430556Z"));
        assertThat(event.auditInfo().initiator()).isEqualTo("Booking confirmation publisher");
        assertThat(event.pivotId().get("id").asText()).isEqualTo("444071456_1");
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
