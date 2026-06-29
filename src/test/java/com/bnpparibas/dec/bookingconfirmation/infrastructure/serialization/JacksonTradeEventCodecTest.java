package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.json.JsonMapper;

class JacksonTradeEventCodecTest {

    private final JacksonTradeEventCodec codec = new JacksonTradeEventCodec(JsonMapper.builder().build());

    @ParameterizedTest
    @MethodSource("typedPayloads")
    void eventType_shouldResolveType_fromEventTypeOrAtTypeFallback(String payload, TradeEventType expected) {
        assertThat(codec.eventType(payload)).contains(expected);
    }

    static Stream<Arguments> typedPayloads() {
        return Stream.of(
                Arguments.of("{\"eventType\":\"CREATED\"}", TradeEventType.CREATED),
                Arguments.of("{\"eventType\":\"AMENDED\"}", TradeEventType.AMENDED),
                Arguments.of("{\"eventType\":\"DELETED\"}", TradeEventType.DELETED),
                Arguments.of("{\"eventType\":\"created\"}", TradeEventType.CREATED),       // case-insensitive
                Arguments.of("{\"@type\":\"TRADE_AMENDED\"}", TradeEventType.AMENDED),      // @type fallback
                Arguments.of("{\"@type\":\"TRADE_DELETED\",\"eventId\":\"x\"}", TradeEventType.DELETED));
    }

    @ParameterizedTest
    @ValueSource(strings = {"not json at all", "{\"eventType\":\"NUKED\"}", "{\"eventId\":\"x\"}", "{}"})
    void eventType_shouldBeEmpty_whenTypeMissingOrUnreadable(String payload) {
        assertThat(codec.eventType(payload)).isEmpty();
    }

    @Test
    void traceId_shouldExtractTraceIdField_whenPresent() {
        assertThat(codec.traceId("{\"traceId\":\"50fc0ac5-6b9c\",\"eventType\":\"CREATED\"}"))
                .contains("50fc0ac5-6b9c");
    }

    @Test
    void traceId_shouldBeEmpty_whenFieldAbsent() {
        assertThat(codec.traceId("{\"eventType\":\"CREATED\"}")).isEmpty();
    }

    @Test
    void traceId_shouldBeEmpty_whenPayloadUnparseable() {
        assertThat(codec.traceId("}{ broken")).isEmpty();
    }

    @Test
    void rewriteType_shouldRewriteBothDiscriminators_andPreserveOtherFields() {
        String amended = "{\"@type\":\"TRADE_AMENDED\",\"eventType\":\"AMENDED\",\"traceId\":\"t-1\",\"seq\":7}";

        String rewritten = codec.rewriteType(amended, TradeEventType.CREATED);

        assertThat(codec.eventType(rewritten)).contains(TradeEventType.CREATED);
        assertThat(rewritten)
                .contains("\"@type\":\"TRADE_CREATED\"")
                .contains("\"eventType\":\"CREATED\"")
                .contains("\"traceId\":\"t-1\"")
                .contains("\"seq\":7");
    }
}
