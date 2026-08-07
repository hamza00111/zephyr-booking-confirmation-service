package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaHeaderCodecTest {

    @Test
    void toJson_shouldSerializeHeaders_asUtf8StringMap() {
        Headers headers = headers(Map.of("idempotency-key", "idem-1", "x-correlation-id", "abc"));

        String json = KafkaHeaderCodec.toJson(headers);

        assertThat(KafkaHeaderCodec.fromJson(json))
                .containsEntry("idempotency-key", "idem-1")
                .containsEntry("x-correlation-id", "abc");
    }

    @Test
    void toJson_shouldReturnNull_whenNoHeaders() {
        assertThat(KafkaHeaderCodec.toJson(new RecordHeaders())).isNull();
    }

    @Test
    void toJson_shouldSkipHeaders_withNullValue() {
        Headers headers = new RecordHeaders();
        headers.add("present", "v".getBytes(StandardCharsets.UTF_8));
        headers.add("absent", null);

        assertThat(KafkaHeaderCodec.fromJson(KafkaHeaderCodec.toJson(headers)))
                .containsOnlyKeys("present");
    }

    @Test
    void roundTrip_shouldPreserveAllEntries() {
        Map<String, String> entries = new LinkedHashMap<>();
        entries.put("a", "1");
        entries.put("b", "2");
        entries.put("traceparent", "00-abc-def-01");

        Map<String, String> restored = KafkaHeaderCodec.fromJson(KafkaHeaderCodec.toJson(headers(entries)));

        assertThat(restored).containsExactlyInAnyOrderEntriesOf(entries);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "not json", "[1,2,3]"})
    void fromJson_shouldReturnEmptyMap_whenNullBlankOrUnparseable(String json) {
        assertThat(KafkaHeaderCodec.fromJson(json)).isEmpty();
    }

    private static Headers headers(Map<String, String> entries) {
        Headers headers = new RecordHeaders();
        entries.forEach((key, value) -> headers.add(key, value.getBytes(StandardCharsets.UTF_8)));
        return headers;
    }
}
