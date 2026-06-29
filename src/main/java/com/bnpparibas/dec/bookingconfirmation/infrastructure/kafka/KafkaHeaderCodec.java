package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Serializes Kafka record headers to/from the JSON stored in the {@code HEADERS} column, so inbound
 * headers survive the inbox/outbox hop and can be re-emitted on the published record.
 *
 * <p>Header values are treated as UTF-8 strings (true for the trade/CDC headers in this flow). A
 * duplicate key keeps the last value, and genuinely binary header values are not guaranteed to
 * round-trip — switch to base64 + a list of pairs if that ever matters. Header handling is
 * best-effort audit/propagation: parsing never throws, so it cannot break ingestion or relay.
 *
 * <p>Uses a private vanilla Jackson 3 mapper (plain string map; the application's TradeEvent mix-in
 * is irrelevant here).
 */
public final class KafkaHeaderCodec {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private KafkaHeaderCodec() {}

    /** Inbound headers to a JSON object, or {@code null} when there are none. Never throws. */
    public static @Nullable String toJson(final Headers headers) {
        final Map<String, String> map = new LinkedHashMap<>();
        for (final Header header : headers) {
            if (header.value() != null) {
                map.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
        }
        if (map.isEmpty()) {
            return null;
        }
        try {
            return MAPPER.writeValueAsString(map);
        } catch (final JacksonException unexpected) {
            return null;
        }
    }

    /** Stored JSON back to header pairs for re-emit; empty when null/blank/unparseable. Never throws. */
    public static Map<String, String> fromJson(@Nullable final String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return MAPPER.readValue(json, new TypeReference<LinkedHashMap<String, String>>() {});
        } catch (final JacksonException unparseable) {
            return Map.of();
        }
    }
}
