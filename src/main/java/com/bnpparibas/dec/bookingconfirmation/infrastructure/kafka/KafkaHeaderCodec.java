package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jspecify.annotations.Nullable;

/**
 * Serializes Kafka record headers to/from the JSON stored in the {@code HEADERS} column, so inbound
 * headers survive the inbox/outbox hop and can be re-emitted on the published record.
 *
 * <p>Header values are treated as UTF-8 strings (true for the trade/CDC headers in this flow). A
 * duplicate key keeps the last value, and genuinely binary header values are not guaranteed to
 * round-trip — switch to base64 + a list of pairs if that ever matters. Header handling is
 * best-effort audit/propagation: parsing never throws, so it cannot break ingestion or relay.
 */
public final class KafkaHeaderCodec {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KafkaHeaderCodec.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

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
        } catch (final JsonProcessingException unexpected) {
            log.warn("Failed to serialize {} Kafka header(s) to JSON — headers will not propagate", map.size(), unexpected);
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
        } catch (final JsonProcessingException unparseable) {
            log.warn("Failed to parse stored HEADERS JSON — re-emitting without inbound headers", unparseable);
            return Map.of();
        }
    }
}
