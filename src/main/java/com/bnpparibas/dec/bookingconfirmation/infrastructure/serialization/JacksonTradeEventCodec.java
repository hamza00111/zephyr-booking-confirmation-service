package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Locale;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;

/**
 * Jackson adapter for the {@link TradeEventCodec} port.
 *
 * <p>Wire format (sample): {@code {"@type":"TRADE_CREATED","eventType":"CREATED","eventId":…}}.
 * {@code eventType} is read first, falling back to {@code @type} (stripping its {@code TRADE_}
 * prefix); {@link #rewriteType} rewrites both fields so the emitted JSON stays consistent for
 * downstream deserialization. Uses a private mapper: payloads are foreign JSON, so the
 * application's serialization settings must not leak into them.
 */
@Component
public class JacksonTradeEventCodec implements TradeEventCodec {

    private static final String TYPE_FIELD = "@type";
    private static final String EVENT_TYPE_FIELD = "eventType";
    private static final String TRACE_ID_FIELD = "traceId";
    private static final String TYPE_PREFIX = "TRADE_";

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Optional<TradeEventType> eventType(final String payload) {
        final JsonNode root;
        try {
            root = objectMapper.readTree(payload);
        } catch (final JsonProcessingException unparseable) {
            return Optional.empty();
        }
        String name = text(root, EVENT_TYPE_FIELD);
        if (name == null) {
            final String wireType = text(root, TYPE_FIELD);
            name = wireType != null && wireType.startsWith(TYPE_PREFIX)
                    ? wireType.substring(TYPE_PREFIX.length())
                    : wireType;
        }
        if (name == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(TradeEventType.valueOf(name.toUpperCase(Locale.ROOT)));
        } catch (final IllegalArgumentException unknownType) {
            return Optional.empty();
        }
    }

    @Override
    public String rewriteType(final String payload, final TradeEventType target) {
        try {
            final JsonNode root = objectMapper.readTree(payload);
            if (!(root instanceof ObjectNode event)) {
                throw new IllegalStateException("TradeEvent payload is not a JSON object");
            }
            event.put(TYPE_FIELD, TYPE_PREFIX + target.name());
            event.put(EVENT_TYPE_FIELD, target.name());
            return objectMapper.writeValueAsString(event);
        } catch (final JsonProcessingException unparseable) {
            // eventType() parsed this payload moments ago; reaching here is a programming error.
            throw new IllegalStateException("Cannot rewrite TradeEvent type", unparseable);
        }
    }

    @Override
    public Optional<String> traceId(final String payload) {
        try {
            return Optional.ofNullable(text(objectMapper.readTree(payload), TRACE_ID_FIELD));
        } catch (final JsonProcessingException unparseable) {
            return Optional.empty();
        }
    }

    private static @Nullable String text(final JsonNode root, final String field) {
        final JsonNode node = root.get(field);
        return node != null && node.isTextual() ? node.asText() : null;
    }
}
