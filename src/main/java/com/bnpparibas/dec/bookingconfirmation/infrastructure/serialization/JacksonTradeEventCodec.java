package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventBindingException;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Jackson adapter for the {@link TradeEventCodec} port.
 *
 * <p>Wire format (current): {@code {"eventType":"TRADE_CREATED","eventId":…}} — the {@code
 * eventType} field is both the change type and the polymorphic discriminator (see {@link
 * TradeEventsMixin}). Reading also accepts the legacy vocabularies ({@code "CREATED"}, {@code
 * "@type":"TRADE_CREATED"}, camel {@code "TradeCreated"}); writing always emits the current {@code
 * TRADE_*} names. The producer's {@code TRADE_DELETED} maps to this service's {@code BUSTED}
 * aggregation type.
 *
 * <p>Uses the dedicated {@code tradeEventObjectMapper}: payloads are foreign JSON, so the
 * application's serialization settings must not leak into them.
 */
@Component
public class JacksonTradeEventCodec implements TradeEventCodec {

    private static final String TYPE_FIELD = "@type";
    private static final String EVENT_TYPE_FIELD = "eventType";
    private static final String TRACE_ID_FIELD = "traceId";
    private static final String TYPE_PREFIX = "TRADE_";

    /** Normalized (uppercased) discriminator value -> local aggregation type, all vocabularies. */
    private static final Map<String, TradeEventType> READ_NAMES = Map.ofEntries(
            // Current wire vocabulary.
            Map.entry("TRADE_CREATED", TradeEventType.CREATED),
            Map.entry("TRADE_AMENDED", TradeEventType.AMENDED),
            Map.entry("TRADE_DELETED", TradeEventType.BUSTED),
            Map.entry("TRADE_BUSTED", TradeEventType.BUSTED),
            // Legacy bare enum names.
            Map.entry("CREATED", TradeEventType.CREATED),
            Map.entry("AMENDED", TradeEventType.AMENDED),
            Map.entry("BUSTED", TradeEventType.BUSTED),
            Map.entry("DELETED", TradeEventType.BUSTED),
            // Camel subtype names (uppercased) used by earlier mixin revisions.
            Map.entry("TRADECREATED", TradeEventType.CREATED),
            Map.entry("TRADEAMENDED", TradeEventType.AMENDED),
            Map.entry("TRADEDELETED", TradeEventType.BUSTED));

    /** Local aggregation type -> canonical wire discriminator (the mixin subtype names). */
    private static final Map<TradeEventType, String> WIRE_NAMES = new EnumMap<>(Map.of(
            TradeEventType.CREATED, "TRADE_CREATED",
            TradeEventType.AMENDED, "TRADE_AMENDED",
            TradeEventType.BUSTED, "TRADE_DELETED"));

    private final ObjectMapper objectMapper;

    public JacksonTradeEventCodec(
            @Qualifier(TradeEventJacksonConfig.TRADE_EVENT_OBJECT_MAPPER) final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Optional<TradeEventType> eventType(final String payload) {
        final JsonNode root;
        try {
            root = objectMapper.readTree(payload);
        } catch (final JacksonException unparseable) {
            return Optional.empty();
        }
        return resolveType(root);
    }

    @Override
    public String rewriteType(final String payload, final TradeEventType target) {
        try {
            final JsonNode root = objectMapper.readTree(payload);
            if (!(root instanceof ObjectNode event)) {
                throw new IllegalStateException("TradeEvent payload is not a JSON object");
            }
            event.put(EVENT_TYPE_FIELD, WIRE_NAMES.get(target));
            // Kept for any legacy reader still keyed on @type; the typed round-trip drops it.
            event.put(TYPE_FIELD, TYPE_PREFIX + target.name());
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
        } catch (final JacksonException unparseable) {
            return Optional.empty();
        }
    }

    @Override
    public TradeEvent deserialize(final String payload) {
        final JsonNode root;
        try {
            root = objectMapper.readTree(payload);
        } catch (final JacksonException unparseable) {
            throw new TradeEventBindingException("TradeEvent payload is not valid JSON", unparseable);
        }
        final TradeEventType type = resolveType(root)
                .orElseThrow(() -> new TradeEventBindingException("TradeEvent payload carries no known event type"));
        if (!(root instanceof ObjectNode event)) {
            throw new TradeEventBindingException("TradeEvent payload is not a JSON object");
        }
        // Normalize the discriminator so legacy-vocabulary payloads bind to the right subtype.
        event.put(EVENT_TYPE_FIELD, WIRE_NAMES.get(type));
        try {
            return objectMapper.treeToValue(event, TradeEvent.class);
        } catch (final JacksonException bindingFailure) {
            throw new TradeEventBindingException("TradeEvent payload failed typed binding", bindingFailure);
        }
    }

    @Override
    public String serialize(final TradeEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (final JsonProcessingException failure) {
            throw new TradeEventBindingException("TradeEvent could not be serialized", failure);
        }
    }

    private static Optional<TradeEventType> resolveType(final JsonNode root) {
        String name = text(root, EVENT_TYPE_FIELD);
        if (name == null) {
            name = text(root, TYPE_FIELD);
        }
        if (name == null) {
            return Optional.empty();
        }
        final String normalized = name.trim().toUpperCase(Locale.ROOT);
        final TradeEventType direct = READ_NAMES.get(normalized);
        if (direct != null) {
            return Optional.of(direct);
        }
        if (normalized.startsWith(TYPE_PREFIX)) {
            return Optional.ofNullable(READ_NAMES.get(normalized.substring(TYPE_PREFIX.length())));
        }
        return Optional.empty();
    }

    private static @Nullable String text(final JsonNode root, final String field) {
        final JsonNode node = root.get(field);
        return node != null && node.isTextual() ? node.asText() : null;
    }
}
