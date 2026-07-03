package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventBindingException;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.trade.TradeEvent;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Jackson adapter for the {@link TradeEventCodec} port.
 *
 * <p>Wire format: {@code {"eventType":"TradeCreated",…}} — the {@code eventType} field carries
 * {@link TradeEventType#type()} and is also the polymorphic discriminator (see
 * {@link TradeEventsMixin}). The vocabulary is exactly the enum's wire strings; unknown values are
 * caught once, at classification ({@link #eventType} → empty → row marked INVALID).
 * {@link #deserialize} then binds under the caller-resolved type without re-resolving it.
 *
 * <p>Uses the dedicated {@code tradeEventJsonMapper}: payloads are foreign JSON, so the
 * application's serialization settings must not leak into them.
 */
@Component
public class JacksonTradeEventCodec implements TradeEventCodec {

    private static final String EVENT_TYPE_FIELD = "eventType";
    private static final String TRACE_ID_FIELD = "traceId";

    /** Wire discriminator ({@link TradeEventType#type()}) -> enum constant. */
    private static final Map<String, TradeEventType> BY_WIRE_NAME = Arrays.stream(TradeEventType.values())
            .collect(Collectors.toUnmodifiableMap(TradeEventType::type, Function.identity()));

    private final JsonMapper jsonMapper;

    public JacksonTradeEventCodec(
            @Qualifier(TradeEventJacksonConfig.TRADE_EVENT_JSON_MAPPER) final JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    @Override
    public Optional<TradeEventType> eventType(final String payload) {
        final JsonNode root;
        try {
            root = jsonMapper.readTree(payload);
        } catch (final JacksonException unparseable) {
            return Optional.empty();
        }
        return resolveType(root);
    }

    @Override
    public Optional<String> traceId(final String payload) {
        try {
            return Optional.ofNullable(text(jsonMapper.readTree(payload), TRACE_ID_FIELD));
        } catch (final JacksonException unparseable) {
            return Optional.empty();
        }
    }

    @Override
    public TradeEvent deserialize(final String payload, final TradeEventType as) {
        final JsonNode root;
        try {
            root = jsonMapper.readTree(payload);
        } catch (final JacksonException unparseable) {
            throw new TradeEventBindingException("TradeEvent payload is not valid JSON", unparseable);
        }
        if (!(root instanceof ObjectNode event)) {
            throw new TradeEventBindingException("TradeEvent payload is not a JSON object");
        }
        // One parse, one stamp: the discriminator drives which subtype Jackson instantiates, so
        // overriding it here is how aggregation retypes a survivor (records cannot change class).
        event.put(EVENT_TYPE_FIELD, as.type());
        try {
            return jsonMapper.treeToValue(event, TradeEvent.class);
        } catch (final JacksonException bindingFailure) {
            throw new TradeEventBindingException("TradeEvent payload failed typed binding", bindingFailure);
        }
    }

    @Override
    public String serialize(final TradeEvent event) {
        try {
            return jsonMapper.writeValueAsString(event);
        } catch (final JsonProcessingException failure) {
            throw new TradeEventBindingException("TradeEvent could not be serialized", failure);
        }
    }

    private static Optional<TradeEventType> resolveType(final JsonNode root) {
        final String name = text(root, EVENT_TYPE_FIELD);
        if (name == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(BY_WIRE_NAME.get(name.trim()));
    }

    private static @Nullable String text(final JsonNode root, final String field) {
        final JsonNode node = root.get(field);
        return node != null && node.isTextual() ? node.asText() : null;
    }
}
