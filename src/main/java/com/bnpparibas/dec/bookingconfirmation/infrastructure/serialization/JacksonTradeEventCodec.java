package com.bnpparibas.dec.bookingconfirmation.infrastructure.serialization;

import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.bnpparibas.dec.zephyr.domain.trade.events.TradeEvent;
import java.util.Locale;
import java.util.Optional;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * Jackson 3 adapter for the {@link TradeEventCodec} port.
 *
 * <p>Wire format (sample): {@code {"@type":"TRADE_CREATED","eventType":"CREATED","eventId":…}}.
 * {@code eventType} is read first, falling back to {@code @type} (stripping its {@code TRADE_}
 * prefix); {@link #rewriteType} rewrites both fields so the emitted JSON stays consistent for
 * downstream deserialization. {@link #deserialize} binds the payload to the typed {@link TradeEvent}
 * domain model (polymorphic via the shared {@code @Primary JsonMapper}'s mix-in) for filter/business
 * decisions only — the published bytes are never rebuilt from that object.
 */
@Component
public class JacksonTradeEventCodec implements TradeEventCodec {

    private static final String TYPE_FIELD = "@type";
    private static final String EVENT_TYPE_FIELD = "eventType";
    private static final String TRACE_ID_FIELD = "traceId";
    private static final String TYPE_PREFIX = "TRADE_";

    private final JsonMapper jsonMapper;

    public JacksonTradeEventCodec(final JsonMapper jsonMapper) {
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

    /**
     * Surgically rewrites only the two type discriminators ({@code @type} and {@code eventType}) on the
     * existing payload, leaving every other byte — the whole trade snapshot — untouched. This is a tree
     * edit by design, NOT a deserialize-and-rebuild: its output is exactly what {@code emit()} publishes
     * for a collapsed/auto-promoted event, so it must preserve the faithful original payload. Rebuilding
     * from a typed object would re-serialize the entire event (the round-trip fidelity risk we avoid) and
     * force copying every wrapper/payload field across subtypes — any miss being silent data loss.
     */
    @Override
    public String rewriteType(final String payload, final TradeEventType target) {
        try {
            final JsonNode root = jsonMapper.readTree(payload);
            if (!(root instanceof ObjectNode event)) {
                throw new IllegalStateException("TradeEvent payload is not a JSON object");
            }
            event.put(TYPE_FIELD, TYPE_PREFIX + target.name());
            event.put(EVENT_TYPE_FIELD, target.name());
            return jsonMapper.writeValueAsString(event);
        } catch (final JacksonException unparseable) {
            // eventType() parsed this payload moments ago; reaching here is a programming error.
            throw new IllegalStateException("Cannot rewrite TradeEvent type", unparseable);
        }
    }

    @Override
    public Optional<TradeEvent> deserialize(final String payload) {
        try {
            return Optional.of(jsonMapper.readValue(payload, TradeEvent.class));
        } catch (final JacksonException bindFailure) {
            // Empty signals the body could not be bound; the PROCESS stage marks such rows INVALID.
            return Optional.empty();
        }
    }

    @Override
    public Optional<String> traceId(final String payload) {
        try {
            return Optional.ofNullable(text(jsonMapper.readTree(payload), TRACE_ID_FIELD));
        } catch (final JacksonException unparseable) {
            return Optional.empty();
        }
    }

    // Jackson 3 node API: asString()/isValueNode() — verify against the artifact on the VM
    // (Jackson 3 renamed Jackson 2's isTextual()/asText()).
    private static @Nullable String text(final JsonNode root, final String field) {
        final JsonNode node = root.get(field);
        return node != null && node.isValueNode() && !node.isNull() ? node.asString() : null;
    }
}
