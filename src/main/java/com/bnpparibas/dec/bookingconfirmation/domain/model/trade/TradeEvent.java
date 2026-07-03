package com.bnpparibas.dec.bookingconfirmation.domain.model.trade;

import com.bnpparibas.dec.bookingconfirmation.domain.model.TradeEventType;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Consumer-side copy of the shared trade-event envelope (owned by the producing system's domain
 * repo — deliberately not a dependency of this service). The three concrete records mirror the
 * original envelope components, with two deliberate divergences:
 *
 * <ul>
 *   <li>{@code pivotId} and {@code payload} are opaque {@link JsonNode}s — the original {@code
 *       TradeId}/{@code Trade} shapes are not replicated here, and JsonNode round-trips them
 *       byte-faithfully (including decimal precision) through deserialize → enrich → serialize.
 *   <li>No {@code EventInvariants} enforcement: this is a consumer-side view, and being stricter
 *       than the producer's contract would reject rows the producer considers legal.
 * </ul>
 *
 * <p>If the shared domain artifact is ever added as a dependency, this package is designed to be
 * swapped for it (same accessors; {@code payload} becomes the typed {@code Trade}).
 *
 * <p>Polymorphic JSON binding is configured externally via {@code TradeEventsMixin} (discriminator:
 * the {@code eventType} field, values {@code TradeCreated} / {@code TradeAmended} /
 * {@code TradeDeleted}), keeping this package free of serialization annotations.
 */
public sealed interface TradeEvent permits TradeCreatedEvent, TradeAmendedEvent, TradeDeletedEvent {

    @Nullable String version();

    TradeEventType eventType();

    @Nullable UUID eventId();

    @Nullable JsonNode pivotId();

    @Nullable UUID traceId();

    @Nullable String externalSystem();

    @Nullable FlowDirection flowDirection();

    @Nullable String hub();

    @Nullable Instant occurredAt();

    @Nullable Instant recordedAt();

    @Nullable AuditInfo auditInfo();

    @Nullable JsonNode payload();
}
