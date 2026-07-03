package com.bnpparibas.dec.bookingconfirmation.domain.model.trade;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * A trade was deleted (busted) upstream — plays the role of this service's {@code BUSTED}
 * aggregation type. See {@link TradeEvent} for the copy/divergence notes.
 */
public record TradeDeletedEvent(
        @Nullable String version,
        EventChangeType eventType,
        @Nullable UUID eventId,
        @Nullable JsonNode pivotId,
        @Nullable UUID traceId,
        @Nullable String externalSystem,
        @Nullable FlowDirection flowDirection,
        @Nullable String hub,
        @Nullable Instant occurredAt,
        @Nullable Instant recordedAt,
        @Nullable AuditInfo auditInfo,
        @Nullable JsonNode payload)
        implements TradeEvent {}
