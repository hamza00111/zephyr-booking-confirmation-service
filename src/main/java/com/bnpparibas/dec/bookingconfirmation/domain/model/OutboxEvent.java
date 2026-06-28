package com.bnpparibas.dec.bookingconfirmation.domain.model;

import org.jspecify.annotations.Nullable;

/**
 * An event staged in (or read from) the outbox, ready to publish to {@code published.<region>}.
 *
 * <p>Used both for insertion in the PROCESS stage ({@code id} {@code null}) and for draining in the
 * RELAY stage ({@code id} populated). The idempotency key is propagated from the inbound message so
 * downstream consumers can dedupe.
 */
public record OutboxEvent(
        @Nullable Long id,
        Region region,
        String idempotencyKey,
        String destination,
        @Nullable String messageKey,
        @Nullable TradeEventType eventType,
        String payload,
        @Nullable Long inboxId,
        @Nullable String traceId,
        @Nullable String headers) {}
