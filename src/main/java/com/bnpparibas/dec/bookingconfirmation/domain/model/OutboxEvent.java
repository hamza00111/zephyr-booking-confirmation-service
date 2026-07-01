package com.bnpparibas.dec.bookingconfirmation.domain.model;

import org.jspecify.annotations.Nullable;

/**
 * An event staged in (or read from) the outbox, ready to publish to {@code published.<region>}.
 *
 * <p>Used both for insertion in the PROCESS stage ({@code id} {@code null}) and for draining in the
 * RELAY stage ({@code id} populated). The idempotency key is propagated from the inbound message so
 * downstream consumers can dedupe.
 *
 * <p>{@code kafkaPartition} is carried from the originating inbox row (the partition of the
 * {@code internal.<region>} message). It is the ordering/parallelism unit for the partition-aligned
 * relay (see ADR 0001): a partition-scoped drain publishes only the partitions its instance owns.
 */
public record OutboxEvent(
        @Nullable Long id,
        Region region,
        String idempotencyKey,
        String destination,
        @Nullable String messageKey,
        @Nullable Integer kafkaPartition,
        String payload,
        @Nullable Long inboxId,
        @Nullable String traceId,
        @Nullable String headers) {}
