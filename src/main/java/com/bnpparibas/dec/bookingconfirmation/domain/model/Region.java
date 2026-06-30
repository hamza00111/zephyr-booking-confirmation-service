package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Logical region — the per-scope isolation unit of this service.
 *
 * <p>Each region consumes its own {@code internal.<region>} topic, owns its own inbox/outbox
 * rows, circuit breaker, and scheduled tasks, and publishes to its own {@code published.<region>}
 * topic. A failure or backlog in one region never affects another.
 */
public enum Region {
    AMER,
    APAC,
    EMEA
}
