package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Lifecycle of an outbox row (the publish half).
 *
 * <pre>
 * NEW ──[RELAY tick]──► SENT
 *   │
 *   ├──[infra send fails]──► SEND_FAILURE ──[REQUEUE tick, backoff elapsed]──► NEW   (retried indefinitely)
 *   │
 *   └──[poison send fails]─► PARKED
 * </pre>
 *
 * <p><b>No data loss.</b> Infrastructure failures (broker down, timeout, circuit open) are retried
 * indefinitely — a message is never abandoned because Kafka was unavailable. Only message-level
 * <em>poison</em> (too-large, non-serializable) is moved to {@link #PARKED}: a durable, alerted,
 * replayable bucket — never a silent drop. The DB outbox is the dead-letter store (a Kafka DLT cannot
 * help when Kafka itself is down).
 */
public enum OutboxStatus {
    NEW,
    SENT,
    SEND_FAILURE,
    /** Needs attention; retained and replayable ({@code PARKED -> NEW} once the root cause is fixed). */
    PARKED
}
