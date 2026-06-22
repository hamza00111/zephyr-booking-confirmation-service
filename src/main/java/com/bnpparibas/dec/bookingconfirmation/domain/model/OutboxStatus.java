package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Lifecycle of an outbox row (the publish half) — identical vocabulary to the publisher's relay.
 *
 * <pre>
 * NEW ──[RELAY tick]──► SENT
 *   │
 *   └──[Kafka send fails]─► SEND_FAILURE ──[REQUEUE tick]──► NEW            (retry budget remaining)
 *                                              │
 *                                              └──────────► RETRY_EXHAUSTED (budget used up — terminal)
 * </pre>
 */
public enum OutboxStatus {
    NEW,
    SENT,
    SEND_FAILURE,
    RETRY_EXHAUSTED
}
