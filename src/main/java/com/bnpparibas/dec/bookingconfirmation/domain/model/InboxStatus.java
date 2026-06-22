package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Lifecycle of an inbox row (the consume + transform half).
 *
 * <pre>
 * NEW ──[PROCESS tick]──► PROCESSED        (its payload was published — possibly re-typed — or filtered out)
 *   │
 *   ├──[aggregation]────► AGGREGATED       (collapsed away: superseded or netted out by another event
 *   │                                       of the same trade within the drain — never published)
 *   ├──[unparseable]────► INVALID          (terminal — no readable event type)
 *   │
 *   └──[transform fails]─► PROCESS_FAILURE (retryable transform/processing failure)
 *                              │
 *                              └─────────► INVALID (terminal — un-transformable payload)
 * </pre>
 *
 * <p>{@code PROCESS_FAILURE} is the analogue of the publisher's {@code MAPPING_ERROR}: it names a
 * transform failure, not a Kafka send failure (sends happen in the outbox/RELAY stage).
 */
public enum InboxStatus {
    NEW,
    PROCESSED,
    AGGREGATED,
    PROCESS_FAILURE,
    INVALID
}
