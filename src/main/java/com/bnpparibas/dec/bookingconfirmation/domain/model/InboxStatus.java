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
 *                              ├──[REQUEUE tick, budget left]──► NEW
 *                              └──[REQUEUE tick, exhausted]───► INVALID (terminal)
 *
 * INGEST_FAILURE ──[REQUEUE tick]──► NEW | INVALID   (parked by the consumer error handler after
 *                                                     ingestion retries were exhausted; same budget
 *                                                     rules as PROCESS_FAILURE)
 * </pre>
 *
 * <p>{@code PROCESS_FAILURE} is the analogue of the publisher's {@code MAPPING_ERROR}: it names a
 * transform failure, not a Kafka send failure (sends happen in the outbox/RELAY stage).
 * {@code INGEST_FAILURE} is the DB-side dead-letter parking for records the consumer could not
 * ingest normally — the raw payload is preserved so nothing is lost when the offset advances.
 */
public enum InboxStatus {
    NEW,
    PROCESSED,
    AGGREGATED,
    PROCESS_FAILURE,
    INGEST_FAILURE,
    INVALID
}
