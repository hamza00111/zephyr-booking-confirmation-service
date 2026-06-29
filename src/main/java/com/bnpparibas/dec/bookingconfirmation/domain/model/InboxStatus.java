package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Lifecycle of an inbox row (the consume + transform half).
 *
 * <pre>
 * NEW ──[PROCESS tick]──► PROCESSED        (its payload was published — possibly re-typed — or filtered out)
 *   │
 *   ├──[aggregation]────► AGGREGATED       (collapsed away: replaced or netted out by another event
 *   │                                       of the same trade within the drain — never published)
 *   │
 *   ├──[create-barrier]─► BLOCKED          (an AMEND/DELETE held until its trade's CREATE reaches SENT;
 *   │                          │            released back to NEW by the RELAY stage, in order)
 *   │                          └─► NEW
 *   │
 *   ├──[transform fails]─► PROCESS_FAILURE ──[REQUEUE tick, backoff]──► NEW       (retry budget remaining)
 *   │                          │
 *   │                          └──────────────────────────────────► PARKED       (budget used up)
 *   │
 *   └──[unparseable]────► INVALID          (no readable event type / unbindable payload)
 * </pre>
 *
 * <p><b>No data loss.</b> {@link #PARKED} and {@link #INVALID} are retained, alerted and replayable
 * (never silently dropped); transform failures retry with backoff before parking. {@code PROCESS_FAILURE}
 * names a transform failure, not a Kafka send failure (sends happen in the outbox/RELAY stage). When a
 * later AMEND is auto-promoted over a terminally-failed CREATE, the superseded CREATE is retired on the
 * <em>outbox</em> ({@code OutboxStatus.SUPERSEDED}), not here.
 */
public enum InboxStatus {
    NEW,
    PROCESSED,
    AGGREGATED,
    /** Held behind the create-barrier until its trade's CREATE is delivered downstream. */
    BLOCKED,
    PROCESS_FAILURE,
    /** Repeated transform failure or unrecoverable: retained, alerted, replayable — never dropped. */
    PARKED,
    INVALID
}
