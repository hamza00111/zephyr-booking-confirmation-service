package com.bnpparibas.dec.bookingconfirmation.domain.model;

/**
 * Lifecycle of an inbox row (the consume + transform half).
 *
 * <pre>
 * NEW ──[PROCESS tick]──► PROCESSED        (its payload was published — possibly re-typed — or filtered out)
 *   │
 *   ├──[aggregation]────► AGGREGATED       (collapsed away: superseded or netted out by another event
 *   │                                       of the same trade within the drain — never published)
 *   │
 *   ├──[create-barrier]─► BLOCKED          (an AMEND/BUST held until its trade's CREATE reaches SENT;
 *   │                          │            released back to NEW by the RELAY stage, in order)
 *   │                          └─► NEW
 *   │
 *   ├──[barrier promote]─► SUPERSEDED      (a stuck CREATE closed because a later AMEND was promoted
 *   │                                       into a CREATE in its place — never published)
 *   │
 *   ├──[transform fails]─► PROCESS_FAILURE ──[REQUEUE tick, backoff]──► NEW       (retry budget remaining)
 *   │                          │
 *   │                          └──────────────────────────────────► PARKED       (budget used up)
 *   │
 *   └──[unparseable]────► INVALID          (no readable event type)
 * </pre>
 *
 * <p><b>No data loss.</b> {@link #PARKED} and {@link #INVALID} are retained, alerted and replayable
 * (never silently dropped); transform failures retry with backoff before parking. {@code PROCESS_FAILURE}
 * names a transform failure, not a Kafka send failure (sends happen in the outbox/RELAY stage).
 */
public enum InboxStatus {
    NEW,
    PROCESSED,
    AGGREGATED,
    /** Held behind the create-barrier until its trade's CREATE is delivered downstream. */
    BLOCKED,
    /** A CREATE closed because a later AMEND was promoted into a CREATE in its place. */
    SUPERSEDED,
    PROCESS_FAILURE,
    /** Repeated transform failure or unrecoverable: retained, alerted, replayable — never dropped. */
    PARKED,
    INVALID
}
