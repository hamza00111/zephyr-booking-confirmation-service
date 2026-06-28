package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.Collection;
import java.util.List;

/**
 * Persistence contract for the inbox (consume + transform half).
 *
 * <p>All reads/writes are region-scoped. {@link #findNew} must claim rows with
 * {@code FOR UPDATE SKIP LOCKED} so concurrent PROCESS ticks on different JVMs take disjoint rows.
 */
public interface InboxRepository {

    /**
     * Inserts a consumed message, ignoring duplicates (dedupe on {@code (region, idempotencyKey)}).
     *
     * @return {@code true} if a new row was written; {@code false} if it already existed.
     */
    boolean insertIfAbsent(InboxMessage message);

    /** Claims up to {@code limit} {@code NEW} rows for the region using {@code FOR UPDATE SKIP LOCKED}. */
    List<InboxMessage> findNew(Region region, int limit);

    void markProcessed(Region region, List<Long> ids);

    /** Marks rows collapsed away by per-trade aggregation (superseded or netted out, never published). */
    void markAggregated(Region region, List<Long> ids);

    /** Holds an AMEND/BUST behind the create-barrier until its trade's CREATE reaches SENT. */
    void markBlocked(Region region, List<Long> ids);

    /** Closes a stuck CREATE because a later AMEND was promoted into a CREATE in its place. */
    void markSuperseded(Region region, List<Long> ids);

    /**
     * Releases rows held behind the create-barrier for the given trade keys: {@code BLOCKED -> NEW}.
     * Called by RELAY once a key's CREATE is delivered. They re-drain in order on the next PROCESS tick.
     *
     * @return number of rows released.
     */
    int releaseBlocked(Region region, Collection<String> messageKeys);

    /** Marks a transform failure and schedules the next retry with backoff ({@code NEXT_ATTEMPT_AT}). */
    void markProcessFailure(Region region, List<Long> ids, String errorMessage);

    /** Terminal-but-retained: no readable event type. Retained, alerted, replayable — never dropped. */
    void markInvalid(Region region, List<Long> ids, String errorMessage);

    /** Moves rows to {@code PARKED} (retained, alerted, replayable — never silently dropped). */
    void markParked(Region region, List<Long> ids, String errorMessage);

    /**
     * Promotes {@code PROCESS_FAILURE} rows whose backoff has elapsed back to {@code NEW} while their
     * retry count is below {@code maxRetries}; rows past the budget are moved to {@code PARKED} (never
     * silently dropped).
     *
     * @return number of rows promoted back to {@code NEW}.
     */
    int requeueFailed(Region region, int maxRetries);

    /**
     * Deletes fully-processed rows ({@code PROCESSED}/{@code AGGREGATED}/{@code SUPERSEDED}) older than
     * {@code retentionDays}. Never deletes a non-success row ({@code PARKED}/{@code INVALID}/in-flight)
     * — that would be silent data loss.
     *
     * @return number of rows purged.
     */
    int purgeProcessed(Region region, int retentionDays);
}
