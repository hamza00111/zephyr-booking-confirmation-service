package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.List;

/**
 * Persistence contract for the outbox (publish half).
 *
 * <p>{@link #insertAll} is called inside the PROCESS transaction alongside the inbox status update,
 * as a single batched insert. {@link #findNew} claims rows with {@code FOR UPDATE SKIP LOCKED} for
 * the RELAY stage.
 */
public interface OutboxRepository {

    /** Batch-inserts all events in one round-trip (no-op when empty). */
    void insertAll(List<OutboxEvent> events);

    List<OutboxEvent> findNew(Region region, int limit);

    void markSent(Region region, List<Long> ids);

    /**
     * Marks rows as {@code SEND_FAILURE} after a transient (infrastructure) send failure and schedules
     * their next retry with exponential backoff ({@code NEXT_ATTEMPT_AT}). These are retried
     * indefinitely — never abandoned — so a broker outage can never lose a message.
     */
    void markSendFailure(Region region, List<Long> ids, String errorMessage);

    /**
     * Moves rows to {@code PARKED} after a poison (message-level) send failure that retrying cannot
     * fix. Parked rows are retained, alerted and replayable; they are never silently dropped.
     */
    void markParked(Region region, List<Long> ids, String errorMessage);

    /**
     * Promotes {@code SEND_FAILURE} rows whose backoff has elapsed ({@code NEXT_ATTEMPT_AT <= now})
     * back to {@code NEW}. There is no terminal exhaustion — infrastructure failures retry forever.
     *
     * @return number of rows transitioned.
     */
    int requeueReady(Region region);

    /**
     * Parks any not-yet-delivered ({@code NEW}/{@code SEND_FAILURE}) rows for a trade key so they are
     * never published. Used by the create-barrier when a trade is deleted before its CREATE was
     * delivered: the staged CREATE must not reach downstream. Rows are retained (audited), not deleted.
     *
     * @return number of rows parked.
     */
    int parkUnsentForKey(Region region, String messageKey, String reason);

    /**
     * Retires the parked CREATE for a trade key ({@code PARKED -> SUPERSEDED}) when a later AMEND was
     * auto-promoted into a CREATE in its place. The dead CREATE is resolved (the trade self-healed), so
     * it drops out of the parked review queue — retained as audit until retention purges it.
     *
     * @return number of rows superseded.
     */
    int supersedeParkedForKey(Region region, String messageKey, String reason);

    /**
     * Deletes resolved ({@code SENT}/{@code SUPERSEDED}) rows older than {@code retentionDays}. Never
     * deletes a {@code PARKED}/{@code SEND_FAILURE}/{@code NEW} row — that would be silent loss.
     *
     * @return number of rows purged.
     */
    int purgeSent(Region region, int retentionDays);
}
