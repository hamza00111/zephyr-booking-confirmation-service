package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.RequeueOutcome;
import java.util.Collection;
import java.util.List;

/**
 * Persistence contract for the outbox (publish half).
 *
 * <p>{@link #insertAll} is called inside the PROCESS transaction alongside the inbox status update,
 * as a single batched insert. {@link #findNew} and {@link #requeueFailed} are restricted to the
 * partitions this instance owns (ADR 0001) with {@code FOR UPDATE SKIP LOCKED} for the RELAY stage.
 */
public interface OutboxRepository {

    /** Batch-inserts all events in one round-trip (no-op when empty). */
    void insertAll(List<OutboxEvent> events);

    /**
     * Claims up to {@code limit} {@code NEW} rows for the region, restricted to the given owned
     * {@code partitions} and gated head-of-line per key: a row is only returned once every earlier
     * same-key row is {@code SENT}, so a failed event never lets its key's later events overtake it.
     * Returns empty if {@code partitions} is empty.
     */
    List<OutboxEvent> findNew(Region region, Collection<Integer> partitions, int limit);

    void markSent(Region region, List<Long> ids);

    void markSendFailure(Region region, List<Long> ids, String errorMessage);

    /**
     * Marks rows whose send was never attempted (e.g. circuit breaker open) as {@code SEND_FAILURE}
     * <em>without</em> consuming retry budget: an infrastructure outage of any length must not drive
     * healthy rows to {@code RETRY_EXHAUSTED}. The requeue stage promotes them like any other
     * failure.
     */
    void markSendRejected(Region region, List<Long> ids, String errorMessage);

    /**
     * Promotes {@code SEND_FAILURE} rows for the owned {@code partitions} back to {@code NEW} while
     * their retry count is below {@code maxRetries}, otherwise marks them {@code RETRY_EXHAUSTED}.
     * No-op when {@code partitions} is empty.
     */
    RequeueOutcome requeueFailed(Region region, Collection<Integer> partitions, int maxRetries);
}
