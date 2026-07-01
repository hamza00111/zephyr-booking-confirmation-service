package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxEvent;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
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
     * {@code partitions}. Returns empty if {@code partitions} is empty.
     */
    List<OutboxEvent> findNew(Region region, Collection<Integer> partitions, int limit);

    void markSent(Region region, List<Long> ids);

    void markSendFailure(Region region, List<Long> ids, String errorMessage);

    /**
     * Promotes {@code SEND_FAILURE} rows for the owned {@code partitions} back to {@code NEW} while
     * their retry count is below {@code maxRetries}, otherwise marks them {@code RETRY_EXHAUSTED}.
     *
     * @return number of rows transitioned (0 if {@code partitions} is empty).
     */
    int requeueFailed(Region region, Collection<Integer> partitions, int maxRetries);
}
