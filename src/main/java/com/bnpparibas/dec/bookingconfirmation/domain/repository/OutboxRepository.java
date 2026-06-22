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

    void markSendFailure(Region region, List<Long> ids, String errorMessage);

    /**
     * Promotes {@code SEND_FAILURE} rows back to {@code NEW} while their retry count is below
     * {@code maxRetries}, otherwise marks them {@code RETRY_EXHAUSTED}.
     *
     * @return number of rows transitioned.
     */
    int requeueFailed(Region region, int maxRetries);
}
