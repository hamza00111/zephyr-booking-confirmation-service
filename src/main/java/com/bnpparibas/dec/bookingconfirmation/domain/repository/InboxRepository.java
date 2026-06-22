package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
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

    void markProcessFailure(Region region, List<Long> ids, String errorMessage);

    void markInvalid(Region region, List<Long> ids, String errorMessage);
}
