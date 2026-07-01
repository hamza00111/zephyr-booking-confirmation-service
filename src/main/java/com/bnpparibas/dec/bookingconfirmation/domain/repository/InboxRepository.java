package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.Collection;
import java.util.List;

/**
 * Persistence contract for the inbox (consume + transform half).
 *
 * <p>All reads/writes are region-scoped. {@link #findNew} claims rows for the partitions this
 * instance owns (ADR 0001) with {@code FOR UPDATE SKIP LOCKED}, so a trade's events are processed by
 * a single instance in order.
 */
public interface InboxRepository {

    /**
     * Inserts a consumed message, ignoring duplicates (dedupe on {@code (region, idempotencyKey)}).
     *
     * @return {@code true} if a new row was written; {@code false} if it already existed.
     */
    boolean insertIfAbsent(InboxMessage message);

    /**
     * Claims up to {@code limit} {@code NEW} rows for the region, restricted to the given owned
     * {@code partitions}, using {@code FOR UPDATE SKIP LOCKED}. Returns empty if {@code partitions}
     * is empty.
     */
    List<InboxMessage> findNew(Region region, Collection<Integer> partitions, int limit);

    void markProcessed(Region region, List<Long> ids);

    /** Marks rows collapsed away by per-trade aggregation (superseded or netted out, never published). */
    void markAggregated(Region region, List<Long> ids);

    void markProcessFailure(Region region, List<Long> ids, String errorMessage);

    void markInvalid(Region region, List<Long> ids, String errorMessage);
}
