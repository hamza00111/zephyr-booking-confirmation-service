package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.AggregatedLink;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxMessage;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.model.RequeueOutcome;
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
     * Batch variant of {@link #insertIfAbsent}: one round trip for a whole consumer poll. Returns
     * one update count per message, in order — {@code 1} inserted, {@code 0} already present
     * (deduped on {@code (region, idempotencyKey)}).
     */
    int[] insertAllIfAbsent(List<InboxMessage> messages);

    /**
     * Claims up to {@code limit} {@code NEW} rows for the region, restricted to the given owned
     * {@code partitions}, using {@code FOR UPDATE SKIP LOCKED}. Returns empty if {@code partitions}
     * is empty.
     */
    List<InboxMessage> findNew(Region region, Collection<Integer> partitions, int limit);

    void markProcessed(Region region, List<Long> ids);

    /**
     * Marks rows collapsed away by per-trade aggregation (superseded or netted out, never
     * published), each recording which surviving row absorbed it — null for netted-out groups.
     */
    void markAggregated(Region region, List<AggregatedLink> links);

    void markProcessFailure(Region region, List<Long> ids, String errorMessage);

    void markInvalid(Region region, List<Long> ids, String errorMessage);

    /**
     * Parks a message whose ingestion failed (status {@code INGEST_FAILURE}), preserving the raw
     * payload so the consumer can safely advance past the record. Dedupes like {@link
     * #insertIfAbsent}.
     *
     * @return {@code true} if a new row was written; {@code false} if it already existed.
     */
    boolean insertParked(InboxMessage message, String errorMessage);

    /**
     * Promotes {@code PROCESS_FAILURE} and {@code INGEST_FAILURE} rows for the owned
     * {@code partitions} back to {@code NEW} while their retry count is below {@code maxRetries},
     * otherwise marks them {@code INVALID} (terminal). No-op when {@code partitions} is empty.
     */
    RequeueOutcome requeueFailed(Region region, Collection<Integer> partitions, int maxRetries);
}
