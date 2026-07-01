package com.bnpparibas.dec.bookingconfirmation.application.partition;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * Thread-safe view of the Kafka partitions this JVM currently owns, per region.
 *
 * <p>Written by the consumer rebalance callbacks (consumer threads) and read by the scheduled
 * PROCESS/RELAY/REQUEUE drains (scheduler threads). A region's drain must only claim rows whose
 * {@code KAFKA_PARTITION} this instance owns, so per-key ordering is preserved — one partition maps
 * to one owner, hence one processor and one producer (ADR 0001).
 *
 * <p>Ownership is tracked incrementally: {@link #add} on assignment, {@link #remove} on
 * revoke/loss. This works for both eager and cooperative rebalancing, since each callback carries
 * only the partitions that changed for the calling consumer.
 */
@Component
public class OwnedPartitions {

    private final ConcurrentHashMap<Region, Set<Integer>> owned = new ConcurrentHashMap<>();

    public void add(final Region region, final int partition) {
        owned.computeIfAbsent(region, ignored -> ConcurrentHashMap.newKeySet()).add(partition);
    }

    public void remove(final Region region, final int partition) {
        final Set<Integer> partitions = owned.get(region);
        if (partitions != null) {
            partitions.remove(partition);
        }
    }

    /** Immutable snapshot of the partitions owned for the region (empty if none). */
    public Set<Integer> forRegion(final Region region) {
        final Set<Integer> partitions = owned.get(region);
        return partitions == null ? Set.of() : Set.copyOf(partitions);
    }
}
