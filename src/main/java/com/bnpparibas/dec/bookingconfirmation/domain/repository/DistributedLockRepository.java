package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.time.Duration;

/**
 * Region+stage distributed lock so that, across horizontally-scaled instances, at most one instance
 * runs a given region's PROCESS/RELAY/REQUEUE tick at a time. The Kafka consumer (CONSUME stage)
 * needs no lock — the consumer group already partitions work across instances.
 *
 * <p>TTL = {@code stage.tickIntervalMs * region.lockTtlMultiplier}: long enough to survive a slow
 * tick, short enough for prompt failover when an instance dies.
 */
public interface DistributedLockRepository {

    /**
     * Atomically acquires an expired/free lock or refreshes this instance's own lock.
     *
     * @return {@code true} if this instance owns the lock; {@code false} if a live instance holds it
     *     and the tick must be skipped.
     */
    boolean acquireOrRefresh(Region region, ProcessType processType, InstanceId instanceId, Duration ttl);

    /** Releases every lock held by this instance — called on graceful shutdown. */
    void releaseAll(InstanceId instanceId);

    /** Releases a single region+stage lock held by this instance (no-op if not held). */
    void releaseForRegion(Region region, ProcessType processType, InstanceId instanceId);

    /** {@code true} if this instance currently holds a live (non-expired) lock for the key. */
    boolean isHeldBy(Region region, ProcessType processType, InstanceId instanceId);
}
