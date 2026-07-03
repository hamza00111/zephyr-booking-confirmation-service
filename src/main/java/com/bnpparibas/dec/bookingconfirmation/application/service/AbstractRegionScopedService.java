package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingConfirmationService;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common tick behaviour for the three scheduled, region-scoped stages.
 *
 * <p>{@link #tick()} is final: it skips when this instance owns no partitions of the region, and
 * otherwise delegates to {@link #doTick(Set)} with exceptions logged and contained so the scheduler
 * keeps running for every other region/stage.
 *
 * <p>Concurrency across horizontally-scaled instances needs no application-level lock: each stage
 * drains only the partitions this instance owns (ADR 0001), so a trade's events are processed and
 * published by a single instance in order, and {@code FOR UPDATE SKIP LOCKED} fences the brief
 * rebalance overlap.
 */
public abstract class AbstractRegionScopedService implements BookingConfirmationService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Region region;
    private final OwnedPartitions ownedPartitions;
    protected final BookingConfirmationMetrics metrics;

    protected AbstractRegionScopedService(
            final Region region, final OwnedPartitions ownedPartitions, final BookingConfirmationMetrics metrics) {
        this.region = region;
        this.ownedPartitions = ownedPartitions;
        this.metrics = metrics;
    }

    @Override
    public Region region() {
        return region;
    }

    @Override
    public String processIdentifier() {
        return processType().keyFor(region);
    }

    @Override
    public final void tick() {
        final Set<Integer> owned = ownedPartitions.forRegion(region);
        if (owned.isEmpty()) {
            // Nothing to drain — but if failures are piling up while this line repeats (or the
            // bc.partitions.owned gauge sits at 0), the consumer has lost its partitions and rows
            // are stuck until it rejoins the group.
            log.debug("[{}] Owns no partitions — skipping tick", processIdentifier());
            return;
        }
        try {
            doTick(owned);
        } catch (final RuntimeException exception) {
            metrics.tickError(region, processType().name());
            log.error("[{}] Tick failed", processIdentifier(), exception);
        }
    }

    /** The stage-specific work, restricted to the partitions this instance owns for the region. */
    protected abstract void doTick(Set<Integer> ownedPartitions);
}
