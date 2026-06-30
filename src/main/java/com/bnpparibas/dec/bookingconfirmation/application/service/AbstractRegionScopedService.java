package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingConfirmationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common tick behaviour for the three scheduled, region-scoped stages.
 *
 * <p>{@link #tick()} is final: it delegates to {@link #doTick()} with exceptions logged and
 * contained so the scheduler keeps running for every other region/stage.
 *
 * <p>Concurrency across horizontally-scaled instances needs no application-level lock: each stage's
 * drain claims rows with {@code FOR UPDATE SKIP LOCKED}, so concurrent ticks on different JVMs take
 * disjoint rows and never double-process a row.
 */
public abstract class AbstractRegionScopedService implements BookingConfirmationService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Region region;

    protected AbstractRegionScopedService(final Region region) {
        this.region = region;
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
        try {
            doTick();
        } catch (final RuntimeException exception) {
            log.error("[{}] Tick failed", processIdentifier(), exception);
        }
    }

    /** The stage-specific work. */
    protected abstract void doTick();
}
