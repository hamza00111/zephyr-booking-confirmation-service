package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingConfirmationService;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common lock + pause + tick-guard behaviour for the three scheduled, region-scoped stages.
 *
 * <p>{@link #tick()} is final: it skips when paused, skips when the per-region distributed lock is
 * held by a live peer, and otherwise delegates to {@link #doTick()} with exceptions logged and
 * contained so the scheduler keeps running for every other region/stage.
 */
public abstract class AbstractRegionScopedService implements BookingConfirmationService {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final Region region;
    private final long tickIntervalMs;
    private final int lockTtlMultiplier;
    private final DistributedLockRepository lockRepository;
    private final InstanceId instanceId;
    private final AtomicBoolean paused = new AtomicBoolean(false);

    protected AbstractRegionScopedService(
            final Region region,
            final long tickIntervalMs,
            final int lockTtlMultiplier,
            final DistributedLockRepository lockRepository,
            final InstanceId instanceId) {
        this.region = region;
        this.tickIntervalMs = tickIntervalMs;
        this.lockTtlMultiplier = lockTtlMultiplier;
        this.lockRepository = lockRepository;
        this.instanceId = instanceId;
    }

    @Override
    public Region region() {
        return region;
    }

    @Override
    public void pause() {
        paused.set(true);
    }

    @Override
    public void resume() {
        paused.set(false);
    }

    @Override
    public boolean isPaused() {
        return paused.get();
    }

    @Override
    public boolean shouldRunTick() {
        return !paused.get();
    }

    @Override
    public Duration lockTtl() {
        return Duration.ofMillis(tickIntervalMs * lockTtlMultiplier);
    }

    @Override
    public boolean refreshLock() {
        return lockRepository.acquireOrRefresh(region, processType(), instanceId, lockTtl());
    }

    @Override
    public String processIdentifier() {
        return processType().keyFor(region);
    }

    @Override
    public final void tick() {
        if (!shouldRunTick()) {
            return;
        }
        if (!refreshLock()) {
            return;
        }
        try {
            doTick();
        } catch (final RuntimeException exception) {
            log.error("[{}] Tick failed", processIdentifier(), exception);
        }
    }

    /** The stage-specific work, run only when this instance owns the region lock. */
    protected abstract void doTick();
}
