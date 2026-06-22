package com.bnpparibas.dec.bookingconfirmation.domain.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.time.Duration;

/**
 * Contract for a region-scoped, lock-guarded scheduled stage (PROCESS, RELAY, or REQUEUE).
 *
 * <p>The scheduler invokes {@link #tick()} on a fixed delay. Each implementation guards its own work
 * behind the administrative pause flag and the per-region distributed lock.
 */
public interface BookingConfirmationService {

    Region region();

    ProcessType processType();

    void pause();

    void resume();

    boolean isPaused();

    /** Whether this tick should run at all (i.e. not administratively paused). */
    boolean shouldRunTick();

    /** Acquires or refreshes this stage's per-region distributed lock for this instance. */
    boolean refreshLock();

    /** One scheduled iteration: guard on pause + lock, then do the stage's work. */
    void tick();

    /** Lock TTL = this stage's tick interval * the region's lock-ttl multiplier. */
    Duration lockTtl();

    /** Human-readable identifier, e.g. {@code AMER.RELAY}. */
    String processIdentifier();
}
