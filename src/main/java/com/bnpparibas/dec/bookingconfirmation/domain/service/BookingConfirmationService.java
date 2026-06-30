package com.bnpparibas.dec.bookingconfirmation.domain.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.ProcessType;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;

/**
 * Contract for a region-scoped scheduled stage (PROCESS, RELAY, or REQUEUE).
 *
 * <p>The scheduler invokes {@link #tick()} on a fixed delay. Each implementation guards its own work
 * behind the administrative pause flag; safe concurrency across instances comes from the drain's
 * {@code FOR UPDATE SKIP LOCKED} row claiming, not an application-level lock.
 */
public interface BookingConfirmationService {

    Region region();

    ProcessType processType();

    void pause();

    void resume();

    boolean isPaused();

    /** Whether this tick should run at all (i.e. not administratively paused). */
    boolean shouldRunTick();

    /** One scheduled iteration: guard on pause, then do the stage's work. */
    void tick();

    /** Human-readable identifier, e.g. {@code AMER.RELAY}. */
    String processIdentifier();
}
