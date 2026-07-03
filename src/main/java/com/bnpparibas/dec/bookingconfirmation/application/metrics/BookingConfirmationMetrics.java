package com.bnpparibas.dec.bookingconfirmation.application.metrics;

import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/**
 * Central counter names for the pipeline, tagged {@code region} (and {@code stage} where relevant).
 * These are the alerting hooks for conditions that otherwise surface only as log lines: parked
 * ingestion failures, exhausted retry budgets, and ticks that fail every run.
 */
public final class BookingConfirmationMetrics {

    private final MeterRegistry registry;

    public BookingConfirmationMetrics(final MeterRegistry registry) {
        this.registry = registry;
    }

    /** In-memory registry for tests and contexts without a backing registry. */
    public static BookingConfirmationMetrics noop() {
        return new BookingConfirmationMetrics(new SimpleMeterRegistry());
    }

    public void inboxIngested(final Region region) {
        count("bc.inbox.ingested", region, 1);
    }

    public void inboxDuplicate(final Region region) {
        count("bc.inbox.duplicate", region, 1);
    }

    /** A record the consumer error handler parked as INGEST_FAILURE — page-worthy if sustained. */
    public void inboxParked(final Region region) {
        count("bc.inbox.parked", region, 1);
    }

    public void processInvalid(final Region region, final int count) {
        count("bc.process.invalid", region, count);
    }

    public void processFailed(final Region region, final int count) {
        count("bc.process.failed", region, count);
    }

    public void relaySent(final Region region, final int count) {
        count("bc.relay.sent", region, count);
    }

    public void relayFailed(final Region region, final int count) {
        count("bc.relay.failed", region, count);
    }

    public void relayRejected(final Region region, final int count) {
        count("bc.relay.rejected", region, count);
    }

    public void requeuePromoted(final Region region, final String stage, final int count) {
        countWithStage("bc.requeue.promoted", region, stage, count);
    }

    /** Rows that ran out of retry budget (RETRY_EXHAUSTED / INVALID) — the primary alerting hook. */
    public void requeueExhausted(final Region region, final String stage, final int count) {
        countWithStage("bc.requeue.exhausted", region, stage, count);
    }

    /** A scheduled tick threw — persistent increments mean a stage is failing every run. */
    public void tickError(final Region region, final String stage) {
        countWithStage("bc.tick.errors", region, stage, 1);
    }

    private void count(final String name, final Region region, final int amount) {
        if (amount > 0) {
            registry.counter(name, "region", region.name()).increment(amount);
        }
    }

    private void countWithStage(final String name, final Region region, final String stage, final int amount) {
        if (amount > 0) {
            registry.counter(name, "region", region.name(), "stage", stage).increment(amount);
        }
    }
}
