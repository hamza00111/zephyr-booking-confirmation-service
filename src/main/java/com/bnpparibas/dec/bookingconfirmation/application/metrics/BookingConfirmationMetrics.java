package com.bnpparibas.dec.bookingconfirmation.application.metrics;

import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.INBOX_DUPLICATE;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.INBOX_INGESTED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.INBOX_PARKED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.PARTITIONS_OWNED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.PROCESS_FAILED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.PROCESS_INVALID;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.RELAY_FAILED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.RELAY_REJECTED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.RELAY_SENT;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.REQUEUE_EXHAUSTED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.REQUEUE_PROMOTED;
import static com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetricType.TICK_ERRORS;

import com.bnpparibas.dec.bookingconfirmation.common.metric.MetricsSupport;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.function.Supplier;

/**
 * This module's tag conventions over the shared {@link MetricsSupport}: every meter is tagged
 * {@code region} (and {@code stage} where relevant), with identities from
 * {@link BookingConfirmationMetricType}. These are the alerting hooks for conditions that otherwise
 * surface only as log lines: parked ingestion failures, exhausted retry budgets, and ticks that
 * fail every run.
 */
public final class BookingConfirmationMetrics {

    private static final String REGION_TAG = "region";
    private static final String STAGE_TAG = "stage";

    private final MetricsSupport support;

    public BookingConfirmationMetrics(final MeterRegistry registry) {
        this.support = new MetricsSupport(registry);
    }

    /** In-memory registry for tests and contexts without a backing registry. */
    public static BookingConfirmationMetrics noop() {
        return new BookingConfirmationMetrics(new SimpleMeterRegistry());
    }

    public void inboxIngested(final Region region) {
        count(INBOX_INGESTED, region, 1);
    }

    public void inboxDuplicate(final Region region) {
        count(INBOX_DUPLICATE, region, 1);
    }

    /** A record the consumer error handler parked as INGEST_FAILURE — page-worthy if sustained. */
    public void inboxParked(final Region region) {
        count(INBOX_PARKED, region, 1);
    }

    public void processInvalid(final Region region, final int count) {
        count(PROCESS_INVALID, region, count);
    }

    public void processFailed(final Region region, final int count) {
        count(PROCESS_FAILED, region, count);
    }

    public void relaySent(final Region region, final int count) {
        count(RELAY_SENT, region, count);
    }

    public void relayFailed(final Region region, final int count) {
        count(RELAY_FAILED, region, count);
    }

    public void relayRejected(final Region region, final int count) {
        count(RELAY_REJECTED, region, count);
    }

    public void requeuePromoted(final Region region, final String stage, final int count) {
        countWithStage(REQUEUE_PROMOTED, region, stage, count);
    }

    /** Rows that ran out of retry budget (RETRY_EXHAUSTED / INVALID) — the primary alerting hook. */
    public void requeueExhausted(final Region region, final String stage, final int count) {
        countWithStage(REQUEUE_EXHAUSTED, region, stage, count);
    }

    /** A scheduled tick threw — persistent increments mean a stage is failing every run. */
    public void tickError(final Region region, final String stage) {
        countWithStage(TICK_ERRORS, region, stage, 1);
    }

    /**
     * How many partitions of the region this instance currently owns. Zero while failure rows
     * accumulate is the stuck-pipeline signature: every drain (including requeue promotion) is
     * partition-scoped, so an instance kicked from the consumer group silently skips all of them.
     */
    public void ownedPartitionsGauge(final Region region, final Supplier<Number> size) {
        support.gauge(PARTITIONS_OWNED, size, Tag.of(REGION_TAG, region.name()));
    }

    private void count(final BookingConfirmationMetricType metric, final Region region, final int amount) {
        if (amount > 0) {
            support.counter(metric).tag(REGION_TAG, region.name()).register().increment(amount);
        }
    }

    private void countWithStage(
            final BookingConfirmationMetricType metric, final Region region, final String stage, final int amount) {
        if (amount > 0) {
            support.counter(metric)
                    .tag(REGION_TAG, region.name())
                    .tag(STAGE_TAG, stage)
                    .register()
                    .increment(amount);
        }
    }
}
