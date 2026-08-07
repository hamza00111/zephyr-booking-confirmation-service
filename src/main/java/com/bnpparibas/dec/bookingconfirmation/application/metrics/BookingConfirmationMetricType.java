package com.bnpparibas.dec.bookingconfirmation.application.metrics;

import com.bnpparibas.dec.bookingconfirmation.common.infrastructure.metric.MetricDefinition;

/**
 * This module's metric catalogue (the processor-side counterpart of the publisher's
 * {@code MetricType}): every meter this service emits, with its Micrometer key and description.
 * The shared {@code MetricsSupport} machinery consumes these through {@link MetricDefinition}.
 */
public enum BookingConfirmationMetricType implements MetricDefinition {
    INBOX_INGESTED("bc.inbox.ingested", "Messages ingested into the inbox"),
    INBOX_DUPLICATE("bc.inbox.duplicate", "Messages skipped as duplicates by the idempotency key"),
    INBOX_PARKED("bc.inbox.parked", "Records parked as INGEST_FAILURE by the consumer error handler"),
    PROCESS_INVALID("bc.process.invalid", "Inbox rows marked INVALID (no readable event type)"),
    PROCESS_FAILED("bc.process.failed", "Inbox rows marked PROCESS_FAILURE (transform failed)"),
    RELAY_SENT("bc.relay.sent", "Outbox events published and acknowledged by the broker"),
    RELAY_FAILED("bc.relay.failed", "Outbox events whose send failed or went unconfirmed"),
    RELAY_REJECTED("bc.relay.rejected", "Outbox sends rejected by the open relay circuit breaker"),
    REQUEUE_PROMOTED("bc.requeue.promoted", "Failed rows promoted back to NEW within the retry budget"),
    REQUEUE_EXHAUSTED("bc.requeue.exhausted", "Rows parked terminally after exhausting the retry budget"),
    TICK_ERRORS("bc.tick.errors", "Scheduled stage ticks that threw"),
    PARTITIONS_OWNED("bc.partitions.owned", "Partitions of the region's internal topic owned by this instance");

    private final String key;
    private final String description;

    BookingConfirmationMetricType(final String key, final String description) {
        this.key = key;
        this.description = description;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
