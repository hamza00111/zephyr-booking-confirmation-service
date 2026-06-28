package com.bnpparibas.dec.bookingconfirmation.domain.repository;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InboxStatus;
import com.bnpparibas.dec.bookingconfirmation.domain.model.OutboxStatus;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;

/**
 * Read-only counts that back the no-data-loss observability gauges (parked/stuck buckets and the age
 * of the oldest not-yet-delivered row). Used by the metrics binder, not the processing pipeline.
 */
public interface PipelineMetricsRepository {

    long inboxCount(Region region, InboxStatus status);

    long outboxCount(Region region, OutboxStatus status);

    /** Age in seconds of the oldest outbox row not yet delivered ({@code NEW}/{@code SEND_FAILURE}); 0 if none. */
    long oldestUnsentOutboxAgeSeconds(Region region);
}
