package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRequeueService;

/**
 * REQUEUE stage (runs on a slower tick than the relay). Drives the no-data-loss recovery for both
 * halves of the pipeline:
 *
 * <ul>
 *   <li><b>Outbox:</b> promotes {@code SEND_FAILURE} rows whose backoff has elapsed back to
 *       {@code NEW}. Infrastructure failures retry indefinitely — never abandoned.
 *   <li><b>Inbox:</b> promotes {@code PROCESS_FAILURE} rows back to {@code NEW} within the retry
 *       budget; rows past the budget are {@code PARKED} (retained, alerted, replayable — never dropped).
 * </ul>
 */
public class DefaultBookingRequeueService extends AbstractRegionScopedService implements BookingRequeueService {

    private final int maxRetries;
    private final int retentionDays;
    private final InboxRepository inboxRepository;
    private final OutboxRepository outboxRepository;

    public DefaultBookingRequeueService(
            final Region region,
            final long tickIntervalMs,
            final int maxRetries,
            final int retentionDays,
            final int lockTtlMultiplier,
            final InboxRepository inboxRepository,
            final OutboxRepository outboxRepository,
            final DistributedLockRepository lockRepository,
            final InstanceId instanceId) {
        super(region, tickIntervalMs, lockTtlMultiplier, lockRepository, instanceId);
        this.maxRetries = maxRetries;
        this.retentionDays = retentionDays;
        this.inboxRepository = inboxRepository;
        this.outboxRepository = outboxRepository;
    }

    @Override
    protected void doTick() {
        final int outboxPromoted = outboxRepository.requeueReady(region());
        final int inboxPromoted = inboxRepository.requeueFailed(region(), maxRetries);
        if (outboxPromoted > 0 || inboxPromoted > 0) {
            log.info("[{}] Requeued {} outbox + {} inbox row(s)",
                    processIdentifier(), outboxPromoted, inboxPromoted);
        }

        // Retention: purge only delivered/processed rows (never PARKED/INVALID/in-flight — no data loss).
        final int purged = outboxRepository.purgeSent(region(), retentionDays)
                + inboxRepository.purgeProcessed(region(), retentionDays);
        if (purged > 0) {
            log.debug("[{}] Purged {} delivered/processed row(s) older than {}d",
                    processIdentifier(), purged, retentionDays);
        }
    }
}
