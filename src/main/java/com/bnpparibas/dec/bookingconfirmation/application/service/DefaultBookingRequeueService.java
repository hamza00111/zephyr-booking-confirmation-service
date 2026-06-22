package com.bnpparibas.dec.bookingconfirmation.application.service;

import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRequeueService;

/**
 * REQUEUE stage: promotes outbox {@code SEND_FAILURE} rows back to {@code NEW} while their retry
 * count is below the budget, otherwise marks them {@code RETRY_EXHAUSTED}. Runs on a slower tick than
 * the relay.
 */
public class DefaultBookingRequeueService extends AbstractRegionScopedService implements BookingRequeueService {

    private final int maxRetries;
    private final OutboxRepository outboxRepository;

    public DefaultBookingRequeueService(
            final Region region,
            final long tickIntervalMs,
            final int maxRetries,
            final int lockTtlMultiplier,
            final OutboxRepository outboxRepository,
            final DistributedLockRepository lockRepository,
            final InstanceId instanceId) {
        super(region, tickIntervalMs, lockTtlMultiplier, lockRepository, instanceId);
        this.maxRetries = maxRetries;
        this.outboxRepository = outboxRepository;
    }

    @Override
    protected void doTick() {
        final int promoted = outboxRepository.requeueFailed(region(), maxRetries);
        if (promoted > 0) {
            log.info("[{}] Requeued {} failed outbox row(s)", processIdentifier(), promoted);
        }
    }
}
