package com.bnpparibas.dec.bookingconfirmation.application.registry;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.service.DefaultBookingRequeueService;
import com.bnpparibas.dec.bookingconfirmation.domain.model.InstanceId;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.DistributedLockRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRequeueService;
import org.springframework.stereotype.Component;

/** Owns the per-region {@link DefaultBookingRequeueService} instances. */
@Component
public class BookingRequeueServiceRegistry extends BaseRegionServiceRegistry<BookingRequeueService> {

    private final OutboxRepository outboxRepository;
    private final DistributedLockRepository lockRepository;
    private final InstanceId instanceId;

    public BookingRequeueServiceRegistry(
            final BookingConfirmationProperties properties,
            final OutboxRepository outboxRepository,
            final DistributedLockRepository lockRepository,
            final InstanceId instanceId) {
        super(properties);
        this.outboxRepository = outboxRepository;
        this.lockRepository = lockRepository;
        this.instanceId = instanceId;
    }

    @Override
    protected BookingRequeueService buildService(final Region region, final RegionProperties regionProperties) {
        return new DefaultBookingRequeueService(
                region,
                properties().requeue().tickIntervalMs(),
                properties().requeue().maxRetries(),
                regionProperties.lockTtlMultiplier(),
                outboxRepository,
                lockRepository,
                instanceId);
    }
}
