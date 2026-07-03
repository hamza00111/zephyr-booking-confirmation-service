package com.bnpparibas.dec.bookingconfirmation.application.registry;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.application.service.DefaultBookingRequeueService;
import com.bnpparibas.dec.bookingconfirmation.application.service.RegionScope;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRequeueService;
import org.springframework.stereotype.Component;

/** Owns the per-region {@link DefaultBookingRequeueService} instances. */
@Component
public class BookingRequeueServiceRegistry extends BaseRegionServiceRegistry<BookingRequeueService> {

    private final OutboxRepository outboxRepository;
    private final InboxRepository inboxRepository;
    private final OwnedPartitions ownedPartitions;
    private final BookingConfirmationMetrics metrics;

    public BookingRequeueServiceRegistry(
            final BookingConfirmationProperties properties,
            final OutboxRepository outboxRepository,
            final InboxRepository inboxRepository,
            final OwnedPartitions ownedPartitions,
            final BookingConfirmationMetrics metrics) {
        super(properties);
        this.outboxRepository = outboxRepository;
        this.inboxRepository = inboxRepository;
        this.ownedPartitions = ownedPartitions;
        this.metrics = metrics;
    }

    @Override
    protected BookingRequeueService buildService(final Region region, final RegionProperties regionProperties) {
        return new DefaultBookingRequeueService(
                new RegionScope(region, ownedPartitions, metrics),
                properties().requeue().maxRetries(),
                properties().requeue().processMaxRetries(),
                outboxRepository,
                inboxRepository);
    }
}
