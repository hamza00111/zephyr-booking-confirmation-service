package com.bnpparibas.dec.bookingconfirmation.application.registry;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.service.DefaultBookingRequeueService;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRequeueService;
import org.springframework.stereotype.Component;

/** Owns the per-region {@link DefaultBookingRequeueService} instances. */
@Component
public class BookingRequeueServiceRegistry extends BaseRegionServiceRegistry<BookingRequeueService> {

    private final OutboxRepository outboxRepository;

    public BookingRequeueServiceRegistry(
            final BookingConfirmationProperties properties,
            final OutboxRepository outboxRepository) {
        super(properties);
        this.outboxRepository = outboxRepository;
    }

    @Override
    protected BookingRequeueService buildService(final Region region, final RegionProperties regionProperties) {
        return new DefaultBookingRequeueService(
                region,
                properties().requeue().maxRetries(),
                outboxRepository);
    }
}
