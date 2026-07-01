package com.bnpparibas.dec.bookingconfirmation.application.registry;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.application.service.DefaultBookingRelayService;
import com.bnpparibas.dec.bookingconfirmation.domain.event.DomainEventPublisher;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingRelayService;
import com.bnpparibas.dec.bookingconfirmation.infrastructure.resilience.BookingConfirmationCircuitBreakerRegistry;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Owns the per-region {@link DefaultBookingRelayService} instances. Registers the region's relay
 * circuit breaker as each service is built, so breakers exist before any relay tick runs.
 */
@Component
public class BookingRelayServiceRegistry extends BaseRegionServiceRegistry<BookingRelayService> {

    private final OutboxRepository outboxRepository;
    private final DomainEventPublisher publisher;
    private final BookingConfirmationCircuitBreakerRegistry breakerRegistry;
    private final TransactionTemplate transactionTemplate;
    private final OwnedPartitions ownedPartitions;

    public BookingRelayServiceRegistry(
            final BookingConfirmationProperties properties,
            final OutboxRepository outboxRepository,
            final DomainEventPublisher publisher,
            final BookingConfirmationCircuitBreakerRegistry breakerRegistry,
            final TransactionTemplate transactionTemplate,
            final OwnedPartitions ownedPartitions) {
        super(properties);
        this.outboxRepository = outboxRepository;
        this.publisher = publisher;
        this.breakerRegistry = breakerRegistry;
        this.transactionTemplate = transactionTemplate;
        this.ownedPartitions = ownedPartitions;
    }

    @Override
    protected BookingRelayService buildService(final Region region, final RegionProperties regionProperties) {
        breakerRegistry.register(region);
        return new DefaultBookingRelayService(
                region,
                ownedPartitions,
                properties().relay().batchSize(),
                outboxRepository,
                publisher,
                transactionTemplate);
    }
}
