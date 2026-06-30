package com.bnpparibas.dec.bookingconfirmation.application.registry;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.service.DefaultBookingProcessService;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeEnricher;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeFilter;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingProcessService;
import com.bnpparibas.dec.bookingconfirmation.domain.service.TradeEventAggregator;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/** Owns the per-region {@link DefaultBookingProcessService} instances. */
@Component
public class BookingProcessServiceRegistry extends BaseRegionServiceRegistry<BookingProcessService> {

    private final InboxRepository inboxRepository;
    private final OutboxRepository outboxRepository;
    private final TradeEventCodec tradeEventCodec;
    // Pure, stateless domain logic — instantiated here rather than Spring-managed.
    private final TradeEventAggregator tradeEventAggregator = new TradeEventAggregator();
    private final TradeEnricher tradeEnricher;
    private final TradeFilter tradeFilter;
    private final TransactionTemplate transactionTemplate;

    public BookingProcessServiceRegistry(
            final BookingConfirmationProperties properties,
            final InboxRepository inboxRepository,
            final OutboxRepository outboxRepository,
            final TradeEventCodec tradeEventCodec,
            final TradeEnricher tradeEnricher,
            final TradeFilter tradeFilter,
            final TransactionTemplate transactionTemplate) {
        super(properties);
        this.inboxRepository = inboxRepository;
        this.outboxRepository = outboxRepository;
        this.tradeEventCodec = tradeEventCodec;
        this.tradeEnricher = tradeEnricher;
        this.tradeFilter = tradeFilter;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    protected BookingProcessService buildService(final Region region, final RegionProperties regionProperties) {
        return new DefaultBookingProcessService(
                region,
                properties().process().batchSize(),
                regionProperties.publishedTopic(),
                inboxRepository,
                outboxRepository,
                tradeEventCodec,
                tradeEventAggregator,
                tradeEnricher,
                tradeFilter,
                transactionTemplate);
    }
}
