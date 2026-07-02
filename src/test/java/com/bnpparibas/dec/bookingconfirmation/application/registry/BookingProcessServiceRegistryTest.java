package com.bnpparibas.dec.bookingconfirmation.application.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.ProcessProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeEnricher;
import com.bnpparibas.dec.bookingconfirmation.application.transform.TradeFilter;
import com.bnpparibas.dec.bookingconfirmation.domain.event.TradeEventCodec;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionTemplate;

class BookingProcessServiceRegistryTest {

    @Test
    void buildsProcessServiceForEachActiveRegion() {
        var properties = new BookingConfirmationProperties(
                Map.of(Region.AMER, new RegionProperties(true, "internal.amer", "published.amer")),
                null,
                null,
                new ProcessProperties(3000, 200),
                null,
                null,
                null);
        var registry = new BookingProcessServiceRegistry(
                properties,
                mock(InboxRepository.class),
                mock(OutboxRepository.class),
                mock(TradeEventCodec.class),
                mock(TradeEnricher.class),
                mock(TradeFilter.class),
                mock(TransactionTemplate.class),
                new OwnedPartitions());

        registry.initialize();

        var service = registry.service(Region.AMER);
        assertThat(service).isNotNull();
        assertThat(service.region()).isEqualTo(Region.AMER);
    }
}
