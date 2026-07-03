package com.bnpparibas.dec.bookingconfirmation.application.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RequeueProperties;
import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.application.partition.OwnedPartitions;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.InboxRepository;
import com.bnpparibas.dec.bookingconfirmation.domain.repository.OutboxRepository;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BookingRequeueServiceRegistryTest {

    @Test
    void buildsRequeueServiceForEachActiveRegion() {
        var properties = new BookingConfirmationProperties(
                Map.of(Region.AMER, new RegionProperties(true, "internal.amer", "published.amer")),
                null,
                null,
                null,
                null,
                new RequeueProperties(60000, 5, 3),
                null);
        var registry = new BookingRequeueServiceRegistry(
                properties,
                mock(OutboxRepository.class),
                mock(InboxRepository.class),
                new OwnedPartitions(),
                BookingConfirmationMetrics.noop());

        registry.initialize();

        var service = registry.service(Region.AMER);
        assertThat(service).isNotNull();
        assertThat(service.region()).isEqualTo(Region.AMER);
    }
}
