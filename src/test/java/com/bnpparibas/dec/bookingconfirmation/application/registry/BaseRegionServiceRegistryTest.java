package com.bnpparibas.dec.bookingconfirmation.application.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import com.bnpparibas.dec.bookingconfirmation.domain.service.BookingConfirmationService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BaseRegionServiceRegistryTest {

    @Test
    void initialize_shouldBuildOneServicePerActiveRegion() {
        var properties = properties(Map.of(
                Region.AMER, region(true),
                Region.APAC, region(false),
                Region.EMEA, region(true)));
        var built = new ArrayList<Region>();
        var registry = new BaseRegionServiceRegistry<BookingConfirmationService>(properties) {
            @Override
            protected BookingConfirmationService buildService(Region region, RegionProperties regionProperties) {
                built.add(region);
                return mock(BookingConfirmationService.class);
            }
        };

        registry.initialize();

        assertThat(built).containsExactlyInAnyOrder(Region.AMER, Region.EMEA);
        assertThat(registry.services()).hasSize(2);
        assertThat(registry.service(Region.AMER)).isNotNull();
        assertThat(registry.service(Region.APAC)).isNull();
    }

    private static RegionProperties region(boolean active) {
        return new RegionProperties(active, 3, "internal", "published");
    }

    private static BookingConfirmationProperties properties(Map<Region, RegionProperties> regions) {
        return new BookingConfirmationProperties(regions, null, null, null, null, null, null);
    }
}
