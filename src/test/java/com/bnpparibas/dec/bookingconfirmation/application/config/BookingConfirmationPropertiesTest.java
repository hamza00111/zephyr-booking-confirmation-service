package com.bnpparibas.dec.bookingconfirmation.application.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.Map;
import org.junit.jupiter.api.Test;

class BookingConfirmationPropertiesTest {

    @Test
    void activeRegions_returnsOnlyActive_andRegionLooksUpByKey() {
        var amer = new RegionProperties(true, "internal.amer", "published.amer");
        var apac = new RegionProperties(false, "internal.apac", "published.apac");
        var properties = new BookingConfirmationProperties(
                Map.of(Region.AMER, amer, Region.APAC, apac), null, null, null, null, null, null);

        assertThat(properties.activeRegions()).containsExactly(Region.AMER);
        assertThat(properties.region(Region.APAC)).isSameAs(apac);
    }
}
