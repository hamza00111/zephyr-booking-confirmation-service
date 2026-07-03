package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties.RegionProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TopicRegionResolverTest {

    @Test
    void internalTopics_shouldReturnOnlyActiveRegionTopics() {
        var resolver = new TopicRegionResolver(properties(Map.of(
                Region.AMER, region(true, "internal.amer"),
                Region.APAC, region(false, "internal.apac"),
                Region.EMEA, region(true, "internal.emea"))));

        assertThat(resolver.internalTopics()).containsExactlyInAnyOrder("internal.amer", "internal.emea");
    }

    @Test
    void regionFor_shouldMapTopicToRegion_whenActive() {
        var resolver = new TopicRegionResolver(properties(Map.of(Region.AMER, region(true, "internal.amer"))));

        assertThat(resolver.regionFor("internal.amer")).isEqualTo(Region.AMER);
    }

    @Test
    void regionFor_shouldThrow_whenTopicNotMapped() {
        var resolver = new TopicRegionResolver(properties(Map.of(Region.AMER, region(true, "internal.amer"))));

        assertThatThrownBy(() -> resolver.regionFor("internal.apac"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("internal.apac");
    }

    private static RegionProperties region(boolean active, String internalTopic) {
        return new RegionProperties(active, internalTopic, "published");
    }

    private static BookingConfirmationProperties properties(Map<Region, RegionProperties> regions) {
        return new BookingConfirmationProperties(regions, null, null, null, null, null, null);
    }
}
