package com.bnpparibas.dec.bookingconfirmation.infrastructure.kafka.consumer;

import com.bnpparibas.dec.bookingconfirmation.application.config.BookingConfirmationProperties;
import com.bnpparibas.dec.bookingconfirmation.domain.model.Region;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Maps inbound {@code internal.<region>} topics to their {@link Region}, and exposes the active
 * internal topic list consumed by the {@code @KafkaListener} (via SpEL {@code #{@topicRegionResolver.internalTopics()}}).
 */
@Component
public class TopicRegionResolver {

    private final Map<String, Region> topicToRegion;
    private final List<String> internalTopics;

    public TopicRegionResolver(final BookingConfirmationProperties properties) {
        final Map<String, Region> mapping = new HashMap<>();
        final List<String> topics = new ArrayList<>();
        properties.regions().forEach((region, regionProps) -> {
            if (regionProps.active()) {
                mapping.put(regionProps.internalTopic(), region);
                topics.add(regionProps.internalTopic());
            }
        });
        this.topicToRegion = Map.copyOf(mapping);
        this.internalTopics = List.copyOf(topics);
    }

    public Region regionFor(final String topic) {
        final Region region = topicToRegion.get(topic);
        if (region == null) {
            throw new IllegalStateException("No active region mapped for internal topic=" + topic);
        }
        return region;
    }

    /** Active internal topics — referenced from the listener's {@code topics} SpEL expression. */
    public List<String> internalTopics() {
        return internalTopics;
    }
}
