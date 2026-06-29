package com.bnpparibas.dec.bookingconfirmation.infrastructure.config.serialization;

import static tools.jackson.core.StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION;
import static tools.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.bnpparibas.dec.zephyr.domain.trade.events.TradeEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import tools.jackson.databind.json.JsonMapper;

/**
 * Jackson 3 ({@code tools.jackson}) configuration. The {@code @Primary JsonMapper} carries the
 * polymorphic mix-in for the annotation-free {@link TradeEvent} domain model, so the PROCESS stage
 * can deserialize an inbound trade event into its concrete subtype for filter/business decisions.
 *
 * <p>Mirrors the publisher service's {@code SerializationConfig}. {@code FAIL_ON_UNKNOWN_PROPERTIES}
 * is disabled so a forward-compatible producer (extra fields) does not break binding.
 */
@Configuration
public class SerializationConfig {

    @Bean
    @Primary
    public JsonMapper jsonMapper() {
        return JsonMapper.builder()
                .findAndAddModules()
                .enable(INCLUDE_SOURCE_IN_LOCATION)
                .disable(FAIL_ON_UNKNOWN_PROPERTIES)
                .addMixIn(TradeEvent.class, TradeEventsMixin.class)
                .build();
    }
}
