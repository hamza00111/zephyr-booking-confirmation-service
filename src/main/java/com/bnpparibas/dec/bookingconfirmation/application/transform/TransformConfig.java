package com.bnpparibas.dec.bookingconfirmation.application.transform;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * No-op transform seam for the consume/produce scaffolding.
 *
 * <p>Both beans are {@code @ConditionalOnMissingBean}, so next week's real IRIS_TRADE_ENRICH /
 * IRIS_TRADE_FILTER implementations (registered as {@code @Component}s) transparently replace them.
 */
@Configuration
public class TransformConfig {

    @Bean
    @ConditionalOnMissingBean
    public TradeEnricher noOpTradeEnricher() {
        return (region, pivotPayload) -> pivotPayload;
    }

    @Bean
    @ConditionalOnMissingBean
    public TradeFilter allowAllTradeFilter() {
        return (region, pivotPayload) -> true;
    }
}
