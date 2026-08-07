package com.bnpparibas.dec.bookingconfirmation.infrastructure.config;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import com.bnpparibas.dec.bookingconfirmation.common.infrastructure.metric.MetricsSupport;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Shared singletons: a {@link TransactionTemplate} for the stages and the metric layer. */
@Configuration
public class ApplicationBeansConfig {

    @Bean
    public TransactionTemplate transactionTemplate(final PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    /**
     * Same declaration {@code BookingConfirmationCommonConfig} carries in the common module
     * (conditional, so whichever is registered first wins). Once this service depends on the common
     * module, delete this bean and the local {@code common.infrastructure.metric} package copy —
     * the common module's takes over with no other change.
     */
    @Bean
    @ConditionalOnMissingBean
    public MetricsSupport metricsSupport(final MeterRegistry meterRegistry) {
        return new MetricsSupport(meterRegistry);
    }

    @Bean
    public BookingConfirmationMetrics bookingConfirmationMetrics(final MetricsSupport metricsSupport) {
        return new BookingConfirmationMetrics(metricsSupport);
    }
}
