package com.bnpparibas.dec.bookingconfirmation.infrastructure.config;

import com.bnpparibas.dec.bookingconfirmation.application.metrics.BookingConfirmationMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Shared singletons: a {@link TransactionTemplate} for the stages. */
@Configuration
public class ApplicationBeansConfig {

    @Bean
    public TransactionTemplate transactionTemplate(final PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }

    @Bean
    public BookingConfirmationMetrics bookingConfirmationMetrics(final MeterRegistry meterRegistry) {
        return new BookingConfirmationMetrics(meterRegistry);
    }
}
